import pickle
import tempfile
import re
import pdb
from past.builtins import execfile  # type: ignore
from typing import Any, TYPE_CHECKING


if TYPE_CHECKING:
    from pymonetdb.sql.cursors import Cursor


class LoopbackObject(object):
    def __init__(self, connection):
        self.__conn = connection

    def execute(self, query):
        self.__conn.execute("""
                  CREATE OR REPLACE FUNCTION export_parameters(*)
                  RETURNS TABLE(s STRING) LANGUAGE PYTHON
                  {
                      import inspect
                      import pickle
                      frame = inspect.currentframe();
                      args, _, _, values = inspect.getargvalues(frame);
                      dd = {x: values[x] for x in args};
                      del dd['_conn']
                      del dd['_columns']
                      del dd['_column_types']
                      return pickle.dumps(dd);
                  };""")
        self.__conn.execute("""
                  SELECT *
                  FROM (%s) AS xx
                  LIMIT 1""" % query)
        query_description = self.__conn.description
        self.__conn.execute("""
                  SELECT *
                  FROM export_parameters ( (%s) );""" % query)
        rows = self.__conn.fetchall()
        arguments = pickle.loads(str(rows[0][0]))
        self.__conn.execute('DROP FUNCTION export_parameters;')
        if len(arguments) != len(query_description):
            raise Exception("Incorrect number of parameters!")
        result = dict()
        for j in range(len(arguments)):
            argname = "arg%d" % (j + 1)
            result[query_description[j][0]] = arguments[argname]
        return result


def debug(cursor, query, fname, sample=-1):
    # type: (Cursor, str, str, int) -> Any
    """ Locally debug a given Python UDF function in a SQL query
        using the PDB debugger. Optionally can run on only a
        sample of the input data, for faster data export.
    """

    # first gather information about the function
    cursor.execute("""
        SELECT func, type
        FROM functions
        WHERE language>=6 AND language <= 11 AND name='%s';""" % fname)
    data = cursor.fetchall()
    if len(data) == 0:
        raise Exception("Function not found!")

    # then gather the input arguments of the function
    cursor.execute("""
        SELECT args.name, args.type
        FROM args
        INNER JOIN functions ON args.func_id=functions.id
        WHERE functions.name='%s' AND args.inout=1
        ORDER BY args.number;""" % fname)
    input_types = cursor.fetchall()

    fcode = data[0][0]
    ftype = data[0][1]

    # now obtain the input columns
    arguments = exportparameters(cursor, ftype, fname, query, len(input_types), sample)

    arglist = "_columns, _column_types, _conn"
    cleaned_arguments = dict()
    for i in range(len(input_types)):
        argname = "arg%d" % (i + 1)
        if argname not in arguments:
            raise Exception("Argument %d not found!" % (i + 1))
        input_name = str(input_types[i][0])
        cleaned_arguments[input_name] = arguments[argname]
        arglist += ", %s" % input_name
    cleaned_arguments['_columns'] = arguments['_columns']
    cleaned_arguments['_column_types'] = arguments['_column_types']

    # create a temporary file for the function execution and run it
    with tempfile.NamedTemporaryFile() as f:
        fcode = fcode.strip()
        fcode = re.sub('^{', '', fcode)
        fcode = re.sub('};$', '', fcode)
        fcode = re.sub('^\n', '', fcode)
        function_definition = "def pyfun(%s):\n %s\n" % (
            arglist, fcode.replace("\n", "\n "))
        f.write(function_definition.encode('utf-8'))
        f.flush()
        execfile(f.name, globals(), locals())

        cleaned_arguments['_conn'] = LoopbackObject(cursor)
        pdb.set_trace()
        return locals()['pyfun'](*[], **cleaned_arguments)


def exportparameters(cursor, ftype, fname, query, quantity_parameters, sample):
    # type: (Cursor, str, str, str, Any, int) -> Any
    """ Exports the input parameters of a given UDF execution
        to the Python process. Used internally for .debug() and
        .export() functions.
    """

    # create a dummy function that only exports its parameters
    # using the pickle module
    if ftype == 5:
        # table producing function
        return_type = "TABLE(s STRING)"
    else:
        return_type = "STRING"

    if sample == -1:
        export_function = """
            CREATE OR REPLACE FUNCTION export_parameters(*)
            RETURNS %s LANGUAGE PYTHON
            {
                import inspect
                import pickle
                frame = inspect.currentframe();
                args, _, _, values = inspect.getargvalues(frame);
                dd = {x: values[x] for x in args};
                del dd['_conn']
                return pickle.dumps(dd);
            };""" % return_type
    else:
        export_function = """
            CREATE OR REPLACE FUNCTION export_parameters(*)
            RETURNS %s LANGUAGE PYTHON
            {
            import inspect
            import pickle
            import numpy
            frame = inspect.currentframe();
            args, _, _, values = inspect.getargvalues(frame);
            dd = {x: values[x] for x in args};
            del dd['_conn']
            result = dict()
            argname = "arg1"
            x = numpy.arange(len(dd[argname]))
            x = numpy.random.choice(x,%s,replace=False)
            for i in range(len(dd)-2):
                argname = "arg" + str(i + 1)
                result = dd[argname]
                aux = []
                for j in range(len(x)):
                    aux.append(result[x[j]])
                dd[argname] = aux
                print(dd[argname])
            print(x)
            return pickle.dumps(dd);
            };
            """ % (return_type, str(sample))

    if fname not in query:
        raise Exception("Function %s not found in query!" % fname)

    query = query.replace(fname, 'export_parameters')
    query = query.replace(';', ' sample 1;')

    cursor.execute(export_function)
    cursor.execute(query)
    input_data = cursor.fetchall()
    cursor.execute('DROP FUNCTION export_parameters;')
    if len(input_data) <= 0:
        raise Exception("Could not load input data!")

    arguments = pickle.loads(input_data[0][0])

    if len(arguments) != quantity_parameters + 2:
        raise Exception("Incorrect amount of input arguments found!")

    return arguments


def export(cursor, query, fname, sample=-1, filespath='./'):
    """ Exports a Python UDF and its input parameters to a given
        file so it can be called locally in an IDE environment.
    """

    # first retrieve UDF information from the server
    cursor.execute("""
        SELECT func,type
        FROM functions
        WHERE language >= 6 AND language <= 11 AND name='%s';""" % fname)
    data = cursor.fetchall()
    cursor.execute("""
        SELECT args.name
        FROM args INNER JOIN functions ON args.func_id=functions.id
        WHERE functions.name='%s' AND args.inout=1
        ORDER BY args.number;""" % fname)
    input_names = cursor.fetchall()
    quantity_parameters = len(input_names)
    # fcode = data[0][0]
    ftype = data[0][1]
    parameter_list = []
    # exporting Python UDF Function
    if len(data) == 0:
        raise Exception("Function not found!")
    else:
        parameters = '('
        for x in range(0, len(input_names)):
            parameter = str(input_names[x]).split('\'')
            if x < len(input_names) - 1:
                parameter_list.append(parameter[1])
                parameters = parameters + parameter[1] + ','
            else:
                parameter_list.append(parameter[1])
                parameters = parameters + parameter[1] + '): \n'

        data = str(data[0]).replace('\\t', '\t').split('\\n')

        python_udf = 'import pickle \n \n \ndef ' + fname + parameters
        for x in range(1, len(data) - 1):
            python_udf = python_udf + '\t' + str(data[x]) + '\n'

    # exporting Columns as Binary Files
    arguments = exportparameters(cursor, ftype, fname, query, quantity_parameters, sample)
    result = dict()
    for i in range(len(arguments) - 2):
        argname = "arg%d" % (i + 1)
        result[parameter_list[i]] = arguments[argname]
    pickle.dump(result, open(filespath + 'input_data.bin', 'wb'))

    # loading Columns in Python & Call Function
    python_udf += '\n' + 'input_parameters = pickle.load(open(\''
    python_udf += filespath + 'input_data.bin\',\'rb\'))' + '\n'
    python_udf += fname + '('
    for i in range(0, quantity_parameters):
        if i < quantity_parameters - 1:
            python_udf += 'input_parameters[\''
            python_udf += parameter_list[i] + '\'],'
        else:
            python_udf += 'input_parameters[\''
            python_udf += parameter_list[i] + '\'])'

    file = open(filespath + fname + '.py', 'w')
    file.write(python_udf)
    file.close()
