import unittest
import os
from pymonetdb import Connection

MAPIPORT = int(os.environ.get('MAPIPORT', 50000))
TSTDB = os.environ.get('TSTDB', 'demo')
TSTHOSTNAME = os.environ.get('TSTHOSTNAME', 'localhost')
TSTUSERNAME = os.environ.get('TSTUSERNAME', 'monetdb')
TSTPASSWORD = os.environ.get('TSTPASSWORD', 'monetdb')


def test_namedtuple():
    con = Connection(database=TSTDB, port=MAPIPORT, hostname=TSTHOSTNAME, username=TSTUSERNAME, password=TSTPASSWORD,
                     autocommit=False)

    cur = con.cursor()
    cur.execute('select * from tables')
    result = cur.fetchone()
    cur.description[0].name
    cur.description[0].type_code
    cur.description[0].display_size
    cur.description[0].internal_size
    cur.description[0].precision
    cur.description[0].scale
    cur.description[0].null_ok
