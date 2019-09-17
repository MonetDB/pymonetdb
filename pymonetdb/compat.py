from six import PY3


def unicode_string(s, encoding='utf-8'):
    # type: (str, str) -> str
    """
    In case of Python3 don't do anything, in case of python2 parse as utf-8 encoded string, return unicode.
    """
    if PY3:
        return s
    else:
        return unicode(s, encoding)
