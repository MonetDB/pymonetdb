# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

from datetime import datetime, timedelta, timezone
import logging
import platform

from pymonetdb.sql import cursors
from pymonetdb import exceptions
from pymonetdb import mapi

logger = logging.getLogger("pymonetdb")


class Connection:
    """A MonetDB SQL database connection"""
    default_cursor = cursors.Cursor

    def __init__(self, database, hostname=None, port=50000, username="monetdb",
                 password="monetdb", unix_socket=None, autocommit=False,
                 host=None, user=None, connect_timeout=-1):
        """ Set up a connection to a MonetDB SQL database.

        args:
            database (str): name of the database, or MAPI URI
            hostname (str): Hostname where monetDB is running
            port (int): port to connect to (default: 50000)
            username (str): username for connection (default: "monetdb")
            password (str): password for connection (default: "monetdb")
            unix_socket (str): socket to connect to. used when hostname not set
                                (default: "/tmp/.s.monetdb.50000")
            autocommit (bool):  enable/disable auto commit (default: False)
            connect_timeout -- the socket timeout while connecting
                               (default: see python socket module)

        MAPI URI:
            tcp socket:         mapi:monetdb://[<username>[:<password>]@]<host>[:<port>]/<database>
            unix domain socket: mapi:monetdb:///[<username>[:<password>]@]path/to/socket?database=<database>

        returns:
            Connection object

        """

        self.autocommit = autocommit
        self.sizeheader = True
        self.replysize = 0

        # The DB API spec is not specific about this
        if host:
            hostname = host
        if user:
            username = user

        if platform.system() == "Windows" and not hostname:
            hostname = "localhost"

        # Level numbers taken from mapi.h.
        # The options start out with member .sent set to False.
        handshake_options = [
            mapi.HandshakeOption(1, "auto_commit", self.set_autocommit, autocommit),
            mapi.HandshakeOption(2, "reply_size", self.set_replysize, 100),
            mapi.HandshakeOption(3, "size_header", self.set_sizeheader, True),
            mapi.HandshakeOption(5, "time_zone", self.set_timezone, _local_timezone_offset_seconds()),
        ]

        self.mapi = mapi.Connection()
        self.mapi.connect(hostname=hostname, port=int(port), username=username,
                          password=password, database=database, language="sql",
                          unix_socket=unix_socket, connect_timeout=connect_timeout,
                          handshake_options=handshake_options)

        # self.mapi.connect() has set .sent to True for all items that
        # have already been arranged during the initial challenge/response.
        # Now take care of the rest.
        for option in handshake_options:
            if not option.sent:
                option.fallback(option.value)

    def close(self):
        """ Close the connection.

        The connection will be unusable from this
        point forward; an Error exception will be raised if any operation
        is attempted with the connection. The same applies to all cursor
        objects trying to use the connection.  Note that closing a connection
        without committing the changes first will cause an implicit rollback
        to be performed.
        """
        if self.mapi:
            if not self.autocommit:
                self.rollback()
            self.mapi.disconnect()
            self.mapi = None
        else:
            raise exceptions.Error("already closed")

    def set_autocommit(self, autocommit):
        """
        Set auto commit on or off. 'autocommit' must be a boolean
        """
        self.command("Xauto_commit %s" % int(autocommit))
        self.autocommit = autocommit

    def set_sizeheader(self, sizeheader):
        """
        Set sizeheader on or off. When enabled monetdb will return
        the size a type. 'sizeheader' must be a boolean.
        """
        self.command("Xsizeheader %s" % int(sizeheader))
        self.sizeheader = sizeheader

    def set_replysize(self, replysize):
        self.command("Xreply_size %s" % int(replysize))
        self.replysize = replysize

    def set_timezone(self, seconds_east_of_utc):
        hours = int(seconds_east_of_utc / 3600)
        remaining = seconds_east_of_utc - 3600 * hours
        minutes = int(remaining / 60)
        cmd = f"SET TIME ZONE INTERVAL '{hours:+03}:{abs(minutes):02}' HOUR TO MINUTE;"
        c = self.cursor()
        c.execute(cmd)
        c.close()

    def commit(self):
        """
        Commit any pending transaction to the database. Note that
        if the database supports an auto-commit feature, this must
        be initially off. An interface method may be provided to
        turn it back on.

        Database modules that do not support transactions should
        implement this method with void functionality.
        """
        self.__mapi_check()
        return self.cursor().execute('COMMIT')

    def rollback(self):
        """
        This method is optional since not all databases provide
        transaction support.

        In case a database does provide transactions this method
        causes the database to roll back to the start of any
        pending transaction.  Closing a connection without
        committing the changes first will cause an implicit
        rollback to be performed.
        """
        self.__mapi_check()
        return self.cursor().execute('ROLLBACK')

    def cursor(self):
        """
        Return a new Cursor Object using the connection.  If the
        database does not provide a direct cursor concept, the
        module will have to emulate cursors using other means to
        the extent needed by this specification.
        """
        return cursors.Cursor(self)

    def execute(self, query):
        """ use this for executing SQL queries """
        return self.command('s' + query + '\n;')

    def command(self, command):
        """ use this function to send low level mapi commands """
        self.__mapi_check()
        return self.mapi.cmd(command)

    def __mapi_check(self):
        """ check if there is a connection with a server """
        if not self.mapi:
            raise exceptions.Error("connection closed")
        return True

    def settimeout(self, timeout):
        """ set the amount of time before a connection times out """
        self.mapi.socket.settimeout(timeout)

    def gettimeout(self):
        """ get the amount of time before a connection times out """
        return self.mapi.socket.gettimeout()

    # these are required by the python DBAPI
    Warning = exceptions.Warning
    Error = exceptions.Error
    InterfaceError = exceptions.InterfaceError
    DatabaseError = exceptions.DatabaseError
    DataError = exceptions.DataError
    OperationalError = exceptions.OperationalError
    IntegrityError = exceptions.IntegrityError
    InternalError = exceptions.InternalError
    ProgrammingError = exceptions.ProgrammingError
    NotSupportedError = exceptions.NotSupportedError


def _local_timezone_offset_seconds():
    # local time
    our_now = datetime.now().replace(microsecond=0).astimezone()
    # same year/month/day/hour/min/etc, but marked as UTC
    utc_now = our_now.replace(tzinfo=timezone(timedelta(0)))
    # UTC reaches a given hour/min/seconds combination later than
    # the time zones east of UTC do. This means the offset is
    # positive if we are east.
    return round(utc_now.timestamp() - our_now.timestamp())
