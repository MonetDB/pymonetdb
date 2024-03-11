# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0.  If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.

from datetime import datetime, timedelta, timezone
import logging
from typing import List

from pymonetdb.exceptions import DatabaseError
from pymonetdb.sql import cursors
from pymonetdb.policy import BatchPolicy
from pymonetdb import exceptions
from pymonetdb import mapi
from pymonetdb.target import Target

logger = logging.getLogger("pymonetdb")


class Connection:
    """A MonetDB SQL database connection, use pymonetdb.connect() to create one."""
    default_cursor = cursors.Cursor

    def __init__(self, target: Target):
        """ Set up a connection to a MonetDB SQL database.

        Do not call this directly, call pymonetdb.connect() instead.
        """

        try:
            target.validate()
        except ValueError as e:
            raise DatabaseError(str(e))

        policy = BatchPolicy()
        policy.binary_level = target.connect_binary(BatchPolicy.MAX_BINARY_LEVEL)
        if target.replysize is not None:
            policy.replysize = target.replysize
        if target.maxprefetch is not None:
            policy.maxprefetch = target.maxprefetch

        self.autocommit = target.autocommit
        self.sizeheader = True
        self._policy = policy
        self._current_replysize = 100     # server default, will be updated after handshake
        self._current_timezone_seconds_east = 0   # server default, will be updated

        if target.timezone is None:
            handshake_timezone_offset = _local_timezone_offset_seconds()
        else:
            handshake_timezone_offset = 60 * target.timezone

        def handshake_options_callback(server_binexport_level: int) -> List[mapi.HandshakeOption]:
            policy.server_binexport_level = server_binexport_level
            return [
                # Level numbers taken from mapi.h.
                mapi.HandshakeOption(1, "auto_commit", self.set_autocommit, target.autocommit),
                mapi.HandshakeOption(2, "reply_size", self._change_replysize, policy.handshake_reply_size()),
                mapi.HandshakeOption(3, "size_header", self.set_sizeheader, True),
                mapi.HandshakeOption(5, "time_zone", self.set_timezone, handshake_timezone_offset),
            ]

        self.mapi = mapi.Connection()
        self.mapi.connect(target, handshake_options_callback=handshake_options_callback)

        self._current_replysize = policy.handshake_reply_size()
        self._current_timezone_seconds_east = handshake_timezone_offset

        if target.schema:
            quoted = target.schema.replace('"', '""')
            with self.cursor() as c:
                c.execute("SET SCHEMA " + quoted)

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

    def __enter__(self):
        """This method is invoked when this Connection is used in a with-statement.
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """This method is invoked when this Connection is used in a with-statement.
        """
        try:
            self.close()
        except exceptions.Error:
            pass
        # Propagate any errors
        return False

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

    def _change_replysize(self, replysize):
        self.command("Xreply_size %s" % int(replysize))
        self._current_replysize = replysize

    def set_timezone(self, seconds_east_of_utc):
        hours = int(seconds_east_of_utc / 3600)
        remaining = seconds_east_of_utc - 3600 * hours
        minutes = int(remaining / 60)
        cmd = f"SET TIME ZONE INTERVAL '{hours:+03}:{abs(minutes):02}' HOUR TO MINUTE;"
        c = self.cursor()
        c.execute(cmd)
        c.close()
        self._current_timezone_seconds_east = seconds_east_of_utc

    def set_uploader(self, uploader):
        """
        Register an Uploader object which will handle file upload requests.

        Must be an instance of class pymonetdb.Uploader or None.
        """
        self.mapi.set_uploader(uploader)

    def set_downloader(self, downloader):
        """
        Register a Downloader object which will handle file download requests.

        Must be an instance of class pymonetdb.Downloader or None
        """
        self.mapi.set_downloader(downloader)

    def get_replysize(self) -> int:
        return self._policy.replysize

    def set_replysize(self, replysize: int):
        self._policy.replysize = replysize

    replysize = property(get_replysize, set_replysize)

    def get_maxprefetch(self) -> int:
        return self._policy.maxprefetch

    def set_maxprefetch(self, maxprefetch: int):
        self._policy.maxprefetch = maxprefetch

    maxprefetch = property(get_maxprefetch, set_maxprefetch)

    def get_binary(self) -> int:
        return 1 if self._policy.binary_level else 0

    def set_binary(self, binary: int):
        self._policy.binary_level = binary > 0

    binary = property(get_binary, set_binary)

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

    def binary_command(self, command):
        """ use this function to send low level mapi commands that return raw bytes"""
        self.__mapi_check()
        return self.mapi.binary_cmd(command)

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
