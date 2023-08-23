# -------------------------------------------------------------------------------
# Name:        Log
# Purpose:     Log the a line to a file. The file is stored in path log
#
#
# Author:      Gesuri
#
# Created:     February 27, 2018
#
# Copyright:   (c) Gesuri 2018
#
# -------------------------------------------------------------------------------

# This Python code defines a class called "Log" that is used for logging messages into a file. Here's a breakdown of
#   the code:
#   Overall, this code provides a flexible logging functionality for writing messages to a file with timestamp and
#       optional printing to the standard output.
#
#   1. The code imports various modules from the Python standard library, including `os.path`, `os`, `sys`, `time`,
#       `datetime`, and `pathlib`.
#
#   2. The code defines a constant variable `PATH_LOGS` which represents the current working directory.
#
#   3. The code defines a function `getStrTime` that returns the current time as a string in a specified format.
#       The format is set to `TIMESTAMP_FORMAT` if provided, otherwise it uses the default format `%Y%m%d_%H%M%S`.
#       The function takes two optional arguments `utc` and `dst` which are used to indicate whether the time should
#       be returned in UTC or with daylight saving time.
#
#   4. The code defines a class called `Log` which is used for logging messages. The class has the following methods:
#       - `__init__(self, name=None, path=PATH_LOGS, timestamp=True, fprint=True)`: The constructor method initializes
#           the `Log` object. It takes optional arguments `name`, `path`, `timestamp`, and `fprint`. If `name` is not
#           provided, it uses the base name of the script file (`argv[0]`) as the default name. If the name is an empty
#           string, it sets the name to `'test.log'`. The `path` argument specifies the directory where the log file
#           should be created (default is `PATH_LOGS`). The `timestamp` argument is a boolean that indicates whether to
#           add a timestamp to each log entry (default is `True`). The `fprint` argument is a boolean that indicates
#           whether to print the log entries to the standard output (default is `True`).
#       - `setName(self, name)`: Sets the name of the log file.
#       - `setPath(self, path)`: Sets the directory path for the log file.
#       - `setTimeStamp(self, timestamp)`: Sets the timestamp flag.
#       - `setFprint(self, fprint)`: Sets the flag for printing log entries to standard output.
#       - `getName(self)`: Returns the name of the log file.
#       - `getPath(self)`: Returns the directory path for the log file.
#       - `getTimeStamp(self)`: Returns the timestamp flag.
#       - `getFprint(self)`: Returns the flag for printing log entries to standard output.
#       - `getFullPath(self)`: Returns the full path of the log file.
#       - `w(self, line, ow=False)`: Writes a log entry to the file. If `ow` is `True`, it overwrites the existing file;
#           otherwise, it appends to the file. The log entry can be a string or any other object that can be converted
#           to a string. If the log file doesn't exist, it creates a new file. If there is an IO error, it tries to
#           change the ownership of the file (using `sudo chown`) and retries the write operation. If there is still an
#           error, it logs an error message to a separate error log file.
#       - `ow(self, line)`: Overwrites the existing log file with a new log entry.
#       - `error(self, line)`: Writes an error log entry.
#       - `warn(self, line)`: Writes a warning log entry.
#       - `info(self, line)`: Writes an information log entry.
#       - `live(self, line)`: Writes a live log entry.
#       - `debug(self, line)`: Writes a debug log entry.
#       - `fatal(self, line)`: Writes a fatal log entry.
#       - `line(self, line)`: Writes a live log entry.
#
# The code also includes some helper methods (`_checkPath_` and `_checkName_`) that are used to validate the path and
#   name of the log file and ensure they meet the requirements.
#


from os.path import splitext, basename
from os import system, getcwd
from sys import argv
from time import localtime
from datetime import datetime, timedelta
from pathlib import Path

PATH_LOGS = getcwd()
TIMESTAMP_FORMAT = '%Y%m%d_%H%M%S'


def getStrTime(formato=None, utc=False, dst=False):
    """Return the current time in a string with format conts.TIMESTAMP_FORMAT."""
    if formato is None:
        formato = TIMESTAMP_FORMAT
    if utc:
        return str(datetime.utcnow().strftime(formato))
    elif dst:
        return str(datetime.now().strftime(formato))
    else:
        if localtime().tm_isdst:
            return str((datetime.now() - timedelta(hours=1)).strftime(formato))
        else:
            return str(datetime.now().strftime(formato))


class Log:
    """Log the line into the file.  V20230822
          line:      line to print
          path:      path without file name (const.PATH_LOGS)
          timestamp: boolean to indicate if add timestamp (True)
          fprint:    boolean if in addition to print in file, print in stdio (True)
    """
    path = None
    name = None

    def __init__(self, path=PATH_LOGS, timestamp=True, fprint=True):
        self.path = Path(path)
        self._checkPath_()
        self.timestamp = timestamp
        self.fprint = fprint

    def _checkPath_(self):
        if self.path.exists():
            if self.path.is_file():
                self.name = self.path.name
            elif self.path.is_dir():
                self.name = None
        elif self.path.suffix == '':
            self.path.mkdir(parents=True)
            self.name = None
        else:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.name = self.path.name
        self._checkName_()

    def _checkName_(self):
        if self.name is None:
            fn, fe = splitext(basename(argv[0]))
            self.name = fn
        if len(str(self.name)) == 0:
            self.name = 'test.log'
        fExt = splitext(self.name)
        if '.' in fExt[1]:
            if len(fExt[1]) == 1:
                ext = '.log'
            else:
                ext = fExt[1]
        else:
            ext = fExt[1] + '.log'
        self.name = fExt[0] + ext

    def setName(self, name):
        self.name = name
        self._checkPath_()

    def setPath(self, path):
        self.path = Path(path)
        self._checkPath_()

    def setTimeStamp(self, timestamp):
        self.timestamp = timestamp

    def setFprint(self, fprint):
        self.fprint = fprint

    def getName(self):
        return self.name

    def getPath(self):
        return self.path

    def getTimeStamp(self):
        return self.timestamp

    def getFprint(self):
        return self.fprint

    def getFullPath(self):
        return self.path

    def w(self, line, ow=False):
        """ write the line into the file """
        if ow:
            wo = 'w'
        else:
            wo = 'a'
        if type(line) != 'str':
            line = str(line)
        if len(line) > 0:
            if not self.path.is_file():
                f = self.path.open('w')
            else:
                try:
                    f = self.path.open(wo)
                except IOError:
                    system('sudo chown pi:pi {}'.format(self.path))
                    try:
                        f = self.path.open(wo)
                    except IOError:
                        system('sudo echo error writing {} in file {} >> errLog.log'.format(line, self.name))
                        return
            now = getStrTime()
            if self.fprint:
                if self.timestamp:
                    print('{}, {}'.format(now, line))
                else:
                    print(line)
            if line[-1] != '\n':
                line += '\n'
            if self.timestamp:
                f.write('{},{}'.format(now, line))
            else:
                f.write(line)
            f.flush()
            f.close()

    def ow(self, line):
        """ Overwrite the same file log """
        self.w(line, ow=True)

    def error(self, line):
        self.w('[Error]: ' + str(line))

    def warn(self, line):
        self.w('[Warning]: ' + str(line))

    def info(self, line):
        self.w('[Info]: ' + str(line))

    def live(self, line):
        self.w('[Live]: ' + str(line))

    def debug(self, line):
        self.w('[Debug]: ' + str(line))

    def fatal(self, line):
        self.w('[Fatal]: ' + str(line))

    def line(self, line):
        self.w('[Live]: ' + str(line))
