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
from os import mkdir, system, getcwd
from sys import argv
from time import localtime
from datetime import datetime, timedelta
from pathlib import Path

PATH_LOGS = getcwd()
TIMESTAMP_FORMAT = '%Y%m%d_%H%M%S'


def getStrTime(format=None, utc=False, dst=False):
    """Return the current time in a string with format conts.TIMESTAMP_FORMAT."""
    if format is None:
        format = TIMESTAMP_FORMAT
    if utc:
        return str(datetime.utcnow().strftime(format))
    elif dst:
        return str(datetime.now().strftime(format))
    else:
        if localtime().tm_isdst:
            return str((datetime.now() - timedelta(hours=1)).strftime(format))
        else:
            return str(datetime.now().strftime(format))


class Log:
    """Log the line into the file.
          line:      line to print
          name:      file name without full path (running_process_name)
          path:      path without file name (const.PATH_LOGS)
          timestamp: boolean to indicate if add timestamp (True)
          fprint:    boolean if in addition to print in file, print in stdio (True)
    """

    def __init__(self, name=None, path=PATH_LOGS, timestamp=True, fprint=True):
        self.name = name
        self.path = Path(path)
        self.timestamp = timestamp
        self.fprint = fprint
        self._checkPath_()
        self._checkName_()

    def _checkPath_(self):
        if not self.path.is_dir():
            mkdir(self.path)

    def _checkName_(self):
        # pdb.set_trace()
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
        # self.fullPathName = join(self.path, self.name)
        self.fullPathName = self.path.joinpath(self.name)

    def setName(self, name):
        self.name = name
        self._checkName_()

    def setPath(self, path):
        self.path = path
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
        return self.fullPathName

    def w(self, line, ow=False):
        """ write the line into the file """
        if ow:
            wo = 'w'
        else:
            wo = 'a'
        if type(line) != 'str':
            line = str(line)
        if len(line) > 0:
            if not self.fullPathName.is_file:
                f = self.fullPathName.open('w')
            else:
                try:
                    f = self.fullPathName.open(wo)
                except IOError:
                    system('sudo chown pi:pi {}'.format(self.fullPathName))
                    try:
                        f = self.fullPathName.open(wo)
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