# -------------------------------------------------------------------------------
# Name:        systemTools
# Purpose:     Tools for system, like create directories, compress files, execute commands, etc.
#
#
# Author:      Gesuri
#
# Created:     January 15 , 2015
# Update:      January 20, 2015
# Copyright:   (c) Gesuri 2015
# Licence:     Apache 2.0
# -------------------------------------------------------------------------------

from time import gmtime, strftime
import os, time, subprocess, glob, datetime
from itertools import (takewhile, repeat)
from pathlib import Path
from dateutil.parser import parse
try:
    from consts import TIMESTAMP_FORMAT_FILES, TIMESTAMP_FORMAT_CS_LINE, TIMESTAMP_FORMAT
except ImportError:
    TIMESTAMP_FORMAT = '%Y%m%d_%H%M%S'


def executeCommand(cmd):
    """
    Execute a command and return the stdiout and errors.
    cmd: list of the command. e.g.: ['ls', '-la']
    """
    try:
        cmd_data = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        output, error = cmd_data.communicate()
        return output, error
    except:
        return 'Error in command:', cmd


def executeCommandBasic(cmd):
    """
    Execute a command and return the stdiout and errors.
    cmd: list of the command. e.g.: ['ls', '-la']
    """
    try:
        cmd_data = subprocess.call(cmd, shell=True)
        return cmd_data
    except:
        return 'Error in command:', cmd


def compressDir(pathDir, prefix, pathOutput):
    """
    Compress the files in the current directory with a postfix name with the time.
    pathDir: it is needed to add an * to add all the files in the directory
    """
    pathOutput = Path(pathOutput)
    tarName = pathOutput.joinpath(f'{prefix}_{getStrTime()}.tar.gz')
    # pathOutput.mkdir(parents=True, exist_ok=True)
    if not createDir(pathOutput):
        return False
    cmd = ['tar', '-zcf', tarName] + glob.glob(pathDir)
    # print cmd
    out, err = executeCommand(cmd)
    if len(err) > 1:
        print('Error:', err)
    if len(out) > 1:
        print('Output:', out)


def getStrTime():
    """
    Return the current time in a string with format YYmmdd_HHMMSS
    """
    return strftime(TIMESTAMP_FORMAT, gmtime(time.time() - time.timezone))


def getDT4Str(strTime, format=None):
    """
    Return a datetime object from a string with format YYmmdd_HHMMSS
    """
    if format is None:
        format = TIMESTAMP_FORMAT
    try:
        dt = datetime.datetime.strptime(strTime, format)
    except:
        print(f'Trying parse because not valid default format ({format}): {strTime}')
        try:
            dt = parse(strTime)
        except:
            print('Error trying to parse:', strTime)
            return None
    return dt


def getTimeNow(form=None, dtO=False):
    """
    Return the current local time
    If form is None, return default format '%Y%m%d_%H%M%S'
    else give format string in form.
    If dtO is True, return a datetime object
    """
    time_ = datetime.datetime.now()
    if dtO:
        return time_
    elif not (form is None):
        return time_.strftime(form)
    else:
        return time_.strftime(TIMESTAMP_FORMAT)


def getPathFilenameExtension(path, extDot=True):
    """
    Return the dir name, file name, extension
    If extDot = False, the extension is without the dot
    """
    if os.path.isdir(path):
        dirname = os.path.abspath(path)
        return dirname, None, None
    else:
        dirname = os.path.dirname(path)
        name, ext = os.path.splitext(os.path.basename(path))
        if not extDot:
            ext = ext[1:]
        return dirname, name, ext


def file_len(fname):
    i = 0
    with open(fname) as f:
        for i, l in enumerate(f):
            pass
    return i + 1


def createDir(dirPath):
    """
    Create a directory. If already exist just return, otherwise, create it and return
    It creates intermediate directories
    """
    dirPath = Path(dirPath)
    try:
        dirPath.mkdir(parents=True, exist_ok=True)
    except PermissionError:
        print('Not possible create folder cause permissions.')
        return False
    except Exception as err:
        print(f'Error creating folder. {err}, {type(err)=}')
        return False
    return True


def rawincount(filename):
    """
    To count the number of lines
    """
    f = open(filename, 'rb')
    bufgen = takewhile(lambda x: x, (f.raw.read(1024*1024) for _ in repeat(None)))
    return sum(buf.count(b'\n') for buf in bufgen)


def sizeof_fmt(num, suffix="B"):
    """
    Converts a file size in bytes into a human-readable format (e.g., KB, MB, GB).

    Args:
        num (float): The file size in bytes.
        suffix (str): The suffix to append to the formatted size (default: "B" for bytes).

    Returns:
        str: A string representing the human-readable file size.

    Example:
        >>> sizeof_fmt(1024)
        '1.0 KiB'
        >>> sizeof_fmt(1048576)
        '1.0 MiB'
    """
    for unit in ("", "Ki", "Mi", "Gi", "Ti"):
        if abs(num) < 1024.0:
            return f"{num:3.1f} {unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f} Yi{suffix}"


def td_format(td_object):
    """
    Formats a `timedelta` object into a human-readable string representing years, months, days, hours, minutes, and seconds.

    Args:
        td_object (timedelta): The time difference to format.

    Returns:
        str: A string representing the time difference in a human-readable format.

    Example:
        >>> td_format(datetime.timedelta(seconds=3661))
        '1 hour, 1 minute, 1 second'
    """
    seconds = int(td_object.total_seconds())
    periods = [
        ('year',        60*60*24*365),
        ('month',       60*60*24*30),
        ('day',         60*60*24),
        ('hour',        60*60),
        ('minute',      60),
        ('second',      1),
    ]
    strings=[]
    for period_name, period_seconds in periods:
        if seconds > period_seconds:
            period_value , seconds = divmod(seconds, period_seconds)
            has_s = 's' if period_value >= 1 else ''
            strings.append("%s %s%s" % (period_value, period_name, has_s))
        #else:
        #    return '0.00 seconds'
    return ", ".join(strings)


class ElapsedTime:
    """
    A class to track and format the elapsed time between a start and end event.

    Attributes:
        startTime (datetime): The time when the timer was started.
        endTime (datetime): The time when the timer was stopped.
        returnStr (bool): Whether to return the elapsed time as a formatted string or as a `timedelta` object.

    Methods:
        start(): Starts the timer by recording the current time.
        end(): Stops the timer and returns the elapsed time.
        elapsed(): Returns the elapsed time as a string or `timedelta` object.
    """
    startTime = None
    endTime = None
    returnStr = True

    def __init__(self, returnStr=True):
        """
        Initializes the ElapsedTime class and starts the timer.

        Args:
            returnStr (bool): Whether to return the elapsed time as a formatted string (default: True).
        """
        self.returnStr = returnStr
        self.start()

    @staticmethod
    def _current_():
        """
        Gets the current time as a datetime object.

        Returns:
            datetime: The current time.
        """
        return datetime.datetime.now()

    def elapsed(self):
        """
        Calculates and returns the elapsed time since the timer started.

        Returns:
            str or timedelta: The elapsed time in a formatted string or as a `timedelta` object.
        """
        if not self.endTime:
            self.endTime = self._current_()
        passedTime = self.endTime - self.startTime
        if self.returnStr:
            return td_format(passedTime)
        else:
            return passedTime

    def start(self):
        """
        Starts the timer by recording the current time.
        """
        self.startTime = self._current_()
        self.endTime = None

    def end(self):
        """
        Stops the timer by recording the current time and returns the elapsed time.

        Returns:
            str or timedelta: The elapsed time in a formatted string or as a `timedelta` object.
        """
        self.endTime = self._current_()
        return self.elapsed()