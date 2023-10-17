# -------------------------------------------------------------------------------
# Name:        systemTools
# Purpose:     Tools to use like execute a program and zip
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
    """   Execute a command and return the stdiout and errors.
   cmd: list of the command. e.g.: ['ls', '-la']   """
    try:
        cmd_data = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        output, error = cmd_data.communicate()
        return output, error
    except:
        return 'Error in command:', cmd


def executeCommandBasic(cmd):
    """   Execute a command and return the stdiout and errors.
   cmd: list of the command. e.g.: ['ls', '-la']   """
    try:
        cmd_data = subprocess.call(cmd, shell=True)
        return cmd_data
    except:
        return 'Error in command:', cmd


def compressDir(pathDir, prefix, pathOutput):
    """ Compress the files in the current directory with a postfix name with the time.
   pathDir: it is needed to add an * to add all the files in the directory   """
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
    """ Return the current time in a string with format YYmmdd_HHMMSS   """
    return strftime(TIMESTAMP_FORMAT, gmtime(time.time() - time.timezone))


def getDT4Str(strTime, format=None):
    """ Return a datetime object from a string with format YYmmdd_HHMMSS   """
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
    """Return the current local time
   If form is None, return default format '%Y%m%d_%H%M%S'
   else give format string in form.
   If dtO is True, return a datetime object """
    time_ = datetime.datetime.now()
    if dtO:
        return time_
    elif not (form is None):
        return time_.strftime(form)
    else:
        return time_.strftime(TIMESTAMP_FORMAT)


def getPathFilenameExtension(path, extDot=True):
    """    Return the dir name, file name, extension
   If extDot = False, the extension is without the dot    """
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
    """Create a directory. If already exist just return, otherwise, create it and return
   It creates intermediate directories"""
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
    """ To count the number of lines"""
    f = open(filename, 'rb')
    bufgen = takewhile(lambda x: x, (f.raw.read(1024*1024) for _ in repeat(None)))
    return sum(buf.count(b'\n') for buf in bufgen)


def sizeof_fmt(num, suffix="B"):
    for unit in ("", "Ki", "Mi", "Gi", "Ti"):
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"
