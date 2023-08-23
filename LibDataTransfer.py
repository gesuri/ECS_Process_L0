# -------------------------------------------------------------------------------
# Name:        LibDataTransfer
# Purpose:     Library for data transfer
#              Functions to manage files on Windows
#
# Author:      Gesuri
#
# Created:     06/09/2013
# Copyright:   (c) Gesuri 2013 - 2021
# Licence:     Apache 2.0
# -------------------------------------------------------------------------------

# The code provided contains several functions related to file and folder operations. Here's a breakdown of the code
# and its functionalities:
#
# 1. Import Statements:
#   'os, time, datetime, shutil, hashlib, glob': These modules are used for various file and folder operations.
#   'zipfile': This module is used for creating and extracting ZIP archives.
#   'pathlib.Path': This class provides an object-oriented approach to work with file paths.
#   'Log': This is a custom module (not included in the provided code) that is likely used for logging purposes.
#
# 2. File Information Functions:
#   'getPathFilenameExtension(pathFileName)': Extracts the directory name, file name, and extension from a given
#       file path.
#   'getNameExtension(fileName)': Returns the name of a file without the extension.
#
# 3. File Listing Functions:
#   'getOnlyFilesNames(path)': Returns a list of only the names of files in a given directory.
#   'listOfFilesWithExtension(path, listFiles, extension)': Returns a list of files with a specific extension from a
#       given list of files.
#   'listOfFilesWithExtensionInPath(path, extension)': Returns a list of files with a specific extension in a given
#       directory.
#   'getListOfFiles(path, match)': Returns a list of files that match a specific pattern.
#
# 4. MD5 Functions:
#   'md5_for_file(path, block_size=256 * 128, hr=False)': Computes the MD5 hash of a file.
#   'checkMD5onZipFile(tempFolder, logFolder, compressedFile)': Checks the MD5 hash of files within a ZIP archive and
#       logs any files with incorrect hashes.
#   'createMD5file(localFolder)': Creates a file in the specified folder containing the MD5 hash of each file in
#       the folder.
#
# 5. ZIP File Functions:
#   'zipFiles(localFolder)': Compresses files in a folder into a ZIP archive.
#   'unzipAfile(fileItem, outputFolder, listFiles=False, onlyExt=[])': Extracts a file or files from a ZIP archive
#       into the specified output folder. It can also list the extracted files and filter them based on file extensions.
#
# 6. File Manipulation Functions:
#   'copyAfile(src, dst)': Copies a file from the source to the destination folder.
#   'delAfile(pathFile)': Deletes a file.
#   'moveAfileWOOW(src, dst)': Moves a file to a destination folder without overwriting existing files.
#   'renameFiles(localFolder)': Renames all files in a folder by appending the creation date and time to the file name.
#   'renameAFileWithDate(pathFile, secondsTZ=60 * 60 * 7)': Renames a file with the creation date and time.
#   'copyFiles(srcFolder, destFolder)': Copies all files from the source folder to the destination folder.
#   'moveAll(src, dst)': Moves a file or directory recursively to another location.
#   'deleteFiles(folder)': Deletes all files in a folder.
#   'numberOfFiles(path)': Returns the number of files in a folder.
#
# 7. Folder Functions:
#   'checkFolder(path)': Checks if a folder can be written to.
#   'createFolder(path)': Creates a folder if it does not exist.
#   'delFolder(path)': Removes or deletes a folder.
#   'checkAndFixPath(path_)': Checks and fixes the format of a path by appending a backslash if it is missing.
#   'joinPath(path1, path2)': Joins two paths together.

import glob
import hashlib
import os
import re
import shutil
import time
import zipfile
from pathlib import Path

import ConverterCambellsciData
import Log
import consts
import systemTools


###########################################
# info of files

# def getInfoFile(pathFileName):
#     """ Return a dictionary with the info of the file
#     basic:
#         folder, site, datalogger, table, fileNameDT, extension
#     advance:
#         creationDT, numberlines, type, stationName, model, serialNumber, os, program, signature, tableName """
#     info = consts.CS_DATA_DICT.copy()
#     info['path'] = pathFileName
#     # check if the file is a Path object and if not convert it
#     if not isinstance(pathFileName, Path):
#         pathFileName = Path(pathFileName)
#     try:
#         # get the name of the file
#         fileName = pathFileName.stem
#         # get the extension of the file
#         info['extension'] = pathFileName.suffix
#         # split the name of the file
#         fileNameSplit = fileName.split('_')
#         # get the date and time of the file
#         info['site'] = fileNameSplit[consts.CS_FILE_NAME_SITE]
#         info['datalogger'] = fileNameSplit[consts.CS_FILE_NAME_DATALOGGER]
#         info['pathLog'] = consts.PATH_CLOUD.joinpath(info['site'], 'logs', fileName[:-15])
#         # check if the fileNameSplit has more than 3 elements and if yes, get the date and time
#         if len(fileNameSplit) > 3:
#             info['fileNameDT'] = systemTools.getDT4Str(fileName[-15:])
#         # get the creation date and time of the file
#         if pathFileName.exists():
#             info['creationDT'] = datetime.datetime.fromtimestamp(pathFileName.stat().st_ctime)
#             if pathFileName.stat().st_size > 1:
#                 info['statusFile'] = consts.STATUS_FILE_OK
#             else:
#                 info['statusFile'] = consts.STATUS_FILE_EMPTY
#         if info['statusFile'] == consts.STATUS_FILE_OK:
#             _meta_ = getMetaDataFile(pathFileName)
#             for item in _meta_:
#                 info[item] = _meta_[item]
#             if len(_meta_['headers']) == 0:
#                 print(f'Error: {pathFileName} has no headers or it is empty')
#                 return info
#             info['headers'] = _meta_['headers']
#             nl = getStrippedHeaderLine(_meta_['headers'][0])
#             for item in consts.CS_FILE_METADATA:
#                 info[item] = nl[consts.CS_FILE_METADATA[item]].replace('"', '')
#                 # get the number of lines of the file
#             info['pathLog'] = info['pathLog'].parent.joinpath(info['tableName'])
#             if 'TOA' in info['type']:
#                 info['numberlines'] = systemTools.rawincount(pathFileName)
#             storageTableName = consts.TABLES_SPECIFIC_NAMES.get(info['tableName'], info['tableName'])
#             info['tableStorage'] = consts.PATH_STORAGE.joinpath(info['site']).joinpath('Tower').joinpath(storageTableName)
#             psf = consts.PATH_CLOUD.joinpath(info['site'], 'Shared')
#             info['pathShared'] = psf.joinpath(pathFileName.stem[:-16]+pathFileName.suffix)
#             info['pathBackup'] = consts.PATH_BACKUP.joinpath(info['site'])
#     except Exception as e:
#         print(f'Error in getInfoFileName: {e}')
#     # return the dictionary
#     info['log'] = Log.Log(info['pathLog'])
#     return info


def getStrippedHeaderLine(line):
    return re.split(r',(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)', line)


# def getTableStoragePath(info):
#     """ Return the path of the storage of the site """
#     storageTableName = consts.TABLES_SPECIFIC_NAMES.get(info['tableName'], info['tableName'])
#     return consts.PATH_STORAGE.joinpath(info['site']).joinpath('Tower').joinpath(storageTableName)


# def getRAWstoragePath(site):


def getHeaderFLlineFile(pathFileName, log=None):  # TODO: check if this function is working as expected
    """ return a dict with the 'headers' that are the first lines
     'firstLineDT', the first line timestamp od data and the 'lastLine' timestamp of data """
    meta = {'headers': [], 'firstLineDT': None, 'lastLineDT': None}
    _log = False
    if log is not None and isinstance(log, Log.Log):
        _log = True
    try:
        with open(pathFileName, 'rb') as f:
            for i in range(len(consts.CS_FILE_HEADER_LINE) - 1):
                meta['headers'].append((f.readline().decode('ascii')).strip())
            # check if the first 10 chars in the first line on the headers existe the substring TOA
            if 'TOA' in meta['headers'][0][:10]:
                meta['firstLineDT'] = getDTfromLine(f.readline().decode('ascii'))
                f.seek(-2, os.SEEK_END)
                while f.read(1) != b'\n':
                    f.seek(-2, os.SEEK_CUR)
                lastLine = getDTfromLine(f.readline().decode('ascii'))
                if lastLine is None:
                    meta['lastLineDT'] = meta['firstLineDT']
                else:
                    meta['lastLineDT'] = lastLine
            elif 'TOB' in meta['headers'][0][:10]:
                msg = f'The file {pathFileName} is a TOB file and need to be converted to TOA'
                if _log:
                    log.error(msg)
                else:
                    print(msg)
    except Exception as e:
        msg = f'Error in getMetaDataFile: {e}'
        if _log:
            log.error(msg)
        else:
            print(msg)
        f.seek(0)
    return meta


def checkAndConvertFile(pathFile, log=None):
    """ This method is going to check and performance some steps before to be used by this class.
     First, it is going to change the name of the curren file adding at the end the timestamp,
     Second, it will check the first 10 char in the first line of the file has the substring TOA or TOB.
        If TOB, it will convert the file to TOA using the method convertTOB2TOA in ConverterCambellsciData.py
            it will get the current full path of the TOA converted file.
        If TOA, it will continue with the next step.  """
    toReturn = {'path': None, 'toaPath': None, 'tobPath': None}
    _log = False
    if log is not None and isinstance(log, Log.Log):
        _log = True
    toReturn['path'] = renameAFileWithDate(pathFile)
    if isinstance(toReturn['path'], Path):
        try:
            with open(str(toReturn['path']), 'rb') as f:
                firstLine = f.readline().decode('ascii')
                if 'TOB' in firstLine[0:10]:
                    toReturn['toaPath'] = ConverterCambellsciData.TOB2TOA(toReturn['path'])
                if 'TOA' in firstLine[0:10]:
                    toReturn['toaPath'] = toReturn['path']
                else:
                    msg = f'The file {toReturn["path"]} is not a TOA or TOB file'
                    if _log:
                        log.error(msg)
                    else:
                        print(msg)
                        toReturn['path'] = None
                    return toReturn
        except Exception as e:
            msg = f'Error in checkFile: {e}'
            if _log:
                log.error(msg)
            else:
                print(msg)
            toReturn['path'] = None
    return toReturn


def getDTfromLine(line):
    """ Return the datetime of the line """
    line = line.split(',')
    if len(line) > 0:
        if len(line[0]) > 0:
            if len(line[0][1:-1]) == 19:
                _format = consts.TIMESTAMP_FORMAT_CS_LINE
            elif len(line[0][1:-1]) > 20:
                _format = consts.TIMESTAMP_FORMAT_CS_LINE_HF
            else:
                _format = consts.TIMESTAMP_FORMAT
            return systemTools.getDT4Str(line[0][1:-1], _format)
    return None


def getCSFromLine(line):
    """ Return a dictionary with the info of the file """
    nl = re.split(r',(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)', line)
    info = {}
    for item in consts.CS_FILE_METADATA:
        info[item] = nl[consts.CS_FILE_METADATA[item]]
    return info


def getPathFilenameExtension(pathFileName, resolve=False):
    """ Return the dir name, file name, extension """
    pathFileName = Path(pathFileName)
    if resolve:
        pathFileName = pathFileName.resolve()
    dirname = pathFileName.parent
    name = pathFileName.stem
    ext = pathFileName.suffix
    return dirname, name, ext


def getNameExtension(fileName):
    """ Return a the name of the file without the extension """
    try:
        return fileName.rsplit('.', 1)
    except:
        return None


# def getStoragePath(site):
#     """ Return the storage path of the file based on the site """
#     consts.CS_STORAGE_PATH.joinpath(site)


###########################################
# list of files
def getOnlyFilesNames(path):
    """ return a list of only the name of files on path """
    dirList = os.listdir(path)
    listOfFiles = []
    for fileItem in dirList:
        if os.path.isfile(os.path.join(path, fileItem)):
            listOfFiles.append(fileItem)
    return listOfFiles


def listOfFilesWithExtension(path, listFiles, extension):
    """ return a list of the files with the matching extension """
    newList = []
    path = Path(path)
    for item in listFiles:
        # fileExtName = sorted(item.rsplit('.',1), reverse = True)
        tPath, filename, fileExtName = getPathFilenameExtension(path.joinpath(item))
        if fileExtName == '.' + extension:
            newList.append(item)
    return newList


def listOfFilesWithExtensionInPath(path, extension):
    """ Return the list of file on the path and with the extension """
    return listOfFilesWithExtension(path, getOnlyFilesNames(path), extension)


def getListOfFiles(path, match):
    """ Return the list of files that match.
    e.g.: getListOfFiles('CR3000_flux_20130909_17*.*')
    return: ['CR3000_flux_20130903_170000.dat', 'CR3000_flux_20130903_173000.dat'] """
    actualDir = os.getcwd()
    os.chdir(path)
    l = glob.glob(match)
    os.chdir(actualDir)
    return l


###########################################
### MD5
def md5_for_file(path, block_size=256 * 128, hr=False):
    """ Block size directly depends on the block size of your filesystem
    to avoid performances issues
    Here I have blocks of 4096 octets (Default NTFS) """
    md5 = hashlib.md5()
    try:
        with open(path, 'rb') as f:
            for chunk in iter(lambda: f.read(block_size), b''):
                md5.update(chunk)
    except IOError:
        return '0'
    if hr:
        return md5.hexdigest()
    return md5.digest()


def checkMD5onZipFile(tempFolder, logFolder, compressedFile):
    """ Check the MD5 and if one file is not correct, it will be erased and log the file that is not correct """
    i = 0
    iFile = numberOfFiles(tempFolder) - 1
    f = open(os.path.join(tempFolder, "MD5.txt"), "r")
    logFile2Move = open(os.path.join(logFolder, "Log_files2move.2do"), "a+")
    # logErrors = open(os.path.join(logFolder, "log_errors.txt"), "a+")
    log = Log.Log('md5.log', logFolder)
    # title = "\n"+time.strftime("%Y-%m-%d %H:%M:%S ", time.gmtime(time.time() - (60*60*7))) + " "+compressedFile
    # logErrors.write(title+"\n")
    # log.info(compressedFile)
    # print "\tInside..."
    for line in f:
        i = i + 1
        data = line.split(',')
        name = data[1].strip()
        md5Log = data[0]
        md5File = md5_for_file(os.path.join(tempFolder, name), 256 * 128, True)
        if not md5Log == md5File:
            print("\tThe file: " + name + " is NOT CORRECT!")
            print("\tLog: " + md5Log + " Zipped: " + md5File)
            # logErrors.write("The file: "+name+" has a different MD5, so it was erased. Need to be moved manually!\n")
            log.error("The file: " + name + " has a different MD5, so it was erased. Need to be moved manually!")
            logFile2Move.write(name + "\n")
            print("\t\terasing file...")
            if not delAfile(os.path.join(tempFolder, name)):
                log.error("The file: " + name + " does not exist!")
            log.error('The file: {} is not correct and need to be moved manually.'.format(name))
    f.close()
    delAfile(os.path.join(tempFolder, "MD5.txt"))
    # if i != iFile:
    #    print("\tThe number of MD5's in MD5.txt and the number of files decompresses are different")
    #    log.error(
    #        "The number of files on MD5.txt and current files are different. MD5's on file: {}, Current files: {}\n".format(
    #            i, iFile))
    # logErrors.close()
    logFile2Move.close()


def createMD5file(localFolder):
    """ Create a file on the current local folder.
    The file contains the name of each file and the corresponding MD5 """
    dirList = os.listdir(localFolder)
    file_ = open(os.path.join(localFolder, "MD5.txt"), 'w+')
    for fileItem in dirList:
        if os.path.isfile(localFolder + fileItem):
            file_.write(md5_for_file(localFolder + fileItem, 256 * 128, True) + ", " + fileItem + "\n")
    file_.close()


###########################################
### zips
def zipFiles(localFolder):
    """ Compress files in a folder to a zip file. """
    compression = zipfile.ZIP_DEFLATED
    zipName = "DataECT" + time.strftime("_%Y%m%d_%H%M%S", time.gmtime(time.time() - (60 * 60 * 7))) + ".zip"
    os.chdir(localFolder)
    dirList = os.listdir(localFolder)
    zipFile_ = zipfile.ZipFile(zipName, "w")
    for fileItem in dirList:
        if os.path.isfile(fileItem):
            print("\tAdding: " + fileItem)
            zipFile_.write(fileItem, compress_type=compression)
    zipFile_.close()
    return zipName


def unzipAfile(fileItem, outputFolder, listFiles=False, onlyExt=[]):
    """ Unzip fileItem in the output folder
            if listFiles True, return list of files inside zip
            if onlyExt list
        """
    outputFolder = Path(outputFolder)
    lf = []
    if type(onlyExt) != list:
        if type(onlyExt) != str:
            onlyExt = []
        else:
            onlyExt = [onlyExt]
    onlyExt = ['.' + x if x[0] != '.' else x for x in onlyExt]
    try:
        fileZ = zipfile.ZipFile(fileItem, "r")
        for name in fileZ.namelist():
            print("\tExtracting: " + name + " ...")
            fileZ.extract(name, outputFolder)
            if len(onlyExt) == 0:
                lf.append(outputFolder.joinpath(name))
            else:
                for item in onlyExt:
                    if item == name[-len(item):]:
                        lf.append(outputFolder.joinpath(name))
    except zipfile.BadZipfile:
        print(f"ERROR: bad zip file {fileItem}")
        return None
    if listFiles:
        return lf
    else:
        return True


###########################################
### file work
def copyAfile(src, dst):
    """ Copy a file from soruce to destination folders """
    shutil.copy(src, dst)


def delAfile(pathFile):
    """ Delete one file. pathFile is the path and the file to delete. """
    try:
        os.remove(pathFile)
    except WindowsError:
        return False
    else:
        return True


def moveAfileWOOW(src, dst):
    """ move a file without overwriting"""
    src = Path(src)
    dst = Path(dst)
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        dst_ = Path(f'{dst}-{systemTools.getStrTime()}')
        print(f'The file {dst} already exists, changing to {dst_.name}')
        dst = dst_
    shutil.copy2(src, dst)
    time.sleep(1)
    if dst.is_file():
        try:
            os.remove(src)
        except WindowsError:
            print('Not possible to erase source file {}'.format(src))
            return -1
    else:
        print('Not copied the file {} into destination {}'.format(src, dst))
        return 0
    return 1


def renameFiles(localFolder):
    """ Rename all the files adding the date and time """
    localFolder = Path(localFolder)
    dirList = [x for x in localFolder.iterdir() if x.is_file()]
    newDirList = []
    for fileItem in dirList:
        r = renameAFileWithDate(fileItem)
        if not r is False:
            newDirList.append(renameAFileWithDate(fileItem))


def renameAFileWithDate(pathFile, log=None):
    """ Rename a file with the created date """
    _log = False
    if log is not None and isinstance(log, Log.Log):
        _log = True
    if pathFile.is_file():
        addName = '_' + time.strftime(consts.TIMESTAMP_FORMAT,
                                      time.gmtime(pathFile.stat().st_ctime - consts.SECONDS_TZ))
        if addName in pathFile.stem:
            if _log:
                log.info(f'The file already have the date in the name {pathFile.name}')
            return pathFile
        completeName = pathFile.parent.joinpath(pathFile.stem + addName + pathFile.suffix)
        # print(f'   {pathFile.name} -> {completeName.name}')
        try:
            pathFile.rename(completeName)
        except (PermissionError, WindowsError) as error:
            msg = f'Not possible to rename the file{pathFile} because {error}'
            if _log:
                log.error(msg)
            else:
                print(msg)
            return False
        return completeName
    else:
        msg = f'Not a file {pathFile}'
        if _log:
            log.error(msg)
        else:
            print(msg)
        return False


def copyFiles(srcFolder, destFolder):
    """ Copy all the files in source folder to destination folder """
    err = []
    dirList = os.listdir(srcFolder)
    for fileItem in dirList:
        if os.path.isfile(os.path.join(srcFolder, fileItem)):
            copyAfile(os.path.join(srcFolder, fileItem), os.path.join(destFolder, fileItem))
            if not os.path.isfile(os.path.join(destFolder, fileItem)):
                err.append(fileItem)
            else:
                # delAfile(os.path.join(srcFolder, fileItem)
                pass
    if len(err) > 0:
        return err
    else:
        return True


def moveAll(src, dst):
    """ Recursively move a file or directory (src) to another location (dst).
    It uses shutil.move(src, dst) """
    try:
        shutil.move(src, dst)
    except OSError:
        print('Error. Maybe it already exists.')
        log = Log.Log('move')
        log.error('no possible moving from {} to {}'.format(src, dst))


def deleteFiles(folder):
    """ Delete all the files on the folder """
    dirList = os.listdir(folder)
    for fileItem in dirList:
        if os.path.isfile(folder + fileItem):
            delAfile(folder + fileItem)


def numberOfFiles(path):
    i = 0
    dirList = os.listdir(path)
    for fileItem in dirList:
        if os.path.isfile(os.path.join(path, fileItem)):
            i = i + 1
    return i


###########################################
### folders
def checkFolder(path):
    """ Check if a folder can be writen. True if yes. """
    if not os.path.isdir(path):
        try:
            os.makedirs(path)
        except OSError:
            return False
    try:
        open(os.path.join(path, 'temp'), 'w')
    except (OSError, IOError):
        return False
    else:
        os.remove(os.path.join(path, 'temp'))
    return True


# def createFolder(path):
#    """ Create a folder. """
#    if not os.path.exists(path):
#        os.makedirs(path)


# def delFolder(path):
#    """ Remove or deleate a folder """
#    if os.path.exists(path):
#        os.rmdir(path)


# def checkAndFixPath(path_):
#    if path_[-1:] != "\\":
#        path_ = path_ + "\\"
#    return path_


# def joinPath(path1, path2):
#    return os.path.join(path1, path2)


if __name__ == '__main__':
    print("Testing code is here!")
    # print getPathFilenameExtension('test.csv')
    # print(getInfoFile('Bahada_CR3000_fluxw.dat'))
