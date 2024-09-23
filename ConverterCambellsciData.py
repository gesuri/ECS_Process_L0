# -------------------------------------------------------------------------------
# Name:        ConverterCambellsciData
# Version:     20230822
# Purpose:      converts TOB files to TOA using the TOB32.exe
#               converts TOA files to TOB using the TOB32.exe
#              TODO: check if each file to convert exists before to try to convert
#              TODO: finish changing everything print to log
#              TODO: check if the tob32.exe exists
#
# Author:      Gesuri
#
# Created:     03/13/2013
# Updated:     June 11, 2015
#               Added the record number in conversion from TOB to TOA
# Copyright:   (c) Gesuri 2013
# Licence:     Apache 2.0
# -------------------------------------------------------------------------------

# The given code contains several functions related to converting data files from one format to another using the
#   TOB32.exe program. Here is a documentation for each function:
# 1. convertTOB2TOA(pathI, fileI, pathO, fileO, tob32ProgramPath=consts.TOB2PROG): This function converts a TOB file to
#   a TOA file using the TOB32.exe program. It takes the input file path (pathI), input file name (fileI), output file
#   path (pathO), output file name (fileO), and an optional tob32ProgramPath parameter specifying the path to the
#   TOB32.exe program.
# 2. TOB2TOA(pathIn, pathOut=None, tempDir=None, programPath=consts.TOB2PROG): This function is a wrapper around the
#   convertTOB2TOA function. It converts a TOB file to a TOA file using the TOB32.exe program. It takes the input file
#   path (pathIn), output file path (pathOut), temporary directory path (tempDir), and the program path (programPath)
#   as optional parameters.
# 3. TOB2TOA_fileList(fileList, pathOutput): This function converts a list of TOB files to TOA files using the
#   TOB32.exe program. The fileList parameter can be either a file path or a list of file paths. It also takes the
#   output path (pathOutput) where the converted files will be saved.
# 4. convertTOA2TOB(pathI, fileI, pathO, fileO, programPath=consts.TOB2PROG): This function converts a TOA5 file to a
#   TOB1 file using the toa_to_tob1.exe program. It takes the input file path (pathI), input file name (fileI), output
#   file path (pathO), output file name (fileO), and an optional programPath parameter specifying the path to the
#   toa_to_tob1.exe program.
# 5. convertAll4TOB2TOA(filesToConv, pathTOB, pathTOA): This function converts all the specified files (e.g., "flux",
#   "soil", "met") from TOB format to TOA format. It takes the files to convert (filesToConv), the input path for TOB
#   files (pathTOB), and the output path for TOA files (pathTOA).
# 6. checkTOBfile(pathFile): This function checks if the given file at pathFile is in TOB format by examining its
#   contents. It returns True if the file is in TOB format, and False otherwise.
# 7. checkTOAfile(pathFile): This function checks if the given file at pathFile is in TOA format by examining its
#   contents. It returns True if the file is in TOA format, and False otherwise.
# 8. checkIfAllAreTOB(path): This function checks if all the files in the specified path are in TOB format. It takes
#   the path (path) as a parameter.
# 9. checkIfAllAreTOA(path): This function checks if all the files in the specified path are in TOA format. It takes the
#   path (path) as a parameter.
# 10. convertAll4TOA2TOB(filesToConv, pathTOA, pathTOB): This function converts all the specified files (e.g., "flux",
#   "soil", "met") from TOA format


import os
import glob
import tempfile
import shutil
import time
from pathlib import Path
import Log
import consts
import LibDataTransfer
import systemTools

pre__ = "*"


def convertTOB2TOA(pathI, fileI, pathO, fileO, tob32ProgramPath=consts.TOB2PROG):
    """ Using the TOB32.exe, convert from TOB to TOA5 file(s) """
    pathI = Path(pathI).joinpath(fileI)
    pathO = Path(pathO).joinpath(fileO)
    # added the record number
    tob32ProgramPath = Path(tob32ProgramPath).joinpath('tob32.exe')
    command = f'{tob32ProgramPath} -a -r -o {pathO} {pathI}'
    # print command
    os.system(command)


def TOB2TOA(pathIn, pathOut=None, tempDir=False, log=None, programPath=consts.TOB2PROG):
    """ Convert a TOB file to a TOA file using the TOB32.exe program
        pathIn: is the file path to convert to TOA
        pathOut: is a path of the folder where will be located the new TOA file
        tempDir: True|False, if True, it will create a temporal directory to save the TOA file
        If pathIn file is already a TOA file, it will do nothing and return the pathIn
    """
    pathIn = Path(pathIn)
    tob32 = Path(programPath).joinpath('tob32.exe')
    extOut = '.TOA'
    _log = False
    if log is not None and isinstance(log, Log.Log):
        _log = True
    if not pathIn.is_file():
        msg = f'[ConverterCambellsciData::TOB2TOA]: No file to convert in pathIn: {pathIn}'
        if _log:
            log.error(msg)
        else:
            print(msg)
        return False
    if pathOut:  # there is a pathOut so here will be the new file
        pathOut = Path(pathOut)
        if pathOut.suffix == '':
            pathOut = Path(pathOut).joinpath(pathIn.stem + extOut)
    elif tempDir:  # there is not a pathOut so it will create a temporal directory and it will pathOut
        pathOut = Path(tempfile.mkdtemp()).joinpath(pathIn.stem + extOut)
    else:  # not pathOut and not tempDir so the pathOut will be the same as pathIn with different extension
        pathOut = pathIn.parent.joinpath(pathIn.stem + extOut)


    if checkTOAfile(pathIn, log):
        msg = f'The file {pathIn.name} is an ASCII'  # (TOA) file then just renaming it.'
        if _log:
            log.warn(msg)
        else:
            print(msg)

        return Path(pathIn)  # pathOut)

    cmd = [str(tob32), '-a', '-r', '-o', str(pathOut), str(pathIn)]
    data, error = systemTools.executeCommand(cmd)

    counterWait = 0

    while not pathOut.is_file():
        # print '\t No ready the file yet'
        if counterWait >= (0.1 * 10 * 5):
            msg = f'[ConvertCambellsciData::TOB2TOA]: Error in file: {pathIn}. It looks that the source file is empty'
            if _log:
                log.error(msg)
            else:
                print(msg)
            break
        counterWait += 1
        time.sleep(0.1)

    return Path(pathOut)


def TOB2TOA_fileList(fileList, pathOutput, log=None):
    """ Convert the list of files that are TOA into TOB.
    The fileList could be the path where is the file that contains the list or
    a list.
    Return False if there is it is not any or a number with the number of files
    that were not converted for any reason. """
    _log = False
    if log is not None and isinstance(log, Log.Log):
        _log = True
    err = 0
    counter = 0
    # pdb.set_trace()
    if type(fileList) is str:
        total = systemTools.file_len(fileList)
        fDirName, fName, ext = systemTools.getPathFilenameExtension(fileList)
        fl = open(fileList)
    elif type(fileList) is list:
        fl = fileList
        total = len(fl)
        fName = 'List'
    else:
        msg = '[ConvertCambellsciData]: Error, the fileList is not a string or a list'
        if _log:
            log.error(msg)
        else:
            print(msg)
        return False
    tempDir = tempfile.mkdtemp()
    for item in fl:
        counter += 1
        percentage = (float(counter) / float(total)) * 100.0
        print(counter, 'of', total, '@', ('%0.3f%%' % percentage), fName)
        item = item.rstrip('\n')
        if TOB2TOA(item, pathOutput, tempDir=tempDir) is False:
            err += 1
    if type(fileList) is str:
        fl.close()
    msg = f'[ConvertCambellsciData::TOB2TOA_fileList]: Done converting! {err} errors in conversions'
    if _log:
        log.info(msg)
    else:
        print(msg)
    time.sleep(50)
    try:
        os.remove(tempDir)
    except OSError:
        msg = f'[ConvertCambellsciData::TOB2TOA_fileList]: Error trying to remove the temporal folder {tempDir}'
        if _log:
            log.error(msg)
        else:
            print(msg)
    return err


def convertTOA2TOB(pathI, fileI, pathO, fileO, programPath=consts.TOB2PROG):
    """ Using the toa_to_tob.exe, convert from TOA5 to TOB1 file(s)
        pathI: input path
        fileI: file name TOA5
        pathO: output path
        fileO: file name of the TOB1 """
    pathI = Path(pathI).joinpath(fileI)
    pathO = Path(pathO).joinpath(fileO)
    programPath = Path(programPath).joinpath('toa_to_tob1.exe')
    command = f'{programPath} {pathI} {pathO}'
    print(command)
    os.system(command)


def readFirstLine(pathFile, log=None):
    _log = False
    if log is not None and isinstance(log, Log.Log):
        _log = True
    try:
        f = pathFile.open('rb')
        return str(f.readline())
    except (IOError, UnicodeDecodeError):
        msg = f'[ConverterCambellsciData::readFirstLine]: No such file or error decoding. {pathFile}'
        if _log:
            log.error(msg)
        else:
            print(msg)
        return False


def checkTOBfile(pathFile, log=None):
    """ Check if the file is TOB file """
    line = readFirstLine(pathFile, log)
    if not line:
        return False
    try:
        if 'TOB' in line[0:10]:
            return True
        else:
            return False
    except (IndexError, TypeError):
        return False


def checkTOAfile(pathFile, log=None):
    line = readFirstLine(pathFile, log)
    if not line:
        return False
    try:
        if 'TOA' in line[0:10]:
            return True
        else:
            return False
    except (IndexError, TypeError):
        return False


def checkIfAllAreTOB(path):
    listDir = glob.glob(path)
    for item in listDir:
        print(item)
        if not checkTOBfile(item):
            print('The file "' + item + '", is not TOB file.')
            # sys.exit(3)
            os.rename(item, item + '.noTOB')


def checkIfAllAreTOA(path):
    listDir = glob.glob(path)
    for item in listDir:
        if not checkTOAfile(item):
            print('The file "' + item + '" is not TOA file.')
            # sys.exit(3)
            os.rename(item, item + '.noTOA')


def convertAll4TOA2TOB(filesToConv, pathTOA, pathTOB):  # TODO: About add more tables MAYBE NOT NEEDED
    """ convert all the files of the EddyTower.
        fileToConv: "flux"|"soil"|"met"|"ts"|"ECTM"
        pathTOA: path where are the .TOA (ASCII) files
        pathTOB: path were will be the .dat files """
    extensionTOB = ".dat"
    if filesToConv.lower() == "flux":
        files2do = '*flux*.*'
    elif filesToConv.lower() == "soil":
        files2do = '*Soil*.*'
    elif filesToConv.lower() == "met":
        files2do = '*met*.*'
    elif filesToConv.lower() == "ts":
        files2do = '*ts*.*'
    elif filesToConv.lower() == "ectm":
        files2do = '*ECTM*.*'
    else:
        print("Error: no valid option")
        return
    pathTOA = Path(pathTOA)
    checkIfAllAreTOA(str(pathTOA.joinpath(files2do)))
    # print pathTOA
    # print files2do
    lista = LibDataTransfer.getListOfFiles(str(pathTOA), files2do)
    # print lista
    for item in lista:
        fileO = LibDataTransfer.getNameExtension(item)[0]
        print(fileO)
        convertTOA2TOB(pathTOA, item, pathTOB, fileO + extensionTOB)


def convertAnyTOA2TOB(pathIn, pathOut):
    """ Convert any file with any extension from TOA to TOB """
    lista = LibDataTransfer.getListOfFiles(pathIn, '*.*')
    # print lista
    for item in lista:
        fileO = LibDataTransfer.getNameExtension(item)[0]
        print(fileO)
        convertTOA2TOB(pathIn, item, pathOut, fileO + '.DAT')


def fixSpaceFileName(path):
    """ Rename files with whitespace. change the space by "_" """
    lista = LibDataTransfer.getListOfFiles(path, '*.*')
    # print lista
    counter = 0
    for item in lista:
        if os.path.isfile(os.path.join(path, item)):
            fixedName = item.replace(' ', '_')
            print('"' + item + '"', '"' + fixedName + '"')
            if item != fixedName:
                if os.path.exists(os.path.join(path, fixedName)):
                    fixedName = item.replace(' ', '__')
                os.rename(os.path.join(path, item), os.path.join(path, fixedName))
                counter += 1
    print(counter, ' files renamed!')


def getOutAnyTableName(item):  # TODO: About add more tables  MAYBE NOT NEEDED *******
    """ from string erase any table name """
    item = item.replace('flux', '')
    item = item.replace('met_data', '')
    item = item.replace('Soil', '')
    item = item.replace('ts_data', '')
    return item


def checkAndFixFileNameWithTableName(path):  # TODO: About add more tables  MAYBE NOT NEEDED
    """ Rename files with the correct table name
    Only for tables ts, flux, soil, and met """
    lista = LibDataTransfer.getListOfFiles(path, '*.*')
    # print lista
    counter = 0
    for item in lista:
        if os.path.isfile(os.path.join(path, item)):
            print(item)
            f = open(os.path.join(path, item))
            line = f.read(500)
            f.close()
            if 'flux' in line:
                if not 'flux' in item:
                    fileName = 'flux_' + getOutAnyTableName(item)
                    os.rename(os.path.join(path, item), os.path.join(path, fileName))
                    counter += 1
            elif 'met_data' in line:
                if not 'met_data' in item:
                    fileName = 'met_data_' + getOutAnyTableName(item)
                    os.rename(os.path.join(path, item), os.path.join(path, fileName))
                    counter += 1
            elif 'Soil' in line:
                if not 'Soil' in item:
                    fileName = 'Soil_' + getOutAnyTableName(item)
                    os.rename(os.path.join(path, item), os.path.join(path, fileName))
                    counter += 1
            elif 'ECTM' in line:
                if not 'ECTM' in item:
                    fileName = 'ECTM_' + getOutAnyTableName(item)
                    os.rename(os.path.join(path, item), os.path.join(path, fileName))
                    counter += 1
            elif 'ts_data' in line:
                if not 'ts_data' in item:
                    fileName = 'ts_data_' + getOutAnyTableName(item)
                    os.rename(os.path.join(path, item), os.path.join(path, fileName))
                    counter += 1
            else:
                os.rename(os.path.join(path, item), os.path.join(path, item + 'INCORRECT'))

    print(counter, ' files renamed!')


def getMetadataTOxFile(toxPath):
    """ Get the metadat of the TOA or TOB File
     it reads the first line of the file and return a dictionary with the metadata """

    toxPath = Path(toxPath)
    fp = toxPath.open('rb')
    line = fp.readline().decode('ascii').strip()
    line.strip()
