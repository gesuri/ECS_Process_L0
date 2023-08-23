# -------------------------------------------------------------------------------
# Name:        InfoFile
# Purpose:     Class to get all the information of a CS file and maybe in the future for other kind of files
#
#
# Author:      Gesuri
#
# Created:     31/07/2023
# Copyright:   (c) Gesuri 2023
# Licence:     Apache 2.0
# -------------------------------------------------------------------------------

from pathlib import Path
from datetime import datetime, timedelta

import consts
import systemTools
from LibDataTransfer import getHeaderFLlineFile, getStrippedHeaderLine, checkAndConvertFile
import Log


class InfoFile:
    """ Class to get all the information of a CS file and maybe in the future for other kind of files
    attributes that start with f_ means that the information is from the file name
    attributes that start with cs_ means that the information is from the header of the file """
    f_ext = None  # files extension
    f_site = None  # site name from file name
    f_site_r = None  # reals site name from file name, got from consts.SITES_4_FILE
    f_datalogger = None  # datalogger name from file name
    f_nameDT = None  # datetime object from file name
    f_creationDT = None  # datetime object from file creation
    numberLines = None  # number of lines in the file if it is TOA5 or ASCII
    cs_headers = None  # headers of the file
    cs_tableName = None  # table name from file header
    cs_type = None  # type of data from file header
    cs_stationName = None  # datalogger name from file header
    cs_model = None  # datalogger model from file header
    cs_serialNumber = None  # datalogger serial number from file header
    cs_os = None  # datalogger OS from file header
    cs_program = None  # datalogger program name from file header
    cs_signature = None  # datalogger program signature from file header
    st_tableName = None  # table name on storage
    pathL1 = []  # storage paths of the files needed. the files are in the cloud
    pathFile = None  # path of the current file
    pathL0TOA = None  # storage path where the table will be saved in TOA5 format
    pathTOA = None  # path of the current file in TOA5 format
    pathL0TOB = None  # storage path where the table will be saved in TOB1 format
    pathTOB = None  # path of the current file in TOB1 format
    statusFile = consts.STATUS_FILE_NOT_EXIST  # The file is OK
    pathLog = None  # path for the log
    log = None  # log object
    firstLineDT = None  # datetime object from first line of the file
    lastLineDT = None  # datetime object from last line of the file
    frequency = None  # frequency of the table
    st_fq = None  # frequency of the table on storage
    level = 0  # level of the file

    def __init__(self, pathFileName):
        if not isinstance(pathFileName, Path):
            self.pathFile = Path(pathFileName)
        else:
            self.pathFile = pathFileName
        if self.pathFile.exists():
            paths = checkAndConvertFile(self.pathFile)
            if paths['path'] is not None:
                self.pathFile = paths['path']
                self.pathTOA = paths['toaPath']
                self.pathTOB = paths['tobPath']
            else:
                self.statusFile = consts.STATUS_FILE_NOT_EXIST
        self.getInfo()

    def getInfo(self):
        try:
            # get the name of the file
            fileName = self.pathFile.stem
            # get the extension of the file
            self.f_ext = self.pathFile.suffix
            # split the name of the file
            fileNameSplit = fileName.split('_')
            # get the date and time of the file
            self.f_site = fileNameSplit[consts.CS_FILE_NAME_SITE]
            self.f_site_r = consts.SITE_4_FILE.get(self.f_site, self.f_site)
            self.f_datalogger = fileNameSplit[consts.CS_FILE_NAME_DATALOGGER]
            self.pathLog = consts.PATH_CLOUD.joinpath(self.f_site_r, consts.ECS_NAME, 'logs', 'log.txt')
            self.log = Log.Log(path=self.pathLog)
            # check if the fileNameSplit has more than 3 elements and if yes, get the date and time
            if len(fileNameSplit) > 3:
                self.f_nameDT = systemTools.getDT4Str(fileName[-15:])
            # get the creation date and time of the file
            if self.pathFile.exists():
                self.f_creationDT = datetime.fromtimestamp(self.pathFile.stat().st_ctime)
                if self.pathFile.stat().st_size > 1:
                    self.statusFile = consts.STATUS_FILE_OK
                else:
                    self.statusFile = consts.STATUS_FILE_EMPTY
            if self.statusFile == consts.STATUS_FILE_OK:
                _meta_ = getHeaderFLlineFile(self.pathFile, self.log)
                self.cs_headers = _meta_['headers']
                self.firstLineDT = _meta_['firstLineDT']
                self.lastLineDT = _meta_['lastLineDT']
                if len(self.cs_headers) == 0:
                    self.log.error(f'Error: {self.pathFile} has no headers or it is empty')
                    return
                nl = getStrippedHeaderLine(self.cs_headers[0])
                self.cs_type = nl[consts.CS_FILE_METADATA['type']].replace('"', '')
                self.cs_stationName = nl[consts.CS_FILE_METADATA['stationName']].replace('"', '')
                self.cs_model = nl[consts.CS_FILE_METADATA['model']].replace('"', '')
                self.cs_serialNumber = nl[consts.CS_FILE_METADATA['serialNumber']].replace('"', '')
                self.cs_os = nl[consts.CS_FILE_METADATA['os']].replace('"', '')
                self.cs_program = nl[consts.CS_FILE_METADATA['program']].replace('"', '')
                self.cs_signature = nl[consts.CS_FILE_METADATA['signature']].replace('"', '')
                self.cs_tableName = nl[consts.CS_FILE_METADATA['tableName']].replace('"', '')
                self.frequency = consts.TABLES_SPECIFIC_FREQUENCY.get(self.cs_tableName, '')
                self.st_fq = consts.TABLES_STORAGE_FREQUENCY.get(self.cs_tableName, consts.FREQ_YEARLY)
                self.pathLog = self.pathLog.parent.parent.joinpath(self.cs_tableName, 'logs', 'log.txt')
                year = str(self.f_creationDT.year)
                month = str(self.f_creationDT.month)
                day = str(self.f_creationDT.day)
                filenameTSformat = "%Y%m%d_%H%M%S"
                # get the number of lines of the file
                if 'TOA' in self.cs_type:
                    self.numberLines = systemTools.rawincount(self.pathFile)
                # file paths
                self.st_tableName = consts.TABLES_STORAGE_NAME.get(self.cs_tableName, self.cs_tableName)
                fcreatioDT = self.f_creationDT.strftime(filenameTSformat)
                filename = f'{self.f_site}_{self.f_datalogger}_{self.st_tableName}_{fcreatioDT}_L{self.level}'
                filenameTOA = f'{filename}.TOA'
                filenameTOB = f'{filename}.DAT'
                basePath = consts.PATH_CLOUD.joinpath(self.f_site, consts.ECS_NAME, self.st_tableName, year, 'Raw_Data')
                self.pathL0TOA = basePath.joinpath('bin', month, day, filenameTOA)
                self.pathL0TOB = basePath.joinpath('RAWbin', month, day, filenameTOB)
                # below here is for the new data structure
                # *********
                # basePath = consts.PATH_CLOUD.joinpath(self.f_site, consts.ECS_NAME, 'L0', self.st_tableName)
                # self.pathL0TOA = basePath.joinpath(year, month, day, filenameTOA)
                # self.pathL0TOB = basePath.joinpath(year, month, day, filenameTOB)
                # *********
                filenameCSV = []
                if self.st_fq == consts.FREQ_YEARLY:
                    for item in range(self.firstLineDT.year, self.lastLineDT.year + 1):
                        filenameCSV.append([f'dataL1_{self.st_tableName}_{item}.csv', item])
                elif self.st_fq == consts.FREQ_DAILY:
                    dtStrF = consts.TABLES_NAME_FORMAT.get(self.cs_tableName, '%Y%m%d')
                    ldt = self.lastLineDT.replace(hour=0, minute=0, second=0, microsecond=0)
                    fdt = self.firstLineDT.replace(hour=23, minute=59)
                    dtDiff = ldt - fdt
                    for item in range(dtDiff.days + 1):
                        dtItem = fdt + timedelta(days=item)
                        filenameCSV.append([f'dataL1_{self.st_tableName}_{dtItem.strftime(dtStrF)}_0000.csv',
                                            dtItem.year])
                # path data structure
                basePath = consts.PATH_CLOUD.joinpath(self.f_site, consts.ECS_NAME, self.st_tableName)
                for item in filenameCSV:
                    self.pathL1.append(basePath.joinpath(item[1], 'Raw_Data', 'ASCII', item[0]))
                    # below here is for the new data structure
                    # *********
                    # if self.st_fq == consts.FREQ_YEARLY:
                    #     folderFq = str(item[1])
                    # else:
                    #     folderFq = ''
                    # self.pathL1.append(basePath.joinpath('L1', self.st_tableName, folderFq, item[0]))
                    # *********

        except Exception as e:
            print(f'Error in getInfoFileName: {e}')

# TODO:
#  set correct path for the pathLog
#  set log object
#  remove backup path
#  create frequency correctly
#  check if this class works
