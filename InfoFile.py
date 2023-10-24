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
import pandas as pd
import time

# import matplotlib.pyplot as plt

import consts
import systemTools
import LibDataTransfer
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
    f_size = None  # size of the file
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
    numberColumns = None  # number of columns of the file
    colNames = None  # names of the columns of the file12
    timestampFormat = None  # timestamp format of the file
    pathL1 = []  # storage paths of the files needed. the files are in the cloud
    pathFile = None  # path of the current file
    pathL0TOA = None  # storage path where the table will be saved in TOA5 format
    pathTOA = None  # path of the current file in TOA5 format
    pathL0TOB = None  # storage path where the table will be saved in TOB1 format
    pathTOB = None  # path of the current file in TOB1 format
    statusFile = None  #consts.STATUS_FILE.copy()  # The file is OK
    pathLog = None  # path for the log
    log = None  # log object
    firstLineDT = None  # datetime object from first line of the file
    lastLineDT = None  # datetime object from last line of the file
    frequency = None  # frequency of the table
    st_fq = None  # frequency of the table on storage
    level = 0  # level of the file
    df = None
    fragmentation = None
    _cleaned_ = False
    hf = False  # high frequency flag

    def __init__(self, pathFileName):
        self.statusFile = consts.STATUS_FILE.copy()
        start_time = time.time()
        self.log = Log.Log(path=consts.PATH_GENERAL_LOGS.joinpath('InfoFile.log'))
        if not isinstance(pathFileName, Path):
            self.pathFile = Path(pathFileName)
        else:
            self.pathFile = pathFileName
        self.pathFile = self.pathFile.resolve()
        rename = True
        if self.pathFile.suffix.lower() == '.csv':
            self.level = 1
            rename = False
        if self.pathFile.exists():  # check if the file/table exists
            paths = LibDataTransfer.checkAndConvertFile(
                self.pathFile, rename=rename, log=self.log)  # check if the file is TOB or TOA, if TOB then convert it
            # set the correct paths for TOB, and TOA
            if paths['path'] is not None:
                self.pathFile = Path(paths['path'])
            if paths['toaPath'] is not None:
                self.pathTOA = Path(paths['toaPath'])
            if paths['tobPath'] is not None:
                self.pathTOB = Path(paths['tobPath'])
            if paths['err'] is not None:
                self.statusFile[paths['err']] = True
        else:
            self.statusFile[consts.STATUS_FILE_NOT_EXIST] = True  # set missing file flag to statusFile
        self.getInfo()
        end_time = time.time()
        self.log.live(f'InfoFile created in {end_time - start_time:.2f} seconds')

    def getInfo(self):
        if self.statusFile[consts.STATUS_FILE_NOT_EXIST] or self.pathTOA is None:
            return
        # try:
        if self.pathTOA.suffix.lower() == '.csv':
            self.level = 1
        # get the name of the file
        fileName = self.pathTOA.stem
        # get the extension of the file
        self.f_ext = self.pathTOA.suffix
        # split the name of the file
        fileNameSplit = fileName.split('_')
        # get the date and time of the file
        self.f_site = fileNameSplit[consts.CS_FILE_NAME_SITE]
        self.f_site_r = consts.SITE_4_FILE.get(self.f_site, self.f_site)
        self.f_datalogger = fileNameSplit[consts.CS_FILE_NAME_DATALOGGER]
        # get the tentative path for the log file
        self.pathLog = consts.PATH_CLOUD.joinpath(self.f_site_r, consts.ECS_NAME, 'logs', 'log.txt')
        self.log = Log.Log(path=self.pathLog)
        # check if the fileNameSplit has more than 3 elements and if yes, get the date and time. Usually must be yes
        if len(fileNameSplit) > 3:
            self.f_nameDT = systemTools.getDT4Str(fileName[-15:])
        # get the creation date and time of the file
        if self.pathTOA.exists():
            self.f_size = self.pathTOA.stat().st_size
            self.statusFile[consts.STATUS_FILE_NOT_EXIST] = False
            self.f_creationDT = datetime.fromtimestamp(self.pathTOA.stat().st_ctime)
            if self.f_size > 1:
                self.statusFile[consts.STATUS_FILE_OK] = True
                # self.statusFile[consts.STATUS_FILE_NOT_EXIST] = False
            else:
                self.statusFile[consts.STATUS_FILE_EMPTY] = True
                self.log.error(f'{self.pathFile} ({self.pathTOA.name}) is empty')
                self.terminate()
                return
        else:
            self.statusFile[consts.STATUS_FILE_NOT_EXIST] = True
        if self.statusFile[consts.STATUS_FILE_NOT_EXIST] or not self.statusFile[consts.STATUS_FILE_OK]:
            self.log.error(f'{self.pathFile} ({self.pathTOA.name}) does not exist or there is some problem with it')
            self.terminate()
            return
        _meta_ = LibDataTransfer.getHeaderFLlineFile(self.pathTOA, self.log)
        self.cs_headers = _meta_['headers']
        self.colNames = LibDataTransfer.getStrippedHeaderLine(self.cs_headers[consts.CS_FILE_HEADER_LINE['FIELDS']])
        self.firstLineDT = _meta_['firstLineDT']
        self.lastLineDT = _meta_['lastLineDT']
        if _meta_['lineNumCols'] != _meta_['headerNumCols']:
            self.log.error(f'{self.pathTOA.name} has different number of columns in the header and in the first line. '
                           f'The number of columns in the header is {_meta_["headerNumCols"]} and in '
                           f'the first line is {_meta_["lineNumCols"]}. This file is going to be renamed and avoided')
            nf = LibDataTransfer.renameAFileWithDate(self.pathTOA, log=self.log)
            LibDataTransfer.moveAfileWOOW(nf, nf.parent.joinpath(f'{nf.name}.avoided'))
            self.statusFile[consts.STATUS_FILE_MISSMATCH_COLUMNS] = True
            self.terminate()
            return
        self.numberColumns = _meta_['lineNumCols']
        if len(self.cs_headers) == 0:
            self.log.error(f'{self.pathFile} ({self.pathTOA.name}) has no headers or it is empty')
            self.statusFile[consts.STATUS_FILE_NOT_HEADER] = True
            self.terminate()
            return
        nl = LibDataTransfer.getStrippedHeaderLine(self.cs_headers[0])
        self.cs_type = nl[consts.CS_FILE_METADATA['type']]
        self.cs_stationName = nl[consts.CS_FILE_METADATA['stationName']]
        self.cs_model = nl[consts.CS_FILE_METADATA['model']]
        self.cs_serialNumber = nl[consts.CS_FILE_METADATA['serialNumber']]
        self.cs_os = nl[consts.CS_FILE_METADATA['os']]
        self.cs_program = nl[consts.CS_FILE_METADATA['program']]
        self.cs_signature = nl[consts.CS_FILE_METADATA['signature']]
        self.cs_tableName = nl[consts.CS_FILE_METADATA['tableName']]
        self.frequency = consts.TABLES_SPECIFIC_FREQUENCY.get(self.cs_tableName, consts.FREQ_10HZ)
        if self.frequency == consts.FREQ_10HZ:
            self.timestampFormat = consts.TIMESTAMP_FORMAT_CS_LINE_HF
            self.hf = True
        else:
            self.timestampFormat = consts.TIMESTAMP_FORMAT_CS_LINE
            self.hf = False
        self.st_fq = consts.TABLES_STORAGE_FREQUENCY.get(self.cs_tableName, consts.FREQ_YEARLY)
        self.pathLog = self.pathLog.parent.parent.joinpath(self.cs_tableName, 'logs', 'log.txt')

        # get the number of lines of the file
        if 'TOA' in self.cs_type:
            self.numberLines = systemTools.rawincount(self.pathTOA) - len(consts.CS_FILE_HEADER_LINE) + 1
        self._setL0paths_()
        # self._setL1paths_()
        self.genDataFrame(clean=True)

    # except Exception as e:
    #    exc_type, exc_obj, exc_tb = sys.exc_info()
    #    self.log.error(f'Exception in getInfoFileName: {e}, line {exc_tb.tb_lineno}\n{sys.exc_info()}')
    #    self.statusFile[consts.STATUS_FILE_EXCEPTION_ERROR] = True
    #    self.terminate()

    def _setL0paths_(self):
        # file paths
        year = str(self.f_creationDT.year)
        month = str(self.f_creationDT.month)
        day = str(self.f_creationDT.day)
        self.st_tableName = consts.TABLES_STORAGE_NAME.get(self.cs_tableName, self.cs_tableName)
        fcreatioDT = self.f_creationDT.strftime(consts.TIMESTAMP_FORMAT_FILES)
        filename = f'{self.f_site_r}_{self.cs_model}_{self.st_tableName}_{fcreatioDT}_L0'
        filenameTOA = f'{filename}.{consts.ST_EXT_TOA}'
        filenameTOB = f'{filename}.{consts.ST_EXT_TOB}'
        folderName = consts.TABLES_STORAGE_FOLDER_NAMES.get(self.cs_tableName, self.cs_tableName)
        basePath = consts.PATH_CLOUD.joinpath(self.f_site_r, consts.ECS_NAME, folderName, year, 'Raw_Data')
        self.pathL0TOA = basePath.joinpath(consts.ST_NAME_TOA, month, day, filenameTOA)
        self.pathL0TOB = basePath.joinpath(consts.ST_NAME_TOB, month, day, filenameTOB)
        # below here is for the new data structure
        # *********
        # basePath = consts.PATH_CLOUD.joinpath(self.f_site, consts.ECS_NAME, 'L0', self.st_tableName)
        # self.pathL0TOA = basePath.joinpath(year, month, day, filenameTOA)
        # self.pathL0TOB = basePath.joinpath(year, month, day, filenameTOB)
        # *********

    def _setL1paths_(self):
        if self.df is None:  # TODO: check if there is at least one data in the df
            self.log.warn(f'No dataframe available, please run genDataFrame()')
            return
        try:
            self.firstLineDT = self.df.index[0]
            self.lastLineDT = self.df.index[-1]
        except Exception as e:
            self.log.error(f'in _setL1paths_ when try to read from df the first and last line {e}')
            self.log.error(f'{self.df}')
            return
        filenameCSV = []
        self.pathL1 = []
        if self.st_fq == consts.FREQ_YEARLY:
            for year in range(self.firstLineDT.year, self.lastLineDT.year + 1):
                filenameCSV.append([f'dataL1_{self.st_tableName}_{year}.csv', year])
        elif self.st_fq == consts.FREQ_DAILY:
            fdt = self.firstLineDT.replace(hour=0, minute=0, second=0, microsecond=0)
            ldt = self.lastLineDT.replace(hour=23, minute=59)
            dtDiff = ldt - fdt
            for item in range(dtDiff.days + 1):
                dtItem = fdt + timedelta(days=item)
                filenameCSV.append([f'dataL1_{self.st_tableName}_{dtItem.strftime(consts.TIMESTAMP_FORMAT_DAILY)}.csv',
                                    dtItem.year])
        # path data structure
        basePath = consts.PATH_CLOUD.joinpath(self.f_site_r, consts.ECS_NAME,
                                              consts.TABLES_STORAGE_FOLDER_NAMES.get(self.cs_tableName,
                                                                                     self.cs_tableName))
        for item in filenameCSV:
            self.pathL1.append(basePath.joinpath(str(item[1]), 'Raw_Data', 'ASCII', item[0]))
            # below here is for the new data structure
            # *********
            # if self.st_fq == consts.FREQ_YEARLY:
            #     folderFq = str(item[1])
            # else:
            #     folderFq = ''
            # self.pathL1.append(basePath.joinpath('L1', self.st_tableName, folderFq, item[0]))
            # *********
        # if self._cleaned_:
        #     group: DataFrame
        #     name: datetime
        #     for name, group in self.df.groupby(pd.Grouper(freq=self.st_fq)):
        #         group = group.dropna(thresh=(group.shape[1] - 2)*consts.MIN_PCT_DATA)
        #         if len(group) == 0:
        #             item = basePath.joinpath(str(name.year), 'Raw_Data', 'ASCII',
        #                                      f'dataL1_{self.st_tableName}_{name.strftime(dtStrF)}{hmChars}.csv')
        #             #self.log.live(f'Going to remove the file {item}')
        #             self.pathL1.remove(item)

    def __str__(self):
        return f'{self.pathFile} ({self.pathTOA.name})'

    def __repr__(self):
        return f'{self.pathFile} ({self.pathTOA.name})'

    def print(self, returnDict=False):
        di = {}
        for item in self.__dir__():
            if not item.startswith('__'):
                di[item] = self.__getattribute__(item)
                if not returnDict:
                    print(f'{item}: {self.__getattribute__(item)}')
        if returnDict:
            return di

    def printPaths(self):
        for item in self.__dir__():
            if 'path' in item:
                print(f'{item}: {self.__getattribute__(item)}')

    def terminate(self):
        msg = ''
        for item in self.statusFile:
            if self.statusFile[item]:
                msg += f'{item}, '
        self.log.live(f'Terminating {self.pathFile.stem} with status those flags: {msg[:-2]}')

    def genDataFrame(self, clean=True):
        self.log.live(f'Generating DataFrame for {self.pathFile.stem}')
        start_time = time.time()
        self.df = pd.read_csv(self.pathTOA, header=None, skiprows=len(consts.CS_FILE_HEADER_LINE) - 1, index_col=0,
                              na_values=[-99999, "NAN"], names=self.colNames, parse_dates=True, date_format='mixed')
        self._cleaned_ = False
        self.setFragmentation()
        if clean:
            self.cleanDataFrame()
        self._setL1paths_()
        end_time = time.time()
        self.log.live(f'DataFrame generated in {end_time - start_time:.2f} seconds from a '
                      f'{systemTools.sizeof_fmt(self.f_size)} file')

    def checkData(self):  # check the percentage of missing data
        """ group the data per day and says what percentage is missing data"""
        name: datetime
        if self._cleaned_ is False:
            self.cleanDataFrame()
        totalRecordsPerDay = pd.Timedelta(days=1) / self.frequency * (self.numberColumns - 1)
        for name, group in self.df.groupby(pd.Grouper(freq='D')):
            missing = group.isna().sum().sum()
            if missing > (self.numberColumns - 1):
                self.log.info(
                    f'On {name.strftime("%Y-%m-%d")} were {missing} missing records ({missing / totalRecordsPerDay * 100:.2f}%)')

    def setFragmentation(self):
        if self.df is None:
            self.log.warn(f'No dataframe available, please run genDataFrame()')
            return
        self.fragmentation = LibDataTransfer.getFragmentation4DF(self.df)
        if self.fragmentation is not None:
            self.fragmentation.index.rename('fragmentation', inplace=True)
            self.frequency = self.fragmentation.index[0]
            self.setStorageFrequency()

    def setStorageFrequency(self, freq=None):
        if freq:
            self.st_fq = freq
        elif self.frequency:
            if self.frequency < consts.FREQ_1MIN:
                self.st_fq = consts.FREQ_DAILY
            else:
                self.st_fq = consts.FREQ_YEARLY

    def cleanDataFrame(self):
        if self.df is None or self.df is False:
            self.log.warn(f'No dataframe available, please run genDataFrame()')
            return
        self.log.live(f'Cleaning DataFrame for {self.pathFile.stem}')
        start_time = time.time()
        ddf = LibDataTransfer.fuseDataFrame(self.df, freq=self.frequency, group=None, log=self.log)
        self.df = ddf.pop(None)
        self._cleaned_ = True
        self.checkData()
        end_time = time.time()
        self.log.live(f'DataFrame cleaned in {end_time - start_time:.2f} seconds')

    def ok(self):
        self.statusFile[consts.STATUS_FILE_OK] = not self.statusFile[consts.STATUS_FILE_OK]
        r = all(not self.statusFile.get(item, False) for item in consts.STATUS_FILE.keys())
        self.statusFile[consts.STATUS_FILE_OK] = not self.statusFile[consts.STATUS_FILE_OK]
        return r



if __name__ == '__main__':
    p = Path(r'data/CR3000_flux_20220707_030000.TOA')
    info = InfoFile(p)
