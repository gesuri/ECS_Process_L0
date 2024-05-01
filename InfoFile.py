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

# when working with static tables, the new data will be appended to the end of the L1 file, so if the same data is
# added, there will be duplicated data.

# TODO: add the way to create plots every time new data is added

# Proposed but not really needed for now
# *TODO but not needed X: check when static table a float that has no decimal print one zero decimal. 0.0 should be 0
# *TODO but not needed 1: for RedLake the Biomet data has multiple values that are int but pandas take it as float.
#    option 1: leave it as it
#    option 2: change the type of the column to string and use float_format(x, 1) to write the file
# *TODO but not needed 2: for RedLake the BiometConstants table has a different format for the microseconds in the
#   timestamp
#    option 1: leave it as it
#    option 2: add a new configuration on config.py for table where we can change the number of decimals to keep
# *TODO but not needed 3: for RedLake the BiometConstants some of the float format. the original is using E-notation
#   and write files are not
#    example: 2745E+09 to 3274500000.0
# *TODO but not needed 4: for RedLake the VariableChecks table has ints and pandas take it as float. The output are 0.0
#   values
# TODO 6 DONE: change f_datalogger to project. Also, check where consts.ECS_NAME is used and change it to project but
#   not needed



from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import time

# import matplotlib.pyplot as plt

import consts
import config
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
    f_project = None  # datalogger name from file name
    f_tableName = None  # table name from file name
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
    _cleanDF_ = False
    rename = True  # option to rename the file to process, good for debugging
    staticTable = False  # flag to indicate if the table is static or not
    metaTable = None  # metadata of the table from consts

    def __init__(self, pathFileName, cleanDataFrame=True, rename=True):
        self.statusFile = consts.STATUS_FILE.copy()
        self._cleanDF_ = cleanDataFrame
        start_time = time.time()
        self.log = Log.Log(path=consts.PATH_GENERAL_LOGS.joinpath('InfoFile.log'))
        if not isinstance(pathFileName, Path):
            self.pathFile = Path(pathFileName)
        else:
            self.pathFile = pathFileName
        self.pathFile = self.pathFile.resolve()
        self.rename = rename
        # check if file is a L1 file
        if self.pathFile.suffix.lower() == '.csv':
            self.level = 1
            self.rename = False
        if self.pathFile.exists():  # check if the file/table exists
            # check file is TOB | TOA, if TOB then convert it to TOA
            paths = LibDataTransfer.checkAndConvertFile(
                self.pathFile, rename=self.rename, log=self.log)
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
        if self.pathTOA.suffix.lower() == '.csv':  # check and set the file level
            self.level = 1
        ## get the metadata from the file name
        # get the name of the file
        fileName = self.pathTOA.stem
        # get the extension of the file
        self.f_ext = self.pathTOA.suffix
        # split the name of the file
        fileNameSplit = fileName.split('_')
        # get the date and time of the file
        self.f_site = fileNameSplit[consts.CS_FILE_NAME_SITE]
        self.f_site_r = consts.SITE_4_FILE.get(self.f_site, self.f_site)
        self.f_project = fileNameSplit[consts.CS_FILE_NAME_DATALOGGER]
        currentYear = datetime.now().year
        if len(fileNameSplit) > 3:
            self.f_tableName = fileName[len(self.f_site) + len(self.f_project) + 2:-(len('_'.join(fileNameSplit[-2:])) + 1)]
        else:
            self.f_tableName = fileNameSplit[consts.CS_FILE_NAME_TABLE]
        logName = f'{self.f_site}_{self.f_project}_{self.f_tableName}_{currentYear}.log'
        # get the tentative path for the log file
        if consts.FILE_STRUCTURE_VERSION == 1:
            self.pathLog = consts.PATH_CLOUD.joinpath(self.f_site_r, consts.ECS_NAME, 'logs', logName)
        elif consts.FILE_STRUCTURE_VERSION == 2:
            self.pathLog = consts.PATH_CLOUD.joinpath(self.f_site_r, self.f_project, 'logs', logName)
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
        ## get the metadata from the actual file
        _meta_ = LibDataTransfer.getHeaderFLlineFile(self.pathTOA, self.log)
        self.cs_headers = _meta_['headers']
        self.colNames = LibDataTransfer.getStrippedHeaderLine(self.cs_headers[consts.CS_FILE_HEADER_LINE['FIELDS']])
        self.firstLineDT = _meta_['firstLineDT']
        self.lastLineDT = _meta_['lastLineDT']

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
        self.metaTable = config.getTable(self.cs_tableName)
        self.frequency = self.metaTable[config.FREQUENCY]  # 'frequency']
        if self.frequency == consts.FREQ_10HZ:
            self.timestampFormat = LibDataTransfer.datetime_format_HF  # to be used with df.index.map(timestampFormat)
            self.hf = True
        elif self.frequency == consts.FREQ_STATIC:
            self.timestampFormat = lambda x: LibDataTransfer.datetime_format(x, 3)  # to be used with df.index.map(timestampFormat)
            self.hf = False
        else:
            self.timestampFormat = None  # no need to uses df.index.map(timestampFormat)
            self.hf = False
        self.st_fq = self.metaTable[config.L1_FILE_FREQUENCY]
        self.pathLog = self.pathLog.parent.parent.joinpath(self.cs_tableName, 'logs', logName)
        if self.metaTable[config.CLASS] == consts.CLASS_STATIC:
            self._cleanDF_ = False
            self.staticTable = True
        if not (self.staticTable) and _meta_['lineNumCols'] != _meta_['headerNumCols']:
            self.log.error(f'{self.pathTOA.name} has different number of columns in the header and the first line. '
                           f'The number of columns in the header is {_meta_["headerNumCols"]} and in '
                           f'the first line is {_meta_["lineNumCols"]}. This file is going to be renamed and avoided')
            nf = LibDataTransfer.renameAFileWithDate(self.pathTOA, log=self.log)
            LibDataTransfer.moveAfileWOOW(nf, nf.parent.joinpath(f'{nf.name}.avoided'))
            self.statusFile[consts.STATUS_FILE_MISSMATCH_COLUMNS] = True
            self.terminate()
            return
        self.numberColumns = _meta_['lineNumCols']
        # get the number of lines of the file
        if 'TOA' in self.cs_type:
            self.numberLines = systemTools.rawincount(self.pathTOA) - len(consts.CS_FILE_HEADER_LINE) + 1
        self._setL0paths_()

        # get the actual data from the file
        self.genDataFrame()

    # except Exception as e:
    #    exc_type, exc_obj, exc_tb = sys.exc_info()
    #    self.log.error(f'Exception in getInfoFileName: {e}, line {exc_tb.tb_lineno}\n{sys.exc_info()}')
    #    self.statusFile[consts.STATUS_FILE_EXCEPTION_ERROR] = True
    #    self.terminate()

    def _setL0paths_(self, version=consts.FILE_STRUCTURE_VERSION):
        # file paths
        year = self.f_creationDT.strftime('%Y')
        month = self.f_creationDT.strftime('%m')
        day = self.f_creationDT.strftime('%d')
        self.st_tableName = self.metaTable[config.L1_FILE_NAME]
        fcreatioDT = self.f_creationDT.strftime(consts.TIMESTAMP_FORMAT_FILES)

        folderName = self.metaTable[config.L1_FOLDER_NAME]
        project = self.f_project
        filename = f'{self.f_site_r}_{project}_{self.st_tableName}_{consts.L0}_{fcreatioDT}'
        filenameTOA = f'{filename}.{consts.ST_EXT_TOA}'
        filenameTOB = f'{filename}.{consts.ST_EXT_TOB}'


        if version == 1:
            basePath = consts.PATH_CLOUD.joinpath(self.f_site_r, consts.ECS_NAME, folderName, year, 'Raw_Data')
            self.pathL0TOA = basePath.joinpath(consts.ST_NAME_TOA, month, day, filenameTOA)
            if self.metaTable[config.SAVE_L0_TOB]:
                self.pathL0TOB = basePath.joinpath(consts.ST_NAME_TOB, month, day, filenameTOB)
            else:
                self.pathL0TOB = None
        elif version == 2:
            basePath = consts.PATH_CLOUD.joinpath(self.f_site_r, project, consts.L0, folderName, year, month, day)
            self.pathL0TOA = basePath.joinpath(filenameTOA)
            if self.metaTable[config.SAVE_L0_TOB]:
                self.pathL0TOB = basePath.joinpath(filenameTOB)
            else:
                self.pathL0TOB = None

    def _setL1paths_(self, version=consts.FILE_STRUCTURE_VERSION):
        if self.df is None or len(self.df) == 0:
            self.log.warn(f'No dataframe available, please run genDataFrame()')
            return
        try:
            self.firstLineDT = self.df.index[0]
            self.lastLineDT = self.df.index[-1]
        except Exception as e:
            self.log.error(f'in _setL1paths_ when try to read from df the first and last line, looks like no data {e}')
            self.log.error(f'{self.df}')
            return
        filenameCSV = []
        self.pathL1 = []
        tableName = self.metaTable[config.L1_FILE_NAME]
        folderName = self.metaTable[config.L1_FOLDER_NAME]
        project = self.f_project
        # file name for yearly data to store
        if self.st_fq == consts.FREQ_YEARLY:
            years = range(self.firstLineDT.year, self.lastLineDT.year + 1)
            if version == 1:
                for year in years:
                    filenameCSV.append([f'{self.f_site_r}_{project}_{tableName}_{consts.L1}_{year}.csv', year])
            elif version == 2:
                for year in years:
                    filenameCSV.append([f'{self.f_site_r}_{project}_{tableName}_{consts.L1}_{year}.csv', year])
        # file name for high frequency data to store, daily
        elif self.st_fq == consts.FREQ_DAILY:
            fdt = self.firstLineDT.replace(hour=0, minute=0, second=0, microsecond=0)
            ldt = self.lastLineDT.replace(hour=23, minute=59)
            days = range((ldt - fdt).days + 1)
            if version == 1:
                for item in days:
                    dtItem = fdt + timedelta(days=item)
                    dtItemStr = dtItem.strftime(consts.TIMESTAMP_FORMAT_DAILY)
                    filenameCSV.append([f'{self.f_site_r}_{project}_{tableName}_{consts.L1}_{dtItemStr}.csv',
                                        dtItem.year])
            elif version == 2:
                for item in days:
                    dtItem = fdt + timedelta(days=item)
                    dtItemStr = dtItem.strftime(consts.TIMESTAMP_FORMAT_DAILY)
                    filenameCSV.append([f'{self.f_site_r}_{project}_{tableName}_{consts.L1}_{dtItemStr}.csv',
                                        dtItem.year])
        # path data structure
        if version == 1:
            basePath = consts.PATH_CLOUD.joinpath(self.f_site_r, consts.ECS_NAME, folderName)
            for item in filenameCSV:
                self.pathL1.append(basePath.joinpath(str(item[1]), 'Raw_Data', 'ASCII', item[0]))
        elif version == 2:
            basePath = consts.PATH_CLOUD.joinpath(self.f_site_r, project, consts.L1, folderName)
            if self.st_fq == consts.FREQ_YEARLY:
                for item in filenameCSV:
                    self.pathL1.append(basePath.joinpath(item[0]))
            elif self.st_fq == consts.FREQ_DAILY:
                for item in filenameCSV:
                    self.pathL1.append(basePath.joinpath(str(item[1]), item[0]))

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
                if item == consts.STATUS_FILE_OK:
                    continue
                msg += f'{item}, '
        self.log.live(f'Terminating {self.pathFile.stem} with status those flags: {msg[:-2]}')

    def genDataFrame(self):
        self.log.live(f'Generating DataFrame for {self.pathFile.stem}')
        start_time = time.time()
        # check if the file to read is a static table, if not use this one, else use the other one
        if self.staticTable:
            self.df = pd.read_csv(self.pathTOA, header=None, skiprows=len(consts.CS_FILE_HEADER_LINE) - 1, index_col=0,
                                  keep_default_na=False, names=self.colNames, parse_dates=True, date_format='mixed')
        else:
            self.df = pd.read_csv(self.pathTOA, header=None, skiprows=len(consts.CS_FILE_HEADER_LINE) - 1, index_col=0,
                                  na_values=[-99999, "NAN"], names=self.colNames, parse_dates=True, date_format='mixed')
        self._cleaned_ = False
        self.setFragmentation()  # set the fragmentation of the file and set the frequency
        if self._cleanDF_:
            self.cleanDataFrame()  # clean the dataframe by removing the flagged data
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
            if self.frequency != -1:
                self.frequency = self.fragmentation.index[0]
            self.setStorageFrequency()

    def setStorageFrequency(self, freq=None):
        if freq:
            self.st_fq = freq
        elif self.metaTable[config.L1_FILE_FREQUENCY] != consts.DEFAULT_L1_FILE_FREQUENCY:
            self.st_fq = self.metaTable[config.L1_FILE_FREQUENCY]
        elif self.frequency:
            if isinstance(self.frequency, pd.Timedelta) and self.frequency < consts.FREQ_1MIN:
                self.st_fq = consts.FREQ_DAILY
            else:
                self.st_fq = consts.FREQ_YEARLY
        return self.st_fq

    def cleanDataFrame(self):
        if self.df is None or self.df is False:
            self.log.warn(f'No dataframe available, please run genDataFrame()')
            return
        self.log.live(f'Cleaning DataFrame for {self.pathFile.stem}')
        start_time = time.time()
        ddf = LibDataTransfer.fuseDataFrame(self.df, freq=self.frequency, group=None, log=self.log)
        self.df = ddf.pop(None)
        self._cleaned_ = True
        if not self.staticTable:
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
