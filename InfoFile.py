# -------------------------------------------------------------------------------
# Name:        InfoFile
# Purpose:     Class to get all the information of a CS file and maybe in the future for other kind of files
#
#   This Python class, InfoFile, is designed to manage and process information from a file, particularly related to
#       environmental monitoring or data logging systems. The class is responsible for extracting metadata and data from
#       files, cleaning the data, resampling it if needed, and setting up paths for storing the processed data. Below is
#       a summary of its key features:
#
#   Attributes:
#       It stores file-related metadata (e.g., file extension, site name, creation date, size).
#       It extracts metadata from the file header (e.g., datalogger type, serial number, program signature).
#       The class also holds paths for storing processed data (TOA5, TOB1 formats, resampled data).
#       It manages the status of files through statusFile, tracking whether the file exists, is empty, has mismatched
#       columns, etc.
#       Data is read into a Pandas DataFrame and may be cleaned or resampled, depending on the configuration.
#
#   Initialization:
#       The class constructor (__init__) accepts a file path, and it begins by checking the file type and converting it
#       if necessary (e.g., from TOB1 to TOA5 format).
#       It extracts the necessary metadata from the file name and header and sets paths for logs and processed files.
#       If resampling is required, the class handles that automatically.
#
#   Methods:
#       getInfo(): Extracts metadata from the file name and header, and logs issues if any are found.
#       _setL0paths_(), _setL1paths_(): Set up storage paths for raw (L0) and processed (L1) data based on metadata like
#       the date, project, and site.
#       genDataFrame(): Reads the file into a Pandas DataFrame, handles cleaning, and sets up fragmentation for large
#       files.
#       checkData(): Groups data by day and checks for missing data, logging the percentage of missing records.
#       setFragmentation(): Calculates fragmentation in the data file, useful for data integrity checks.
#
#   Logging:
#       The class uses a Log object to log errors, warnings, and performance metrics (e.g., time taken to process a
#       file, missing data).
#
#   Error Handling:
#       The class checks for missing files, empty files, mismatched column numbers, and other issues, terminating the
#       process if problems are detected.
#
#  In essence, this class automates the process of managing large datasets from environmental monitoring systems,
#  ensuring data is cleaned, processed, and stored correctly while keeping detailed logs of the process.
#
# Author:      Gesuri
#
# Created:     31/07/2023
# Copyright:   (c) Gesuri 2023
# Licence:     Apache 2.0
# -------------------------------------------------------------------------------

# NOTE: when working with static tables, the new data will be appended to the end of the L1 file, so if the same data is
#       added, there will be duplicated data.

# PROPOSE:
#   add the way to create plots every time new data is added
#   check when static table a float that has no decimal print one zero decimal. 0.0 should be 0
#   for RedLake the Biomet data has multiple values that are int but pandas take it as float.
#       option 1: leave it as it
#       option 2: change the type of the column to string and use float_format(x, 1) to write the file
#   for RedLake the BiometConstants table has a different format for the microseconds in the timestamp
#       option 1: leave it as it
#       option 2: add a new configuration on config.py for table where we can change the number of decimals to keep
#   for RedLake the BiometConstants some of the float format. the original is using E-notation and write files are not
#       example: 2745E+09 to 3274500000.0
#   for RedLake the VariableChecks table has ints and pandas take it as float. The output are 0.0 values


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
    """
    A class to manage and process environmental monitoring or data logging system files.

    The InfoFile class is responsible for:
    - Extracting metadata from file headers and filenames.
    - Handling file type conversions (e.g., TOB1 to TOA5).
    - Processing, cleaning, and resampling data stored in the files.
    - Setting up file paths for storing raw and processed data.
    - Logging any issues during file processing.
    - Checking and reporting data completeness, such as missing or fragmented data.

    Attributes
    ----------
    path_file : pathlib.Path
        The path to the file being processed.
    path_log : pathlib.Path
        Path to the directory where log files are stored.
    ext : str
        The file extension (e.g., '.dat', '.csv').
    site : str
        The site name or identifier extracted from the file.
    date_created : str
        The creation date of the file extracted from its metadata.
    file_size : float
        The size of the file in bytes.
    datalogger_type : str
        The type of datalogger used for the file, extracted from the file header.
    serial_number : str
        The serial number of the datalogger used.
    program_signature : str
        The signature of the datalogger program used, extracted from the header.
    station_name : str
        The name of the station where data was logged, extracted from metadata.
    path_L0 : pathlib.Path
        Path to store the raw L0 data (before processing).
    path_L1 : pathlib.Path
        Path to store the processed L1 data.
    path_L1_resampled : pathlib.Path
        Path to store resampled L1 data, if applicable.
    path_L1_fragments : pathlib.Path
        Path to store any fragmented L1 data.
    resampling : bool
        Whether the file should be resampled.
    data : pandas.DataFrame
        The data extracted from the file and stored in a DataFrame.
    statusFile : dict
        A dictionary that keeps track of file status (exists, empty, mismatched columns, etc.).

    Methods
    -------
    __init__(self, path_file, path_log):
        Initializes the InfoFile class by checking the file type, extracting metadata, and setting file paths.

    getInfo(self):
        Extracts metadata from the filename and file header, including datalogger type, serial number, program
        signature, and file creation date.

    _setL0paths_(self):
        Sets up paths to store raw (L0) data based on file metadata (e.g., site, date).

    _setL1paths_(self):
        Sets up paths to store processed (L1) data, including resampled and fragmented data if necessary.

    genDataFrame(self):
        Reads the file into a Pandas DataFrame, cleans the data, and handles large file fragmentation. Logs any issues
        found during data processing.

    checkData(self):
        Groups data by day, checks for missing data, and logs the percentage of missing records.

    setFragmentation(self):
        Analyzes the data file for fragmentation, useful for ensuring data integrity.

    Logging
    -------
    The class uses a logging mechanism to record:
    - Errors or issues related to missing files, empty files, or incorrect column numbers.
    - The time taken to process the file.
    - Missing or fragmented data, if applicable.

    Usage Example
    -------------
    >>> file_processor = InfoFile(path_file='/path/to/file.dat', path_log='/path/to/logs')
    >>> file_processor.getInfo()
    >>> file_processor.genDataFrame()
    >>> file_processor.checkData()
    >>> file_processor.setFragmentation()
    """
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
    pathL1Resample = []  # path for the resampled data
    statusFile = None  # The file is OK
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
    resample = False  # if string, then it is the frequency of the resample

    def __init__(self, pathFileName, cleanDataFrame=True, rename=True):
        """
        Initializes the InfoFile class with the given file path and optional parameters.

        Args:
            pathFileName (str or Path): Path to the file to be processed.
            cleanDataFrame (bool): Whether to clean the data after loading into a DataFrame (default: True).
            rename (bool): Whether to rename the file during processing (default: True).

        Initializes class attributes such as file path, log, and metadata. It also checks for file existence
        and converts file format if necessary (e.g., TOB to TOA).
        """
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

        # check if the file/table exists
        if self.pathFile.exists():
            # check file is TOB | TOA, if TOB then convert it to TOA
            paths = LibDataTransfer.checkAndConvertFile(self.pathFile, rename=self.rename, log=self.log)

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
        self.metaTable = config.getTable(self.cs_tableName)  # get the metadata of the table
        self.getInfo()  # get the information from the file

        # if resample required, resample the data
        if self.metaTable[config.RESAMPLE]:
            self.resample = self.metaTable[config.RESAMPLE]

        end_time = time.time()
        self.log.live(f'InfoFile created in {end_time - start_time:.2f} seconds')

    def getInfo(self):
        """
        Extracts and processes metadata from the file's name and header.

        This method identifies the type of data logger used, the station name, table name, and other key metadata
        from the file name and header. It also logs issues like missing headers or mismatched columns.

        If the file is not found or has critical errors, the method terminates early and logs the issue.
        """
        # Check flags if the file exists and is readable
        if self.statusFile[consts.STATUS_FILE_NOT_EXIST] or self.pathTOA is None:
            return

        # try:

        # check and set the file level
        if self.pathTOA.suffix.lower() == '.csv':
            self.level = 1

        # get the metadata from the file name
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
            self.f_tableName = fileName[
                               len(self.f_site) + len(self.f_project) + 2:-(len('_'.join(fileNameSplit[-2:])) + 1)]
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

        # get the metadata from the actual file
        _meta_ = LibDataTransfer.getHeaderFLlineFile(self.pathTOA, self.log)
        self.cs_headers = _meta_['headers']
        self.colNames = LibDataTransfer.getStrippedHeaderLine(self.cs_headers[consts.CS_FILE_HEADER_LINE['FIELDS']])
        self.firstLineDT = _meta_['firstLineDT']
        self.lastLineDT = _meta_['lastLineDT']

        # if the file is empty or has no headers, log the issue and terminate
        if len(self.cs_headers) == 0:
            self.log.error(f'{self.pathFile} ({self.pathTOA.name}) has no headers or it is empty')
            self.statusFile[consts.STATUS_FILE_NOT_HEADER] = True
            self.terminate()
            return

        # get the metadata from the file header
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

        # set frequency or if static table
        if self.frequency == consts.FREQ_10HZ:
            self.timestampFormat = LibDataTransfer.datetime_format_HF  # to be used with df.index.map(timestampFormat)
            self.hf = True
        elif self.frequency == consts.FREQ_STATIC:
            # to be used with df.index.map(timestampFormat)
            self.timestampFormat = lambda x: LibDataTransfer.datetime_format(x, 3)
            self.hf = False
        else:
            self.timestampFormat = None  # no need to uses df.index.map(timestampFormat)
            self.hf = False

        self.st_fq = self.metaTable[config.L1_FILE_FREQUENCY]
        self.pathLog = self.pathLog.parent.parent.joinpath(self.cs_tableName, 'logs', logName)

        # check if the table is static
        if self.metaTable[config.CLASS] == consts.CLASS_STATIC:
            self._cleanDF_ = False
            self.staticTable = True

        # check if the number of columns in the header and the first line are the same, if not, terminate
        if not self.staticTable and _meta_['lineNumCols'] != _meta_['headerNumCols']:
            self.log.error(f'{self.pathTOA.name} has different number of columns in the header and the first line. '
                           f'The number of columns in the header is {_meta_["headerNumCols"]} and in '
                           f'the first line is {_meta_["lineNumCols"]}. This file is going to be renamed and avoided')
            nf = LibDataTransfer.renameAFileWithDate(self.pathTOA, log=self.log)
            LibDataTransfer.moveAfileWOOW(nf, nf.parent.joinpath(f'{nf.name}.avoided'))
            self.statusFile[consts.STATUS_FILE_MISSMATCH_COLUMNS] = True
            self.terminate()
            return

        # get the number of columns of the file
        self.numberColumns = _meta_['lineNumCols']

        # get the number of lines of the file
        if 'TOA' in self.cs_type:
            self.numberLines = systemTools.rawincount(self.pathTOA) - len(consts.CS_FILE_HEADER_LINE) + 1

        # get the L0 paths
        self._setL0paths_()

        # get the actual data from the file
        self.genDataFrame()

    # except Exception as e:
    #    exc_type, exc_obj, exc_tb = sys.exc_info()
    #    self.log.error(f'Exception in getInfoFileName: {e}, line {exc_tb.tb_lineno}\n{sys.exc_info()}')
    #    self.statusFile[consts.STATUS_FILE_EXCEPTION_ERROR] = True
    #    self.terminate()

    def _setL0paths_(self, version=consts.FILE_STRUCTURE_VERSION):
        """
        Sets the paths for L1 (processed data) storage, including paths for resampled and fragmented data.

        Args:
            version (int): The version of the file structure being used to determine folder structure (default: 2).

        This method determines the storage paths for processed (L1) data, generating paths for both the main
        L1 data and any resampled versions, based on the file's metadata and timestamps.
        """
        # get time from file
        year = self.f_creationDT.strftime('%Y')
        month = self.f_creationDT.strftime('%m')
        day = self.f_creationDT.strftime('%d')

        # set the table name from the metaTable in config.py
        self.st_tableName = self.metaTable[config.L1_FILE_NAME]

        # set the timestamp for the file creation
        fcreatioDT = self.f_creationDT.strftime(consts.TIMESTAMP_FORMAT_FILES)

        folderName = self.metaTable[config.L1_FOLDER_NAME]
        project = self.f_project
        filename = f'{self.f_site_r}_{project}_{self.st_tableName}_{consts.L0}_{fcreatioDT}'
        filenameTOA = f'{filename}.{consts.ST_EXT_TOA}'
        filenameTOB = f'{filename}.{consts.ST_EXT_TOB}'

        # set the path for the L0 data based on the version of the file structure
        if version == 1:  # old file structure version, not anymore used
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

    def _setL1paths_(self):
        """
        Sets the paths for L1 (processed data) storage, including paths for resampled and fragmented data.

        This method determines the storage paths for processed (L1) data, generating paths for both the main
        L1 data and any resampled versions, based on the file's metadata and timestamps.
        """

        # check if the dataframe is empty, if yes, return
        if self.df is None or len(self.df) == 0:
            self.log.warn(f'No dataframe available, please run genDataFrame()')
            return

        # set the first and last line of the dataframe
        try:
            self.firstLineDT = self.df.index[0]
            self.lastLineDT = self.df.index[-1]
        except Exception as e:
            self.log.error(f'in _setL1paths_ when try to read from df the first and last line, looks like no data {e}')
            self.log.error(f'{self.df}')
            return

        filenameCSV = []
        filenameCSV_res = []
        self.pathL1 = []
        self.pathL1Resample = []

        # get metadata from config.py
        tableName = self.metaTable[config.L1_FILE_NAME]
        folderName = self.metaTable[config.L1_FOLDER_NAME]
        project = self.f_project

        # file name for yearly data to store
        if self.st_fq == consts.FREQ_YEARLY:
            years = range(self.firstLineDT.year, self.lastLineDT.year + 1)
            for year in years:
                filenameCSV.append([f'{self.f_site_r}_{project}_{tableName}_{consts.L1}_{year}.csv', year])
                filenameCSV_res.append([f'{self.f_site_r}_{project}_{tableName}_{consts.L1}_{year}_1min.csv', year])

        # file name for high frequency data to store, daily
        elif self.st_fq == consts.FREQ_DAILY:
            fdt = self.firstLineDT.replace(hour=0, minute=0, second=0, microsecond=0)
            ldt = self.lastLineDT.replace(hour=23, minute=59)
            days = range((ldt - fdt).days + 1)
            for item in days:
                dtItem = fdt + timedelta(days=item)
                dtItemStr = dtItem.strftime(consts.TIMESTAMP_FORMAT_DAILY)
                filenameCSV.append([f'{self.f_site_r}_{project}_{tableName}_{consts.L1}_{dtItemStr}.csv', dtItem.year])
                filenameCSV_res.append([f'{self.f_site_r}_{project}_{tableName}_{consts.L1}_{dtItemStr}_1min.csv',
                                        dtItem.year])

        # path data structure
        basePath = consts.PATH_CLOUD.joinpath(self.f_site_r, project, consts.L1, folderName)
        if self.st_fq == consts.FREQ_YEARLY:
            for item in filenameCSV:
                self.pathL1.append(basePath.joinpath(item[0]))
        elif self.st_fq == consts.FREQ_DAILY:
            for item in filenameCSV:
                self.pathL1.append(basePath.joinpath(str(item[1]), item[0]))
        for item in filenameCSV_res:
            self.pathL1Resample.append(basePath.joinpath(f'{item[1]}_1min', item[0]))

    def genDataFrame(self):
        """
        Loads the file data into a pandas DataFrame and processes the data.

        This method reads the file's data into a DataFrame, applying appropriate parsing options based on the
        file type (e.g., TOA5 format, handling flagged data, and fragmentation).

        It cleans the data if necessary, generates fragmentation details, and sets the paths for L1 data storage.
        Logs the time taken to process the file and any errors encountered.
        """
        self.log.live(f'Generating DataFrame for {self.pathFile.stem}')
        start_time = time.time()

        # check if the file to read is a static table, if not use this one, else use the other one
        # then read the TOA file
        if self.staticTable:
            self.df = pd.read_csv(self.pathTOA, header=None, skiprows=len(consts.CS_FILE_HEADER_LINE) - 1, index_col=0,
                                  keep_default_na=False, names=self.colNames, parse_dates=True, date_format='mixed')
        else:
            self.df = pd.read_csv(self.pathTOA, header=None, skiprows=len(consts.CS_FILE_HEADER_LINE) - 1, index_col=0,
                                  na_values=[consts.FLAG, "NAN"], names=self.colNames, parse_dates=True,
                                  date_format='mixed')
        self._cleaned_ = False

        # set the fragmentation of the file and set the frequency
        self.setFragmentation()

        # clean the dataframe by removing the flagged data
        if self._cleanDF_:
            self.cleanDataFrame()

        # set the paths for the L1 data
        self._setL1paths_()

        end_time = time.time()
        self.log.live(f'DataFrame generated in {end_time - start_time:.2f} seconds from a '
                      f'{systemTools.sizeof_fmt(self.f_size)} file')

    def checkData(self):
        """
        Checks for missing data by grouping the data per day and calculating the percentage of missing records.

        This method checks each day’s data in the DataFrame and logs the percentage of missing records. It calculates
        the total expected records per day based on the file's frequency and number of columns.
        """
        name: datetime

        # clean dataframe if not cleaned
        if self._cleaned_ is False:
            self.cleanDataFrame()
        totalRecordsPerDay = pd.Timedelta(days=1) / self.frequency * (self.numberColumns - 1)
        for name, group in self.df.groupby(pd.Grouper(freq='D')):
            missing = group.isna().sum().sum()
            if missing > (self.numberColumns - 1):
                self.log.info(
                    f'On {name.strftime("%Y-%m-%d")} were {missing} missing records ({missing / totalRecordsPerDay * 100:.2f}%)')

    def setFragmentation(self):
        """
        Analyzes the file's data to determine fragmentation and sets the appropriate storage frequency.

        This method checks the DataFrame for fragmented data and determines the file's frequency. It then updates
        the `st_fq` attribute with the correct storage frequency based on the data fragmentation.
        """
        if self.df is None:
            self.log.warn(f'No dataframe available, please run genDataFrame()')
            return
        self.fragmentation = LibDataTransfer.getFragmentation4DF(self.df)

        if self.fragmentation is not None:
            self.fragmentation.index.rename('fragmentation', inplace=True)
            if self.frequency != -1 and len(self.fragmentation) > 0:
                self.frequency = self.fragmentation.index[0]
            self.setStorageFrequency()

    def setStorageFrequency(self, freq=None):
        """
        Sets the storage frequency for the data file based on the file’s frequency or a provided frequency.

        Args:
            freq (str or pd.Timedelta): Optional frequency to set for storage. If not provided, the method uses the
                                        file’s existing frequency or a default frequency.

        Returns:
            str: The storage frequency of the data.
        """
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
        """
        Cleans the DataFrame by removing flagged or invalid data.

        This method cleans the loaded DataFrame, removing rows with flagged data (e.g., missing or corrupt values).
        After cleaning, it checks the data for completeness and logs the time taken to clean the file.
        """
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
        """
        Checks if the file is in a good state based on its status flags.

        Returns:
            bool: True if the file is OK and no issues were flagged, False otherwise.

        This method toggles the status flag `STATUS_FILE_OK` and checks if any other status flags indicate a problem
        (e.g., missing data, mismatched columns). It restores the `STATUS_FILE_OK` flag before returning.
        """
        self.statusFile[consts.STATUS_FILE_OK] = not self.statusFile[consts.STATUS_FILE_OK]
        r = all(not self.statusFile.get(item, False) for item in consts.STATUS_FILE.keys())
        self.statusFile[consts.STATUS_FILE_OK] = not self.statusFile[consts.STATUS_FILE_OK]
        return r

    def print(self, returnDict=False):
        """
        Prints the current attributes and values of the InfoFile instance.

        Args:
            returnDict (bool): If True, returns a dictionary of attribute-value pairs instead of printing them
            (default: False).

        Returns:
            dict: A dictionary of attributes and their values if `returnDict` is True.
        """
        di = {}
        for item in self.__dir__():
            if not item.startswith('__'):
                di[item] = self.__getattribute__(item)
                if not returnDict:
                    print(f'{item}: {self.__getattribute__(item)}')
        if returnDict:
            return di

    def printPaths(self):
        """
        Prints all attributes related to file paths in the InfoFile instance.

        This method filters and prints only the attributes that contain the word "path", which usually refer to
        various file paths handled by the class.
        """
        for item in self.__dir__():
            if 'path' in item:
                print(f'{item}: {self.__getattribute__(item)}')

    def terminate(self):
        """
        Terminates the current file processing and logs any active status flags.

        This method logs all active status flags for the file and terminates further processing if any issues
        are encountered (e.g., missing file, mismatched columns).
        """
        msg = ''
        for item in self.statusFile:
            if self.statusFile[item]:
                if item == consts.STATUS_FILE_OK:
                    continue
                msg += f'{item}, '
        self.log.live(f'Terminating {self.pathFile.stem} with status those flags: {msg[:-2]}')

    def __str__(self):
        return f'{self.pathFile} ({self.pathTOA.name})'

    def __repr__(self):
        if self.pathFile is None:
            return 'No file'
        elif self.pathTOA is None:
            return f'{self.pathFile} (Not available L1 file)'
        else:
            return f'{self.pathFile} ({self.pathTOA.name})'


if __name__ == '__main__':
    p = Path(r'data/CR3000_flux_20220707_030000.TOA')
    info = InfoFile(p)
