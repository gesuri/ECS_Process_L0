# -------------------------------------------------------------------------------
# Name:        Const file for EddyCovarinceSystem Process for L0 Data
# Purpose:     Have constants and some configuration for all the scripts on this system
#
# Version:     0.1
#
# Author:      Gesuri Ramirez
#
# Created:     06/13/2023
# Copyright:   (c) Gesuri 2023
# Licence:     Apache 2.0
# -------------------------------------------------------------------------------

# Current version of data structure
# SharePoint/Data/[SITE]/TOWER/[TABLE]/[YEAR]/Raw_Data/[bin|ASCII|RAWbin]
# Upgraded version of data structure
# SharePoint/Data/[SITE]/ECS/[LEVEL]/[TABLE]/

# info about CS tables:
# https://help.campbellsci.com/GRANITE9-10/Content/shared/Details/Data/About_Data_Tables.htm?TocPath=Working%20with%20data%7C_____4

"""
### Module: `consts`

#### Purpose:
This module provides a collection of constants and configurations used across scripts in the Eddy Covariance System
(ECS) data processing pipeline. It defines paths, file structure versions, time formats, logging configurations, and
file handling rules for different stages (L0, L1) of data processing. The constants are primarily used to handle data
from Campbell Scientific dataloggers.

---

### General Constants:

- **`dev (bool)`**:
  - **Purpose**: Flag to indicate if the system is in development (`True`) or production (`False`).
  - **Example**: Set to `True` for development environments, `False` for production.

---

### Site Constants:

- **`_SITES_ (list)`**:
  - **Purpose**: List of site names that are processed by the system.
  - **Example**: `['Pecan5R', 'RedLake', 'Kimberly', 'Bahada']`.

- **`SITE_4_FILE (dict)`**:
  - **Purpose**: Maps the site names used in filenames to the actual site names.
  - **Example**: `{'Pecan5R': 'Pecan5R', 'RedLake': 'RedLake', ...}`.

- **`ECS_NAME (str)`**:
  - **Purpose**: The folder name for Eddy Covariance system files.
  - **Default**: `'Tower'`.

---

### Paths:

- **`PATH_HARVESTED_DATA (Path)`**:
  - **Purpose**: Path where LoggerNet saves collected data.
  - **Example**: `'C:/temp/Collected/'` (development) or `'E:/temp/Collected/'` (production).

- **`PATH_TEMP_BACKUP (Path)`**:
  - **Purpose**: Temporary backup folder for data before moving to permanent storage.
  - **Example**: `'C:/LatestData/'` (development) or `'E:/LatestData/'` (production).

- **`PATH_CLOUD (Path)`**:
  - **Purpose**: Path to permanent storage (SharePoint).
  - **Example**: `'C:/Data/'` (development) or `'E:/Data/'` (production).

- **`PATH_GENERAL_LOGS (Path)`**:
  - **Purpose**: Directory where general logs are stored.
  - **Default**: `PATH_CLOUD.joinpath('Logs')`.

- **`PATH_CHECK_FILES (Path)`**:
  - **Purpose**: Directory where files that need review are stored (unprocessed or problematic files).
  - **Default**: `PATH_HARVESTED_DATA.joinpath('CheckFiles')`.

- **`PATH_FILES_NOT_UPLOADED (Path)`**:
  - **Purpose**: Directory where files that failed to upload are stored.
  - **Default**: `PATH_HARVESTED_DATA.joinpath('NotUploaded')`.

---

### File Structure and Metadata:

- **`CS_FILE_METADATA (dict)`**:
  - **Purpose**: Mapping of metadata fields for Campbell Scientific files (e.g., file type, station name).
  - **Example**: `{'type': 0, 'stationName': 1, 'model': 2, ...}`.

- **`CS_FILE_HEADER_LINE (dict)`**:
  - **Purpose**: Metadata fields for file headers (e.g., hardware info, fields, units).
  - **Example**: `{'HARDWARE': 0, 'FIELDS': 1, ...}`.

- **`CS_FILE_NAME_SITE (int)`**:
  - **Purpose**: The position of the site name in the file name.
  - **Default**: `0`.

- **`CS_FILE_NAME_DATALOGGER (int)`**:
  - **Purpose**: The position of the datalogger name in the file name.
  - **Default**: `1`.

- **`CS_FILE_NAME_TABLE (int)`**:
  - **Purpose**: The position of the table name in the file name.
  - **Default**: `2`.

- **`FILE_STRUCTURE_VERSION (int)`**:
  - **Purpose**: Specifies the file structure version (1 for old, 2 for new).
  - **Default**: `2`.

---

### Data Levels:

- **`L0 (str)`**:
  - **Purpose**: Constant for Level 0 data (raw data).
  - **Default**: `'L0'`.

- **`L1 (str)`**:
  - **Purpose**: Constant for Level 1 data (processed data).
  - **Default**: `'L1'`.

---

### Time and Date Formats:

- **`SECONDS_TZ (int)`**:
  - **Purpose**: Time zone offset in seconds (Mountain Time Zone).
  - **Default**: `25200` (7 hours).

- **Timestamp Formats**:
  - `TIMESTAMP_FORMAT`: `'Ymd_HMS'` (for file names).
  - `TIMESTAMP_FORMAT_FILES`: `'Ymd_HMS'` (for file storage).
  - `TIMESTAMP_FORMAT_CS_LINE_HF`: `'Y-m-d H:M:S.f'` (high-frequency data format).
  - `TIMESTAMP_FORMAT_CS_LINE`: `'Y-m-d H:M:S'` (standard timestamp).
  - `TIMESTAMP_FORMAT_DAILY`: `'Ymd_0000'` (daily format).
  - `TIMESTAMP_FORMAT_YEARLY`: `'Y'` (yearly format).

---

### Frequency Constants:

- **`FREQ_YEARLY`**: `'Y'` (pandas yearly frequency).
- **`FREQ_DAILY`**: `'D'` (pandas daily frequency).
- **`FREQ_30MIN`**: 30-minute interval as a `Timedelta`.
- **`FREQ_2HZ`**: 0.5-second interval as a `Timedelta` (2 Hz frequency).
- **`FREQ_1MIN`**: 1-minute interval as a `Timedelta`.
- **`FREQ_10HZ`**: 0.1-second interval as a `Timedelta` (10 Hz frequency).

---

### File Extensions and Status:

- **File Extensions**:
  - `ST_NAME_TOA`: `'bin'` (folder for TOA files).
  - `ST_NAME_TOB`: `'RAWbin'` (folder for TOB files).
  - `ST_EXT_TOA`: `'TOA'` (extension for TOA files).
  - `ST_EXT_TOB`: `'DAT'` (extension for TOB files).

- **File Status Constants**:
  - `STATUS_FILE_NOT_EXIST`: File does not exist.
  - `STATUS_FILE_OK`: File is okay.
  - `STATUS_FILE_EMPTY`: File is empty.
  - `STATUS_FILE_NOT_HEADER`: File is missing a header.
  - `STATUS_FILE_MISSMATCH_COLUMNS`: File has mismatched columns.
  - `STATUS_FILE_EXCEPTION_ERROR`: File has an exception error.
  - `STATUS_FILE_UNKNOWN_FORMAT`: File has an unknown format.
  - `STATUS_FILE_NOT_READABLE`: File is not readable.

---

### General Data Handling:

- **`CS_DATA_DICT (dict)`**:
  - **Purpose**: Template dictionary to store metadata and file paths for Campbell Scientific files.
  - **Fields**: Includes fields like `extension`, `site`, `fileNameDT`, `headers`, `path`, `statusFile`, etc.

- **`MIN_PCT_DATA (float)`**:
  - **Purpose**: Minimum percentage of data required to consider the data valid.
  - **Default**: `0.1` (10%).

- **`TIME_REMOVE_TEMP_BACKUP (timedelta)`**:
  - **Purpose**: Time limit after which files are removed from the temporary backup.
  - **Default**: `7 days`.

- **`FLAG (int)`**:
  - **Purpose**: The flag value used for missing data.
  - **Default**: `-9999`.

- **`CLASS_STATIC`**: Indicates a static table.
- **`CLASS_DYNAMIC`**: Indicates a dynamic table.

- **`DEFAULT_L1_NAME_POSTFIX (str)`**:
  - **Purpose**: Default name postfix for L1 files (yearly format).
  - **Default**: `TIMESTAMP_FORMAT_YEARLY`.

- **`DEFAULT_FREQUENCY`**: Default table frequency (set to `FREQ_30MIN`).
- **`DEFAULT_SAVE_L0_TOB (bool)`**: Whether to save L0 TOB files.
- **`DEFAULT_ARCHIVE_AFTER (timedelta)`**:
  - **Purpose**: Default time after which files are archived.
  - **Default**: `2 years`.

---

### Usage:
The constants provided in this module are used throughout various data processing scripts to maintain consistency in
file handling, time formatting, logging, and managing the structure of the Eddy Covariance System. The file provides
flexibility for both development and production environments.

"""




import datetime
from pathlib import Path
import pandas as pd


dev = True  # True if it is a development version, False if it is a production version

_SITES_ = ['Pecan5R', 'RedLake', 'Kimberly', 'Bahada']  # Sites that are processed
SITE_4_FILE = {
    'Pecan5R': 'Pecan5R',
    'RedLake': 'RedLake',
    'Kimberly': 'Kimberly',
    'Bahada': 'Bahada',
}  # Site name for the file name
ECS_NAME = 'Tower'  # Name of the Eddy Covariance system folder

if not dev:  # production version
    # Paths
    PATH_HARVESTED_DATA = Path(r'E:/temp/Collected/')  # Path(r'E:/LoggerNet/')  # Where LoggerNet save the data
    PATH_TEMP_BACKUP = Path(r'E:/LatestData')  # Where the data is moved to temporary backup
    PATH_CLOUD = Path(r'E:/Data/')  # Where the data is moved to permanent storage
    PATH_TEMPSHARE = Path(r'E:/TempShare/')  # data stored temporarily, data to be processed
else:  # development version
    # Paths
    PATH_HARVESTED_DATA = Path(r'C:/temp/Collected/')  # Where LoggerNet save the data
    PATH_TEMP_BACKUP = Path(r'C:/LatestData')  # Where the data is moved to temporary backup
    PATH_CLOUD = Path(r'C:/Data/')  # SharePoint/Data/
    PATH_TEMPSHARE = Path(r'C:/TempShare/')  # data stored temporarily, data to be processed

PATH_GENERAL_LOGS = PATH_CLOUD.joinpath('Logs')  # Where the logs are saved
# Where the files that are not processed for some reason are saved
PATH_CHECK_FILES = PATH_HARVESTED_DATA.joinpath('CheckFiles')
PATH_FILES_NOT_UPLOADED = PATH_HARVESTED_DATA.joinpath('NotUploaded')  # Where the files that are not uploaded are saved
TOB2PROG = Path(__file__).parent.resolve().joinpath('Programs')

# Campbell Scientific files, Meta data info
# header metadata first line info position
CS_FILE_METADATA = {
    'type': 0, 'stationName': 1, 'model': 2, 'serialNumber': 3, 'os': 4, 'program': 5,
    'signature': 6, 'tableName': 7}

# lines info
CS_FILE_HEADER_LINE = {
    'HARDWARE': 0,  # info (metadata) of the datalogger and table names
    'FIELDS': 1,  # name of each field or column
    'UNITS': 2,  # units per field
    'PROC': 3,  # Data processing abbreviations
    'TYPE': 4,  # Only for TOB1, type of data stored
}

# Data LoggerNet file info
CS_FILE_NAME_SITE = 0  # File name info: site name
CS_FILE_NAME_DATALOGGER = 1  # File name info: datalogger name
CS_FILE_NAME_TABLE = 2  # File name info: table name

# File structure version
FILE_STRUCTURE_VERSION = 2  # version of the file structure. 1 for old version, 2 for new version

# Abbrebiation for data level
L0 = 'L0'  # Level 0 data
L1 = 'L1'  # Level 1 data

# time constants
SECONDS_TZ = 60 * 60 * 7  # seconds in 7 hours for Mountain Time Zone

# time format
TIMESTAMP_FORMAT = '%Y%m%d_%H%M%S'
TIMESTAMP_FORMAT_FILES = '%Y%m%d_%H%M%S'
TIMESTAMP_FORMAT_CS_LINE_HF = '%Y-%m-%d %H:%M:%S.%f'
TIMESTAMP_FORMAT_CS_LINE = '%Y-%m-%d %H:%M:%S'
TIMESTAMP_FORMAT_DAILY = '%Y%m%d_0000'
TIMESTAMP_FORMAT_YEARLY = '%Y'

# table file storage frequency
FREQ_YEARLY = 'Y'  # yearly frequency. in pandas freq=Y
FREQ_DAILY = 'D'  # daily frequency. in pandas freq='D'
FREQ_30MIN = pd.Timedelta(minutes=30)  # '30min'  # 30 min frequency in pandas freq='30T' or freq='30min'
FREQ_2HZ = pd.Timedelta(seconds=0.5)  # '500L'  # 2 Hz frequency. in pandas freq='500L'
FREQ_1MIN = pd.Timedelta(minutes=1)  # '1min'  # 1 min frequency. in pandas freq='T' or freq='min'
FREQ_10HZ = pd.Timedelta(seconds=0.1)  # '100L'  # 10 Hz frequency. in pandas freq='100L'

ST_NAME_TOA = 'bin'  # name of the folder where the TOA files are stored
ST_NAME_TOB = 'RAWbin'  # name of the folder where the TOB files are stored
ST_EXT_TOA = 'TOA'  # extension of the TOA files
ST_EXT_TOB = 'DAT'  # extension of the TOB files

STATUS_FILE_NOT_EXIST = 'The file does not exist'
STATUS_FILE_OK = 'OK'
STATUS_FILE_EMPTY = 'The file is empty'
STATUS_FILE_NOT_HEADER = 'The file does not have header'
STATUS_FILE_MISSMATCH_COLUMNS = 'The file does not have the same number of columns in header and data'
STATUS_FILE_EXCEPTION_ERROR = 'The file has an exception error'
STATUS_FILE_UNKNOWN_FORMAT = 'The file has an unknown format'
STATUS_FILE_NOT_READABLE = 'The file is not readable'
STATUS_FILE = {
    STATUS_FILE_NOT_EXIST: False,
    STATUS_FILE_OK: False,
    STATUS_FILE_EMPTY: False,
    STATUS_FILE_NOT_HEADER: False,
    STATUS_FILE_MISSMATCH_COLUMNS: False,
    STATUS_FILE_EXCEPTION_ERROR: False,
    STATUS_FILE_UNKNOWN_FORMAT: False,
    STATUS_FILE_NOT_READABLE: False,
}

CS_DATA_DICT = {
    'extension': '',  # files extension
    'site': '',  # site name from file name
    'datalogger': '',  # datalogger name from file name
    'fileNameDT': None,  # datetime object from file name
    'creationDT': None,  # datetime object from file creation
    'numberlines': 0,  # number of lines in the file if it is TOA5 or ASCII
    'headers': None,  # headers of the file
    'tableName': '',  # table name from file header
    'type': '',  # type of data from file header
    'stationName': '',  # datalogger name from file header
    'model': '',  # datalogger model from file header
    'serialNumber': '',  # datalogger serial number from file header
    'os': '',  # datalogger OS from file header
    'program': '',  # datalogger program name from file header
    'signature': '',  # datalogger program signature from file header
    'tableStorage': '',  # storage path where the table will be saved
    'path': None,  # path of the current file
    'pathShared': None,  # path of the current file in the shared folder
    'pathBackup': None,  # path of the current file in the backup folder
    'statusFile': STATUS_FILE_NOT_EXIST,  # The file is OK
    'pathLog': None,  # path for the log
    'log': None,  # log object
    'firstLineDT': None,  # datetime object from first line of the file
    'lastLineDT': None,  # datetime object from last line of the file
    'frequency': '',  # frequency of the table
    }

YEAR_ = '-YYYY-'  # string to replace the year

MIN_PCT_DATA = 0.1  # minimum percentage of data to be considered valid
TIME_REMOVE_TEMP_BACKUP = datetime.timedelta(days=7)  # time to remove the files from the temporary backup

FLAG = -9999  # flag for missing data
CLASS_STATIC = 'static'  # static table
CLASS_DYNAMIC = 'dynamic'  # dynamic table
DEFAULT_L1_NAME_POSTFIX = TIMESTAMP_FORMAT_YEARLY  # default name postfix for L1 files
DEFAULT_FREQUENCY = FREQ_30MIN  # default frequency
DEFAULT_L1_FILE_FREQUENCY = -1  # FREQ_YEARLY  # default frequency for L1 files
DEFAULT_CLASS = CLASS_DYNAMIC  # default class
DEFAULT_COLS_2_PLOT = []  # default columns to plot
FREQ_STATIC = -1  # default frequency for static tables
DEFAULT_SAVE_L0_TOB = True  # default value for saveL0TOB
DEFAULT_ARCHIVE_AFTER = datetime.timedelta(days=2*365)  # default value for archiveAfter
DEFAULT_NAN_VALUE = FLAG  # default value for nanValue
