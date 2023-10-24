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

if not dev:
    # Paths
    PATH_HARVESTED_DATA = Path(r'C:/Campbellsci/LoggerNet/')  # Where LoggerNet save the data
    PATH_WORKING_DATA = Path(r'C:/LatestData')  # Where the data is moved to temporary processed
    PATH_CLOUD = Path(r'C:/Users/CZO_data/OneDrive - University of Texas at El Paso/Data/')  # Where the data is moved to permanent storage
else:
    # Paths
    PATH_HARVESTED_DATA = Path(r'C:/temp/Collected/')  # Where LoggerNet save the data
    PATH_WORKING_DATA = Path(r'C:/temp/LatestData')  # Where the data is moved to temporary processed
    PATH_CLOUD = Path(r'C:/temp/Bahada_test')  # SharePoint/Data/

PATH_GENERAL_LOGS = PATH_CLOUD.joinpath('Logs')  # Where the logs are saved
PATH_CHECK_FILES = PATH_HARVESTED_DATA.joinpath('CheckFiles')  # Where the files that are not processed for some reason are saved
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

# time constants
SECONDS_TZ = 60 * 60 * 7  # seconds in 7 hours for Mountain Time Zone

# time format
TIMESTAMP_FORMAT = '%Y%m%d_%H%M%S'
TIMESTAMP_FORMAT_FILES = '%Y%m%d_%H%M%S'
TIMESTAMP_FORMAT_CS_LINE_HF = '%Y-%m-%d %H:%M:%S.%f'
TIMESTAMP_FORMAT_CS_LINE = '%Y-%m-%d %H:%M:%S'
TIMESTAMP_FORMAT_DAILY = '%Y%m%d_0000'
TIMESTAMP_FORMAT_YEARLY = '%Y'
TABLES_NAME_FORMAT = {
    'ts_data': TIMESTAMP_FORMAT_DAILY,
    'flux': TIMESTAMP_FORMAT_YEARLY,
    'met_data': TIMESTAMP_FORMAT_YEARLY,
    'Soil_CS650': TIMESTAMP_FORMAT_YEARLY, }

# table file storage frequency
#TABLES_FREQUENCY_DAILY = ['ts']  # tables that are stored daily
#TABLES_FREQUENCY_YEARLY = ['flux', 'met_data', 'Soil_CS650']  # tables that are stored yearly
FREQ_YEARLY = 'Y'  # yearly frequency. in pandas freq=Y
FREQ_DAILY = 'D'  # daily frequency. in pandas freq='D'
FREQ_30MIN = pd.Timedelta(minutes=30)  #'30min'  # 30 min frequency in pandas freq='30T' or freq='30min'
FREQ_1MIN = pd.Timedelta(minutes=1)  # '1min'  # 1 min frequency. in pandas freq='T' or freq='min'
FREQ_10HZ = pd.Timedelta(seconds=0.1)  # '100L'  # 10 Hz frequency. in pandas freq='100L'

# table storage path specific names
TABLES_SPECIFIC_FREQUENCY = {
    'ts_data': FREQ_10HZ,
    'flux': FREQ_30MIN,
    'met_data': FREQ_1MIN,
    'Soil_CS650': FREQ_30MIN, }

TABLES_STORAGE_FREQUENCY = {
    'ts_data': FREQ_DAILY,
    'flux': FREQ_YEARLY,
    'met_data': FREQ_YEARLY,
    'Soil_CS650': FREQ_YEARLY, }

# table storage path specific names
TABLES_STORAGE_FOLDER_NAMES = {
    'ts_data': 'EddyCovariance_ts',
    'flux': 'Flux',
    'met_data': 'TowerClimate_met',
    'Soil_CS650': 'SoilSensor_CS650',
    'ts_data_2': 'EC_ts_2', }

TABLES_STORAGE_NAME = {
    'ts_data': 'ts',
    'flux': 'flux',
    'met_data': 'met',
    'Soil_CS650': 'Soil', }

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

FLAG = -9999  # flag for missing data
MIN_PCT_DATA = 0.5  # minimum percentage of data to be considered valid
