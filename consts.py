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

_SITES_ = ['Pecan5R', 'RedLake', 'Kimberly', 'Bahada']  # Sites that are processed
SITE_4_FILE = {
    'Pecan5R': 'Pecan5R',
    'RedLake': 'RedLake',
    'Kimberly': 'Kimberly',
    'Bahada': 'Bahada',
}  # Site name for the file name
ECS_NAME = 'Tower'  # Name of the Eddy Covariance system folder

# Paths
PATH_HARVESTED_DATA = Path(r'C:/Campbellsci/LoggerNet/')  # Where LoggerNet save the data
PATH_WORKING_DATA = Path(r'C:/LatestData')  # Where the data is moved to temporary processed
#PATH_CLOUD_BASE = Path(r'C:/Users/CZO_data/OneDrive - University of Texas at El Paso/Data/')
PATH_CLOUD = Path(r'C:/TempSharedFolder/')  # SharePoint/Data/
##PATH_STORAGE = PATH_CLOUD_BASE.joinpath('storage/')  # Where the data is moved to permanent storage, SharePoint
PATH_GENERAL_LOGS = PATH_CLOUD.joinpath('Logs')  # Where the logs are saved
PATH_CHECK_FILES = PATH_HARVESTED_DATA.joinpath('CheckFiles')  # Where the files that are not processed for some reason are saved
#PATH_TEMP_SHARED_FOLDER = {}   # Where the files are shared with the other users
#for site in _SITES_:
#    PATH_TEMP_SHARED_FOLDER[site] = PATH_CLOUD_BASE.joinpath(site, 'Shared')
#PATH_BACKUP = PATH_CLOUD_BASE.joinpath('Backup')  # Where the files are backed up
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
TABLES_NAME_FORMAT = {
    'ts_data': '%Y%m%d',
    'flux': '%Y',
    'met_data': '%Y',
    'Soil_CS650': '%Y', }

# table file storage frequency
#TABLES_FREQUENCY_DAILY = ['ts']  # tables that are stored daily
#TABLES_FREQUENCY_YEARLY = ['flux', 'met_data', 'Soil_CS650']  # tables that are stored yearly
FREQ_YEARLY = 'yearly'  # yearly frequency
FREQ_DAILY = 'daily'  # daily frequency
FREQ_30MIN = '30min'  # 30 min frequency
FREQ_1MIN = '1min'  # 1 min frequency
FREQ_10HZ = '10Hz'  # 10 Hz frequency

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
    'Soil_CS650': 'SoilSensor_CS650', }

TABLES_STORAGE_NAME = {
    'ts_data': 'ts',
    'flux': 'flux',
    'met_data': 'met',
    'Soil_CS650': 'Soil', }

STATUS_FILE_NOT_EXIST = 0
STATUS_FILE_OK = 1
STATUS_FILE_EMPTY = 2

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