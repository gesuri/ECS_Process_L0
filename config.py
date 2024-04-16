import consts
import LibDataTransfer
from pandas import DateOffset

DEFAULT = 'default'
L1_FOLDER_NAME = 'l1FolderName'
L1_FILE_NAME = 'l1FileName'
L1_NAME_POSTFIX = 'l1NamePostfix'
FREQUENCY = 'frequency'
L1_FILE_FREQUENCY = 'l1FileFrequency'
CLASS = 'class'
SAVE_L0_TOB = 'saveL0TOB'
ARCHIVE_AFTER = 'archiveAfter'
NAN_VALUE = 'nanValue'
INDEX_MAP_FUNC = 'indexMapFunc'
COLS_2_PLOT = 'cols2Plot'
TIME_2_PLOT = 'time2Plot'
PROJECT = 'project'


# Here is the definition of the tables. If you don't know what tables are of if there is a new table, the system will
# try to guess the best configuration for the table. If you know the table, you can add the table here and the system
TABLES = {
    # DEFAULT. Here is listed the default values for the tables and a quick description
    DEFAULT: {
        # folder name for the L1 table, eg. fo Flux table CLOUD\Bahada\ECS\Flux\2020\Raw_Data\ASCII
        L1_FOLDER_NAME: DEFAULT,  # actually this is the name of the table that will be used as folder name
        # file name for the L1 table, eg. for Flux dataL1_Flux_2020.csv
        L1_FILE_NAME: DEFAULT,
        # postfix on the name, like the year for yearly tables
        L1_NAME_POSTFIX: consts.DEFAULT_L1_NAME_POSTFIX,
        # frequency of the table, eg. consts.FREQ_30MIN for 30minutes frequency
        FREQUENCY: consts.DEFAULT_FREQUENCY,
        # in storage each files frequency, eg. consts.FREQ_YEARLY for yearly files
        L1_FILE_FREQUENCY: consts.DEFAULT_L1_FILE_FREQUENCY,
        # what kind of table is, static or dynamic
        CLASS: consts.DEFAULT_CLASS,
        # boolean to save the L0 TOB file, binanry data
        SAVE_L0_TOB: consts.DEFAULT_SAVE_L0_TOB,
        # archive the file after this time in datetime.timedelta. eg. datetime.timedelta(days=2*365) for 2 years
        ARCHIVE_AFTER: consts.DEFAULT_ARCHIVE_AFTER,
        # what will be the NaN value, the dafault here is -9999
        NAN_VALUE: consts.DEFAULT_NAN_VALUE,
        # function to map the index of the table, eg. LibDataTransfer.datetime_format_HF for hourly frequency
        INDEX_MAP_FUNC: None,
        # column names to plot, eg. ["panel_temp_Avg", "batt_volt_Avg"]
        COLS_2_PLOT: [],
        # time to plot, the time should be a pandas DateOffset, eg. pd.DateOffset(days=30) for 30 days
        TIME_2_PLOT: DateOffset(days=30)
        # project that the table belongs to, eg. ['ECS_AboveCanopy']
        #PROJECT: [consts.ECS_NAME],
    },
    # Bahada
    'ts_data': {
        L1_FOLDER_NAME: 'EddyCovariance_ts',
        L1_FILE_NAME: 'ts',
        L1_NAME_POSTFIX: consts.TIMESTAMP_FORMAT_DAILY,
        FREQUENCY: consts.FREQ_10HZ,
        L1_FILE_FREQUENCY: consts.FREQ_DAILY,
        #'CLASS: CLASS_DYNAMIC,
        #'saveL0TOB': DEFAULT_SAVE_L0_TOB,
        #'archiveAfter': DEFAULT_ARCHIVE_AFTER,
        #'nanValue': DEFAULT_NAN_VALUE,
        INDEX_MAP_FUNC: LibDataTransfer.datetime_format_HF,
        COLS_2_PLOT: ["CO2", "H2O", "t_hmp"],
    },
    'flux': {
        L1_FOLDER_NAME: 'Flux',
        L1_FILE_NAME: 'flux',
        #'l1NamePostfix': consts.TIMESTAMP_FORMAT_YEARLY,
        FREQUENCY: consts.FREQ_30MIN,
        #'l1FileFrequency': consts.FREQ_YEARLY,
        #'class': consts.CLASS_DYNAMIC,
        #'saveL0TOB': consts.DEFAULT_SAVE_L0_TOB,
        #'archiveAfter': consts.DEFAULT_ARCHIVE_AFTER,
        COLS_2_PLOT: ["panel_temp_Avg", "batt_volt_Avg"],
    },
    'met_data': {
        L1_FOLDER_NAME: 'TowerClimate_met',
        L1_FILE_NAME: 'met',
        #'l1NamePostfix': consts.TIMESTAMP_FORMAT_YEARLY,
        FREQUENCY: consts.FREQ_1MIN,
        #'l1FileFrequency': consts.FREQ_YEARLY,
        #'class': consts.CLASS_DYNAMIC,
        #'saveL0TOB': consts.DEFAULT_SAVE_L0_TOB,
        #'archiveAfter': consts.DEFAULT_ARCHIVE_AFTER,
        COLS_2_PLOT: ["t_hmp", "rh_hmp", "CO2_raw", "H2O_raw"],
    },
    'Soil_CS650': {
        L1_FOLDER_NAME: 'SoilSensor_CS650',
        L1_FILE_NAME: 'Soil',
        #'l1NamePostfix': consts.TIMESTAMP_FORMAT_YEARLY,
        FREQUENCY: consts.FREQ_1MIN,
        #'l1FileFrequency': consts.FREQ_YEARLY,
        #'class': consts.CLASS_DYNAMIC,
        #'saveL0TOB': consts.DEFAULT_SAVE_L0_TOB,
        #'archiveAfter': consts.DEFAULT_ARCHIVE_AFTER,
        COLS_2_PLOT: ["t_hmp", "rh_hmp", "CO2_raw", "H2O_raw"],
    },
    'ts_data_2': {
        L1_FOLDER_NAME: 'EddyCovariance_ts_2',
        L1_FILE_NAME: 'ts_2',
        L1_NAME_POSTFIX: consts.TIMESTAMP_FORMAT_DAILY,
        FREQUENCY: consts.FREQ_10HZ,
        L1_FILE_FREQUENCY: consts.FREQ_DAILY,
        #'class': consts.CLASS_DYNAMIC,
        #'saveL0TOB': consts.DEFAULT_SAVE_L0_TOB,
        #'archiveAfter': consts.DEFAULT_ARCHIVE_AFTER,
        INDEX_MAP_FUNC: LibDataTransfer.datetime_format_HF,
        COLS_2_PLOT: ["CO2", "H2O", "t_hmp"],
    },
    # Pecan5R
    'Config_Setting_Notes': {
        #'l1FolderName': 'Config_Setting_Notes',
        #'l1FileName': 'Config_Setting_Notes',
        #'l1NamePostfix': consts.TIMESTAMP_FORMAT_YEARLY,
        FREQUENCY: consts.FREQ_STATIC,
        #'l1FileFrequency': consts.FREQ_YEARLY,
        CLASS: consts.CLASS_STATIC,
        #'saveL0TOB': consts.DEFAULT_SAVE_L0_TOB,
        #'archiveAfter': consts.DEFAULT_ARCHIVE_AFTER,
        #'cols2Plot': [],
    },
    'Const_Table': {
        #'l1FolderName': 'Const_Table',
        #'l1FileName': 'Const_Table',
        #'l1NamePostfix': consts.TIMESTAMP_FORMAT_YEARLY,
        FREQUENCY: consts.FREQ_STATIC,
        #'l1FileFrequency': consts.FREQ_YEARLY,
        CLASS: consts.CLASS_STATIC,
        #'saveL0TOB': consts.DEFAULT_SAVE_L0_TOB,
        #'archiveAfter': consts.DEFAULT_ARCHIVE_AFTER,
        INDEX_MAP_FUNC: lambda x: LibDataTransfer.datetime_format(x, 3),
        #'cols2Plot': [],
    },
    'CPIStatus': {
        #'l1FolderName': 'CPIStatus',
        #'l1FileName': 'CPIStatus',
        #'l1NamePostfix': consts.TIMESTAMP_FORMAT_YEARLY,
        FREQUENCY: consts.FREQ_STATIC,
        #'l1FileFrequency': consts.FREQ_YEARLY,
        CLASS: consts.CLASS_STATIC,
        #'saveL0TOB': consts.DEFAULT_SAVE_L0_TOB,
        #'archiveAfter': consts.DEFAULT_ARCHIVE_AFTER,
        INDEX_MAP_FUNC: lambda x: LibDataTransfer.datetime_format(x, 3),
        #'cols2Plot': [],
    },
    'Diagnostic': {
        #'l1FolderName': 'Const_Table',
        #'l1FileName': 'Const_Table',
        #'l1NamePostfix': consts.TIMESTAMP_FORMAT_YEARLY,
        FREQUENCY: consts.FREQ_STATIC,
        #'l1FileFrequency': consts.FREQ_YEARLY,
        CLASS: consts.CLASS_STATIC,
        #'saveL0TOB': consts.DEFAULT_SAVE_L0_TOB,
        #'archiveAfter': consts.DEFAULT_ARCHIVE_AFTER,
        #'cols2Plot': [],
        },
    'Flux_Notes': {
        FREQUENCY: consts.FREQ_STATIC,
        CLASS: consts.CLASS_STATIC,
         },
    'Flux_AmeriFluxFormat': {
        FREQUENCY: consts.FREQ_30MIN,
    },
    'Flux_CSFormat': {
        FREQUENCY: consts.FREQ_30MIN,
    },
    'Time_Series': {
        FREQUENCY: consts.FREQ_10HZ,
        L1_NAME_POSTFIX: consts.TIMESTAMP_FORMAT_DAILY,
        L1_FILE_FREQUENCY: consts.FREQ_DAILY,
    },
    'System_Operatn_Notes': {
        FREQUENCY: consts.FREQ_STATIC,
        CLASS: consts.CLASS_STATIC,
    },
    # RedLake
    'BiometConstants': {
        FREQUENCY: consts.FREQ_STATIC,
        CLASS: consts.CLASS_STATIC,
    },
}


def getTable(table):
    """ Return the table info """
    current_table = TABLES['default'].copy()
    n_table = TABLES.get(table, TABLES['default'])
    for item in n_table:
        current_table[item] = n_table[item]
    if current_table['l1FolderName'] == 'default':
        current_table['l1FolderName'] = table
        current_table['l1FileName'] = table
    return current_table