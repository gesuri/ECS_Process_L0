import consts
import LibDataTransfer

# Here is the definition of the tables. If you don't know what tables are of if there is a new table, the system will
# try to guess the best configuration for the table. If you know the table, you can add the table here and the system
TABLES = {
    # DEFAULT. Here is listed the default values for the tables and a quick description
    'default': {
        # folder name for the L1 table, eg. fo Flux table CLOUD\Bahada\ECS\Flux\2020\Raw_Data\ASCII
        'l1FolderName': 'default',
        # file name for the L1 table, eg. for Flux dataL1_Flux_2020.csv
        'l1FileName': 'default',
        # postfix on the name, like the year for yearly tables
        'l1NamePostfix': consts.DEFAULT_L1_NAME_POSTFIX,
        # frequency of the table, eg. consts.FREQ_30MIN for 30minutes frequency
        'frequency': consts.DEFAULT_FREQUENCY,
        # in storage each files frequency, eg. consts.FREQ_YEARLY for yearly files
        'l1FileFrequency': consts.DEFAULT_L1_FILE_FREQUENCY,
        # what kind of table is, static or dynamic
        'class': consts.DEFAULT_CLASS,
        # boolean to save the L0 TOB file, binanry data
        'saveL0TOB': consts.DEFAULT_SAVE_L0_TOB,
        # archive the file after this time in datetime.timedelta. eg. datetime.timedelta(days=2*365) for 2 years
        'archiveAfter': consts.DEFAULT_ARCHIVE_AFTER,
        # what will be the NaN value, the dafault here is -9999
        'nanValue': consts.DEFAULT_NAN_VALUE,
        # function to map the index of the table, eg. LibDataTransfer.datetime_format_HF for hourly frequency
        'indexMapFunc': None,
        # column names to plot, eg. ["panel_temp_Avg", "batt_volt_Avg"]
        'cols2Plot': [],
    },
    # Bahada
    'ts_data': {
        'l1FolderName': 'EddyCovariance_ts',
        'l1FileName': 'ts',
        'l1NamePostfix': consts.TIMESTAMP_FORMAT_DAILY,
        'frequency': consts.FREQ_10HZ,
        'l1FileFrequency': consts.FREQ_DAILY,
        #'class': CLASS_DYNAMIC,
        #'saveL0TOB': DEFAULT_SAVE_L0_TOB,
        #'archiveAfter': DEFAULT_ARCHIVE_AFTER,
        #'nanValue': DEFAULT_NAN_VALUE,
        'indexMapFunc': LibDataTransfer.datetime_format_HF,
        'cols2Plot': ["CO2", "H2O", "t_hmp"],
    },
    'flux': {
        'l1FolderName': 'Flux',
        'l1FileName': 'flux',
        #'l1NamePostfix': consts.TIMESTAMP_FORMAT_YEARLY,
        'frequency': consts.FREQ_30MIN,
        #'l1FileFrequency': consts.FREQ_YEARLY,
        #'class': consts.CLASS_DYNAMIC,
        #'saveL0TOB': consts.DEFAULT_SAVE_L0_TOB,
        #'archiveAfter': consts.DEFAULT_ARCHIVE_AFTER,
        'cols2Plot': ["panel_temp_Avg", "batt_volt_Avg"],
    },
    'met_data': {
        'l1FolderName': 'TowerClimate_met',
        'l1FileName': 'met',
        #'l1NamePostfix': consts.TIMESTAMP_FORMAT_YEARLY,
        'frequency': consts.FREQ_1MIN,
        #'l1FileFrequency': consts.FREQ_YEARLY,
        #'class': consts.CLASS_DYNAMIC,
        #'saveL0TOB': consts.DEFAULT_SAVE_L0_TOB,
        #'archiveAfter': consts.DEFAULT_ARCHIVE_AFTER,
        'cols2Plot': ["t_hmp", "rh_hmp", "CO2_raw", "H2O_raw"],
    },
    'Soil_CS650': {
        'l1FolderName': 'SoilSensor_CS650',
        'l1FileName': 'Soil',
        #'l1NamePostfix': consts.TIMESTAMP_FORMAT_YEARLY,
        'frequency': consts.FREQ_1MIN,
        #'l1FileFrequency': consts.FREQ_YEARLY,
        #'class': consts.CLASS_DYNAMIC,
        #'saveL0TOB': consts.DEFAULT_SAVE_L0_TOB,
        #'archiveAfter': consts.DEFAULT_ARCHIVE_AFTER,
        'cols2Plot': ["t_hmp", "rh_hmp", "CO2_raw", "H2O_raw"],
    },
    'ts_data_2': {
        'l1FolderName': 'EddyCovariance_ts_2',
        'l1FileName': 'ts_2',
        'l1NamePostfix': consts.TIMESTAMP_FORMAT_DAILY,
        'frequency': consts.FREQ_10HZ,
        'l1FileFrequency': consts.FREQ_DAILY,
        #'class': consts.CLASS_DYNAMIC,
        #'saveL0TOB': consts.DEFAULT_SAVE_L0_TOB,
        #'archiveAfter': consts.DEFAULT_ARCHIVE_AFTER,
        'indexMapFunc': LibDataTransfer.datetime_format_HF,
        'cols2Plot': ["CO2", "H2O", "t_hmp"],
    },
    # Pecan5R
    'Config_Setting_Notes': {
        #'l1FolderName': 'Config_Setting_Notes',
        #'l1FileName': 'Config_Setting_Notes',
        #'l1NamePostfix': consts.TIMESTAMP_FORMAT_YEARLY,
        'frequency': consts.FREQ_STATIC,
        #'l1FileFrequency': consts.FREQ_YEARLY,
        'class': consts.CLASS_STATIC,
        #'saveL0TOB': consts.DEFAULT_SAVE_L0_TOB,
        #'archiveAfter': consts.DEFAULT_ARCHIVE_AFTER,
        #'cols2Plot': [],
    },
    'Const_Table': {
        #'l1FolderName': 'Const_Table',
        #'l1FileName': 'Const_Table',
        #'l1NamePostfix': consts.TIMESTAMP_FORMAT_YEARLY,
        'frequency': consts.FREQ_STATIC,
        #'l1FileFrequency': consts.FREQ_YEARLY,
        'class': consts.CLASS_STATIC,
        #'saveL0TOB': consts.DEFAULT_SAVE_L0_TOB,
        #'archiveAfter': consts.DEFAULT_ARCHIVE_AFTER,
        'indexMapFunc': lambda x: LibDataTransfer.datetime_format(x, 3),
        #'cols2Plot': [],
    },
    'CPIStatus': {
        #'l1FolderName': 'CPIStatus',
        #'l1FileName': 'CPIStatus',
        #'l1NamePostfix': consts.TIMESTAMP_FORMAT_YEARLY,
        'frequency': consts.FREQ_STATIC,
        #'l1FileFrequency': consts.FREQ_YEARLY,
        'class': consts.CLASS_STATIC,
        #'saveL0TOB': consts.DEFAULT_SAVE_L0_TOB,
        #'archiveAfter': consts.DEFAULT_ARCHIVE_AFTER,
        'indexMapFunc': lambda x: LibDataTransfer.datetime_format(x, 3),
        #'cols2Plot': [],
    },
    'Diagnostic': {
        #'l1FolderName': 'Const_Table',
        #'l1FileName': 'Const_Table',
        #'l1NamePostfix': consts.TIMESTAMP_FORMAT_YEARLY,
        'frequency': consts.FREQ_STATIC,
        #'l1FileFrequency': consts.FREQ_YEARLY,
        'class': consts.CLASS_STATIC,
        #'saveL0TOB': consts.DEFAULT_SAVE_L0_TOB,
        #'archiveAfter': consts.DEFAULT_ARCHIVE_AFTER,
        #'cols2Plot': [],
        },
    'Flux_Notes': {
        'frequency': consts.FREQ_STATIC,
        'class': consts.CLASS_STATIC,
         },
    'Flux_AmeriFluxFormat': {
        'frequency': consts.FREQ_30MIN,
    },
    'Flux_CSFormat': {
        'frequency': consts.FREQ_30MIN,
    },
    'Time_Series': {
        'frequency': consts.FREQ_10HZ,
        'l1NamePostfix': consts.TIMESTAMP_FORMAT_DAILY,
        'l1FileFrequency': consts.FREQ_DAILY,
    },
    # RedLake
}


def getTable(table):
    """ Return the table info """
    current_table = TABLES['default']
    n_table = TABLES.get(table, TABLES['default'])
    for item in n_table:
        current_table[item] = n_table[item]
    if current_table['l1FolderName'] == 'default':
        current_table['l1FolderName'] = table
        current_table['l1FileName'] = table
    return current_table