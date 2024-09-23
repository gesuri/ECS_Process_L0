"""
### Module: `config`

#### Purpose:
The `config` module provides metadata and configurations for processing multiple sites, dataloggers, and tables used in
environmental monitoring and data logging. The configuration primarily helps to manage Campbell Scientific (CS)
dataloggers and tables, defining their attributes such as file names, folders, frequencies, classes (dynamic or static),
 and other parameters. This metadata is used in conjunction with the data processing pipeline to organize, analyze, and
 store time-series data.

---

### Metadata Constants:

- **`DEFAULT (str)`**:
  - **Purpose**: Used as a key in the table definitions to indicate default configurations.
  - **Default**: `'default'`.

- **`L1_FOLDER_NAME (str)`**:
  - **Purpose**: Key to specify the folder name for storing Level 1 (processed) data.
  - **Example**: `'l1FolderName'`.

- **`L1_FILE_NAME (str)`**:
  - **Purpose**: Key to specify the file name for Level 1 data files.
  - **Example**: `'l1FileName'`.

- **`L1_NAME_POSTFIX (str)`**:
  - **Purpose**: Key to specify a postfix (e.g., year) for file names to differentiate by time.
  - **Example**: `'l1NamePostfix'`.

- **`FREQUENCY (str)`**:
  - **Purpose**: Key to define the frequency of the table, either a dynamic (time-series) or static frequency.
  - **Example**: `'frequency'`.

- **`L1_FILE_FREQUENCY (str)`**:
  - **Purpose**: Key to define how frequently Level 1 files are stored (e.g., daily, yearly).
  - **Example**: `'l1FileFrequency'`.

- **`CLASS (str)`**:
  - **Purpose**: Key to classify the table as `static` or `dynamic`.
  - **Example**: `'class'`.

- **`SAVE_L0_TOB (bool)`**:
  - **Purpose**: Key to specify whether Level 0 TOB files (raw binary) should be saved.
  - **Example**: `'saveL0TOB'`.

- **`ARCHIVE_AFTER (timedelta)`**:
  - **Purpose**: Key to define when a file should be archived after a specific time period.
  - **Example**: `'archiveAfter'`.

- **`NAN_VALUE (int)`**:
  - **Purpose**: Key to define the value used for missing data (NaN).
  - **Example**: `'nanValue'`.

- **`INDEX_MAP_FUNC (function)`**:
  - **Purpose**: Key to define a function used to map and format the index (typically timestamps).
  - **Example**: `'indexMapFunc'`.

- **`COLS_2_PLOT (list)`**:
  - **Purpose**: Key to specify which columns should be plotted for visualization.
  - **Example**: `'cols2Plot'`.

- **`TIME_2_PLOT (DateOffset)`**:
  - **Purpose**: Key to define a time offset for plotting, such as `30 days`.
  - **Example**: `'time2Plot'`.

- **`PROJECT (str)`**:
  - **Purpose**: Key to associate the table with a specific project.
  - **Example**: `'project'`.

- **`RESAMPLE (str or bool)`**:
  - **Purpose**: Key to define if the table data should be resampled to a specific frequency (e.g., '1T' for 1 minute).
  - **Example**: `'resampled'`.

---

### Tables Configuration:

The `TABLES` dictionary contains metadata for each table that is being processed. Each table can inherit default values
or define its own custom configuration.

---

#### **Default Table Configuration (`DEFAULT`)**:
- **`L1_FOLDER_NAME`**: Default folder name for storing Level 1 data.
- **`L1_FILE_NAME`**: Default file name for Level 1 data.
- **`L1_NAME_POSTFIX`**: Postfix for filenames (default is yearly format).
- **`FREQUENCY`**: Default frequency (e.g., `30 minutes`).
- **`L1_FILE_FREQUENCY`**: How frequently Level 1 files are stored (default: yearly).
- **`CLASS`**: Specifies if the table is static or dynamic.
- **`SAVE_L0_TOB`**: Whether to save raw binary data for Level 0 files.
- **`ARCHIVE_AFTER`**: Archive files after a default period (e.g., 2 years).
- **`NAN_VALUE`**: Default NaN value (`-9999`).
- **`INDEX_MAP_FUNC`**: Function for formatting timestamps (default: None).
- **`COLS_2_PLOT`**: List of columns to plot (default: empty).
- **`TIME_2_PLOT`**: Default plotting period (e.g., 30 days).
- **`RESAMPLE`**: Default resampling setting (default: False).

---

### **`getTable(table)`**:
- **Purpose**: Returns the configuration for a given table. If the table is not defined, it uses the default
configuration.
- **Parameters**:
  - `table (str)`: The name of the table to retrieve.
- **Returns**: A dictionary of the table configuration.

---

### Usage:
This module defines and organizes the metadata needed for processing data from multiple dataloggers and tables in
environmental monitoring. It helps the main system by providing the necessary configuration (e.g., folder names, file
frequencies, and plotting preferences) for each table, allowing for easy integration and data handling in the
processing pipeline.

"""

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
RESAMPLE = 'resampled'


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
        TIME_2_PLOT: DateOffset(days=30),
        # project that the table belongs to, eg. ['ECS_AboveCanopy']
        #PROJECT: [consts.ECS_NAME],
        # additional table resampled. False|'1T' for 1 minute|'1H' for 1 hour|
        #   'D' for days|'S' for seconds|'L' for milliseconds
        RESAMPLE: False,
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
        #FREQUENCY: consts.FREQ_1MIN,
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
        RESAMPLE: '1T',
    },
    # Pecan5R
    'Config_Setting_Notes': {  ## TO REMOVE
        FREQUENCY: consts.FREQ_STATIC,
        CLASS: consts.CLASS_STATIC,
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
        INDEX_MAP_FUNC: LibDataTransfer.datetime_format_HF,
    },
    'System_Operatn_Notes': {  ## TO REMOVE
        FREQUENCY: consts.FREQ_STATIC,
        CLASS: consts.CLASS_STATIC,
    },
    'IntAvg': {
        L1_FOLDER_NAME: 'IntAvg',
        L1_FILE_NAME: 'IntAvg',
    },
    'message_log': {
        FREQUENCY: consts.FREQ_STATIC,
        CLASS: consts.CLASS_STATIC,
    },
    'RawData': {
        L1_FOLDER_NAME: 'RawData',
        L1_FILE_NAME: 'RawData',
        L1_NAME_POSTFIX: consts.TIMESTAMP_FORMAT_DAILY,
        FREQUENCY: consts.FREQ_2HZ,
        L1_FILE_FREQUENCY: consts.FREQ_DAILY,
        INDEX_MAP_FUNC: LibDataTransfer.datetime_format_HF,
    },
    'SiteAvg': {
        'l1NamePostfix': consts.TIMESTAMP_FORMAT_YEARLY,
        'l1FileFrequency': consts.FREQ_YEARLY,
    },
    'TimeInfo': {
        FREQUENCY: consts.FREQ_STATIC,
        CLASS: consts.CLASS_STATIC,
    },
    # RedLake
    #'BiometConstants': {  ## TO REMOVE
    #    FREQUENCY: consts.FREQ_STATIC,
    #    CLASS: consts.CLASS_STATIC,
    #},
    #'Biomet': {
    #    L1_FOLDER_NAME: 'Biomet',
    #    L1_FILE_NAME: 'Biomet',
    #},
    #'VariableChecks': {
    #    L1_FOLDER_NAME: 'VariableChecks',
    #    L1_FILE_NAME: 'VariableChecks',
    #},
}


def getTable(table):
    """ Return the table info """
    current_table = TABLES[DEFAULT].copy()
    n_table = TABLES.get(table, TABLES[DEFAULT])
    for item in n_table:
        current_table[item] = n_table[item]
    if current_table[L1_FOLDER_NAME] == DEFAULT:
        current_table[L1_FOLDER_NAME] = table
        current_table[L1_FILE_NAME] = table
    return current_table