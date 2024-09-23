# This Python script defines a class ResampleData to resample CSV data to a lower frequency. It reads CSV files,
#   processes them to resample data based on a specified frequency, and then saves the resampled data to new CSV files.
#
# Below is a summary of the script with key points highlighted:
#
# Key Features:
#   Initialization Parameters:
#       inPath: Input path, either a file or directory containing CSV files.
#       freq: The frequency to which data will be resampled (default: '1 minute').
#       outPath: Output path where resampled files will be saved (optional).
#       method: The method of resampling (e.g., 'last' for the last value in the resampling interval).
#       debug: A flag to toggle debugging behavior (default: False).
#
#   CSV Handling:
#       The script uses pandas to load CSV data into a DataFrame, applying specific header lines and skipping metadata.
#       It handles data resampling based on the provided frequency (1T means 1 minute intervals by default).
#
#   Resampling:
#       The resampling is done using LibDataTransfer.resampleDataFrame, a method that resamples the data according to
#           the frequency and method specified during initialization.
#
#   Saving the Resampled Data:
#       After resampling, the data is saved back to CSV with the header information using LibDataTransfer.writeDF2csv.
#
# Documentation Summary:
#   Class: ResampleData is designed to handle CSV files, resample their time-series data to a lower frequency, and save
#       the resampled data to a new file.
#
#   Methods:
#       __init__: Initializes the class with the required parameters like inPath, freq, outPath, and method.
#       doIt: Processes all files in the input path, performing resampling on each one.
#       getDF: Reads a CSV file into a pandas DataFrame.
#       resampleData: Resamples the data in the DataFrame according to the specified frequency and method.
#       saveDF: Saves the resampled DataFrame to a new CSV file.
#
#   Attributes:
#       Each instance of the class has several attributes related to the input/output files and DataFrame manipulation.
#
#   External Dependencies:
#       This class depends on functions from LibDataTransfer and constants from consts. Make sure those are defined and
#           available in your environment.

from pathlib import Path
import pandas as pd

import consts
import LibDataTransfer


class ResampleData:
    """
    A class to resample time-series data in CSV files to a lower frequency.

    Attributes:
        inPath (Path): The input directory or file path containing the CSV files.
        freq (str): The resampling frequency (default: '1T' for 1 minute).
        outPath (Path): The output directory or file path to save the resampled CSV files.
        method (str): The method of resampling (e.g., 'last', 'mean') (default: 'last').
        debug (bool): A flag to indicate whether to run in debug mode (default: False).
        df (pd.DataFrame): The DataFrame holding the data from the CSV file.
        meta (dict): Metadata from the CSV file (e.g., headers).
        inPathFile (Path): The current file being processed.
        outPathFile (Path): The path for the output resampled CSV file.
        colNames (list): The column names for the DataFrame.
        inFiles (list): List of input CSV files to be processed.
    """
    def __init__(self, inPath, freq='1T', outPath=None, method='last', debug=False):
        """
        Initialize the ResampleData class with input path, frequency, output path, and method.

        Args:
            inPath (str or Path): The input file or directory containing CSV files.
            freq (str): The resampling frequency (default: '1T' for 1 minute).
            outPath (str or Path): The output path where resampled files will be saved (default: None).
            method (str): The method of resampling, e.g., 'last', 'mean' (default: 'last').
            debug (bool): A flag to toggle debug mode (default: False).
        """
        self.debug = debug
        self.df = None
        self.meta = None
        self.inPathFile = None
        self.outPathFile = None
        self.colNames = None
        self.inPath = Path(inPath)
        self.method = method
        self.freq = freq
        if self.freq == '1T':
            self.strFreq = '1min'
        else:
            self.strFreq = self.freq
        self.method = method
        if self.inPath.is_dir():
            self.inFiles = [x for x in self.inPath.glob('*.csv') if x.is_file()]
        else:
            self.inFiles = [self.inPath]
        if outPath is None:
            if self.inPath.is_dir():
                outPath = self.inPath.parent.joinpath(f'{self.inPath.stem}_{self.strFreq}')
            else:
                outPath = self.inPath.parent.parent.joinpath(f'{self.inPath.parent.name}_{self.strFreq}')
        self.outPath = Path(outPath)
        self.doIt()

    def doIt(self):
        """
        Process each file by reading the data, resampling it, and saving the resampled data to a new CSV.
        """
        for item in self.inFiles:
            self.inPathFile = item
            self.meta = LibDataTransfer.getHeaderFLlineFile(self.inPathFile)
            self.colNames = LibDataTransfer.getStrippedHeaderLine(self.meta['headers'][consts.CS_FILE_HEADER_LINE['FIELDS']])
            self.outPathFile = self.outPath.joinpath(f'{self.inPathFile.stem}_{self.strFreq}{self.inPathFile.suffix}')
            print(f'Processing {self.inPathFile.name} to ...\\{self.outPathFile.relative_to(self.inPathFile.parent.parent)}')
            self.df = self.getDF()
            if not self.debug:
                self.df = self.resampleData()
                self.saveDF()

    def getDF(self):
        """
        Read the CSV file into a pandas DataFrame.

        Returns:
            pd.DataFrame: The loaded DataFrame with the data.
        """
        self.df = pd.read_csv(self.inPathFile, header=None, skiprows=len(consts.CS_FILE_HEADER_LINE) - 1, index_col=0,
                              na_values=[consts.FLAG, "NAN"], names=self.colNames, parse_dates=True,
                              date_format='mixed')
        return self.df

    def resampleData(self):
        """
        Resample the data to a lower frequency using the method specified during initialization.

        Returns:
            pd.DataFrame: The resampled DataFrame.
        """
        self.df = LibDataTransfer.resampleDataFrame(self.df, self.freq, self.method)
        return self.df

    def saveDF(self):
        """
        Save the resampled DataFrame to a CSV file with the original metadata headers.

        Args:
            pathFile (str or Path): The file path where the DataFrame will be saved.
            df (pd.DataFrame): The DataFrame to save.
        """
        LibDataTransfer.writeDF2csv(pathFile=self.outPathFile, dataframe=self.df, header=self.meta['headers'])


if '__main__' == __name__:
    # example usage
    inPath = Path(r'C:\Data\Bahada\CR3000\L1\EddyCovariance_ts_2\2024')
    ResampleData(inPath)
    print('Done!')
