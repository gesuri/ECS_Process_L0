# Description: Resample the data to a lower frequency
# the file must be a csv file


from pathlib import Path
import pandas as pd

import consts
import LibDataTransfer


class ResampleData:

    def __init__(self, inPath, freq='1T', outPath=None, method='last'):
        """ Resample the data to a lower frequency
        return: pd.DataFrame  """
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
        self.freq = freq
        self.method = method
        if self.inPath.is_dir():
            self.inFiles = [x for x in self.inPath.glob('*.csv') if x.is_file()]
        else:
            self.inFiles = [self.inPath]
        if outPath is None:
            if self.inPath.is_dir():
                outPath = self.inPath.parent.joinpath(f'{self.inPath.stem}_{self.strFreq}')
            else:
                outPath = self.inPath.parent.joinpath(f'{self.inPath.stem}_{self.strFreq}{self.inPath.suffix}')
        self.outPath = Path(outPath)
        self.doIt()

    def doIt(self):
        for item in self.inFiles:
            self.inPathFile = item
            self.meta = LibDataTransfer.getHeaderFLlineFile(self.inPathFile)
            self.colNames = LibDataTransfer.getStrippedHeaderLine(self.meta['headers'][consts.CS_FILE_HEADER_LINE['FIELDS']])
            self.outPathFile = self.outPath.joinpath(f'{self.inPathFile.stem}_{self.strFreq}{self.inPathFile.suffix}')
            print(f'Processing {self.inPathFile.name} to ...\\{self.outPathFile.relative_to(self.inPathFile.parent.parent)}')
            self.df = self.getDF()
            self.df = self.resampleData()
            self.saveDF()

    def getDF(self):
        """ Get the dataframe from the file
        pathFile: str|Path
        return: pd.DataFrame
        """
        self.df = pd.read_csv(self.inPathFile, header=None, skiprows=len(consts.CS_FILE_HEADER_LINE) - 1, index_col=0,
                              na_values=[-99999, "NAN"], names=self.colNames, parse_dates=True, date_format='mixed')
        return self.df

    def resampleData(self):
        """ Resample the data to a lower frequency
        return: pd.DataFrame
        """
        self.df = LibDataTransfer.resampleDataFrame(self.df, self.freq, self.method)
        return self.df

    def saveDF(self):
        """ Save the dataframe to a file
        pathFile: str|Path
        df: pd.DataFrame
        """
        LibDataTransfer.writeDF2csv(pathFile=self.outPathFile, dataframe=self.df, header=self.meta['headers'])


if '__main__' == __name__:
    inPath = Path(r'C:\Data\Bahada\CR3000\L1\EddyCovariance_ts_2\2024')
    ResampleData(inPath)
    print('Done!')
