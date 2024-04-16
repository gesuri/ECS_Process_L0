# this script is used to plot the L1 data
import InfoFile
import config
import pandas as pd
import matplotlib.pyplot as plt

# this will plot the L1 data. So the plot should be L1 data


def plotL1Data(infoFile):
    """ Plot the L1 data
    infoFile: InfoFile object
    """
    # TODO: add title, units, and labels to the plot
    fromTime = pd.Timestamp.now() - infoFile.metaTable[config.TIME_2_PLOT]
    toTime = pd.Timestamp.now()
    #fromTime = infoFile.lastLineDT - infoFile.metaTable[config.TIME_2_PLOT]
    #toTime = infoFile.lastLineDT
    # check if infoFile is a InfoFile object
    if not isinstance(infoFile, InfoFile.InfoFile):
        raise TypeError('infoFile should be a InfoFile object')
    # check if the level is 1
    #if infoFile.level != 1:
    #    raise ValueError('infoFile should be a L1 InfoFile object')
    # check if there are fields to plot
    if len(infoFile.metaTable[config.COLS_2_PLOT]) == 0:
        raise ValueError('There are no fields to plot')
    lastData = infoFile.df.loc[fromTime:toTime]
    #lastData[infoFile.metaTable[config.COLS_2_PLOT]].plot()
    for item in infoFile.metaTable[config.COLS_2_PLOT]:
        infoFile.df[item].plot()
        plt.savefig(f'{infoFile.f_site_r}_{infoFile.f_project}_{infoFile.st_tableName}.jpg')
        plt.show()


if __name__ == '__main__':
    info = InfoFile.InfoFile(r'c:\temp\Collected\Bahada_CR3000_flux.dat', rename=False)
    plotL1Data(info)
    