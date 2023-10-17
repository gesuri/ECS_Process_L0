# -------------------------------------------------------------------------------
# Name:        EddyCovarinceSystem Process for L0 Data
# Purpose:     take the files from loggernet, change de file name with the
#              current timestamp. Then the releated stored tables are updated
#              with the current file.
#
# Version:     0.1
#
# Author:      Gesuri Ramirez
#
# Created:     06/13/2023
# Copyright:   (c) Gesuri 2023
# Licence:     Apache 2.0
# -------------------------------------------------------------------------------

# info about CS tables:
# https://help.campbellsci.com/GRANITE9-10/Content/shared/Details/Data/About_Data_Tables.htm?TocPath=Working%20with%20data%7C_____4


from pathlib import Path

import time
import getopt
import sys

import systemTools
import consts
import Log
import InfoFile
import LibDataTransfer

log = Log.Log(path=consts.PATH_GENERAL_LOGS.joinpath('ECS_Process_L0.log'))
# Sample DataFrame with a datetime index
# data = {
#     'timestamp': pd.date_range(start='2023-09-01', periods=86400, freq='1min'),
#     'value': range(86400)
# }
#
# df = pd.DataFrame(data)
# df.set_index('timestamp', inplace=True)
#
# # Split the DataFrame into daily DataFrames
# daily_dataframes = []
#
# for name, group in df.groupby(pd.Grouper(freq='D')):
#     print(name)
#     daily_dataframes.append(group)
#
# # Now, daily_dataframes is a list of DataFrames, each containing data for one day.
# # You can access each daily DataFrame like this:
# for i, daily_df in enumerate(daily_dataframes):
#     print(f"Day {i + 1} Data:")
#     #print(daily_df)


def getReadyFiles(dirList):
    newDirList = []
    for item in dirList:
        log.live(f'Renaming {item.name}')
        nf = LibDataTransfer.renameAFileWithDate(item)
        if nf:
            newDirList.append(nf)
    return newDirList

def setup():
    systemTools.createDir(consts.PATH_WORKING_DATA)
    systemTools.createDir(consts.PATH_STORAGE)
    systemTools.createDir(consts.PATH_LOGS)
    systemTools.createDir(consts.PATH_CHECK_FILES)


def _help():
    print("Help:")
    print('      Default values:')
    print('         Storage Folder:       ', consts.PATH_STORAGE)
    print('         LoggerNet working Folder:', consts.PATH_HARVESTED_DATA)
    print('         Logs Folder:          ', consts.PATH_LOGS)
    print('         Check | error Folder: ', consts.PATH_CHECK_FILES)
    print('   mover.py')
    print('   To use default folders use no parameters')
    print('   To change folders, modify consts.py file only if you really know what are you doing!')


def arguments(argv):
    try:
        opts, args = getopt.getopt(argv, "h", ["help"])
    except getopt.GetoptError:
        _help()
        sys.exit(2)
    if not opts:
        pass
    for opt, arg in opts:
        if opt == '-h':
            _help()
            sys.exit()
        else:
            _help()
            sys.exit()


def run():
    """Main function
    it will read folder const.PATH_HARVESTED_DATA and each file will be processed
    # TODO: when end each iteration, move the L0 files (toa, tob) to the corresponding folder
    #       Also check the column named record cause the number on ts_data_2 has it as float. Maybe replace with a secuential number starting on the beginig of the file
    """
    # get the list of files in the folder
    files = [x for x in consts.PATH_HARVESTED_DATA.iterdir() if x.is_file()]
    # rename the files
    files = getReadyFiles(files)
    # process the files
    for file in files:  # for each file in the collect folder
        start1 = time.time()
        log.live(f'Processing L0 file: {file.name}')
        l0 = InfoFile.InfoFile(file)  # create the object that read the CS file (file Level 0)
        # from fL0 get the related stored files and load them using InfoFile
        gDF = LibDataTransfer.fuseDataFrame(l0.df, freq=l0.frequency, group=l0.st_fq)
        i_gDF = list(gDF.keys())
        for fL1 in l0.pathL1:  # for each stored or cloud file related to the current file
            start2 = time.time()
            log.live(f'For {file.name}, processing L1 {fL1.name}')
            l1 = InfoFile.InfoFile(fL1)
            idx = i_gDF.pop(0)
            c_df = gDF.pop(idx)
            if l1.ok():  # there is available L1 file for the current file (the L1 file exists and has data)
                # compare headers of the current with any of the stored files
                chFrom = list(set(l1.cs_headers) - set(l0.cs_headers))
                chTo = list(set(l0.cs_headers) - set(l1.cs_headers))
                if len(chFrom) > 0:  # there are changes. log the changes
                    log.warn(f'For site {l1.f_site}, the table {l1.cs_tableName} changed from: "{chFrom}" to: "{chTo}"')
                    if l1.numberColumns != l0.numberColumns:  # check number of columns
                        log.warn(f'For site {l1.f_site}, the table {l1.cs_tableName} changed the number of columns '
                                 f'from: "{l1.numberColumns}" to: "{l0.numberColumns}"')
                    if l1.colNames != l0.colNames:  # check columns names
                        log.warn(f'For site {l1.f_site}, the table {l1.cs_tableName} changed the columns names from: "'
                                 f'{l1.colNames}" to: "{l0.colNames}"')
                else:
                    log.info(f'For site {l0.f_site}, the table {l0.cs_tableName} there is a L1 file named: '
                             f'{l1.pathFile.name}')
                # this section is for the header that is the same from the current to the stored file
                c_df = LibDataTransfer.fuseDataFrame(l1.df, c_df, freq=l0.frequency, group=l0.st_fq)
                if len(c_df) > 1:
                    log.error(f'For site {l0.f_site}, the table {l0.cs_tableName} on files {l0.pathFile.name} and '
                              f'{l1.pathFile.name} have more than a set of data grouped on "{l0.st_fq}", {c_df.keys()}.'
                              f' Skipped this file.')
                    continue
                if idx in c_df.keys():
                    c_df = c_df.pop(idx)
                else:
                    log.error(f'For site {l0.f_site}, the table {l0.cs_tableName} on files {l0.pathFile.name} and '
                              f'{l1.pathFile.name} have different grouped ({l0.st_fq}) data {c_df.keys()}. Skipped this'
                              f' file.')
                    continue
            else:  # there is not L1 file for the current file so creating a new one
                log.info(f'For site {l0.f_site}, table {l0.cs_tableName} there is not L1 file. Creating: {fL1.name}')
            if l0.hf:
                c_df.index = c_df.index.map(LibDataTransfer.datetime_format_HF)
            c_df['RECORD'] = c_df['RECORD'].fillna(consts.FLAG).astype(int)
            LibDataTransfer.writeDF2csv(pathFile=fL1, dataframe=c_df, header=l0.cs_headers, log=log)
            end2 = time.time()
            log.live(f'Total time for file L1: {fL1.name}: {end2 - start2}')
        end1 = time.time()
        log.live(f'Total time for file L0: {file.name} {file.name}: {end1 - start1}')

# TODO:

if __name__ == '__main__':
    start = time.time()
    run()
    end = time.time()
    log.info(f'Total time for all the files: {end - start}')