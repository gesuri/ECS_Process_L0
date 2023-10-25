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


def getReadyFiles(dirList):
    newDirList = []
    for item in dirList:
        log.live(f'Renaming {item.name}')
        nf = LibDataTransfer.renameAFileWithDate(item)
        if nf:
            newDirList.append(nf)
    return newDirList


def cmd_help():
    print('Help:')
    print("      Default values:")
    print('         Storage Folder:           ', consts.PATH_CLOUD)
    print('         LoggerNet working Folder: ', consts.PATH_HARVESTED_DATA)
    print('         Logs Folder:              ', consts.PATH_GENERAL_LOGS)
    print('         Check | error Folder:     ', consts.PATH_CHECK_FILES)
    print('   ECS_Process_L0.py')
    print('   To use default folders use no parameters')
    print('   To change folders, modify consts.py file only if you really know what are you doing!')


def arguments(argv):
    try:
        opts, args = getopt.getopt(argv, "h", ["help"])
    except getopt.GetoptError:
        cmd_help()
        sys.exit(2)
    if not opts:
        pass
    for opt, arg in opts:
        if opt == '-h':
            cmd_help()
            sys.exit()
        else:
            cmd_help()
            sys.exit()


def run():
    """Main function
    it will read folder const.PATH_HARVESTED_DATA and each file will be processed
    """
    # get the list of files in the folder
    files = [x for x in consts.PATH_HARVESTED_DATA.iterdir() if x.is_file()]
    # rename the files
    files = getReadyFiles(files)
    # process the files
    for file in files:  # for each file in the collect folder
        elapsedTime1 = systemTools.ElapsedTime()
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
                    newName = LibDataTransfer.renameAFileWithDate(l1.pathFile, log)
                    log.warn(f'For site {l1.f_site}, the file L1 {l1.pathFile.name} was renamed because the header to: '
                             f' {newName.name}')
                    l1.df = None
                else:
                    log.info(f'For site {l0.f_site}, the table {l0.cs_tableName} there is a L1 file named: '
                             f'{l1.pathFile.name}')
                # this section is for the header that is the same from the current to the stored file
                c_df = LibDataTransfer.fuseDataFrame(c_df, l1.df, freq=l0.frequency, group=l0.st_fq)
                if len(c_df) > 1:
                    log.error(f'For site {l0.f_site}, the table {l0.cs_tableName} on files {l0.pathFile.name} and '
                              f'{l1.pathFile.name} have more than a set of data grouped on "{l0.st_fq}", {c_df.keys()}.'
                              f' Skipped this file.')
                    continue
                if idx in c_df.keys():
                    c_df = c_df.pop(idx)
                else:
                    log.error(f'!!!!!For site {l0.f_site}, the table {l0.cs_tableName} on files {l0.pathFile.name} and '
                              f'{l1.pathFile.name} have different grouped ({l0.st_fq}) data {c_df.keys()}. Skipped this'
                              f' file.')
                    continue
            else:  # there is not L1 file for the current file so creating a new one
                log.info(f'For site {l0.f_site}, table {l0.cs_tableName} there is not L1 file. Creating: {fL1.name}')
            if l0.hf:  # if high frequency data
                startDate = c_df.index[0]
                if startDate != startDate.floor(freq='D'):
                    log.info(f'For site {l0.f_site}, table {l0.cs_tableName}: Is going to create flagged data from '
                             'beginning of this day')
                    c_df = LibDataTransfer.createFlaggedData(df=c_df, freq=consts.FREQ_10HZ, st_fq=consts.FREQ_DAILY)
                # the index has a slight difference format
                c_df.index = c_df.index.map(LibDataTransfer.datetime_format_HF)
            # for RECORD, remove NaN with FLAG and convert to int RECORD column
            c_df['RECORD'] = c_df['RECORD'].fillna(consts.FLAG).astype(int)
            # write the data to a csv file that is L1
            LibDataTransfer.writeDF2csv(pathFile=fL1, dataframe=c_df, header=l0.cs_headers, log=log)
            end2 = time.time()
            log.live(f'Total time for file L1: {fL1.name}: {end2 - start2:.2f} seconds')
        # move the L0 files to the corresponding folder
        if l0.pathTOA and l0.pathTOA.is_file():
            log.debug(f'Moving {l0.pathTOA} to {l0.pathL0TOA}')
            LibDataTransfer.moveAfileWOOW(l0.pathTOA, l0.pathL0TOA)
        if l0.pathTOB and l0.pathTOB.is_file():
            log.debug(f'Moving {l0.pathTOB} to {l0.pathL0TOB}')
            LibDataTransfer.moveAfileWOOW(l0.pathTOB, l0.pathL0TOB)
        log.live(f'Total time for file L0: {file.name} {file.name}: {elapsedTime1.elapsed()}')


if __name__ == '__main__':
    elapsedTime = systemTools.ElapsedTime()
    arguments(sys.argv[1:])
    run()
    log.info(f'Total time for all the files: {elapsedTime.elapsed()}')

# TODO: check if new file has different header. 1. less/more columns, 2. different columns names
