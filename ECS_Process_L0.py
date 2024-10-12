# -------------------------------------------------------------------------------
# Name:        CampbellSci Process for L0 and L1 Data
# Purpose:     This script process the data from the CampbellSci data loggers. The process consist in:
#               1. Rename the files from the LoggerNet folder adding the current timestamp.
#               2. Load the metadata of each file and classify the files by site and table name.
#               3. For each file, the script will check if there is a L1 file in the SharePoint folder.
#               4. If there is a L1 file, the script will compare the headers of the current file with the stored file.
#               5. If there are changes in the headers, the script will rename the stored file adding the current
#                   timestamp.
#               6. The script will append the current file to the stored file and save the new file.
#               7. If there is not a L1 file, the script will create a new file with the current data.
#               8. The script will move the L0 files to the corresponding folder.
#               9. The local files are uploaded to the SharePoint folder and backed up in a temporal folder.
#               10. Finally, the files on the temporal backup folder are removed after a certain time.
#
# Version:     1.0
#
# Author:      Gesuri Ramirez
#
# Created:     06/13/2023
# Copyright:   (c) Gesuri 2024
# Licence:     Apache 2.0
# -------------------------------------------------------------------------------

# info about CS tables:
# https://help.campbellsci.com/GRANITE9-10/Content/shared/Details/Data/About_Data_Tables.htm?TocPath=Working%20with%20data%7C_____4


from pathlib import Path
from datetime import datetime, timedelta
import time
import getopt
import sys
import os

import systemTools
import consts
import Log
import InfoFile
import LibDataTransfer

# Add the path to the MSSP_file_driver folder, this a different repository
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'MSSP_file_driver'))
import office365_api

_PATH_DATA_2_PROCESS_ = consts.PATH_HARVESTED_DATA

# Initialize the general log file
log = Log.Log(path=consts.PATH_GENERAL_LOGS.joinpath('ECS_Process_L0.log'))


def getReadyFiles(dirList):
    """
    Rename the files in the directory with the current timestamp and return the new list of files.

    Args:
       dirList (list): List of directory paths where files are located.

    Returns:
       newDirList (list): List of files with updated names.
    """
    newDirList = []
    for item in dirList:
        log.live(f'Renaming {item.name}')
        nf = LibDataTransfer.renameAFileWithDate(item)
        if nf:
            newDirList.append(nf)
    return newDirList


def check_folders():
    """
    Check if the required folders exist and create them if they do not.
    """
    for folder in [consts.PATH_CLOUD, consts.PATH_HARVESTED_DATA, consts.PATH_GENERAL_LOGS, consts.PATH_CHECK_FILES,
                   consts.PATH_TEMP_BACKUP, consts.PATH_TEMPSHARE]:
        if not systemTools.createDir(folder):
            log.error(f'Error creating folder: {folder}')


def cmd_help():
    """
    Display help message for the script usage and default paths.
    """
    print('Help:')
    print('   ECS_Process_L0.py')
    print('   This script process the data from the CampbellSci data loggers. The process consist in:')
    print("      Default values:")
    print('         Storage Folder:           ', consts.PATH_CLOUD)
    print('         LoggerNet working Folder: ', consts.PATH_HARVESTED_DATA)
    print('         Logs Folder:              ', consts.PATH_GENERAL_LOGS)
    print('         Check | error Folder:     ', consts.PATH_CHECK_FILES)
    print('         Temporal Backup Folder:   ', consts.PATH_TEMP_BACKUP)
    print('         Time to remove files:     ', consts.TIME_REMOVE_TEMP_BACKUP)
    print('         After process path:       ', consts.PATH_TEMPSHARE)
    print('   If required to process extra data, you need to run the script without any parameters.')
    print('   If the script will be run atomatically by the system, the parameter -a must be added.')
    print('   ')
    print('   To use default folders use no parameters')
    print('   To change folders, modify consts.py file only if you really know what are you doing!')


def arguments(argv):
    """
    Parse command line arguments for the script and trigger the help function if necessary.

    Args:
        argv (list): List of command line arguments.
    """
    global _PATH_DATA_2_PROCESS_
    try:
        opts, args = getopt.getopt(argv, "ah", ["help"])
    except getopt.GetoptError:
        cmd_help()
        sys.exit(2)
    if not opts:
        _PATH_DATA_2_PROCESS_ = consts.PATH_TEMPSHARE
    for opt, arg in opts:
        if opt == '-a':
            _PATH_DATA_2_PROCESS_ = consts.PATH_HARVESTED_DATA
        elif opt in ('-h', '--help'):
            cmd_help()
            sys.exit()
        else:
            cmd_help()
            sys.exit()


def check_log_file(path):
    """
    Check if a given file is a log file or resides in a log folder.

    Args:
        path (Path): Path object representing the file.

    Returns:
        bool: True if the file is a log file or resides in a log folder, else False.
    """
    log_folder_present = any(folder.name.lower() in ['logs', 'log'] for folder in path.parents)
    if log_folder_present or path.suffix.lower() == '.log':
        return True
    return False


def check_temp_backup():
    """
    Check the temporal backup folder and remove files older than the defined time in consts.TIME_REMOVE_TEMP_BACKUP.
    """
    for file in consts.PATH_TEMP_BACKUP.rglob('*'):
        if file.is_file() and datetime.fromtimestamp(file.stat().st_mtime) < datetime.now() - consts.TIME_REMOVE_TEMP_BACKUP:
            log.info(f'Removing file {file.name} from temporal backup')
            file.unlink()


def download_SP_files(pathfiles):
    """
    Download files from SharePoint, checking local existence and metadata before downloading.
    If the local file is newer or larger than the file in SharePoint, the file in SharePoint will be renamed and
    the local file will be used.
    Args:
        pathfiles (list or str): List of paths or single path to download files.
    """
    et = systemTools.ElapsedTime()
    sp = office365_api.SharePoint(log=log)
    if not isinstance(pathfiles, list):
        pathfiles = [pathfiles]
    for file in pathfiles:
        pf = Path(file)
        file_name = pf.name
        folder_name = pf.relative_to(consts.PATH_CLOUD).parent
        if pf.exists():
            log.info(f'File {pf.name} already exists in local folder')
            remote_file_properties = sp.get_file_properties(file_name, folder_name)
            if not remote_file_properties:
                log.debug(f"The file {file_name} doesn't exist on SharePoint, skiping this file.")
                continue
            flag = False
            # Compare size and modification date of the local file with the file in SharePoint
            if pf.stat().st_size > remote_file_properties['file_size']:
                log.warn(f'The local file {pf.name} looks to have more information than the file in SharePoint')
                flag = True
            if datetime.fromtimestamp(pf.stat().st_mtime) > remote_file_properties['time_last_modified']:
                log.warn(f'File {pf.name} in local folder is newer than the file in SharePoint')
                flag = True
            if flag:
                new_name = f'{file_name}_{Log.getStrTime()}'
                log.warn(f'The file in SharePoint will be renamed to {new_name}')
                sp.rename_file(f'{folder_name}/{file_name}', f'{folder_name}/{new_name}')
                time.sleep(5)
        if not pf.parent.exists():
            pf.parent.mkdir(parents=True)
        # download the file from SharePoint
        log.live(f'Downloading {file_name} from SharePoint')
        if not sp.download_large_file(file_name, folder_name, pf):
            log.warn(f'Not possible download {file_name} from SharePoint.')
    log.info(f'Time downloading the files was: {et.elapsed()}')


def upload_SP_files():
    """
    Upload files from the local folder to SharePoint, resuming uploads based on modification time.
    If the file is successfully uploaded, it will be moved to the temporal backup folder.
    """
    # Create the elapsed time object
    et = systemTools.ElapsedTime()
    # set connection to SharePoint
    sp = office365_api.SharePoint(log=log)
    # Get the current time and the time from 7 days ago
    last_mod_time = datetime.now() - timedelta(days=7)
    # Get the list of files in the local folder
    files = [f for f in consts.PATH_CLOUD.rglob('*') if
             f.is_file() and datetime.fromtimestamp(f.stat().st_mtime) >= last_mod_time]
    idx = 1
    for item in files:
        log.live(f'File: {item.name}, ({idx}/{len(files)})')
        upload_file = item.relative_to(consts.PATH_CLOUD)
        # Upload the files to the SharePoint folder
        if sp.upload_large_file(local_file_path=item, target_file_url=upload_file):
            if not check_log_file(item):
                log.info(f'Local copy of {item.name} was uploaded to SharePoint and local file moved to temporal backup')
                LibDataTransfer.moveAfileWOOW(item, consts.PATH_TEMP_BACKUP.joinpath(upload_file), log)
            else:
                log.info(f'Local copy of {item.name} was uploaded to SharePoint')
        else:
            log.warn(f'Unable to upload {item.name} to SharePoint. File will be left for next attempt')
        idx += 1
    # Log the elapsed time
    log.info(f'Uploaded {len(files)} files in {et.elapsed()}.')


def run():
    """
    Main function to process L0 files, update tables, and manage file transfers.
    """
    # get the list of files in the folder
    files = [x for x in _PATH_DATA_2_PROCESS_.iterdir() if x.is_file() and x.name != 'README.txt' and x.suffix !='.backup']

    # rename the files
    files = getReadyFiles(files)

    # process the files
    for file in files:  # for each file in the collect folder
        elapsedTime1 = systemTools.ElapsedTime()
        log.live(f'>>>>>>>>>>>>>>>>>> {file.name} >>>>>>>>>>>>>>>>>>')
        log.live(f'Processing L0 file: {file.name}')

        # create the object that read the CS file (file Level 0) from fL0 get the related stored files and load them
        # using InfoFile
        l0 = InfoFile.InfoFile(file)
        if not l0.ok():
            log.error(f'The file {l0} is skiping because has problems.\n{l0.statusFile}')
            continue

        # the l0 dataframe is cleaned (if the frequency is correct and not an static table) and organized by the
        # storage frequency
        gDF = LibDataTransfer.fuseDataFrame(l0.df, freq=l0.frequency, group=l0.st_fq, log=log)

        # created a list based on the storage frequency. If days, for ts, then each day is a key.
        i_gDF = list(gDF.keys())

        # download the L1 files needed from SharePoint for the current file
        download_SP_files(l0.pathL1)
        for idx_pL1 in range(len(l0.pathL1)):  # for each stored or cloud file (L1 file) related to the current file, L0 file
            fL1 = l0.pathL1[idx_pL1]
            start2 = time.time()  # keep track of the time for each L1 file
            createNewFile = False  # flag to create a new file
            log.live(f'For {file.name}, processing L1 {fL1.name}')
            l1 = InfoFile.InfoFile(fL1)  # get the info for the L1 file
            idx = i_gDF.pop(0)  # get the first key, year or day, of the list that should be the oldest L1 file
            c_df = gDF.pop(idx)  # get the dataframe for the oldest L1 file that is the key idx

            # start the process to check the current L0 to be appended to the L1 file
            if l1.ok():  # there is available L1 file for the current file (the L1 file exists and has data)

                # compare headers of the current with any of the stored files
                chFrom = list(set(l1.cs_headers) - set(l0.cs_headers))
                chTo = list(set(l0.cs_headers) - set(l1.cs_headers))

                if len(chFrom) > 0:  # there are changes. log the changes
                    log.warn(f'For site {l1.f_site}, the table {l1.cs_tableName} changed from: "{chFrom}" to: "{chTo}"')

                    if l1.numberColumns != l0.numberColumns:  # check NUMBER of columns
                        log.warn(f'For site {l1.f_site}, the table {l1.cs_tableName} changed the number of columns '
                                 f'from: "{l1.numberColumns}" to: "{l0.numberColumns}"')
                        createNewFile = True

                    if l1.colNames != l0.colNames:  # check columns NAMES
                        a = set(l1.colNames) - set(l0.colNames)
                        b = set(l0.colNames) - set(l1.colNames)
                        log.warn(f'For site {l1.f_site}, the table {l1.cs_tableName} changed the columns names from: "'
                                 f'{a}" to: "{b}"')
                        createNewFile = True

                    if l1.cs_signature != l0.cs_signature:
                        log.warn(f'For site {l1.f_site}, the table {l1.cs_tableName} changed the signature from: "'
                                 f'{l1.cs_signature}" to: "{l0.cs_signature}"')

                    if l1.cs_serialNumber != l0.cs_serialNumber:
                        log.warn(f'For site {l1.f_site}, the table {l1.cs_tableName} changed the serial number from: "'
                                 f'{l1.cs_serialNumber}" to: "{l0.cs_serialNumber}"')

                    if l1.cs_program != l0.cs_program:
                        log.warn(f'For site {l1.f_site}, the table {l1.cs_tableName} changed the program from: "'
                                 f'{l1.cs_program}" to: "{l0.cs_program}"')

                    if l1.cs_os != l0.cs_os:
                        log.warn(f'For site {l1.f_site}, the table {l1.cs_tableName} changed the OS from: "'
                                 f'{l1.cs_os}" to: "{l0.cs_os}"')

                if createNewFile:
                    newName = LibDataTransfer.renameAFileWithDate(l1.pathFile, log)
                    log.warn(f'For site {l1.f_site}, the file L1 {l1.pathFile.name} was renamed because the header '
                             f'is different to L0. The new file name is: {newName.name}')
                    l1.df = None
                else:
                    log.info(f'For site {l0.f_site}, the table {l0.cs_tableName} there is a L1 file named: '
                             f'{l1.pathFile.name}')

                # this section is for the header that is the same from the current to the stored file
                # this line add the current data to the stored file, in other words, L0 is appended to L1
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

            # update the L1 resample files if needed
            if l0.resample:
                log.info(f'For site {l0.f_site}, table {l0.cs_tableName} resampling to {l0.resample}')
                resampleDF = LibDataTransfer.resampleDataFrame(df=c_df, freq=l0.resample, method='last')
                pathResample = l0.pathL1Resample[idx_pL1]
                log.debug(f'For site {l0.f_site}, table {l0.cs_tableName} resampled saved to {pathResample}')
                LibDataTransfer.writeDF2csv(pathFile=pathResample, dataframe=resampleDF, header=l0.cs_headers,
                                            indexMapFunc=l0.metaTable['indexMapFunc'], log=log)

            # write the data to a csv file that is L1
            LibDataTransfer.writeDF2csv(pathFile=fL1, dataframe=c_df, header=l0.cs_headers,
                                        indexMapFunc=l0.metaTable['indexMapFunc'], log=log)

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
        log.live(f'<<<<<<<<<<<<<<<<< {file.name} <<<<<<<<<<<<<<<<<<<')
    # move the files to the SharePoint
    upload_SP_files()


if __name__ == '__main__':
    elapsedTime = systemTools.ElapsedTime()
    check_folders()
    arguments(sys.argv[1:])
    run()
    check_temp_backup()
    log.info(f'Total time for all the files: {elapsedTime.elapsed()}')
