# This script will list all the files from a folder and it will classify by its name where is the site and table name.
# Then each file will be renamed with the date and time at the end of the file name. Then the file will be moved to
# the corresponding folder based on the site.
# This code uses the LibDataTransfer.py to get the metadata of each file used to classify the file.


import getopt
import sys
from pathlib import Path
import LibDataTransfer
import consts
import Log
import systemTools


def setup():
    systemTools.createDir(consts.PATH_WORKING_DATA)
    systemTools.createDir(consts.PATH_STORAGE)
    systemTools.createDir(consts.PATH_LOGS)
    systemTools.createDir(consts.PATH_CHECK_FILES)


def help():
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
        help()
        sys.exit(2)
    if not opts:
        pass
    for opt, arg in opts:
        if opt == '-h':
            help()
            sys.exit()
        else:
            help()
            sys.exit()


def getReadyFiles(dirList):
    newDirList = []
    for item in dirList:
        print(f'Renaming {item.name}')
        nf = LibDataTransfer.renameAFileWithDate(item)
        if nf:
            newDirList.append(nf)
    return newDirList


def moveFiles():
    dirList = [x for x in consts.PATH_HARVESTED_DATA.glob('*.dat')]
    dirList = getReadyFiles(dirList)
    # dirDic = {}
    for item in dirList:
        info = LibDataTransfer.getInfoFile(item)
        print(f'The file {item.name} belows to site {info["site"]} and table {info["tableName"]} and will be stored in {info["tableStorage"]}, {info["pathShared"]}')
        if info['pathShared']:
            print('Moving file to shared folder')
            LibDataTransfer.moveAfileWOOW(item, info['pathShared'])


def removeDTinFileName(pathFile):
    if len(pathFile.stem)> 20:
        pathFile.rename(pathFile.with_name(pathFile.stem[:-16]+pathFile.suffix))
        print(f'Removed date and time from {pathFile.name}')


def resetFileNames():
    dirList = [x for x in consts.PATH_HARVESTED_DATA.glob('*.dat')]
    for item in dirList:
        removeDTinFileName(item)


if __name__ == '__main__':
    moveFiles()
