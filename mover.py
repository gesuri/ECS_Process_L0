# This script will list all the files from a folder and it will classify by its name where is the site and table name.
# Then each file will be renamed with the date and time at the end of the file name. Then the file will be moved to
# the corresponding folder based on the site.
# This code uses the LibDataTransfer.py to get the metadata of each file used to classify the file.


import getopt
import sys
#from pathlib import Path
#from deepdiff import DeepDiff
import LibDataTransfer
import consts
import Log
import systemTools

log = Log.Log(name='mover', path=consts.PATH_LOGS, timestamp=True, fprint=True)


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


def getReadyFiles(dirList):
    newDirList = []
    for item in dirList:
        log.live(f'Renaming {item.name}')
        nf = LibDataTransfer.renameAFileWithDate(item)
        if nf:
            newDirList.append(nf)
    return newDirList


def updateSharedTable(currentTable):
    """ This function will receive a dict from LibDataTransfer.getInfoFile() as currentTable and will check what is the
    'pathShared' and will read it with LibDataTransfer.getInfoFile() as sharedTable to get the metadata of the file.
    Then it will check the 'header' of currentTable and sharedTable and if there are different, it will show the different
    and will log it in the log file per table, and then the sharedTable file will be renamed with the date and time.
    a new sharedTable file will be created with the header and data of the currentTable file.
    If the both headers are the same, the sharedTable file will be open to append at the end of the file and the data of
    the currentTable file will be appended at the end of the sharedTable file."""
    sharedTable = LibDataTransfer.getInfoFile(currentTable['pathShared'])
    diff = differenceHeader(currentTable['header'], sharedTable['header'])
    if len(diff) > 0:
        log.info(f'For site {sharedTable["site"]} the table {sharedTable["tableName"]} changed: {diff}')
        # The file on sharedTable['pathShared'] is going to be renamed with the datetime by using
        newName = LibDataTransfer.renameAFileWithDate(sharedTable['pathShared'])
        sharedTable['log'].info(f'The table {sharedTable["tableName"]} was renamed to {newName.name} because it was a '
                                f'change on the header')
        if newName:
            # create a new file with the header of the currentTable and the data of the currentTable
            # the file will be created with the name of the sharedTable['pathShared'] and it will be writen with the
            # header of the currentTable
            with open(sharedTable['pathShared'], 'w') as f:
                f.write(currentTable['header'])
        else:
            msg = f'Error renaming the file {sharedTable["pathShared"]}'
            log.error(msg)
            sharedTable['log'].error(msg)
    # the file on sharedTable['pathShared'] is going to be open to append the data of the currentTable
    # the sharedTable['pathShared
    if currentTable['firstLineDT'] > sharedTable['lastLineDT']:
        # the currentTable file has data that is not on the sharedTable file
        # the data of the currentTable file will be appended at the end of the sharedTable file
        with open(sharedTable['pathShared'], 'a') as fShared:
            with open(currentTable['path'], 'r') as fCurrent:
                for item in range(len(consts.CS_FILE_HEADER_LINE)):
                    fCurrent.readline()
                fShared.write(fCurrent.read())
                while line := fCurrent.readline():
                    # check the datetime of the line to see if there is a need to create a new file based on the date
                    lineDT = LibDataTransfer.getDTfromLine(line)
                    fShared.write(line)
    else:
        # the currentTable file has data that is already on the sharedTable file
        # the data of the currentTable file will be ignored
        msg = f'The table {sharedTable["tableName"]} has data that is already on the sharedTable file'
        log.info(msg)
        sharedTable['log'].info(msg)


def differenceHeader(headerA, headerB):
    """ Compare the two headers line pero line using differenceLine.
    Return a string per line if there is a difference.
    The headerA or B is a list of strings """
    result = ''
    for item in range(len(consts.CS_FILE_HEADER_LINE)):
        line = differenceLine(headerA[item], headerB[item])
        if len(line) > 0:
            result += ' and ' + line
    return result


def differenceLine(lineA, lineB):
    ''' Check the difference of two strings and return the diff '''
    A = set(LibDataTransfer.getStrippedHeaderLine(lineA))
    B = set(LibDataTransfer.getStrippedHeaderLine(lineB))
    aDiff = A.difference(B)
    bDiff = B.difference(A)
    if len(aDiff) > 0:
        print(f'This line changed from {aDiff} to {bDiff}')
        return f'[{",".join(aDiff)}] --> [{", ".join(bDiff)}]'
    return ''


def moveFiles():
    dirList = [x for x in consts.PATH_HARVESTED_DATA.glob('*.dat')]
    #dirList = getReadyFiles(dirList)
    # dirDic = {}
    for item in dirList:
        info = LibDataTransfer.getInfoFile(item)
        print(
            f'The file {item.name} belows to site {info["site"]} and table {info["tableName"]} and will be stored in {info["tableStorage"]}, {info["pathShared"]}')

        print('Moving file to shared folder')
        # LibDataTransfer.moveAfileWOOW(item, info['pathShared'])


def removeDTinFileName(pathFile):
    if len(pathFile.stem) > 20:
        pathFile.rename(pathFile.with_name(pathFile.stem[:-16] + pathFile.suffix))
        print(f'Removed date and time from {pathFile.name}')


def resetFileNames():
    dirList = [x for x in consts.PATH_HARVESTED_DATA.glob('*.dat')]
    for item in dirList:
        removeDTinFileName(item)


if __name__ == '__main__':
    moveFiles()
