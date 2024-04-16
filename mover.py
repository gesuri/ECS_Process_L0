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
import pandas as pd



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


def dft(dt):
    dec_ms = dt.second+dt.microsecond/1e6
    for_ms = f'{int(dec_ms)}' if dec_ms%1 == 0 else f'{dec_ms:.1f}'
    return dt.strftime('%Y-%m-%d %H:%M:') + for_ms


def writeDF2csv(pathFile, dataframe, header):
    """ Write a dataframe to a csv file with multiline header """
    from csv import QUOTE_NONNUMERIC
    dataframe.index = dataframe.index.map(dft)
    with open(pathFile, 'w') as f:
        for line in header:
            f.write(line + '\n')
        dataframe.to_csv(f, header=False, index=True, na_rep=consts.FLAG, lineterminator='\n', quoting=QUOTE_NONNUMERIC)


def remove_trailing_zeros(timestamp_string):
    parts = timestamp_string.split('.')
    if len(parts) == 2:
        integer_part, fractional_part = parts
        fractional_part = fractional_part.rstrip('0')
        if fractional_part:
            result_string = f"{integer_part}.{fractional_part}"
        else:
            result_string = integer_part
    else:
        result_string = timestamp_string
    return result_string


def datetime_format(dt, numDec=1):
    """ Return the datetime in the format of the CS logger """
    print(f'* {dt.strftime("%Y-%m-%d %H:%M:%S.%f")}')
    if numDec == 0:
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    dec_sec = dt.second+dt.microsecond/1e6
    format_spec = f'04.{numDec}f'
    if dec_sec % 1 == 0:
        for_sec = f'{int(dec_sec):02.0f}'
    else:
        for_sec = f'{dec_sec:{format_spec}}'#.rstrip('0')
        print(f'   for_sec={for_sec}')
        for_sec = for_sec.rstrip('0')
        print(f'   for_sec={for_sec}')
    return dt.strftime('%Y-%m-%d %H:%M:') + for_sec


def datetime_format_2(dt, numDec=1):
    """ Return the datetime in the format of the CS logger """
    parts = dt.strftime("%Y-%m-%d %H:%M:%S.%f").split('.')
    date_part = parts[0]
    fractional_part = ''
    if len(parts) == 2:
        date_part, fractional_part = parts
        fractional_part = fractional_part[:numDec]
        fractional_part = fractional_part.rstrip('0')
    if fractional_part:
        result_string = f"{date_part}.{fractional_part}"
    else:
        result_string = date_part
    return result_string


def datetime_format_HF(dt):
    """ Return the datetime in the format of the CS logger for high frequency data like ts_data """
    dec_sec = dt.second+dt.microsecond/1e6
    for_sec = f'{int(dec_sec):02.0f}' if dec_sec%1 == 0 else f'{dec_sec:04.1f}'
    return dt.strftime('%Y-%m-%d %H:%M:') + for_sec


if __name__ == '__main__':
    #from pathlib import Path
    #import InfoFile
    #p_csv = Path(r'c:\temp\Collected\Bahada_CR3000_ts_data_2_test.csv')
    #p_dat = Path(r'c:\temp\Collected\Bahada_CR3000_ts_data_2_test_20231016_184040.dat')
    #info = InfoFile.InfoFile(p_dat)
    #writeDF2csv(p_csv, info.df, info.cs_headers)
    #a = remove_trailing_zeros('2023-10-16 18:40:40.00')
    dt = pd.to_datetime('2023-10-16 18:23:01.0109999')
    for item in range(0, 5):
        print(f'dec={item}, {datetime_format_2(dt, item)}')
    print(datetime_format_HF(dt))