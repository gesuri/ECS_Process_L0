from pathlib import Path
import time

#a = Path('Pecan5R_CR6_Time_Series.dat.1.backup')
b = Path('Pecan5R_CR6_Time_Series')
#c = Path('Pecan5R_CR6_Time_Series.dat')

def blocks(files, size=65536):
    while True:
        b = files.read(size)
        if not b: break
        yield b


def numLines(filePath):
    with open(filePath, "r", encoding="utf-8", errors='ignore') as f:
        return sum(bl.count("\n") for bl in blocks(f))



			
			
def renameAFileWithDate(pathFile):
    """ Rename a file with the created date """
    pathFile = Path(pathFile)
    if pathFile.is_file():
        addName = '_' + time.strftime('%Y%m%d_%H%M%S',
                                      time.gmtime(pathFile.stat().st_ctime - (60 * 60 * 7)))
        if addName in pathFile.stem:
            print(f'The file already have the date in the name {pathFile.name}')
            return pathFile
        completeName = pathFile.parent.joinpath(pathFile.stem + addName + pathFile.suffix)
        try:
            pathFile.rename(completeName)
        except (PermissionError, WindowsError) as error:
            try:
                pathFile.rename(completeName.parent.joinpath(f'{completeName.stem}_{strftime("%Y%m%d_%H%M%S", time.gmtime(time.time() - time.timezone))}{completeName.suffix}'))
            except (PermissionError, WindowsError) as error:
                msg = f'Not possible to rename the file{pathFile} because {error}'
                print(msg)
                return False
        return completeName
    else:
        msg = f'Not a file {pathFile}'
        print(msg)
        return False
        
        
def splitFile(filePath, numLines):
    #filePath = renameAFileWithDate(filePath)
    lines_per_file = numLines
    smallfile = None
    numF = 0
    with open(filePath) as bigfile:
        head = [next(bigfile) for _ in range(4)]
        for lineno, line in enumerate(bigfile):
            if lineno % lines_per_file == 0:
                if smallfile:
                    smallfile.close()
                    print(f'Closed {small_filename}')
                small_filename = f'{filePath.stem}_{numF}{filePath.suffix}'
                smallfile = open(small_filename, "w")
                for item in head:
                    smallfile.write(item)
                numF = numF + 1
            smallfile.write(line)
        if smallfile:
            smallfile.close()
            #renameAFileWithDate(small_filename)
            
splitFile(b, 5853226)