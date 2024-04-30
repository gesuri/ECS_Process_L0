# This code check if the given folder there are duplicated files.
# this is done calculating the hash of the file and comparing with the hash of the other files.

import hashlib
from pathlib import Path


def generate_file_md5(filePath, blocksize=2**20):
    filePath = Path(filePath)
    m = hashlib.md5()
    with filePath.open('rb') as f:
        while True:
            buf = f.read(blocksize)
            if not buf:
                break
            m.update(buf)
    return m.hexdigest()


def checkDuplicatedFiles(folder):
    folder = Path(folder)
    files = [x for x in folder.glob('*.*')]
    hashDic = {}
    for file in files:
        print(f'Checking file {file.name}')
        hash = generate_file_md5(file)
        print(f'Hash {hash}')
        if hash in hashDic:
            print(f'File {file.name} is duplicated with {hashDic[hash].name}')
        else:
            hashDic[hash] = file
    return hashDic


if __name__ == '__main__':
    pd = Path('.')
    hashDic = checkDuplicatedFiles(pd)
