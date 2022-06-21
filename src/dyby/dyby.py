from argparse import ArgumentError
import sys, os
from pathlib import Path
import yaml
import sqlite3
import hashlib
BUF_SIZE = 65536
from collections import namedtuple
from time import asctime, time_ns
import shutil

emptyConfig = """
---
dybypath : ".dyby"
test : "test"
"""

Record = namedtuple("Record", ["name", "hash", "path"])



class FileTypeError(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class Dyby:
    def __init__(self, config) -> None:
        self.config = config
        self.dbPath = str(Path(config["dybypath"] + "/dyby.db").absolute())
        self.storePath = str(Path(config["dybypath"] + "/store/").absolute())

    def add(self, fileName : str, tag : str = None, fileHash : str = None) -> None:
        if not tag:
            tag = fileName.split("/")[-1]
        path = str(Path(fileName).absolute())
        fileHash = fileHash if fileHash else file_hash(fileName)
        if self.is_in(fileName):
            print(f"[dyby] file: {fileName} already in database")
            return None
            
        con = sqlite3.connect(self.dbPath)
        cur = con.cursor()
        cur.execute("insert into files values (?, ?, ?, ?, ?)", 
            (tag, fileHash, path, time_ns(), asctime()))
        con.commit()
        con.close()        
        print(f"[dyby] added record: {(tag, path)}")
        return fileHash


    def get(self, fileName : str = None, tag : str = None) -> str:
        if not fileName and not tag:
            return None
        
        con = sqlite3.connect(self.dbPath)
        cur = con.cursor()
        if fileName == None:
            cur.execute("SELECT path FROM files WHERE name=(?);", (tag,))
        elif fileName != None and tag == None:    
            cur.execute("SELECT path FROM files WHERE hash=(?);", (file_hash(fileName),))
        result = cur.fetchall()
        con.close()
        print(result)
            
        if result == []:
            print(f"[dyby] file {tag if tag else fileName} not found")
            return None
        elif len(result) == 1:
            return result[0][0]
        else:
            return [r[0] for r in result]


    def is_in(self, fileName : str = None, fileHash : str = None) -> bool:
        if not fileName and not fileHash:
            return ArgumentError("fileName or fileHash required")

        con = sqlite3.connect(self.dbPath)
        cur = con.cursor()
        cur.execute("SELECT COUNT(*) as count FROM files WHERE hash = (?);", 
            (fileHash if fileHash else file_hash(fileName), ))
        result = cur.fetchall()[0][0]
        con.close()
        return result == 1

    def count(self,) -> int:
        con = sqlite3.connect(self.dbPath)
        cur = con.cursor()
        cur.execute("SELECT COUNT(*) as count FROM files;")
        result = cur.fetchall()[0][0]
        con.close() 
        return result

    def store_this_file(self,) -> bool:
        return self.store_file(sys.argv[0])


    def store_file(self, fileName : str) -> bool:
        fileHash = file_hash(fileName)
        targetPath = f"{self.storePath}/{fileHash}"
        shutil.copyfile(fileName, targetPath)
        if Path(targetPath).is_file():
            self.add(fileName, fileHash = fileHash)
            if self.is_in(fileHash = fileHash):
                print(f"[dyby] stored file {fileName}")
                print(targetPath)
                return True
        return False


def db(path = './dyby.yaml') -> Dyby:
    config = get_config(path)
    prepare_dyby(config["dybypath"])

    return Dyby(config)

def file_hash(fileName) -> str:
    md5 = hashlib.md5()
    with open(fileName, "rb") as file:
        while True:
            data = file.read(BUF_SIZE)
            if not data:
                break
            md5.update(data)
    return str(md5.hexdigest())


def get_config(path : str) -> dict:
    configPath = Path(path)
    if configPath.exists():
        if configPath.is_dir():
            return get_config(configPath.joinpath(Path("dyby.yaml")))
        configPathStr = str(configPath)
        if configPathStr[-4:] == "yaml" or configPathStr[-3:] == "yml":
            with open(configPathStr, 'r') as file:
                return yaml.load(file, yaml.SafeLoader)
        else:
            raise FileTypeError(f"{path} is not a yaml config file.")
    else:
        write_config(str(configPath))
        return get_config(str(configPath))


def write_config(path : str) -> None:
    with open(path, 'w') as file:
        file.write(emptyConfig)


def prepare_dyby(path : str) -> None:
    path = Path(path)
    if not path.exists():
        os.mkdir(path)
    elif not path.is_dir():
        raise NotADirectoryError(path) 
    
    storePath =  Path(path).joinpath("store")
    if not storePath.exists():
        os.mkdir(storePath)
    elif not storePath.is_dir():
        raise NotADirectoryError(storePath)

    dbpath = Path(path).joinpath("dyby.db").absolute()
    if not dbpath.exists():
        create_db(dbpath)
    else:
        con = sqlite3.connect(dbpath)
        con.close()
    

def create_db(path : str) -> None:
    con = sqlite3.connect(path)
    cur = con.cursor()
    cur.execute('''CREATE TABLE files
               (name, hash, path, time, nicetime)''')
    con.commit()
    con.close()
