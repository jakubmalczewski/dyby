import sys, os
from pathlib import Path
import yaml
import sqlite3
import hashlib
BUF_SIZE = 65536
from collections import namedtuple
from time import asctime, time_ns

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


    def add(self, fileName : str, name : str = None) -> None:
        if not name:
            name = fileName.split("/")[-1]
        path = str(Path(fileName).absolute())
        fileHash = file_hash(fileName)
        if self.is_in(fileName):
            print(f"[dyby] file: {fileName} already in database")
            return None
            
        con = sqlite3.connect(self.dbPath)
        cur = con.cursor()
        cur.execute("insert into files values (?, ?, ?, ?, ?)", 
            (name, fileHash, path, time_ns(), asctime()))
        con.commit()
        con.close()        
        print(f"[dyby] added record: {(name, path)}")

    def get(self, fileName : str = None, name : str = None) -> str:
        if not fileName and not name:
            return None
        
        con = sqlite3.connect(self.dbPath)
        cur = con.cursor()
        if fileName == None:
            cur.execute("SELECT path FROM files WHERE name=(?);", (name,))
        elif fileName != None and name == None:    
            cur.execute("SELECT path FROM files WHERE hash=(?);", (file_hash(fileName),))
        result = cur.fetchall()
        con.close()
        print(result)
            
        if result == []:
            print(f"[dyby] file {name if name else fileName} not found")
            return None
        
        return result[0][0]

    def is_in(self, fileName : str) -> bool:
        con = sqlite3.connect(self.dbPath)
        cur = con.cursor()
        cur.execute("SELECT COUNT(*) as count FROM files WHERE hash = (?);", 
            (file_hash(fileName), ))
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
