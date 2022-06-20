from pathlib import Path
import sys
import os

path_file = Path(__file__)
path_dyby = str(path_file.absolute().parents[1]) + "/src"
if path_dyby not in sys.path:
    sys.path.append(path_dyby)

import dyby
import tempfile
import pytest
import sqlite3


def test_config_is_folder():
    with tempfile.TemporaryDirectory() as tempDirName:
        db = dyby.db(tempDirName)
        assert Path(tempDirName + "/dyby.yaml").is_file()


def test_config_is_not_yaml():
    from dyby import FileTypeError
    with tempfile.TemporaryDirectory() as tempDir:
        os.chdir(tempDir)
        filePath = tempDir + "/temp.yam"
        with open(filePath, "w") as tempFile:
            tempFile.write("42\n")
        try:
            dyby.db(filePath)
        except FileTypeError:
            pass
        else:
            assert False


def test_create_config():
    import yaml
    with tempfile.TemporaryDirectory() as tempDir:
        os.chdir(tempDir)
        dbConfig = dyby.get_config("dyby.yaml")  
        assert Path("./dyby.yaml").exists()
        
        with open("dyby.yaml") as file:
            config = yaml.load(file, yaml.SafeLoader)
        assert type(config) == dict
        assert type(config["dybypath"] == str )


def test_prepare_dyby():
    with tempfile.TemporaryDirectory() as tempDir:
        os.chdir(tempDir)
        dyby.prepare_dyby("dyby")
        assert Path("dyby").is_dir()
        con = sqlite3.connect("dyby/dyby.db")
        cur = con.cursor()
        cur.execute("INSERT INTO files VALUES ('test',1548581324584,'/test',1,1)")
        assert len(list(cur.execute('SELECT * FROM files'))) == 1
        con.close()


def test_Dyby_content():
    with tempfile.TemporaryDirectory() as tempDir:
        os.chdir(tempDir)
        db = dyby.db()
        assert Path(db.config["dybypath"]).is_dir()


def test_add_get_file():
    with tempfile.TemporaryDirectory() as tempDir:
        os.chdir(tempDir)
        db = dyby.db()
        with open("testfile1.txt", "w") as file:
            file.write("42")
        with open("testfile2.txt", "w") as file:
            file.write("84")
        db.add("testfile1.txt", name = "testfile")
        assert db.count() == 1
        db.add("testfile1.txt")
        assert db.count() == 1
        db.add("testfile1.txt")
        assert db.count() == 1
        db.add("testfile2.txt")
        assert db.count() == 2
        with open("testfile1.txt", "a") as file:
            file.write("42")
        db.add("testfile1.txt")
        assert db.count() == 3
        
        assert Path(db.get(name = "testfile")).is_file()
        assert Path(db.get(fileName = "testfile1.txt")).is_file()


if __name__ == "__main__":
    test_config_is_not_yaml()