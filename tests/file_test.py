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

def test_store_this():
    with tempfile.TemporaryDirectory() as tempDir:
        os.chdir(tempDir)
        db = dyby.db()
        db.store_this_file()
        thisFileHash = dyby.file_hash(sys.argv[0])
        print(thisFileHash)
        print(db.get())
        assert Path(f"./.dyby/store/{thisFileHash}").is_file()

def test_store_files():
    with tempfile.TemporaryDirectory() as tempDir:
        os.chdir(tempDir)
        db = dyby.db()

        with open("testfile1.txt", "w") as file:
            file.write("42")
        db.store_file("testfile1.txt")
        hash1 = dyby.file_hash("testfile1.txt")

        with open("testfile1.txt", "a") as file:
            file.write("84")
        db.store_file("testfile1.txt")
        hash2 = dyby.file_hash("testfile1.txt")
        
        assert Path(f"./.dyby/store/{hash1}").is_file()
        assert Path(f"./.dyby/store/{hash2}").is_file()


# def test_store():
#     with tempfile.TemporaryDirectory() as tempDir:
#         os.chdir(tempDir)
#         db = dyby.db()
#         db.store({"var": 1})