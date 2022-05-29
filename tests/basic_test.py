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

def test_config_is_folder():
    with tempfile.TemporaryDirectory() as tempDirName:
        db = dyby.db(tempDirName)
        assert Path(tempDirName + "/dyby.yaml").is_file()


def test_config_is_not_yaml():
    from dyby import FileTypeError
    with tempfile.TemporaryDirectory() as tempDir:
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
        
        config = yaml.load("dyby.yaml", yaml.SafeLoader)
        assert config == dbConfig


if __name__ == "__main__":
    test_config_is_not_yaml()