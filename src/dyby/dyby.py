import sys
from pathlib import Path
import yaml

class FileTypeError(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class Dyby:
    def __init__(self) -> None:
        self.a = 1


def db(path = './dyby.yaml') -> Dyby:
    config = get_config(path)


def get_config(path : str) -> dict:
    configPath = Path(path)
    if configPath.exists():
        if configPath.is_dir():
            return get_config(str(path + "/dyby.yaml"))
        configPathStr = str(configPath)
        if configPathStr[-4:] == "yaml" or configPathStr[-3:] == "yml":
            return yaml.load(configPathStr, yaml.SafeLoader)
        else:
            raise FileTypeError(f"{path} is not a yaml config file.")
    else:
        create_config(configPath)
        return get_config(str(configPath))


def create_config(path : Path) -> None:
    with open(path, 'w') as file:
        yaml.dump({"test": 1}, file, yaml.SafeDumper)



    
    