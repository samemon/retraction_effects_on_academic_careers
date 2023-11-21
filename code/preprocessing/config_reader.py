# config_reader.py
from configparser import ConfigParser, ExtendedInterpolation

CONFIG_FILE_PATH = 'preprocessing_config.ini'

def read_config():
    # preprocessing the config file
    parser = ConfigParser(interpolation=ExtendedInterpolation())
    parser.read(CONFIG_FILE_PATH)
    return parser['Paths']