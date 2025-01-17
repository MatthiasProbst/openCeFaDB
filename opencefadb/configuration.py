import configparser
import logging
import pathlib
from typing import Union

from opencefadb import paths

logger = logging.getLogger("opencefadb")

_config = None


def _get_default_database_config():
    return {
        "metadata_dir": paths['global_package_dir'] / 'database/metadata',
        "rawdata_dir": paths['global_package_dir'] / 'database/rawdata',
        "rawdata_store": "hdf5_sql_db",
        "metadata_store": "rdf_file_db",
        "log_level": str(logging.ERROR),
        "log_file": paths['global_package_dir'] / 'opencefadb.log',
    }


def _get_test_config():
    return {
        "rawdata_store": "hdf5_sql_db",
        "metadata_store": "rdf_file_db",
        "log_level": str(logging.DEBUG)
    }


def _get_test_local_graphdb():
    return {
        "rawdata_store": "hdf5_sql_db",
        "metadata_store": "local_graphdb",
        "log_level": str(logging.DEBUG),
        "graphdb.host": "localhost",
        "graphdb.port": "7201",
        "graphdb.user": "admin",
        "graphdb.password": "admin",
        "graphdb.repository": "test",
    }


def _get_test_local_sql():
    return {
        "rawdata_store": "hdf5_sql_db",
        "log_level": str(logging.DEBUG),
        "sql.host": "localhost",
        "sql.port": "5432",
    }


class OpenCeFaDBConfiguration:

    def __init__(self, configparser: configparser.ConfigParser, filename: pathlib.Path):
        self._configparser = configparser
        self.filename = filename
        stp = get_setup()
        self.profile = stp.profile

    def __repr__(self):
        return f"<{self.__class__.__name__} ({self.profile})>"

    def __str__(self) -> str:
        out = f"Configuration profile: {self.profile}\n"
        for k in self[self.profile]:
            out += f" > {k}: {self[self.profile][k]}\n"
        return out

    def delete(self):
        global _config
        _bak = paths["config"].with_suffix(".ini.bak")
        if _bak.exists():
            _bak.unlink()
        paths["config"].rename(paths["config"].with_suffix(".ini.bak"))
        _config = None
        return get_config()

    @property
    def setup(self):
        return get_setup().profile

    def keys(self):
        return self._configparser[self.profile].keys()

    def select_profile(self, profile: str):
        if profile not in self._configparser:
            raise ValueError(f"Profile {profile} not found in config file")
        get_setup().profile = profile
        self.profile = profile

    def __getitem__(self, item):
        return self._configparser[self.profile][item]

    def __contains__(self, item):
        return item in self._configparser[self.profile]

    def _set_config(self, section: Union[str], key, value):
        """Set a configuration value."""
        self._configparser[str(section)][key] = value
        with open(paths["config"], 'w') as f:
            self._configparser.write(f)
        return self

    @property
    def logging_level(self):
        """Get the logging configuration."""
        log_level = self._configparser[self.profile]["log_level"]
        try:
            return int(log_level)
        except ValueError:
            return str(log_level)

    @logging_level.setter
    def logging_level(self, value):
        """Set the logging configuration."""
        self._set_config(self.profile, 'log_level', value)

    @property
    def metadata_directory(self):
        return pathlib.Path(self._configparser[self.profile]['metadata_dir'])

    @property
    def rawdata_directory(self):
        return pathlib.Path(self._configparser[self.profile]['rawdata_dir'])

    @property
    def rawdata_store(self):
        try:
            return self._configparser[self.profile]['rawdata_store']
        except KeyError:
            raise KeyError("The selected profile does not have a rawdata_store configuration")

    @property
    def metadata_datastore(self):
        try:
            return self._configparser[self.profile]['metadata_store']
        except KeyError:
            raise KeyError("The selected profile does not have a metadata_store configuration")


def get_config(overwrite: bool = False) -> OpenCeFaDBConfiguration:
    """Initialize the configuration."""
    global _config
    if _config:
        return _config

    logger.debug(f"Initializing config file {paths['config']}...")
    if paths["config"].exists() and not overwrite:
        logger.debug(f"Config file {paths['config']} already exists")
        _config = OpenCeFaDBConfiguration(_read_config(), paths["config"])
        return _config

    config = configparser.ConfigParser()
    config["DEFAULT"] = _get_default_database_config()
    config["test"] = _get_test_config()
    config["local_graphdb.test"] = _get_test_local_graphdb()
    config["local_sql.test"] = _get_test_local_sql()
    with open(paths["config"], 'w') as f:
        config.write(f)
    _config = OpenCeFaDBConfiguration(config, paths["config"])
    return _config


def _read_config() -> configparser.ConfigParser:
    """Read the configuration."""
    logger.debug(f"Reading config file {paths['config']}...")
    config = configparser.ConfigParser()
    assert paths["config"].exists(), f"Config file {paths['config']} does not exist"
    config.read(paths["config"])
    return config


class OpenCeFaDBSetup:
    def __init__(self, _configparser: configparser.ConfigParser):
        self._configparser = _configparser

    @property
    def profile(self):
        return self._configparser["DEFAULT"]["profile"]

    @profile.setter
    def profile(self, value):
        self._configparser["DEFAULT"]["profile"] = value
        with open(paths["setup"], 'w') as f:
            self._configparser.write(f)

    def delete(self):
        _bak = paths["setup"].with_suffix(".ini.bak")
        if _bak.exists():
            _bak.unlink()
        paths["setup"].rename(paths["setup"].with_suffix(".ini.bak"))
        return self


def _read_setup() -> configparser.ConfigParser:
    """Read the configuration."""
    logger.debug(f"Reading setup file {paths['setup']}...")
    config = configparser.ConfigParser()
    assert paths["setup"].exists(), f"Setup file {paths['config']} does not exist"
    config.read(paths["setup"])
    return config


def get_setup(overwrite: bool = False) -> OpenCeFaDBSetup:
    logger.debug(f"Initializing setup file {paths['config']}...")
    if paths["setup"].exists() and not overwrite:
        logger.debug(f"Setup file {paths['setup']} already exists")
        return OpenCeFaDBSetup(_read_setup())

    config = configparser.ConfigParser()
    config["DEFAULT"] = {
        "profile": "DEFAULT"
    }
    with open(paths["setup"], 'w') as f:
        config.write(f)
    return OpenCeFaDBSetup(config)
