"""Tests for dbtools.py"""

import pytest
import os
import configparser

from policy_dbtools import dbtools


@pytest.fixture(autouse=True)
def setup_teardown():
    # Run setup code before each test
    config_file_path = 'config.ini'
    yield
    # Run teardown code after each test
    if os.path.exists(config_file_path):
        os.remove(config_file_path)


def test_set_config_without_arguments_config_exists():
    """Test set_config() with no arguments when config file exists with some arguments"""

    # set config file with some data
    original_config_data = {'MONGO_CLUSTER': 'mycluster',
                            'MONGO_DB_NAME': 'mydb'}
    config = configparser.ConfigParser()
    config['MONGODB'] = original_config_data
    with open('config.ini', 'w') as configfile:
        config.write(configfile)

    # set config file with no arguments
    dbtools.set_config()

    # check that the config file is unchanged
    config = configparser.ConfigParser()
    config.read('config.ini')
    assert config['MONGODB']['MONGO_CLUSTER'] == original_config_data['MONGO_CLUSTER']
    assert config['MONGODB']['MONGO_DB_NAME'] == original_config_data['MONGO_DB_NAME']


def test_set_config_without_arguments_config_does_not_exist():
    """Test set_config() with no arguments when config file does not exist"""

    # set config file with no arguments
    dbtools.set_config()

    # check that the config file is is created with no data
    config = configparser.ConfigParser()
    config.read('config.ini')
    assert config['MONGODB'] == {}


def test_set_config_with_arguments_config_exists():
    """Test set_config() with arguments when config file exists with some arguments
    In this test the arguments passed to set_config should be different from the
    data in the config file, to ensure that new data is added without old data being overwritten
    """

    # set config file with some data
    original_config_data = {'MONGO_CLUSTER': 'mycluster'}
    config = configparser.ConfigParser()
    config['MONGODB'] = original_config_data
    with open('config.ini', 'w') as configfile:
        config.write(configfile)

    # run set_config with some arguments
    dbtools.set_config(username='myusername', password='mypassword')

    # check that the config file is updated with the new data
    config = configparser.ConfigParser()
    config.read('config.ini')
    assert config['MONGODB']['MONGO_CLUSTER'] == original_config_data['MONGO_CLUSTER']
    assert config['MONGODB']['MONGO_USERNAME'] == 'myusername'
    assert config['MONGODB']['MONGO_PASSWORD'] == 'mypassword'


def test_set_config_overwrite():
    """Test set_config() with arguments that override data that exists in the config file"""

    # set config file with some data
    original_config_data = {'MONGO_CLUSTER': 'mycluster',
                            'MONGO_DB_NAME': 'mydb'}
    config = configparser.ConfigParser()
    config['MONGODB'] = original_config_data
    with open('config.ini', 'w') as configfile:
        config.write(configfile)

    # run set_config with some arguments
    dbtools.set_config(username='myusername', password='mypassword', cluster='myothercluster', db='myotherdb')

    # check that the config file is updated with the new data
    config = configparser.ConfigParser()
    config.read('config.ini')
    assert config['MONGODB']['MONGO_CLUSTER'] == 'myothercluster'
    assert config['MONGODB']['MONGO_DB_NAME'] == 'myotherdb'
    assert config['MONGODB']['MONGO_USERNAME'] == 'myusername'
    assert config['MONGODB']['MONGO_PASSWORD'] == 'mypassword'



