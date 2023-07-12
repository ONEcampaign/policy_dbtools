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

def test_set_config_without_arguments():
    # Ensure the function doesn't modify the existing config file
    config = configparser.ConfigParser()
    config.read('config.ini')
    original_config_data = config['MONGODB'].copy()

    dbtools.set_config()

    config.read('config.ini')
    assert config['MONGODB'] == original_config_data

def test_set_config_with_arguments():
    dbtools.set_config(username='myusername', password='mypassword', cluster='mycluster', db='mydb')

    config = configparser.ConfigParser()
    config.read('config.ini')

    assert config['MONGODB']['MONGO_USERNAME'] == 'myusername'
    assert config['MONGODB']['MONGO_PASSWORD'] == 'mypassword'
    assert config['MONGODB']['MONGO_CLUSTER'] == 'mycluster'
    assert config['MONGODB']['MONGO_DB_NAME'] == 'mydb'

def test_set_config_no_existing_file():
    config_file_path = 'config.ini'

    if os.path.exists(config_file_path):
        os.remove(config_file_path)

    dbtools.set_config(username='myusername', password='mypassword', cluster='mycluster', db='mydb')

    assert os.path.exists(config_file_path)

    config = configparser.ConfigParser()
    config.read(config_file_path)

    assert config['MONGODB']['MONGO_USERNAME'] == 'myusername'
    assert config['MONGODB']['MONGO_PASSWORD'] == 'mypassword'
    assert config['MONGODB']['MONGO_CLUSTER'] == 'mycluster'
    assert config['MONGODB']['MONGO_DB_NAME'] == 'mydb'

