"""Tests for dbtools.py"""

import pytest
from unittest.mock import patch, Mock
from unittest import mock
import unittest
import os
import configparser
import pymongo


from policy_dbtools import dbtools


@pytest.fixture(autouse=True)
def setup_teardown():
    # Run setup code before each test
    config_file_path = "config.ini"
    yield
    # Run teardown code after each test
    if os.path.exists(config_file_path):
        os.remove(config_file_path)


def test_set_config_without_arguments_config_exists():
    """Test set_config() with no arguments when config file exists with some arguments"""

    # set config file with some data
    original_config_data = {"MONGO_CLUSTER": "mycluster", "MONGO_DB_NAME": "mydb"}
    config = configparser.ConfigParser()
    config["MONGODB"] = original_config_data
    with open("config.ini", "w") as configfile:
        config.write(configfile)

    # set config file with no arguments
    dbtools.set_config()

    # check that the config file is unchanged
    config = configparser.ConfigParser()
    config.read("config.ini")
    assert config["MONGODB"]["MONGO_CLUSTER"] == original_config_data["MONGO_CLUSTER"]
    assert config["MONGODB"]["MONGO_DB_NAME"] == original_config_data["MONGO_DB_NAME"]


def test_set_config_without_arguments_config_does_not_exist():
    """Test set_config() with no arguments when config file does not exist"""

    # set config file with no arguments
    dbtools.set_config()

    # check that the config file is is created with no data
    config = configparser.ConfigParser()
    config.read("config.ini")
    assert config["MONGODB"] == {}


def test_set_config_with_arguments_config_exists():
    """Test set_config() with arguments when config file exists with some arguments
    In this test the arguments passed to set_config should be different from the
    data in the config file, to ensure that new data is added without old data being overwritten
    """

    # set config file with some data
    original_config_data = {"MONGO_CLUSTER": "mycluster"}
    config = configparser.ConfigParser()
    config["MONGODB"] = original_config_data
    with open("config.ini", "w") as configfile:
        config.write(configfile)

    # run set_config with some arguments
    dbtools.set_config(username="myusername", password="mypassword")

    # check that the config file is updated with the new data
    config = configparser.ConfigParser()
    config.read("config.ini")
    assert config["MONGODB"]["MONGO_CLUSTER"] == original_config_data["MONGO_CLUSTER"]
    assert config["MONGODB"]["MONGO_USERNAME"] == "myusername"
    assert config["MONGODB"]["MONGO_PASSWORD"] == "mypassword"


def test_set_config_overwrite():
    """Test set_config() with arguments that override data that exists in the config file"""

    # set config file with some data
    original_config_data = {"MONGO_CLUSTER": "mycluster", "MONGO_DB_NAME": "mydb"}
    config = configparser.ConfigParser()
    config["MONGODB"] = original_config_data
    with open("config.ini", "w") as configfile:
        config.write(configfile)

    # run set_config with some arguments
    dbtools.set_config(
        username="myusername",
        password="mypassword",
        cluster="myothercluster",
        db="myotherdb",
    )

    # check that the config file is updated with the new data
    config = configparser.ConfigParser()
    config.read("config.ini")
    assert config["MONGODB"]["MONGO_CLUSTER"] == "myothercluster"
    assert config["MONGODB"]["MONGO_DB_NAME"] == "myotherdb"
    assert config["MONGODB"]["MONGO_USERNAME"] == "myusername"
    assert config["MONGODB"]["MONGO_PASSWORD"] == "mypassword"


def test_create_uri_no_special_characters():
    """Test _create_uri when no special characters are in the string arguemnts"""

    # Test case 1: No special characters, quote_plus not needed
    cluster = "my-cluster"
    username = "my-username"
    password = "my-password"
    expected_uri = "mongodb+srv://my-username:my-password@my-cluster.sln0w.mongodb.net/?retryWrites=true&w=majority"

    assert dbtools._create_uri(cluster, username, password) == expected_uri


def test_create_uri_special_characters():
    """Test _create_uri when special characters are in the string arguemnts for which quote_plus is needed"""

    cluster = "my-cluster"
    username = "my+username"
    password = "my&password"
    expected_uri = "mongodb+srv://my%2Busername:my%26password@my-cluster.sln0w.mongodb.net/?retryWrites=true&w=majority"

    assert dbtools._create_uri(cluster, username, password) == expected_uri


class TestPolicyClient:
    @patch("policy_dbtools.dbtools.MongoClient")
    def test_policy_client_init(self, mock_mongo_client):
        # Mock the MongoClient object
        mock_client_instance = Mock()
        mock_mongo_client.return_value = mock_client_instance

        # Initialize the PolicyClient
        policy_client = dbtools.PolicyClient(
            username="test_user",
            password="test_password",
            cluster="test_cluster",
            db="test_db",
        )

        # Assertions

        assert policy_client.client == mock_client_instance

        # Clean up
        policy_client.close()

    @patch("policy_dbtools.dbtools.MongoClient")
    def test_policy_client_init_no_arguments(self, mock_mongo_client):
        """Test when no arguments are passed to the PolicyClient constructor and
        the config file exists with some data"""

        # Mock the MongoClient object
        mock_client_instance = Mock()
        mock_mongo_client.return_value = mock_client_instance

        # Set config file with some data
        original_config_data = {
            "MONGO_CLUSTER": "mycluster",
            "MONGO_USERNAME": "myusername",
            "MONGO_PASSWORD": "mypassword",
        }
        config = configparser.ConfigParser()
        config["MONGODB"] = original_config_data
        with open("config.ini", "w") as configfile:
            config.write(configfile)

        # Initialize the PolicyClient
        policy_client = dbtools.PolicyClient()

        # Assertions
        assert policy_client.client == mock_client_instance

        # Clean up
        policy_client.close()

    def test_policy_client_init_no_arguments_config_does_not_exist(self):
        """Test when no arguments are passed to the PolicyClient constructor and
        the config file does not exist"""

        # test that a Value error is raised
        with pytest.raises(ValueError):
            # Initialize the PolicyClient
            _ = dbtools.PolicyClient()

    @patch("policy_dbtools.dbtools.MongoClient")
    def test_context_manager(self, mock_mongo_client):
        """Test PolicyClient as a context manager"""

        # Mock the MongoClient object
        mock_client_instance = Mock()
        mock_mongo_client.return_value = mock_client_instance

        # Initialize the PolicyClient
        with dbtools.PolicyClient(
            username="test_user",
            password="test_password",
            cluster="test_cluster",
            db="test_db",
        ) as policy_client:
            # Assertions
            assert policy_client.client == mock_client_instance

        # assert that the closed method was called
        mock_client_instance.close.assert_called_once()
