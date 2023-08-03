"""Tests for dbtools.py"""

import pytest
from unittest.mock import patch, Mock, MagicMock
from pymongo.database import Database
from pymongo.collection import Collection
import os
import configparser
from pathlib import Path
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

from policy_dbtools import dbtools


@pytest.fixture(autouse=True)
def setup_teardown():
    # Run setup code before each test
    config_file_path = "config.ini"
    yield
    # Run teardown code after each test
    if os.path.exists(config_file_path):
        os.remove(config_file_path)
    if os.path.exists(dbtools.CONFIG_PATH):
        os.remove(dbtools.CONFIG_PATH)


def test_set_config_path():
    """Test set_config_path"""

    # set config file path
    config_file_path = "config.ini"
    dbtools.set_config_path(config_file_path)

    # check that the config file path is set
    assert dbtools.CONFIG_PATH == Path(config_file_path).resolve()


class TestSetConfig:
    dbtools.set_config_path("tests/config.ini")

    def test_set_config_without_arguments_config_exists(self):
        """Test set_config() with no arguments when config file exists with some arguments"""

        # set config file with some data
        original_config_data = {"MONGO_CLUSTER": "mycluster"}
        config = configparser.ConfigParser()
        config["MONGODB"] = original_config_data
        with open(dbtools.CONFIG_PATH, "w") as configfile:
            config.write(configfile)

        # set config file with no arguments
        dbtools.set_config()

        # check that the config file is unchanged
        config = configparser.ConfigParser()
        config.read(dbtools.CONFIG_PATH)
        assert (
            config["MONGODB"]["MONGO_CLUSTER"] == original_config_data["MONGO_CLUSTER"]
        )

    def test_set_config_without_arguments_config_does_not_exist(self):
        """Test set_config() with no arguments when config file does not exist"""

        # set config file with no arguments
        dbtools.set_config()

        # check that the config file is is created with no data
        config = configparser.ConfigParser()
        config.read(dbtools.CONFIG_PATH)
        assert config["MONGODB"] == {}

    def test_set_config_with_arguments_config_exists(self):
        """Test set_config() with arguments when config file exists with some arguments

        In this test the arguments passed to set_config should be different from the
        data in the config file, to ensure that new data is added without old data being overwritten
        """

        # set config file with some data
        original_config_data = {"MONGO_CLUSTER": "mycluster"}
        config = configparser.ConfigParser()
        config["MONGODB"] = original_config_data
        with open(dbtools.CONFIG_PATH, "w") as configfile:
            config.write(configfile)

        # run set_config with some arguments
        dbtools.set_config(username="myusername", password="mypassword")

        # check that the config file is updated with the new data
        config = configparser.ConfigParser()
        config.read(dbtools.CONFIG_PATH)
        assert (
            config["MONGODB"]["MONGO_CLUSTER"] == original_config_data["MONGO_CLUSTER"]
        )
        assert config["MONGODB"]["MONGO_USERNAME"] == "myusername"
        assert config["MONGODB"]["MONGO_PASSWORD"] == "mypassword"

    def test_set_config_overwrite(self):
        """Test set_config() with arguments that override data that exists in the config file"""

        # set config file with some data
        original_config_data = {
            "MONGO_CLUSTER": "mycluster",
            "MONGO_USERNAME": "myusername",
        }
        config = configparser.ConfigParser()
        config["MONGODB"] = original_config_data
        with open(dbtools.CONFIG_PATH, "w") as configfile:
            config.write(configfile)

        # run set_config with some arguments
        dbtools.set_config(
            password="mypassword",
            cluster="myothercluster",
        )

        # check that the config file is updated with the new data
        config = configparser.ConfigParser()
        config.read(dbtools.CONFIG_PATH)
        assert config["MONGODB"]["MONGO_CLUSTER"] == "myothercluster"
        assert config["MONGODB"]["MONGO_USERNAME"] == "myusername"
        assert config["MONGODB"]["MONGO_PASSWORD"] == "mypassword"


def test_create_uri_no_special_characters():
    """Test _create_uri when no special characters are in the string arguemnts"""

    # Test case 1: No special characters, quote_plus not needed
    cluster = "my-cluster"
    username = "my-username"
    password = "my-password"
    expected_uri = (
        "mongodb+srv://my-username:my-password@my-cluster.sln0w.mongodb.net/"
        "?retryWrites=true&w=majority"
    )

    assert dbtools._create_uri(cluster, username, password) == expected_uri


def test_create_uri_special_characters():
    """Test _create_uri when special characters are in the string arguemnts
    for which quote_plus is needed
    """

    cluster = "my-cluster"
    username = "my+username"
    password = "my&password"
    expected_uri = (
        "mongodb+srv://my%2Busername:my%26password@my-cluster"
        ".sln0w.mongodb.net/?retryWrites=true&w=majority"
    )

    assert dbtools._create_uri(cluster, username, password) == expected_uri


class TestCheckCredentials:
    """Tests for _check_credentials"""

    dbtools.set_config_path("tests/config.ini")

    def test_no_credentials_and_no_config_error(self):
        """Test that _check_credentials raises an error when no credentials are passed
        and no config file exists"""

        with pytest.raises(
            ValueError,
            match="No credentials provided and config.ini file does not exist.",
        ):
            dbtools._check_credentials()

    def test_no_credential_config_exists(self):
        """Test that an error is raised when no username is passed and the
        config file does not contain a username

        This error should be raise when the user has not set credentials using set_config()
        """

        dbtools.set_config_path("config.ini")
        dbtools.set_config(
            username="myusername", password="mypassword", cluster="mycluster"
        )

        # assert os.path.exists(dbtools.CONFIG_PATH)

        assert dbtools._check_credentials() == {
            "username": "myusername",
            "password": "mypassword",
            "cluster": "mycluster",
        }

    def test_credentials_passed_no_config(self):
        """Test _check_credentials when all credentials are passed to the function"""

        assert dbtools._check_credentials(
            username="myusername", password="mypassword", cluster="mycluster"
        ) == {
            "username": "myusername",
            "password": "mypassword",
            "cluster": "mycluster",
        }

    def test_1_credential_passed_others_in_config(self):
        """Test _check_credentials when one credential is passed to the function
        and the others are in the config file"""

        dbtools.set_config(cluster="mycluster")

        assert dbtools._check_credentials(
            username="myusername", password="mypassword"
        ) == {
            "username": "myusername",
            "password": "mypassword",
            "cluster": "mycluster",
        }

    def test_raise_error_missing_credential(self):
        """test _check_credentials when a credential is not passed to the function
        and it does not exist in the config file"""

        dbtools.set_config(cluster="mycluster")

        with pytest.raises(
            ValueError,
            match="Missing credentials. `password` must provided or "
            "set using set_config().",
        ):
            dbtools._check_credentials(username="myusername")


def test_test_connection_successful():
    """Test test_connection() with a successful connection"""

    # Create a Mock MongoClient
    client = Mock(spec=MongoClient)

    # Create a Mock for the `admin` attribute and its `command` method
    admin = Mock()
    admin.command.return_value = {"ok": 1.0}
    client.admin = admin

    # Call the test function
    dbtools.check_connection(client)

    # Assert that the MongoClient's `admin.command` method was called with "ping"
    admin.command.assert_called_once_with("ping")

    # Assert that the `close` method was called once
    client.close.assert_called_once()


def test_test_connection_failed():
    """Test test_connection() with a failed connection"""
    # Create a Mock MongoClient
    client = Mock(spec=MongoClient)

    # Create a Mock for the `admin` attribute and its `command` method
    admin = Mock()
    admin.command.side_effect = ConnectionFailure("Connection failed")
    client.admin = admin

    # Call the test function and expect it to raise a ConnectionFailure exception
    with pytest.raises(ConnectionFailure):
        dbtools.check_connection(client)

    # Assert that the MongoClient's `admin.command` method was called with "ping"
    client.admin.command.assert_called_once_with("ping")


def test_test_connection_other_failed():
    """Test test_connection() with a failed connection"""
    # Create a Mock MongoClient
    client = Mock(spec=MongoClient)

    # Create a Mock for the `admin` attribute and its `command` method
    admin = Mock()
    admin.command.side_effect = Exception("Connection failed")
    client.admin = admin

    # Call the test function and expect it to raise a ConnectionFailure exception
    with pytest.raises(Exception):
        dbtools.check_connection(client)

    # Assert that the MongoClient's `admin.command` method was called with "ping"
    client.admin.command.assert_called_once_with("ping")


def test_check_valid_db():
    """Test check_valid_db function"""

    # Create a Mock MongoClient
    client = Mock(spec=MongoClient)
    client.list_database_names.return_value = ["test_db"]
    #
    # assert client.check_valid_db("test_db", client)
    #
    # # check error
    # with pytest.raises(ValueError):
    #     client.check_valid_db("test_db_2", client)


class TestCheckValidCollection:
    """Tests for check_valid_collection"""

    def mock_client(self, mock_mongo_client):
        # Mock the MongoClient and Database instances
        mock_client_instance = mock_mongo_client.return_value
        mock_database_instance = Mock(spec=Database)
        mock_client_instance.__getitem__.return_value = mock_database_instance

        # mock the close method
        mock_client_instance.close.return_value = None

        mock_client_instance.list_database_names.return_value = ["test_db"]
        mock_database_instance.list_collection_names.return_value = ["test_collection"]

        return mock_client_instance

    @patch("policy_dbtools.dbtools.MongoClient")
    def test_check_valid_collection(self, mock_mongo_client):
        """ """

        mock_client_instance = self.mock_client(mock_mongo_client)
        assert (
            dbtools.check_valid_collection(
                "test_collection", "test_db", mock_client_instance
            )
            is True
        )

        with pytest.raises(ValueError):
            dbtools.check_valid_collection(
                "test_collection_invalid", "test_db", mock_client_instance
            )


class TestAuthenticatedCursor:
    """Tests for the AuthentificatedCursor class"""

    def mock_client(self, mock_mongo_client):
        # Mock the MongoClient and Database instances
        mock_client_instance = mock_mongo_client.return_value

        mock_database_instance = MagicMock(spec=Database)
        mock_client_instance.__getitem__.return_value = mock_database_instance

        # Mock collection
        mock_collection_instance = MagicMock(spec=Collection)
        mock_database_instance.__getitem__.return_value = mock_collection_instance

        # Create a Mock for the `admin` attribute and its `command` method
        admin = Mock()
        admin.command.return_value = {"ok": 1.0}
        mock_client_instance.admin = admin

        # mock the close method
        mock_client_instance.close.return_value = None

        # mock the list_database_names method
        mock_client_instance.list_database_names.return_value = ["test_db"]
        mock_database_instance.list_collection_names.return_value = ["test_collection"]

        return mock_client_instance

    @patch("policy_dbtools.dbtools.MongoClient")
    def test_init(self, mock_mongo_client):
        """Test the instantiation of the class"""

        mock_client_instance = self.mock_client(mock_mongo_client)

        cursor = dbtools.AuthenticatedCursor(
            username="test_user",
            password="test_password",
            cluster="test_cluster",
            db_name="test_db",
            collection_name="test_collection",
        )

        # check that __uri is not None
        assert cursor._AuthenticatedCursor__uri is not None
        mock_client_instance.admin.command.assert_called_once_with(
            "ping"
        )  # check that the connection was tested

        assert cursor._db_name == "test_db"  # check that the db name is set
        mock_client_instance.list_database_names.assert_called_once()  # check that the db was checked

        assert (
            cursor._collection_name == "test_collection"
        )  # check that the collection name is set
        mock_client_instance.__getitem__.assert_called_once_with(
            "test_db"
        )  # check that the collection was checked

    @patch("policy_dbtools.dbtools.MongoClient")
    def test_init_no_credentials(self, mock_mongo_client):
        """test the instantiation of the class when no credentials are passed
        but exist in a config file"""

        mock_client_instance = self.mock_client(mock_mongo_client)

        # set up config file
        dbtools.set_config_path("config.ini")
        dbtools.set_config(
            username="myusername", password="mypassword", cluster="mycluster"
        )

        cursor = dbtools.AuthenticatedCursor()
        assert (
            cursor._AuthenticatedCursor__uri is not None
        )  # check that the hidden attribute __uri is set
        mock_client_instance.admin.command.assert_called_once_with(
            "ping"
        )  # check that the connection was tested

    @patch("policy_dbtools.dbtools.MongoClient")
    def test_connect(self, mock_mongo_client):
        """Test the connect method"""

        cursor = dbtools.AuthenticatedCursor(
            username="test_user", password="test_password", cluster="test_cluster"
        )

        mock_client_instance = self.mock_client(mock_mongo_client)

        cursor.connect()
        assert cursor.client == mock_client_instance

    @patch("policy_dbtools.dbtools.MongoClient")
    def test_close_connection(self, mock_mongo_client):
        """Test the close method"""

        cursor = dbtools.AuthenticatedCursor(
            username="test_user", password="test_password", cluster="test_cluster"
        )

        cursor.connect()
        cursor.close()

        assert cursor.client is None

    @patch("policy_dbtools.dbtools.MongoClient")
    def test_context_manager(self, mock_mongo_client):
        """Test PolicyClient as a context manager"""

        mock_client_instance = self.mock_client(mock_mongo_client)

        # Initialize the PolicyClient
        with dbtools.AuthenticatedCursor(
            username="test_user", password="test_password", cluster="test_cluster"
        ) as cursor:
            cursor.connect()

            # Assertions
            assert (
                cursor._AuthenticatedCursor__uri is not None
            )  # check that the hidden attribute __uri is set
            assert cursor.client == mock_client_instance

        # assert that the the close method was called 3 times
        assert mock_client_instance.close.call_count == 2

    @patch("policy_dbtools.dbtools.MongoClient")
    def test_set_db(self, mock_mongo_client):
        """Test setting the database name"""

        self.mock_client(mock_mongo_client)

        cursor = dbtools.AuthenticatedCursor(
            username="test_user", password="test_password", cluster="test_cluster"
        )

        cursor.set_db("test_db")
        assert cursor._db_name == "test_db"

    @patch("policy_dbtools.dbtools.MongoClient")
    def test_db(self, mock_mongo_client):
        """Test the db property"""

        mock_mongo_client = self.mock_client(mock_mongo_client)

        cursor = dbtools.AuthenticatedCursor(
            username="test_user", password="test_password", cluster="test_cluster"
        )

        cursor.connect()

        cursor.set_db("test_db")
        assert cursor.db == mock_mongo_client["test_db"]
        assert isinstance(cursor.db, Database)

    @patch("policy_dbtools.dbtools.MongoClient")
    def test_set_collection(self, mock_mongo_client):
        """Test setting the collection"""

        self.mock_client(mock_mongo_client)
        cursor = dbtools.AuthenticatedCursor(
            username="test_user", password="test_password", cluster="test_cluster"
        )

        cursor._db_name = "test_db"

        cursor.set_collection("test_collection")
        assert cursor._collection_name == "test_collection"

    @patch("policy_dbtools.dbtools.MongoClient")
    def test_collection(self, mock_mongo_client):
        """Test the collection property"""

        mock_mongo_client = self.mock_client(mock_mongo_client)

        cursor = dbtools.AuthenticatedCursor(
            username="test_user", password="test_password", cluster="test_cluster"
        )

        cursor.connect()
        cursor._db_name = "test_db"
        cursor._collection_name = "test_collection"

        assert isinstance(cursor.collection, Collection)
        assert cursor.collection == mock_mongo_client["test_db"]["test_collection"]
