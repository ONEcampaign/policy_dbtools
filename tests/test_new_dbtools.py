import configparser
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from policy_dbtools import dbtools
from policy_dbtools.dbtools import (
    AuthenticatedCursor,
    ConnectionFailure,
    _check_credentials,
    _create_uri,
    set_config,
    set_config_path,
)


# Fixture to reset CONFIG_PATH after each test
@pytest.fixture(autouse=True)
def reset_config_path():
    original_path = dbtools.CONFIG_PATH
    yield
    set_config_path(original_path)
    # Run teardown code after each test
    if os.path.exists(dbtools.CONFIG_PATH):
        os.remove(dbtools.CONFIG_PATH)


def test_set_config_path():
    """Test if set_config_path changes the CONFIG_PATH correctly."""
    # new temp path
    new_path = Path("./config.ini").resolve()

    # set it
    set_config_path(new_path)

    # check
    assert dbtools.CONFIG_PATH == new_path


def test_set_config_creates_file():
    """est set_config creates config.ini if it doesn't exist"""
    # set path
    path = Path("./config2.ini").resolve()
    set_config_path(path)
    # remove the file if it exists from previous tests
    if os.path.exists(path):
        os.remove(path)

    # try to set config
    set_config(username="test", password="test_pass", cluster="test_cluster")

    # check
    assert os.path.exists(path)


def test_set_config_updates_values():
    """Test set_config updates values correctly"""

    # Given
    path = Path("./config3.ini").resolve()
    set_config_path(path)
    config = configparser.ConfigParser()

    # When
    set_config(username="test", password="test_pass", cluster="test_cluster")
    config.read(path)

    # Then
    assert config["MONGODB"]["MONGO_USERNAME"] == "test"
    assert config["MONGODB"]["MONGO_PASSWORD"] == "test_pass"
    assert config["MONGODB"]["MONGO_CLUSTER"] == "test_cluster"


# -----------------------------------------------------------------------------------


def test_create_uri():
    """Test _create_uri by mocking inputs and checking if output string is correctly formatted"""
    # basics
    username = "test_user"
    password = "test_pass"
    cluster = "test_cluster"
    expected_uri = (
        "mongodb+srv://test_user:test_pass"
        "@test_cluster.sln0w.mongodb.net/?retryWrites=true&w=majority"
    )

    # When
    result_uri = _create_uri(cluster, username, password)

    # Then
    assert result_uri == expected_uri


def test_check_credentials_provided():
    """Test _check_credentials if credentials are provided, it should return them"""
    # Test parameters
    username = "test_user"
    password = "test_pass"
    cluster = "test_cluster"

    # check
    result = _check_credentials(username, password, cluster)

    # Then
    assert result["username"] == username
    assert result["password"] == password
    assert result["cluster"] == cluster


@patch("configparser.ConfigParser.read")
@patch("os.path.exists", return_value=True)
def test_check_credentials_from_config(mock_exists, mock_read):
    """Test _check_credentials to read from mock config file if credentials are not provided"""

    mock_config = {
        "MONGODB": {
            "MONGO_USERNAME": "config_user",
            "MONGO_PASSWORD": "config_pass",
            "MONGO_CLUSTER": "config_cluster",
        }
    }
    with patch.object(configparser.ConfigParser, "has_option", return_value=True):
        with patch.object(
            configparser.ConfigParser, "__getitem__", lambda _, key: mock_config[key]
        ):
            # When
            result = _check_credentials()

            # Then
            assert result["username"] == "config_user"
            assert result["password"] == "config_pass"
            assert result["cluster"] == "config_cluster"


def test_check_credentials_no_config_no_credentials():
    """Test _check_credentials to raise error if config file doesn't exist and no credentials"""
    # patch the check
    with patch("os.path.exists", return_value=False):
        # Then
        with pytest.raises(
            ValueError,
            match="No credentials provided and config.ini file does not exist.",
        ):
            _check_credentials()


def test_check_credentials_missing_config_credentials():
    """Test _check_credentials to raise error if certain credentials are missing
    from the config"""
    # config with missing credentials
    mock_config = {
        "MONGODB": {
            "MONGO_USERNAME": "config_user",
        }
    }
    # patch the path check
    with patch("os.path.exists", return_value=True):
        # patch the config read
        with patch.object(
            configparser.ConfigParser,
            "has_option",
            side_effect=lambda section, option: option in mock_config["MONGODB"],
        ):
            with patch.object(
                configparser.ConfigParser,
                "__getitem__",
                lambda _, key: mock_config[key],
            ):
                # Then
                with pytest.raises(
                    ValueError,
                    match="Missing credentials. "
                    "`password` must provided or set using set_config().",
                ):
                    _check_credentials()


# -----------------------------------------------------------------------------------


@patch("policy_dbtools.dbtools._check_credentials")
@patch("policy_dbtools.dbtools.MongoClient")
def test_check_connection_success(mock_mongo_client, mock_check_credentials):
    # Given
    mock_credentials = {
        "username": "mock_user",
        "password": "mock_pass",
        "cluster": "mock_cluster",
    }
    mock_check_credentials.return_value = mock_credentials

    mock_admin_instance = MagicMock()
    mock_client_instance = MagicMock()
    mock_client_instance.admin = mock_admin_instance

    # Mocking the MongoClient's __init__ to prevent it from attempting a connection
    mock_mongo_client.return_value = mock_client_instance

    # When: Mocking out the check_connection call during initialization
    with patch.object(AuthenticatedCursor, "check_connection", return_value=None):
        cursor = AuthenticatedCursor(
            username="mock_user", password="mock_pass", cluster="mock_cluster"
        )

    # Explicitly call check_connection for our test
    cursor.check_connection()

    # Then
    mock_admin_instance.command.assert_called_once_with("ping")
    mock_client_instance.close.assert_called_once()


# Test check_connection with a failed connection
@patch("policy_dbtools.dbtools.MongoClient")
def test_check_connection_failure(mock_mongo_client):
    # Given
    mock_mongo_client.side_effect = ConnectionFailure("Failed to connect")

    with patch.object(AuthenticatedCursor, "check_connection", return_value=None):
        cursor = AuthenticatedCursor(
            username="mock_user", password="mock_pass", cluster="mock_cluster"
        )

    # Then
    with pytest.raises(ConnectionFailure, match="Failed to connect"):
        cursor.check_connection()


# Test check_valid_db with a valid database
@patch("policy_dbtools.dbtools.MongoClient")
def test_check_valid_db_success(mock_mongo_client):
    # Given
    mock_client_instance = MagicMock()
    mock_client_instance.list_database_names.return_value = ["test_db"]

    # Mock the context manager methods (__enter__ and __exit__) for the MongoClient instance
    mock_mongo_client_instance = MagicMock()
    mock_mongo_client_instance.__enter__.return_value = mock_client_instance
    mock_mongo_client.return_value = mock_mongo_client_instance

    with patch.object(AuthenticatedCursor, "check_connection", return_value=None):
        cursor = AuthenticatedCursor(
            username="mock_user", password="mock_pass", cluster="mock_cluster"
        )

    # When
    result = cursor.check_valid_db("test_db")

    # Then
    assert result is True


# Test check_valid_db with an invalid database
@patch("policy_dbtools.dbtools.MongoClient")
def test_check_valid_db_failure(mock_mongo_client):
    # Given
    mock_client_instance = MagicMock()
    mock_client_instance.list_database_names.return_value = ["some_other_db"]

    # Mock the context manager methods (__enter__ and __exit__) for the MongoClient instance
    mock_mongo_client_instance = MagicMock()
    mock_mongo_client_instance.__enter__.return_value = mock_client_instance
    mock_mongo_client.return_value = mock_mongo_client_instance

    with patch.object(AuthenticatedCursor, "check_connection", return_value=None):
        cursor = AuthenticatedCursor(
            username="mock_user", password="mock_pass", cluster="mock_cluster"
        )

    # Then
    with pytest.raises(ValueError, match="Database test_db does not exist."):
        cursor.check_valid_db("test_db")


# Test check_valid_collection with a valid collection
@patch("policy_dbtools.dbtools.MongoClient")
def test_check_valid_collection_success(mock_mongo_client):
    # Given
    mock_db_instance = MagicMock()
    mock_db_instance.list_collection_names.return_value = ["test_collection"]

    mock_mongo_client_instance = MagicMock()
    mock_mongo_client_instance.__getitem__.return_value = mock_db_instance
    mock_mongo_client_instance.__enter__.return_value = mock_mongo_client_instance
    mock_mongo_client.return_value = mock_mongo_client_instance

    with patch.object(AuthenticatedCursor, "check_connection", return_value=None):
        cursor = AuthenticatedCursor(
            username="mock_user", password="mock_pass", cluster="mock_cluster"
        )

    # When
    result = cursor.check_valid_collection("test_collection", "test_db")

    # Then
    assert result is True


# Test check_valid_collection with an invalid collection
@patch("policy_dbtools.dbtools.MongoClient")
def test_check_valid_collection_failure(mock_mongo_client):
    # Given
    mock_client_instance = MagicMock()
    mock_db_instance = MagicMock()
    mock_db_instance.list_collection_names.return_value = ["some_other_collection"]
    mock_client_instance.__getitem__.return_value = mock_db_instance
    mock_mongo_client.return_value = mock_client_instance

    with patch.object(AuthenticatedCursor, "check_connection", return_value=None):
        cursor = AuthenticatedCursor(
            username="mock_user", password="mock_pass", cluster="mock_cluster"
        )

    # Then
    with pytest.raises(
        ValueError,
        match="Collection test_collection does not exist in database test_db.",
    ):
        cursor.check_valid_collection("test_collection", "test_db")


# Test connect and close methods
@patch("policy_dbtools.dbtools.MongoClient")
def test_connect_and_close(mock_mongo_client):
    # Given
    with patch.object(AuthenticatedCursor, "check_connection", return_value=None):
        cursor = AuthenticatedCursor(
            username="mock_user", password="mock_pass", cluster="mock_cluster"
        )

    # When
    cursor.connect()

    # Then
    assert cursor.client is not None  # Ensures client is instantiated

    # When
    cursor.close()

    # Then
    assert cursor.client is None  # Ensures client is set to None


# Test set_db and set_collection methods
@patch("policy_dbtools.dbtools.MongoClient")
def test_set_db_and_set_collection(mock_mongo_client):
    # Given
    mock_db_instance = MagicMock()
    mock_db_instance.list_collection_names.return_value = ["test_collection"]

    mock_mongo_client_instance = MagicMock()
    mock_mongo_client_instance.list_database_names.return_value = ["test_db"]
    mock_mongo_client_instance.__getitem__.return_value = mock_db_instance
    mock_mongo_client_instance.__enter__.return_value = mock_mongo_client_instance
    mock_mongo_client.return_value = mock_mongo_client_instance

    with patch.object(AuthenticatedCursor, "check_connection", return_value=None):
        cursor = AuthenticatedCursor(
            username="mock_user", password="mock_pass", cluster="mock_cluster"
        )

    # When
    cursor.set_db("test_db")

    # Then
    assert cursor._db_name == "test_db"

    # When
    cursor.set_collection("test_collection")

    # Then
    assert cursor._collection_name == "test_collection"
