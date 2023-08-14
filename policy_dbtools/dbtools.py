"""Tools to handle connection to a MongoDB database."""
import pandas as pd
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
import os
from urllib.parse import quote_plus
import configparser
from pathlib import Path
from pymongo.errors import ConnectionFailure

from policy_dbtools.config import logger


CONFIG_PATH = Path(__file__).parent / "config.ini"


def set_config_path(path: str) -> None:
    """Set the path to the config.ini file."""

    global CONFIG_PATH
    CONFIG_PATH = Path(path).resolve()


def set_config(username: str = None, password: str = None, cluster: str = None) -> None:
    """Set configuration file for MongoDB connection.

    This functions allows you to set the configuration and credentials for the MongoDB connection,
    including the username, password, cluster name, and database name which are stored in
    config.ini file.

    Args:
        username (str): Username to authenticate with.
        password (str): Password to authenticate with.
        cluster (str): Name of the MongoDB cluster to connect to.
    """

    config = configparser.ConfigParser()
    config.read(CONFIG_PATH)

    # if config file does not exist, create it
    if not os.path.exists(CONFIG_PATH):
        config["MONGODB"] = {}

    # if username is provided, set in config file
    if username is not None:
        config["MONGODB"]["MONGO_USERNAME"] = username
    # if password is provided, set in config file
    if password is not None:
        config["MONGODB"]["MONGO_PASSWORD"] = password
    # if cluster_name is provided, set in config file
    if cluster is not None:
        config["MONGODB"]["MONGO_CLUSTER"] = cluster

    # write config file
    with open(CONFIG_PATH, "w") as configfile:
        config.write(configfile)


def _create_uri(cluster: str, username: str, password: str) -> str:
    """Create a MongoDB connection string.

    Args:
        cluster (str): Name of the MongoDB cluster to connect to.
        username (str): Username to authenticate with.
        password (str): Password to authenticate with.

    Returns:
        str: MongoDB connection string.
    """

    return (
        f"mongodb+srv://{quote_plus(username)}:{quote_plus(password)}"
        f"@{cluster}.sln0w.mongodb.net/?retryWrites=true&w=majority"
    )


def _check_credentials(
    username: str = None, password: str = None, cluster: str = None
) -> dict:
    """Checks credentials required for MongoDB connection.

    If credentials are not provided, they will attempt to be read from the configuration file.
    If the credentials are not provided in the configuration file, an error will be raised.

    Args:
        username (str): Username to authenticate with.
        password (str): Password to authenticate with.
        cluster (str): Name of the MongoDB cluster to connect to.

    Returns:
        Dictionary containing the username, password, and cluster name.
    """

    # if an argument is not provided check the config file
    if (username is None) | (password is None) | (cluster is None):
        if not os.path.exists(CONFIG_PATH):
            raise ValueError(
                "No credentials provided and config.ini file does not exist."
                "Provide credentials or set credentials using set_config()."
            )
        else:
            config = configparser.ConfigParser()
            config.read(CONFIG_PATH)

            # if username is not provided
            if username is None:
                if not config.has_option("MONGODB", "MONGO_USERNAME"):
                    raise ValueError(
                        "Missing credentials. `username` must provided or "
                        "set using set_config()."
                    )
                username = config["MONGODB"]["MONGO_USERNAME"]

            # if password is not provided
            if password is None:
                if not config.has_option("MONGODB", "MONGO_PASSWORD"):
                    raise ValueError(
                        "Missing credentials. `password` must provided or "
                        "set using set_config()."
                    )
                password = config["MONGODB"]["MONGO_PASSWORD"]

            # if cluster is not provided
            if cluster is None:
                if not config.has_option("MONGODB", "MONGO_CLUSTER"):
                    raise ValueError(
                        "Missing credentials. `cluster` must provided or "
                        "set using set_config()."
                    )
                cluster = config["MONGODB"]["MONGO_CLUSTER"]

    return {"username": username, "password": password, "cluster": cluster}


class AuthenticatedCursor:
    """An authenticated cursor to a MongoDB database.

    This wrapper class for pymongo.MongoClient provides authentication to connect to a MongoDB
    database. It can be used as a context manager to automatically connect and close the
    connection to the database. It can also be used as a regular class to connect and close
    the connection manually. You can access the pymongo.MongoClient object using the `client`
    attribute and perform any operations available to that object. Credentials can be provided
    at instantiation or read from a configuration file if not passed as arguments. Use the
    function `set_config()` to set the configuration file.

     Args:
        username: Username to authenticate with. If not provided, will attempt to read from
            config.ini file.
        password: Password to authenticate with. If not provided, will attempt to read from
            config.ini file.
        cluster: Name of the MongoDB cluster to connect to. If not provided, will attempt to
            read from config.ini file.
        collection_name: Name of the collection to connect to. This is optional and can be set
            later.
        db_name: Name of the database to connect to. This is optional and can be set later
    """

    def __init__(
        self,
        username: str = None,
        password: str = None,
        cluster: str = None,
        db_name: str = None,
        collection_name: str = None,
    ):
        """Initialize the AuthenticatedCursor object."""

        credentials = _check_credentials(username, password, cluster)
        self.__uri = _create_uri(**credentials)

        # test connection to the cluster
        self.check_connection()
        self._client = None  # client object

        self._db_name = None
        if db_name is not None:
            self.set_db(db_name)

        self._collection_name = None
        if collection_name is not None:
            self.set_collection(collection_name)

    def check_connection(self) -> None:
        """Test connection to MongoDB database."""

        try:
            client = MongoClient(self.__uri)
            client.admin.command("ping")
            client.close()
            logger.info("Connection to MongoDB database authenticated.")
        except ConnectionFailure as e:
            raise e
        except Exception as e:
            raise e

    def check_valid_collection(self, collection_name: str, db_name: str) -> bool:
        """Check if a collection exists in a database. If it does not, raise an error.

        Args:
            collection_name: Name of the collection to check.
            db_name: Name of the database to check.

        Returns:
            True if the collection exists.
        """

        with MongoClient(self.__uri) as client:
            collection_list = client[db_name].list_collection_names()
            if collection_name not in collection_list:
                raise ValueError(
                    f"Collection {collection_name} does not exist in database {db_name}."
                )
        logger.info(f"Collection authenticated: {collection_name}")
        return True

    def check_valid_db(self, db_name: str) -> bool:
        """Check if a database exists. If it does not, raise an error.

        Args:
            db_name: Name of the database to check


        Returns:
            True if the database exists.
        """

        with MongoClient(self.__uri) as client:
            db_list = client.list_database_names()
            if db_name not in db_list:
                raise ValueError(f"Database {db_name} does not exist.")

        logger.info(f"Database authenticated: {db_name}")
        return True

    def connect(self):
        """Connect to the MongoDB database."""

        self._client = MongoClient(self.__uri)

    def close(self):
        """Close connection to the MongoDB database."""

        self._client.close()
        self._client = None

    def __enter__(self):
        """Enter context manager. Connect to the MongoDB database."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Exit context manager."""

        if self._client is not None:
            self.close()
        if exc_type is not None:
            logger.exception(
                "Exception occurred", exc_info=(exc_type, exc_value, traceback)
            )

    @property
    def client(self) -> MongoClient:
        """MongoDB client object."""
        return self._client

    def set_db(self, db_name: str):
        """Set the database to connect to."""

        self.check_valid_db(db_name=db_name)
        self._db_name = db_name

    @property
    def db(self) -> Database | None:
        """MongoDB client object."""

        if self._db_name is None:
            return None
        if self._client is None:
            return None
        return self._client[self._db_name]

    def set_collection(self, collection_name: str) -> None:
        """Set the collection to connect to."""

        if self._db_name is None:
            raise ValueError("Database not set. Use set_db() to set the database.")
        self.check_valid_collection(collection_name, self._db_name)
        self._collection_name = collection_name

    @property
    def collection(self) -> Collection | None:
        """ """
        if self._collection_name is None:
            return None
        if self._client is None:
            return None
        return self.db[self._collection_name]


class PolicyReader:
    """Class to read data from a MongoDB database."""

    def __init__(self, cursor: AuthenticatedCursor):
        """Initialize the PolicyReader object."""
        self.cursor = cursor

        # database have been set
        if self.cursor._db_name is None:
            raise ValueError(
                "Database not set. Use set_db() on the Authenticated cursor object"
                " to set the database."
            )

    def get_df(self, query: dict | None = None) -> pd.DataFrame:
        """Get collection as a pandas DataFrame.

        Args:
            query: Query to filter the collection.

        Returns:
            A pandas DataFrame with the collection data.
        """
        if query is None:
            query = {}

        with self.cursor as cursor:
            return pd.DataFrame(cursor.collection.find(query))


class PolicyWriter:
    """Class to write data from a MongoDB database."""

    pass
