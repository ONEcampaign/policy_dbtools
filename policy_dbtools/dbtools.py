"""Tools to handle connection to a MongoDB database."""

from pymongo import MongoClient
import os
from urllib.parse import quote_plus
import configparser
from pathlib import Path

from policy_dbtools.config import logger

CONFIG_PATH = Path(__file__).parent / "config.ini"


def set_config_path(path: str) -> None:
    """Set the path to the config.ini file."""

    global CONFIG_PATH
    CONFIG_PATH = Path(path).resolve()


def set_config(
    username: str = None, password: str = None, cluster: str = None
) -> None:
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


def _check_credentials(username: str = None, password: str = None, cluster: str = None) -> dict:
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
    if username is None or password is None or cluster is None:
        if not os.path.exists(CONFIG_PATH):
            raise ValueError("No credentials provided and config.ini file does not exist."
                             "Provide credentials or set credentials using set_config().")
        else:
            config = configparser.ConfigParser()
            config.read(CONFIG_PATH)

            # if username is not provided
            if username is None:
                if not config.has_option("MONGODB", "MONGO_USERNAME"):
                    raise ValueError("Missing credentials. `username` must provided or "
                                     "set using set_config().")
                username = config["MONGODB"]["MONGO_USERNAME"]

            # if password is not provided
            if password is None:
                if not config.has_option("MONGODB", "MONGO_PASSWORD"):
                    raise ValueError("Missing credentials. `password` must provided or "
                                     "set using set_config().")
                password = config["MONGODB"]["MONGO_PASSWORD"]

            # if cluster is not provided
            if cluster is None:
                if not config.has_option("MONGODB", "MONGO_CLUSTER"):
                    raise ValueError(
                        "Missing credentials. `cluster` must provided or "
                        "set using set_config().")
                cluster = config["MONGODB"]["MONGO_CLUSTER"]

    return {"username": username, "password": password, "cluster": cluster}


class PolicyCursor:
    """Class to handle connection to a MongoDB database.

    This class handles the connection to a ONE Policy MongoDB database. It can be used as a context
    manager to automatically close the connection when the context is exited. It can also be used
    as a regular class, in which case the connection must be closed manually using the
    close() method. A connection is established when the object is initialized. Credentials can be
    provided when the object is initialized or they can be read from the config.ini file. If the
    config file does not exist and credentials are not provided, an error will be raised.

    Args:
        username: Username to authenticate with. If not provided, will attempt to read from
            config.ini file.
        password: Password to authenticate with. If not provided, will attempt to read from
            config.ini file.
        cluster: Name of the MongoDB cluster to connect to. If not provided, will attempt to
            read from config.ini file.
    """

    def __init__(self, username: str = None, password: str = None, cluster: str = None):
        """Initialize the PolicyCursor object."""

        self._client = None
        self.connect(username, password, cluster)

    def connect(self, username: str = None, password: str = None, cluster: str = None) -> "PolicyCursor":
        """Connect to the MongoDB cluster.

        Args:
            username: Username to authenticate with. If not provided, will attempt to read from
                config.ini file.
            password: Password to authenticate with. If not provided, will attempt to read from
                config.ini file.
            cluster: Name of the MongoDB cluster to connect to. If not provided, will attempt to
                read from config.ini file.

        Returns:
            PolicyCursor object.
        """

        credentials = _check_credentials(username, password, cluster)
        uri = _create_uri(**credentials)
        self._client = MongoClient(uri)

        # Send a ping to confirm a successful connection
        try:
            self._client.admin.command("ping")
            logger.info("Connection to MongoDB database established.")
        except Exception as e:
            raise e

        return self

    def close(self) -> None:
        """Close connection to the MongoDB cluster."""

        self._client.close()
        logger.info("Connection to MongoDB database closed.")

    def __enter__(self):
        """Enter context manager."""

        logger.info("Entering context")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Exit context manager."""

        logger.info("Exiting context")
        self.close()
        if exc_type is not None:
            logger.exception(
                "Exception occurred", exc_info=(exc_type, exc_value, traceback)
            )

    @property
    def client(self) -> MongoClient:
        """MongoDB client object."""
        return self._client


class PolicyReader:
    """Class to read data from a MongoDB database."""

    def __init__(self, client: MongoClient):
        """Initialize the PolicyReader object."""

        self._client = client


class PolicyWriter:
    """Class to write data from a MongoDB database."""

    def __init__(self, client: MongoClient):
        """Initialize the PolicyReader object."""

        self._client = client

