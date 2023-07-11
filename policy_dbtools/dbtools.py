"""Tools to handle connection to a MongoDB database."""

from pymongo import MongoClient
import os
from urllib.parse import quote_plus
import configparser

from policy_dbtools.config import logger


def set_config(username: str = None,
               password: str = None,
               cluster: str = None,
               db: str = None) -> None:
    """Set configuration file for MongoDB connection.

    This functions allows you to set the configuration and credentials for the MongoDB connection,
    including the username, password, cluster name, and database name which are stored in
    config.ini file.

    Args:
        username (str): Username to authenticate with.
        password (str): Password to authenticate with.
        cluster (str): Name of the MongoDB cluster to connect to.
        db (str): Name of the database to connect to.

    """

    config = configparser.ConfigParser()
    config.read('config.ini')

    # if config file does not exist, create it
    if not os.path.exists('config.ini'):
        config['MONGODB'] = {}

    # if username is provided, set in config file
    if username is not None:
        config['MONGODB']['MONGO_USERNAME'] = username
    # if password is provided, set in config file
    if password is not None:
        config['MONGODB']['MONGO_PASSWORD'] = password
    # if cluster_name is provided, set in config file
    if cluster is not None:
        config['MONGODB']['MONGO_CLUSTER'] = cluster
    # if db_name is provided, set in config file
    if db is not None:
        config['MONGODB']['MONGO_DB_NAME'] = db

    # write config file
    with open('config.ini', 'w') as configfile:
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

    return f"mongodb+srv://{quote_plus(username)}:{quote_plus(password)}" \
           f"@{cluster}.sln0w.mongodb.net/?retryWrites=true&w=majority"

# cluster: str = 'globalpolicy', db_name: str = 'policy_data'





class PolicyClient:
    """Establish a connection to a MongoDB database and handle authentication.
    """

    def __init__(self, cluster: str | None = None, db_name: str | None = None,
                 username: str = None, password: str = None):
        """
        """

        # set arguments to config file if provided
        set_config(username, password, cluster, db_name)

        # create uri
        config = configparser.ConfigParser()
        config.read('config.ini')
        # if username or password or cluster are not provided in config file, raise error
        if (config['MONGODB']['MONGO_USERNAME'] is None
            or config['MONGODB']['MONGO_PASSWORD'] is None
            or config['MONGODB']['MONGO_CLUSTER'] is None):

            raise ValueError("Username, password, and cluster name must provided or must be set using set_config().")

        config['MONGODB']['URI'] = _create_uri(config['MONGODB']['MONGO_CLUSTER'],
                                               config['MONGODB']['MONGO_USERNAME'],
                                               config['MONGODB']['MONGO_PASSWORD'])
        # write config file
        with open('config.ini', 'w') as configfile:
            config.write(configfile)

        self._client = None
        self.connect()

    def connect(self) -> "PolicyClient":
        """ """

        config = configparser.ConfigParser()
        config.read('config.ini')

        self._client = MongoClient(config['MONGODB']['URI'])

        # Send a ping to confirm a successful connection
        try:
            self._client.admin.command('ping')
            logger.info("Connection to MongoDB database established.")
        except Exception as e:
            raise e

        return self

    def close(self):
        """ """
        self._client.close()
        logger.info("Connection to MongoDB database closed.")

    def __enter__(self):
        """ """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """ """

        self._client.close()
        self._client = None
        self._db = None
        logger.info("Closing connection to MongoDB database.")

    @property
    def client(self) -> MongoClient:
        """ """
        return self._client







