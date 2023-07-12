"""Tools to handle connection to a MongoDB database."""

from pymongo import MongoClient
import os
from urllib.parse import quote_plus
import configparser

from policy_dbtools.config import logger


def set_config(
    username: str = None, password: str = None, cluster: str = None, db: str = None
) -> None:
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
    config.read("config.ini")

    # if config file does not exist, create it
    if not os.path.exists("config.ini"):
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
    # if db_name is provided, set in config file
    if db is not None:
        config["MONGODB"]["MONGO_DB_NAME"] = db

    # write config file
    with open("config.ini", "w") as configfile:
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


class PolicyClient:
    """Class to handle connection to a ONE Policy MongoDB database.

    This class handles the connection to a ONE Policy MongoDB database. It can be used as a context
    manager to automatically close the connection when the context is exited. It can also be used as a
    regular class, in which case the connection must be closed manually using the close() method.
    It facilitates authentication be setting the optional arguments username, password, cluster, and db_name
    to a configuration file used to create the connection string. If these arguments are not provided,
    the credentials will try to be read from the config.ini file. If the config.ini file does not exist,
    an error will be raised. Optionally, the set_config() function can be used to set the credentials.
    If credentials are set there is no need to provide them as arguments to the class.

    """

    def __init__(
        self,
        username: str = None,
        password: str = None,
        cluster: str | None = None,
        db: str | None = None,
    ):
        """Create a PolicyClient object and connect to the MongoDB cluster. If no arguments are provided, the credentials will attempt to be
        read from the configuration file. If the credentials are not provided in the configuration file
        file, an error will be raised.

        Args:
            username (str): Username to authenticate with. Defaults to None.
            password (str): Password to authenticate with. Defaults to None.
            cluster (str): Name of the MongoDB cluster to connect to. Defaults to None.
            db (str): Name of the database to connect to. Defaults to None.
        """

        # set arguments to config file if provided
        set_config(username, password, cluster, db)

        # create uri
        config = configparser.ConfigParser()
        config.read("config.ini")
        # if username or password or cluster are not provided in config file, raise error
        if (
            not config.has_option("MONGODB", "MONGO_USERNAME")
            or not config.has_option("MONGODB", "MONGO_PASSWORD")
            or not config.has_option("MONGODB", "MONGO_CLUSTER")
        ):
            raise ValueError(
                "Missing credentials. `username`, `password` and `cluster`"
                " must provided or set using set_config()."
            )

        config["MONGODB"]["URI"] = _create_uri(
            config["MONGODB"]["MONGO_CLUSTER"],
            config["MONGODB"]["MONGO_USERNAME"],
            config["MONGODB"]["MONGO_PASSWORD"],
        )
        # write config file
        with open("config.ini", "w") as configfile:
            config.write(configfile)

        self._client: MongoClient | None = None
        self._db: MongoClient | None = None

        self.connect()

        # if db_name is provided, set the database
        if db is not None:
            self.set_db(db)

        # if db_name is not provided, try to read it from the config file
        elif config.has_option("MONGODB", "MONGO_DB_NAME"):
            self.set_db(config["MONGODB"]["MONGO_DB_NAME"])

    def connect(self):
        """Connect to the MongoDB cluster.

        Returns:
            PolicyClient: PolicyClient object.
        """

        # get connection string from the config file and connect to the cluster
        config = configparser.ConfigParser()
        config.read("config.ini")
        self._client = MongoClient(config["MONGODB"]["URI"])

        # Send a ping to confirm a successful connection
        try:
            self._client.admin.command("ping")
            logger.info("Connection to MongoDB database established.")
        except Exception as e:
            raise e

    def set_db(self, db_name: str = None) -> "PolicyClient":
        """Set and connect to a database

        This method allows a user to set the dabase and connect to it. If no database name
        is provided, it will attempt to read the database name from the configuration file.
        If a database name is provided, it will be set in the configuration file. If no database
        name is provided and it does not exist in the configuration file, an error will be raised.

        Args:
            db_name (str): Name of the database to connect to. Defaults to None.
        """

        if db_name is None:
            # read config file
            config = configparser.ConfigParser()
            config.read("config.ini")
            # check if db_name is in config file
            if config.has_option("MONGODB", "MONGO_DB_NAME"):
                db_name = config["MONGODB"]["MONGO_DB_NAME"]
            else:
                raise ValueError(
                    "Missing database name. `db_name` must provided or set in config file."
                    " Use the method `set_db()` passing in the db name to set the database"
                    " or use the function `set_config()` to set the db name in the config file."
                )

        # connect to the database
        self._db = self._client[db_name]
        # set db_name in config file
        set_config(db=db_name)
        logger.info(f"Connected to database {db_name}.")
        return self

    def close(self):
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

    @property
    def db(self) -> MongoClient:
        """MongoDB database object. If no database has been set, an error will be raised."""

        if self._db is None:
            raise AttributeError(
                "No database has been set. Use the method `set_db()` to set and connect to a "
                "database."
            )

        return self._db
