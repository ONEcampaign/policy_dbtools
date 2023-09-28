"""Tools to handle connection and CRUD operations to a MongoDB database.

main functions and classes:
    set_config_path - set the path to the config.ini file
    set_config - set the configuration file for MongoDB connection
    AuthenticatedCursor - an authenticated cursor to a MongoDB database
    MongoWriter - a class to write data to a MongoDB database
    MongoReader - a class to read data from a MongoDB database
"""

import pandas as pd
import pymongo
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
    """Set the path to the config.ini file.

    Args:
        path: Path to the config.ini file.
    """

    global CONFIG_PATH
    CONFIG_PATH = Path(path).resolve()


def set_config(username: str = None, password: str = None, cluster: str = None, db: str = None) -> None:
    """Set configuration file for MongoDB connection.

    This functions allows you to set the configuration and credentials for the MongoDB connection,
    including the username, password, cluster name, and database name which are stored in
    config.ini file.

    Args:
        username: Username to authenticate with.
        password: Password to authenticate with.
        cluster: Name of the MongoDB cluster to connect to.
        db: Name of the database to connect to.
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
    if db is not None:
        config["MONGODB"]["MONGO_DB"] = db

    # write config file
    with open(CONFIG_PATH, "w") as configfile:
        config.write(configfile)


def _create_uri(cluster: str, username: str, password: str) -> str:
    """Create a MongoDB connection string.

    Args:
        cluster: Name of the MongoDB cluster to connect to.
        username: Username to authenticate with.
        password: Password to authenticate with.

    Returns:
        MongoDB connection string.
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
        username: Username to authenticate with.
        password: Password to authenticate with.
        cluster: Name of the MongoDB cluster to connect to.

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

    Attributes:
        db_name: Name of the database to connect to.
        db: MongoDB database object.

    Methods:
        check_connection - test connection to the MongoDB database
        check_valid_collection - check if a collection exists in the database
        check_valid_db - check if a database exists
        connect - connect to the MongoDB database
        close - close connection to the MongoDB database
        set_db - set the database to connect to

    """

    def __init__(
        self,
        username: str = None,
        password: str = None,
        cluster: str = None,
        db_name: str = None,
    ):
        """Initialize the AuthenticatedCursor object.

        Args:
            username: Username to authenticate with. If not provided, will attempt to read from
                config.ini file.
            password: Password to authenticate with. If not provided, will attempt to read from
                config.ini file.
            cluster: Name of the MongoDB cluster to connect to. If not provided, will attempt to
                read from config.ini file.
            db_name: Name of the database to connect to. This is optional and can be set later
        """

        credentials = _check_credentials(username, password, cluster)
        self.__uri = _create_uri(**credentials)

        # test connection to the cluster
        self.check_connection()
        self._client = None  # client object

        # set the database
        self.db_name = None
        config = configparser.ConfigParser()
        config.read(CONFIG_PATH)

        if db_name is not None:
            self.set_db(db_name)

        elif config.has_option("MONGODB", "MONGO_DB"):
            self.set_db(config["MONGODB"]["MONGO_DB"])

        else:
            raise ValueError(
                "No database name provided. `db_name` must provided or set in config file using set_config()."
            )

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

    def check_valid_collection(self, collection_name: str) -> bool:
        """Check if a collection exists in a database. If it does not, raise an error.

        Args:
            collection_name: Name of the collection to check.

        Returns:
            True if the collection exists.
        """

        with MongoClient(self.__uri) as client:

            collection_list = client[self.db_name].list_collection_names()
            if collection_name not in collection_list:
                raise ValueError(
                    f"Collection {collection_name} does not exist in database {self.db_name}."
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

    def set_db(self, db_name: str) -> None:
        """Set the database to connect to.

        Args:
            db_name: Name of the database to connect to.
        """

        self.check_valid_db(db_name=db_name)
        self.db_name = db_name

    @property
    def db(self) -> Database | None:
        """MongoDB database object."""

        if self._client is None:
            return None
        return self._client[self.db_name]


class MongoReader:
    """Class to read data from MongoDB

    This class is used to query and read data from a MongoDB collection. The data can
    be read into a pandas DataFrame or a list of dictionaries. Methods use a
    context manager to ensure that the connection to the database is closed after
    each read. Optionally you can disable _id from being returned when reading data.

    Attributes:
        cursor: AuthenticatedCursor object to connect to the database.
        collection_name: Name of the collection to read from.
        collection: MongoDB collection object.
        include_id: If False, _id will be excluded in the response when using
            read methods, unless it is explicitly excluded in the method call. Defaults to False.

    Methods:
        get_data - get the collection data as a list of dictionaries
        get_df - get the collection data as a pandas DataFrame
    """

    def __init__(self, cursor: AuthenticatedCursor, collection_name: str,*, include_id: bool = False):
        """Initialize the PolicyReader object.

        Args:
            cursor: AuthenticatedCursor object to connect to the database.
            collection_name: Name of the collection to read from.
            include_id: If False, _id will be excluded in the response when using
        """
        self.cursor = cursor

        self.collection_name = collection_name
        self.set_collection(collection_name)
        self.include_id = include_id

    def set_collection(self, collection_name: str) -> None:
        """Set the collection to connect to

        This method checks that the collection exists in the database, then sets the
        collection and collection_name attributes.

        Args:
            collection_name: Name of the collection to connect to.
        """

        self.cursor.check_valid_collection(collection_name)
        self.collection_name = collection_name

    @property
    def collection(self) -> Collection | None:
        """MongoDB collection object."""

        if self.cursor._client is None:
            return None
        return self.cursor.db[self.collection_name]

    def _find(
        self,
        query: dict | None = None,
        fields: list | None = None,
        *args,
        **kwargs,
    ) -> pymongo.cursor.Cursor:
        """Get collection as a pandas DataFrame.

        Args:
            query: Query to filter the collection. If None, the entire collection is returned.
                Defaults to None.
            fields: Fields to include in the response. Defaults to None. If None, all fields
                will be returned. If exclude_id is set to True, _id will be excluded from the
                response unless it is explicitly included in fields.

            *args: Additional arguments to pass to the pymongo.collection.find() method.
            **kwargs: Additional keyword arguments to pass to the pymongo.collection.find() method.

        Returns:
            A pymongo cursor object.
        """

        # if query is None set it to an empty dictionary
        if query is None:
            query = {}

        # if fields is None set it to an empty dictionary
        if fields is None:
            fields = {}
        # if fields is a list convert it to a dictionary with all fields set to 1
        else:
            fields = {field: 1 for field in fields}

        # if exclude_id is True and _id is not in fields, set _id to 0
        if self.include_id is False and "_id" not in fields:
            fields["_id"] = 0

        return self.collection.find(query, fields, *args, **kwargs)

    def get_data(
        self, query: dict | None = None, fields: list | None = None, *args, **kwargs
    ) -> list[dict]:
        """Get the collection data as a list of dictionaries.

        Args:
            query: Query to filter the collection. If None, the entire collection is returned.
                Defaults to None.
            fields: Fields to include in the response. Defaults to None. If None, all fields
                will be returned. If exclude_id is set to True, _id will be excluded from the
                response unless it is explicitly included in fields.

            *args: Additional arguments to pass to the pymongo.collection.find() method.
            **kwargs: Additional keyword arguments to pass to the pymongo.collection.find() method.

        Returns:
            A list of dictionaries with the collection data.
        """

        with self.cursor:
            cursor_data = self._find(query, fields, *args, **kwargs)
            data = list(cursor_data)

            # warn if the list is empty
            if not data:
                logger.warning("No data found.")

            return data

    def get_df(
        self, query: dict | None = None, fields: list | None = None, *args, **kwargs
    ) -> pd.DataFrame:
        """Get collection data as a pandas DataFrame.

        Args:
            query: Query to filter the collection. If None, the entire collection is returned.
                Defaults to None.
            fields: Fields to include in the response. Defaults to None. If None, all fields
                will be returned. If exclude_id is set to True, _id will be excluded from the
                response unless it is explicitly included in fields.

            *args: Additional arguments to pass to the pymongo.collection.find() method.
            **kwargs: Additional keyword arguments to pass to the pymongo.collection.find() method.

        Returns:
            A pandas DataFrame with the collection data.
        """
        with self.cursor:
            cursor_data = self._find(query, fields, *args, **kwargs)
            df = pd.DataFrame.from_records(cursor_data)

            # warn if the DataFrame is empty
            if df.empty:
                logger.warning("No data found.")

            return df

    # method to find if at least one document exists in the collection
    def exists(self, query: dict | None = None, *args, **kwargs) -> bool:
        """Find if at least one document exists in the collection that matches a query.

        Args:
            query: Query to filter the collection.

        Returns:
            True if at least one document exists in the collection that matches the query.
            False otherwise.
        """

        with self.cursor:
            if query is None:
                query = {}

            count = self.collection.count_documents(query, *args, **kwargs, limit=1)

        if count > 0:
            return True
        else:
            return False


class MongoWriter:
    """Class to write data to MongoDB.

    This class is used to load data into a MongoDB collection. It can be used to replace all the data in a collection
    or to append data to a collection. It contains functionality to backup the collection before
    performing a write operation and to restore the backup if an error occurs. Methods use a context manager to
    ensure that any connection to the database is closed after the write operation is complete.

    Attributes:
        cursor: AuthenticatedCursor object to connect to the database.
        collection_name: Name of the collection to write to.
        collection: MongoDB collection object.

    Methods:
        drop_all_and_insert - replace all the data in a collection
        insert - insert data to a collection

    """

    def __init__(self, cursor: AuthenticatedCursor, collection_name: str):
        """Initialize the PolicyWriter object.

        Args:
            cursor: AuthenticatedCursor object to connect to the database.
            collection_name: Name of the collection to write to.
        """
        self.cursor = cursor
        self.collection_name = collection_name
        self.set_collection(collection_name)

    def set_collection(self, collection_name: str) -> None:
        """Set the collection to connect to

        This method sets checks that the collection exists in the database, then sets the
        collection and collection_name attributes.

        Args:
            collection_name: Name of the collection to connect to.
        """

        self.cursor.check_valid_collection(collection_name)
        self.collection_name = collection_name

    @property
    def collection(self) -> Collection | None:
        """MongoDB collection object."""

        if self.cursor._client is None:
            return None
        return self.cursor.db[self.collection_name]

    def drop_all_and_insert(
        self, data: list[dict] | pd.DataFrame, *, preserve_backup: bool = False
    ) -> None:
        """Replace all the data in a collection

        This function will replace all the data in a collection with the data provided.
        It will first backup the collection, then drop all the data and insert the new data.
        If an exception occurs, it will restore the backup. Otherwise, it will delete the backup.

        Args:
            data: Data to insert in the collection. Can be a list of dictionaries or a pandas DataFrame.
                  If a DataFrame is provided, it will be converted to a list of dictionaries.
            preserve_backup: If True, the backup will not be deleted after a successful insert. Defaults to False.
                  If the backup is preserved, it is accessible as <collection_name>_backup in the database.
        """

        with self.cursor as cursor:
            # backup the collection by renaming it and create a new collection with the same name
            self.collection.rename(f"{self.collection.name}_backup")
            cursor.db.create_collection(self.collection.name)

            try:
                # if data is a pandas DataFrame, convert it to a list of dictionaries
                if isinstance(data, pd.DataFrame):
                    data = data.to_dict(orient="records")

                bulk_operations = [
                    pymongo.DeleteMany({}),
                    *[pymongo.InsertOne(document) for document in data],
                ]
                result = self.collection.bulk_write(bulk_operations)
                logger.info(
                    f"Dropped data and inserted {result.inserted_count} documents in collection {self.collection.name}"
                )

                # if preserve_backup is True, do not delete it after a successful insert
                if preserve_backup is False:
                    cursor.db.drop_collection(f"{self.collection.name}_backup")

            except Exception as e:
                logger.exception(f"Exception occurred. Restoring backup.")
                self.collection.rename(self.collection.name)
                cursor.db.drop_collection(f"{self.collection.name}_backup")
                raise e

    def insert(
        self, data: list[dict] | pd.DataFrame, *, preserve_backup: bool = False
    ) -> None:
        """Insert data to a collection

        This function will insert data to a collection. It will first backup the collection,
        then append the data to the existing data in the collection. If an exception occurs,
        it will restore the backup.

        Args:
            data: Data to insert in the collection. Can be a list of dictionaries or a pandas DataFrame.
                    If a DataFrame is provided, it will be converted to a list of dictionaries.
            preserve_backup: If True, the backup will not be deleted after a successful insert. Defaults to False.
                    If the backup is preserved, it is accessible as <collection_name>_backup in the database.
        """

        with self.cursor as cursor:
            # backup the collection by creating a mirror
            self.collection.aggregate([{"$out": f"{self.collection.name}_backup"}])

            try:
                # if data is a pandas DataFrame, convert it to a list of dictionaries
                if isinstance(data, pd.DataFrame):
                    data = data.to_dict(orient="records")

                bulk_operations = [pymongo.InsertOne(document) for document in data]
                result = self.collection.bulk_write(bulk_operations)
                logger.info(
                    f"Inserted {result.inserted_count} documents in collection {self.collection.name}"
                )

                # drop the backup collection if the insert was successful
                if preserve_backup is False:
                    cursor.db.drop_collection(f"{self.collection.name}_backup")

            except Exception as e:
                logger.exception(
                    f"Exception occurred. Restoring backup. Exception: {e}"
                )

                # restore the backup collection if the insert failed
                cursor.db.drop_collection(f"{self.collection.name}")
                cursor.db[f"{self.collection.name}_backup"].rename(
                    self.collection.name
                )
                raise e
