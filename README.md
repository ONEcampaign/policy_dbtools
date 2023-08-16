![Python Version](https://img.shields.io/badge/python-3.10-blue.svg)
![Black](https://img.shields.io/badge/code%20style-black-000000.svg)

# policy_dbtools

Database tools for the ONE Policy team

This package contains tools for connecting to 
the ONE Policy team MongoDB database and performing 
various database operations. This package lightly wraps
the `pymongo` package, adding some functionality to
easily authenticate and connect to the database, perform 
various operations, and simplify using pandas DataFrames
with MongoDB. The package also uses context managers to 
ensure that connections are closed properly after operations
are performed.

## Installation

Install the package from GitHub using pip:

```bash
pip install git+https://github.com/ONECampaign/policy_dbtools.git
```

## Usage

### Setting credentials

The package contains functionality to use a configuration file containing
credentials, or set the credentials directly in the code. The credentials
are contained in a `.ini` file. 

If you are connecting to an `ini` file stored locally, 
it should be formatted as follows:

```ini
[MONGODB]
mongo_username = your_username
mongo_password = your_password
mongo_cluster = your_cluster
```

All or none of the fields can be filled in. Use the function
`set_config_path` to set the path to the configuration file.

```python
from policy_dbtools import dbtools as dbt

dbt.set_config_path('path/to/config.ini')
```

You can set credentials directly in the code using the function `set_config`:

```python
from policy_dbtools import dbtools as dbt

dbt.set_config(username='your_username', 
                    password='your_password', 
                    cluster='your_cluster')
```

If you have connected to an `ini` file stored locally, using the
function `set_config` overwrites the credentials in the file.

It is not required to set the credentials path. Using the function
`set_config` without having specified a path will create a config file
in the package directory.

### Connecting to the database

The object `AuthenticatedCursor` allows you to authenticate and connect to
the database.

On initialization, the object will authenticate the connection string verifying
that the credentials are correct. If the credentials are incorrect, the object
will raise an error. The object also authenticates that a database exists 
in the cluster and a collection exists in the database, if specified. The object
is also a context manager, so it can be used in a `with` statement.

```python
from policy_dbtools import dbtools as dbt

# Initialize the object
cursor = dbt.AuthenticatedCursor(username='your_username', 
                                 password='your_password',
                                 cluster='your_cluster')

# Connect to the database
cursor.connect()

# close the connection
cursor.close()
```

If credentials are set in a configuration file, the object can be initialized
without specifying the credentials:

```python
from policy_dbtools import dbtools as dbt

# Initialize the object
cursor = dbt.AuthenticatedCursor()
```

On initialization, you can specify a database and collection to connect to.
If the database and collection are specified, the object will authenticate
that the database and collection exist in the cluster. The database and collection
can be accessed using the `db` and `collection` attributes of the object, and pymongo
operations can be performed. 

```python
from policy_dbtools import dbtools as dbt

# Initialize the object
cursor = dbt.AuthenticatedCursor(
                                 database='your_database',
                                 collection='your_collection')

# Connect to the database
cursor.connect()

# Access the database and list the collections
cursor.db.list_collection_names()

>>> ['your_collection']
```

```python
# Access the collection and list the documents
list(cursor.collection.find_one())

>>> [{'_id': ...., 'your_field': 'your_value'}]
```

The database and collection can also be specified after initialization using the
`set_database` and `set_collection` methods:

The object can also be used as a context manager. When the context is entered, 
the object will connect to the database. When the context is exited, the object
will close the connection.

```python
from policy_dbtools import dbtools as dbt

# Initialize the object
with dbt.AuthenticatedCursor(database='your_database',
                             collection='your_collection') as cursor:

    # Access the database and list the collections
    cursor.db.list_collection_names()

>>> ['your_collection']
```

### Reading from the database

The object `MongoReader` allows you to read from the database. To use the object, 
first create an instance of the `AuthenticatedCursor` object, specifying
the database and collection to connect to. Then, pass the
`AuthenticatedCursor` object to the `MongoReader` object.

```python
from policy_dbtools import dbtools as dbt

cursor = dbt.AuthenticatedCursor(database='your_database',
                                 collection='your_collection')

reader = dbt.MongoReader(cursor)

```

Use the method `get_data` to read data from the collection. The method uses a context
manager, so it will connect to the database, and close the connection after reading
the data.

```python
data = reader.get_data()

>>> [{'your_field': 'your_value', 'your_field2': 'your_value2'}, ...]
```

The method also accepts a query to filter the data and a list of fields to return:

```python
data = reader.get_data(query={'your_field': 'your_value'},
                       fields=['your_field'])

>>> [{'your_field': 'your_value'}, ...]
```

This function makes use of the `pymongo` `find` method, so other arguments can be
passed to the function. See the `pymongo` documentation for more information.

Working with pandas DataFrames is made easy using the `get_df`, which returns the
data as a pandas DataFrame:

```python
df = reader.get_df(query={'your_field': 'your_value'},
                   fields=['your_field'])
```

By default, the `_id` field is not read, unless it is specified in the `fields` argument.
Optionally if `_id` is required in all returned data, it can be specified on initialization
by setting `include_id=True`:

```python

reader = dbt.MongoReader(cursor, include_id=True)

data = reader.get_data()

>>> [{'_id': ..., 'your_field': 'your_value', 'your_field2': 'your_value2'}, ...]
```


### Writing to the database

The object `MongoWriter` allows you to write to the database. To use the object,
first create an instance of the `AuthenticatedCursor` object, specifying the
database and collection to connect to. Then, pass the `AuthenticatedCursor` object
to the `MongoWriter` object.

```python
from policy_dbtools import dbtools as dbt

cursor = dbt.AuthenticatedCursor(database='your_database',
                                 collection='your_collection')
```

The `MongoWriter` object has different methods to write data to the database, 
based on the operation to perform. 

To insert new data, use the method `insert`:

```python

writer = dbt.MongoWriter(cursor)
writer.insert(data=[{'your_field': 'your_value'}...])
```

This will append the new data to the existing data in the collection.

Pandas dataframes can be passed directly to the `insert` method, and the
method will handle converting the dataframe to a list of dictionaries:

```python
writer.insert(data=df)
```

The method `drop_all_and_insert` will replace all the data in a collection
with new data:

```python
writer.drop_all_and_insert(data=[{'your_field': 'your_value'}...])
```

The method can also accept a pandas dataframe.





The write methods attempt to maximize efficiency and reduce transfer costs. This is
done by using bulk writes. All the methods also back up the data before
attempting to write to the database. If the write fails, the data is restored
to the collection. Backups are created by creating new collections in the database
with the suffix `_backup`. If a write operation fails, the backup data is restored.
If the write operation succeeds, the backup data is deleted. The backup collections
can be preserved by passing `preserve_backup=True` to the method.

```python
writer.insert(data=[{'your_field': 'your_value'}...], preserve_backup=True)
```

The backup collection will remain in the database as `your_collection_backup`. 






