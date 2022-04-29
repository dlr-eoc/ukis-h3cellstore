# h3cellstorepy package

## Submodules

## h3cellstorepy.clickhouse module


### h3cellstorepy.clickhouse.CompactedTableSchema()
alias of `PyCompactedTableSchema`


### h3cellstorepy.clickhouse.CompactedTableSchemaBuilder()
alias of `PyCompactedTableSchemaBuilder`


### h3cellstorepy.clickhouse.CompressionMethod()
alias of `PyCompressionMethod`


### _class_ h3cellstorepy.clickhouse.GRPCConnection()
Bases: `object`

GPRC connection to the Clickhouse DB server.

Uses async communication using a internal tokio runtime.


#### create_tableset()
create the schema based on the schema definition in the database


#### database_exists()
Check if the given DB exists


#### database_name()
Name of the DB the connection connects to


#### deduplicate_schema()
deduplicate the contents of the given database schema


#### drop_tableset()
drop the tableset with the given name


#### execute()
execute the given query in the database without returning any result


#### execute_into_dataframe()
execute the given query and return a non-H3 dataframe of it


#### execute_into_h3dataframe()
execute the given query and return a H3 dataframe of it


#### insert_dataframe()
insert a dataframe into a table


#### insert_h3dataframe_into_tableset()
insert a dataframe into a tableset


#### list_tablesets()
list the tablesets found it the current database


#### query_tableset_cells()

#### traverse_tableset_area_of_interest()
Traversal using multiple GRPC connections with pre-loading in the background without blocking
the python interpreter.

The area_of_interest can be provided in multiple forms:


* As a geometry or other object implementing pythons __geo_interface__. For example created by the shapely or geojson libraries.


* As a numpy array of H3 cells. These will be transformed to a resolution suitable for traversal. See the max_fetch_count argument

Options (provided as keyword arguments):


* max_fetch_count: The maximum number of cells to fetch in one DB query.


* num_connections: Number of parallel DB connections to use in the background. Default is 3. Depending with the number of connections used the amount of memory used increases as well as the load put onto the DB-Server. The benefit is getting data faster as it is pre-loaded in the background.


* filter_query: This query will be applied to the tables in the reduced traversal_h3_resolution and only cells found by this query will be loaded from the tables in the requested full resolution


### _class_ h3cellstorepy.clickhouse.GRPCRuntime()
Bases: `object`


### h3cellstorepy.clickhouse.InsertOptions()
alias of `PyInsertOptions`


### h3cellstorepy.clickhouse.TableSet()
alias of `PyTableSet`


### h3cellstorepy.clickhouse.TableSetQuery()
alias of `PyTableSetQuery`


### h3cellstorepy.clickhouse.Traverser()
alias of `PyTraverser`

## h3cellstorepy.frame module


### _class_ h3cellstorepy.frame.DataFrameWrapper(df: Union[PyDataFrame, PyH3DataFrame, pyarrow.lib.Table, polars.internals.frame.DataFrame, pandas.core.frame.DataFrame])
Bases: `object`

implements most of the arrow/dataframe conversion fun


#### h3index_column_name()
name of the column the h3indexes are stored in


#### to_arrow()

#### to_pandas()
Convert to a pandas dataframe.

Requires having pandas installed.


#### to_polars()
Convert to a polars dataframe.

In most cases this should be a zero-copy operation

Requires having polars installed.


### h3cellstorepy.frame.ensure_wrapped(framelike: Union[h3cellstorepy.frame.DataFrameWrapper, PyDataFrame, PyH3DataFrame, pyarrow.lib.Table, polars.internals.frame.DataFrame, pandas.core.frame.DataFrame])
Create a DataFrameWrapper instance from the given input object

## h3cellstorepy.h3cellstorepy module


### _class_ h3cellstorepy.h3cellstorepy.PyDataFrame()
Bases: `object`

A wrapper for internal dataframe.

Allows exporting the data to arrow recordbatches using the to_arrow method.

This class should not be used directly in python, it is used within DataFrameWrapper.


#### shape()

#### to_arrow()

### _class_ h3cellstorepy.h3cellstorepy.PyH3DataFrame()
Bases: `object`

A wrapper for internal dataframe with an associated name for the column containing H3 cells.

Allows exporting the data to arrow recordbatches using the to_arrow method.

This class should not be used directly in python, it is used within DataFrameWrapper.


#### h3index_column_name()

#### shape()

#### to_arrow()

### h3cellstorepy.h3cellstorepy.is_release_build()
Check if this module has been compiled in release mode.


### h3cellstorepy.h3cellstorepy.version()
version of the module

## Module contents


### h3cellstorepy.is_release_build()
