# ukis_h3cellstorepy package

## Submodules

## ukis_h3cellstorepy.clickhouse module


### ukis_h3cellstorepy.clickhouse.CompactedTableSchema()
alias of `PyCompactedTableSchema`


### ukis_h3cellstorepy.clickhouse.CompactedTableSchemaBuilder()
alias of `PyCompactedTableSchemaBuilder`


### ukis_h3cellstorepy.clickhouse.CompressionMethod()
alias of `PyCompressionMethod`


### _class_ ukis_h3cellstorepy.clickhouse.GRPCConnection(grpc_endpoint, database_name, create_db=False, runtime=None, max_message_size=None)
Bases: `object`

GPRC connection to the Clickhouse DB server.

Uses async communication using a internal tokio runtime.


#### create_tableset(schema)
create the schema based on the schema definition in the database


#### database_exists(database_name)
Check if the given DB exists


#### database_name()
Name of the DB the connection connects to


#### deduplicate_schema(schema)
deduplicate the contents of the given database schema


#### drop_tableset(tableset_name)
drop the tableset with the given name


#### execute(query)
execute the given query in the database without returning any result


#### execute_into_dataframe(query)
execute the given query and return a non-H3 dataframe of it


#### execute_into_h3dataframe(query, h3index_column_name)
execute the given query and return a H3 dataframe of it


#### insert_dataframe(table_name, dataframe)
insert a dataframe into a table


#### insert_h3dataframe_into_tableset(schema, dataframe, options=None)
insert a dataframe into a tableset


#### list_tablesets()
list the tablesets found it the current database


#### query_tableset_cells(tableset_name, query, cells, h3_resolution, do_uncompact=True)

#### tableset_stats(tableset_name)
get stats about the number of cells and compacted cells in all the
resolutions of the tableset


#### traverse_tableset_area_of_interest(tableset_name, query, area_of_interest, h3_resolution, \*\*kwargs)
Traversal using multiple GRPC connections with pre-loading in the background without blocking
the python interpreter.

The area_of_interest can be provided in multiple forms:


* As a geometry or other object implementing pythons __geo_interface__. For example created by the shapely or geojson libraries.


* As a numpy array of H3 cells. These will be transformed to a resolution suitable for traversal. See the max_fetch_count argument

Options (provided as keyword arguments):


* max_fetch_count: The maximum number of cells to fetch in one DB query.


* num_connections: Number of parallel DB connections to use in the background. Default is 3. Depending with the number of connections used the amount of memory used increases as well as the load put onto the DB-Server. The benefit is getting data faster as it is pre-loaded in the background.


* filter_query: This query will be applied to the tables in the reduced traversal_h3_resolution and only cells found by this query will be loaded from the tables in the requested full resolution


### _class_ ukis_h3cellstorepy.clickhouse.GRPCRuntime(num_worker_threads=None)
Bases: `object`


### ukis_h3cellstorepy.clickhouse.InsertOptions()
alias of `PyInsertOptions`


### ukis_h3cellstorepy.clickhouse.TableSet()
alias of `PyTableSet`


### ukis_h3cellstorepy.clickhouse.TableSetQuery()
alias of `PyTableSetQuery`


### ukis_h3cellstorepy.clickhouse.Traverser()
alias of `PyTraverser`

## ukis_h3cellstorepy.frame module


### _class_ ukis_h3cellstorepy.frame.DataFrameWrapper(df: PyDataFrame | PyH3DataFrame | Table | DataFrame | DataFrame)
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


### ukis_h3cellstorepy.frame.ensure_wrapped(framelike: DataFrameWrapper | PyDataFrame | PyH3DataFrame | Table | DataFrame | DataFrame)
Create a DataFrameWrapper instance from the given input object

## ukis_h3cellstorepy.geom module


### ukis_h3cellstorepy.geom.border_cells(geometry, h3_resolution, width=1)
find the cells located directly within the exterior ring of the given polygon

The border cells are not guaranteed to be exactly one cell wide. Due to grid orientation
the line may be two cells wide at some places.

width: Width of the border in (approx.) number of cells. Default: 1

## ukis_h3cellstorepy.ukis_h3cellstorepy module


### _class_ ukis_h3cellstorepy.ukis_h3cellstorepy.PyDataFrame()
Bases: `object`

A wrapper for internal dataframe.

Allows exporting the data to arrow recordbatches using the to_arrow method.

This class should not be used directly in python, it is used within DataFrameWrapper.


#### shape()

#### to_arrow()

### _class_ ukis_h3cellstorepy.ukis_h3cellstorepy.PyH3DataFrame()
Bases: `object`

A wrapper for internal dataframe with an associated name for the column containing H3 cells.

Allows exporting the data to arrow recordbatches using the to_arrow method.

This class should not be used directly in python, it is used within DataFrameWrapper.


#### h3index_column_name()

#### shape()

#### to_arrow()

### ukis_h3cellstorepy.ukis_h3cellstorepy.is_release_build()
Check if this module has been compiled in release mode.


### ukis_h3cellstorepy.ukis_h3cellstorepy.version()
version of the module

## Module contents


### ukis_h3cellstorepy.is_release_build()
