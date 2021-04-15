# Bamboo_H3 Tutorial

This tutorial is based on Ubuntu 18.04.

## Prerequisites

### Install PostgresQL / PostGIS

```
sudo apt install postgresql postgresql-contrib
sudo apt install postgresql-10-postgis-2.4
```

### Set up a PostGIS-enabled database 

```BASH
sudo -u postgres createdb h3out
sudo -u postgres psql -d h3out
```

### Enable PostGIS and set user privileges

```SQL
CREATE EXTENSION postgis;
CREATE USER <user_name> WITH PASSWORD '<password>';
GRANT ALL PRIVILEGES ON DATABASE h3out to <user_name>;
\q
```

### Open SSH tunnel

Use a separate terminal to execute the following command:

```BASH
ssh $USER@torvalds.eoc.dlr.de \
  -L 9010:localhost:9010 \
  -L 5433:localhost:5432
```

### Install Anaconda

```BASH
wget https://repo.anaconda.com/archive/Anaconda3-2020.11-Linux-x86_64.sh
bash Anaconda3-2020.11-Linux-x86_64.sh
```

### Setup conda environment

```BASH
conda create -n py38dev python=3.8
conda activate py38dev

pip install psycopg2
pip install -i https://eoc-gzs-db01-vm.eoc.dlr.de:8080/repository/py-all/simple bamboo_h3
pip install -i https://eoc-gzs-db01-vm.eoc.dlr.de:8080/repository/py-all/simple h3ronpy>=0.7.1
```

## Run example_processor.py

Make sure to copy the file to a custom location, so that the imports do not refer to local directories.


```python
import json
import time
from datetime import datetime

import h3.api.numpy_int as h3
import bamboo_h3
import h3ronpy
import pandas as pd
import psycopg2
import shapely.wkb
from bamboo_h3.concurrent import process_polygon
from bamboo_h3.postgres import fetch_using_intersecting_h3indexes
from shapely.geometry import shape, Polygon

#...
```

## Check results

### Connect to DB

```BASH
sudo -i -u postgres
psql -d h3out -U <user_name>
```

### Query the first item of the results

```SQL
\dt
SELECT * FROM water_out LIMIT 1;
```


```python

```
