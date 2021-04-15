# Bamboo_H3 Tutorial

This tutorial is based on Ubuntu (18.04), but should be easily adaptable
to other Linux flavors as well as Windows.

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
GRANT ALL PRIVILEGES ON DATABASE water_out to <user_name>;
\q
```


## Setup environment

### Option 1: pyenv/Poetry

#### pyenv Installation

Note: Installing pyenv is not necessary if a recent version of Python (>= 3.7) is installed system-wide.

```BASH
sudo apt install libbz2-dev

curl -L https://github.com/pyenv/pyenv-installer/raw/master/bin/pyenv-installer | bash
```

add the following lines to $USER/.bashrc:

```BASH
export PATH="$HOME/.pyenv/bin:$PATH"
eval "$(pyenv init -)"
eval "$(pyenv virtualenv-init -)"
```

Install a Python version, tested with 3.8.0 and 3.9.4.

```BASH
exec $SHELL
pyenv update
pyenv install --list
pyenv install -v 3.9.4
```

#### Poetry Installation

```BASH
curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python -
exec $SHELL
poetry --version
```

#### Create and configure a Poetry project

```BASH
mkdir -p $USER/venvs
cd $USER/venvs

poetry new bamboo_h3_example
cd bamboo_h3_example
```


Add the following lines in `bamboo_h3_example/pyproject.toml`:

```BASH

  [[tool.poetry.source]]
  name = "py-all"
  url = "https://eoc-gzs-db01-vm.eoc.dlr.de:8080/repository/py-all/simple"

  [tool.poetry.dependencies]
  python = "^3.8"
  h3ronpy = "^0.7.1"
  bamboo_h3 = ""
  h3 = ""
  psycopg2 = ""
```


Set the desired Python environment and fetch the respective dependencies.

```BASH
pyenv local 3.9.4
poetry update  
```


### Option 2: Anaconda / Miniconda

Note: Installing Anaconda / Miniconda is not necessary if a recent version of Python (>= 3.7) is installed system-wide.
Using at least a `virtualenv` is encouraged, though.

#### Installation

```BASH
# Anaconda
wget https://repo.anaconda.com/archive/Anaconda3-2020.11-Linux-x86_64.sh
bash Anaconda3-2020.11-Linux-x86_64.sh

# Miniconda
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh
```

#### Setup conda environment

```BASH
conda create -n py38dev python=3.8
conda activate py38dev

pip install psycopg2
pip install -i https://eoc-gzs-db01-vm.eoc.dlr.de:8080/repository/py-all/simple bamboo_h3
pip install -i https://eoc-gzs-db01-vm.eoc.dlr.de:8080/repository/py-all/simple h3ronpy>=0.7.1
```


## Open SSH tunnel

Use a separate terminal to execute the following command:

```BASH
ssh $USER@torvalds.eoc.dlr.de \
  -L 9010:localhost:9010 \
  -L 5433:localhost:5432
```



## Run `example_processor.py`

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

By default, the example processor stores the results in a table named `water_out`.

Connect to DB:

```BASH
sudo -i -u postgres
psql -d water_out -U <user_name>
```

Query the first item of the results:

```SQL
\dt
SELECT * FROM water_results LIMIT 1;
```

