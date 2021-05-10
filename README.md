# bamboo_h3

Python bindings to integrate clickhouse H3 databases with the python data-science world.

![](doc/img/bamboo_h3.png)

## Goals

1. Provide an integration with the widely known Python libraries.
2. Abstraction of most storage details of H3 data.
3. Enable and encourage parallelization.
4. Handling of compute-intensive tasks on the client instead of the DB servers as the 
   clients are far easier to scale.
5. Handle compute-intensive tasks in native code instead of Python.
6. Eventually provide a bridge into the [arrow ecosystem](https://arrow.apache.org/).

# Usage

See the [README of the python library](bamboo_h3/README.md).

# Links

## other relevant libraries

* [offical h3 bindings](https://github.com/uber/h3-py)
* [h3ronpy](https://github.com/nmandery/h3ron/tree/master/h3ronpy)

## other

* [HexagDLy - Processing Hexagonal Data with PyTorch](https://github.com/ai4iacts/hexagdly) ([paper](https://www.sciencedirect.com/science/article/pii/S2352711018302723))
