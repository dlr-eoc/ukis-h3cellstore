"""
helpers for multiprocessing
"""
import concurrent.futures
import math
import random
from typing import Callable

from shapely.geometry import box, Polygon


def chunk_polygon(geometry: Polygon, num_chunks_approx=10):
    """cut a shapely geometry into chunks to distribute it across multiple processes

    :returns: list of shapely polygons
    """
    bounds = geometry.bounds
    xmin, ymin, xmax, ymax = bounds
    width = xmax - xmin
    height = ymax - ymin

    cell_size = min(height, width) / max(height, width) / float(num_chunks_approx)
    chunks = []
    for i in range(math.ceil(width / cell_size)):
        for j in range(math.ceil(height / cell_size)):
            b = box(
                xmin + i * cell_size,
                ymin + j * cell_size,
                xmin + (i + 1) * cell_size,
                ymin + (j + 1) * cell_size
            )
            g = geometry.intersection(b)
            if g.is_empty:
                continue
            if g.type == "Polygon":
                chunks.append(g)
            elif g.type == "MultiPolygon":
                for gg in g.geoms:
                    if not gg.is_empty:
                        chunks.append(gg)
            else:
                raise ValueError(f"unsupported geometry type: {g.type}")

    # evenly distribute chunks to distribute load by distributing chunks with a
    # high data density across the whole list
    random.shuffle(chunks)

    return chunks


def process_polygon(n_concurrent_processes: int, polygon: Polygon, processing_callback: Callable):
    """cut the `polygon` into chunks and concurrently apply the `processing_callback`
        to each of the chunks using `n_concurrent_processes` subprocesses.

        `processing_callback` must be a callable taking a geometry instance as argument.
        The callable will be dispatched to a process pool, so it must not depend on
        any global state which can not be pickled. Database connections, etc should
        only be established within the callable.

        @:returns list of the return values of the `processing_callback`
    """

    results = []
    if n_concurrent_processes == 1:
        # using just a single process without spawning a separate process
        results.append(processing_callback(polygon))
    elif n_concurrent_processes > 1:

        # let the kernel immediately kill all child processes on Ctrl-C
        import signal
        signal.signal(signal.SIGINT, signal.SIG_DFL)

        # split the geometry into chunks to distribute these across multiple processes
        polygon_chunks = chunk_polygon(polygon, num_chunks_approx=n_concurrent_processes * 2)

        with concurrent.futures.ProcessPoolExecutor(max_workers=n_concurrent_processes) as executor:
            pending = [executor.submit(processing_callback, p) for p in polygon_chunks]
            while len(pending) != 0:
                finished, pending = concurrent.futures.wait(pending, timeout=10,
                                                            return_when=concurrent.futures.FIRST_EXCEPTION)
                for fut in finished:
                    results.append(
                        fut.result())  # re-raises the exception occurred in the subprocess when there was one
    else:
        raise ValueError("n_concurrent_processes must be > 1")
    return results


if __name__ == '__main__':
    import json
    from shapely.geometry import MultiPolygon, shape

    geom = """
{
        "type": "Polygon",
        "coordinates": [
          [
            [
              13.787841796875,
              47.989921667414194
            ],
            [
              13.82080078125,
              47.85740289465826
            ],
            [
              16.89697265625,
              48.04870994288686
            ],
            [
              16.80908203125,
              48.55297816440071
            ],
            [
              16.226806640625,
              48.929717630629554
            ],
            [
              15.281982421875002,
              49.04506962208049
            ],
            [
              13.458251953125,
              49.160154652338015
            ],
            [
              12.711181640625,
              49.01625665778159
            ],
            [
              12.755126953125,
              48.7996273507997
            ],
            [
              13.205566406249998,
              48.76343113791796
            ],
            [
              15.523681640625002,
              48.79239019646406
            ],
            [
              16.007080078125,
              48.516604348867475
            ],
            [
              15.303955078125,
              48.52388120259336
            ],
            [
              13.985595703125,
              48.60385760823255
            ],
            [
              12.7001953125,
              48.58932584966975
            ],
            [
              12.83203125,
              48.31973404047173
            ],
            [
              13.699951171875,
              48.31242790407178
            ],
            [
              15.029296875,
              48.334343174592014
            ],
            [
              16.226806640625,
              48.334343174592014
            ],
            [
              15.194091796874998,
              48.151428143221224
            ],
            [
              12.83203125,
              48.1367666796927
            ],
            [
              12.645263671875,
              48.03401915864286
            ],
            [
              13.0517578125,
              47.97521412341618
            ],
            [
              13.787841796875,
              47.989921667414194
            ]
          ]
        ]
      }
    """
    g = chunk_polygon(shape(json.loads(geom)), num_chunks_approx=20)
    print(json.dumps(MultiPolygon(g).__geo_interface__))
