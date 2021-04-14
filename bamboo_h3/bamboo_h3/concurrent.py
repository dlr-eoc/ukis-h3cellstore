"""
helpers for multiprocessing
"""
import concurrent.futures
import math
import random
from typing import Callable, List, Any

from shapely.geometry import box, Polygon

from . import ClickhouseResultSet


def chunk_polygon(geometry: Polygon, num_chunks_approx: int = 10) -> List[Polygon]:
    """cut a shapely geometry into chunks to distribute it across multiple processes

    :returns: list of shapely polygons
    """
    bounds = geometry.bounds
    xmin, ymin, xmax, ymax = bounds
    width = xmax - xmin
    height = ymax - ymin

    cell_size = min(height, width) / max(height, width) * float(math.sqrt(num_chunks_approx)) / 2.0
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

    # evenly distribute chunks to balance the processing load by
    # distributing chunks with a high local data density across the whole list
    random.shuffle(chunks)

    return chunks


def process_polygon(n_concurrent_processes: int, polygon: Polygon,
                    processing_callback: Callable[[ClickhouseResultSet], Any],
                    num_chunks_per_proccess_approx: int = 2) -> List[Any]:
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
        polygon_chunks = chunk_polygon(polygon,
                                       num_chunks_approx=n_concurrent_processes * num_chunks_per_proccess_approx)

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
    # run doctests
    import doctest

    doctest.testmod(verbose=True)
