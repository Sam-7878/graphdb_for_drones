# bench_utils.py
# Utility functions for benchmarking Scenario A (PostgreSQL RDB)

import time
import statistics
from typing import Tuple


def get_bench_query(start_hq: str, depth: int) -> str:
    """
    Returns a SQL query string using a recursive CTE to follow delegation chains outward
    from the given headquarters ID up to the specified depth.

    Delegation table schema:
      delegation(drone_id INT PRIMARY KEY, hq_id TEXT NOT NULL)
    We consider edges from hq_id -> drone_id, so start by selecting where hq_id = start_hq.
    """
    # Explicitly cast drone_id to TEXT for proper comparison with hq_id
    query = f"""
    WITH RECURSIVE chain(drone_id, hq_id, lvl) AS (
      -- level 1: all drones directly under the headquarters
      SELECT drone_id, hq_id, 1 AS lvl
        FROM delegation
       WHERE hq_id = '{start_hq}'
    UNION ALL
      -- levels 2..N: find drones whose parent is a drone in the previous level
      SELECT d.drone_id, d.hq_id, c.lvl + 1
        FROM delegation d
        JOIN chain c ON d.hq_id = c.drone_id::TEXT
       WHERE c.lvl < {depth}
    )
    SELECT COUNT(*) FROM chain;
    """
    return query


def benchmark_query(cur, query: str, iterations: int) -> Tuple[float, float, float, float]:
    """
    Executes the given query multiple times, measuring latencies.

    Returns:
      p50, p95, p99 latencies (in seconds) and throughput (queries per second).
    """
    latencies = []
    start_all = time.perf_counter()

    for _ in range(iterations):
        t0 = time.perf_counter()
        cur.execute(query)
        cur.fetchone()
        latencies.append(time.perf_counter() - t0)

    elapsed_all = time.perf_counter() - start_all

    # Compute percentiles
    p50 = statistics.quantiles(latencies, n=100)[49]
    p95 = statistics.quantiles(latencies, n=100)[94]
    p99 = statistics.quantiles(latencies, n=100)[98]
    tps = iterations / elapsed_all if elapsed_all > 0 else 0.0
    return p50, p95, p99, tps

def benchmark_query_parametric(cur, query: str, iterations: int, params: tuple) -> Tuple[float, float, float, float]:
    """
    Executes a parameterized SQL query multiple times with the given parameters, measuring latencies.

    Parameters:
      cur: psycopg cursor
      query: SQL query with placeholders
      iterations: number of repetitions
      params: query parameters as a tuple

    Returns:
      p50, p95, p99 latencies (in seconds), and throughput (queries per second)
    """
    import time, statistics
    latencies = []
    start_all = time.perf_counter()

    for _ in range(iterations):
        t0 = time.perf_counter()
        cur.execute(query, params)
        cur.fetchone()
        latencies.append(time.perf_counter() - t0)

    elapsed_all = time.perf_counter() - start_all
    
    p50 = statistics.quantiles(latencies, n=100)[49]
    p95 = statistics.quantiles(latencies, n=100)[94]
    p99 = statistics.quantiles(latencies, n=100)[98]
    tps = iterations / elapsed_all if elapsed_all > 0 else 0.0
    return p50, p95, p99, tps
