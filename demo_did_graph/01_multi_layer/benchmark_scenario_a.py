# benchmark_scenario_a.py
# Scenario A Benchmark: 전통적 비대칭키 서명 + PostgreSQL RDB

import time
import psycopg
import statistics
import json
from pathlib import Path
import sys, os


# 프로젝트 루트를 PYTHONPATH에 추가 (common 모듈 로드용)
ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))
from common.load_config import TestConfig


# ─────────────────────────────────────────────────────────
# 1) 설정 파일 로드
# ─────────────────────────────────────────────────────────
CONFIG_JSON = ROOT / "config" / "test_large.json"
with open(CONFIG_JSON, 'r') as f:
    cfg_raw = json.load(f)
# 기존 TestConfig로 DB 정보만 로드
cfg = TestConfig(str(CONFIG_JSON))
# 시나리오 파라미터
sp = cfg_raw.get("scenario_parameters", {})
HQ_ID            = sp.get("HQ_ID", "HQ1")
DEPTHS           = sp.get("depths", [4])
SCALE_UP_NODES   = sp.get("scale_up_nodes", [])
ITERATIONS       = sp.get("iterations", 200)

# ─────────────────────────────────────────────────────────
# 2) DB 연결
# ─────────────────────────────────────────────────────────
conn = psycopg.connect(
    host=cfg.db_host,
    port=cfg.db_port,
    dbname=cfg.db_name,
    user=cfg.db_user,
    password=cfg.db_password
)
cur = conn.cursor()

# ─────────────────────────────────────────────────────────
# 3) 재귀 CTE 쿼리 생성 함수
# ─────────────────────────────────────────────────────────
def get_bench_query(hq_id: str, max_depth: int) -> str:
    return f"""
WITH RECURSIVE delegation(node_id, role, depth) AS (
  SELECT id AS node_id, 'HQ' AS role, 0 AS depth
    FROM hq
   WHERE id = '{hq_id}'
  UNION ALL
  SELECT r.child_id, r.child_type, d.depth + 1
    FROM delegation d
    JOIN delegation_relation r
      ON r.parent_id = d.node_id
   WHERE d.depth < {max_depth}
)
SELECT count(m.mission_id) AS mission_count
  FROM delegation del
  JOIN mission_test m
    ON m.drone_id = del.node_id
 WHERE del.role = 'Drone';
"""

# ─────────────────────────────────────────────────────────
# 4) 벤치마크 함수
# ─────────────────────────────────────────────────────────
def benchmark_query(query: str, iterations: int):
    latencies = []
    # 워밍업
    cur.execute(query)
    cur.fetchone()

    # 반복 실행
    start_all = time.perf_counter()
    for _ in range(iterations):
        t0 = time.perf_counter()
        cur.execute(query)
        _ = cur.fetchone()[0]
        latencies.append(time.perf_counter() - t0)
    elapsed_all = time.perf_counter() - start_all

    # 통계 계산
    p50 = statistics.quantiles(latencies, n=100)[49]
    p95 = statistics.quantiles(latencies, n=100)[94]
    p99 = statistics.quantiles(latencies, n=100)[98]
    tps = iterations / elapsed_all
    return p50, p95, p99, tps

# ─────────────────────────────────────────────────────────
# 5) 메인 실행 흐름
# ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=== Scenario A Multi-Scale Benchmark ===")
    for total_nodes in SCALE_UP_NODES:
        print(f"\n-- Scale-up: {total_nodes} nodes --")
        # DB에 해당 total_nodes로 로드된 상태여야 합니다.
        for depth in DEPTHS:
            print(f"Depth: {depth}")
            query = get_bench_query(HQ_ID, depth)
            p50, p95, p99, tps = benchmark_query(query, ITERATIONS)
            print(f"P50 latency : {p50*1000:.2f} ms")
            print(f"P95 latency : {p95*1000:.2f} ms")
            print(f"P99 latency : {p99*1000:.2f} ms")
            print(f"Throughput  : {tps:.2f} qps")

    cur.close()
    conn.close()
