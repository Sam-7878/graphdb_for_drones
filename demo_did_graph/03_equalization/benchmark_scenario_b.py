#!/usr/bin/env python3
# benchmark_scenario_b.py
# Scenario B Dynamic Topology Benchmark: DID + VC + PostgreSQL RDB

import time
import psycopg
import statistics
import json
import random
import argparse
import csv
from pathlib import Path
import sys, os

# 프로젝트 루트를 PYTHONPATH에 추가 (common 모듈 로드용)
ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))
from common.load_config import TestConfig
from common.bench_utils import benchmark_query



# 수정된 get_bench_query 함수 (VC 개수 기반)
def get_bench_query(start_hq: str, depth: int) -> str:
    """
    VC 기준으로 카운트:
    delegation 테이블을 재귀적으로 따라가며
    drone_id → vc_test.subject_did 를 매칭해 VC 개수를 센다.
    """
    return f"""
    WITH RECURSIVE chain(drone_id, hq_id, lvl) AS (
      SELECT drone_id, hq_id, 1 AS lvl
        FROM delegation
       WHERE hq_id = '{start_hq}'
    UNION ALL
      SELECT d.drone_id, d.hq_id, c.lvl + 1
        FROM delegation d
        JOIN chain c ON d.hq_id = c.drone_id::TEXT
       WHERE c.lvl < {depth}
    )
    SELECT COUNT(*) AS vc_count
      FROM chain c
      JOIN vc_test v ON v.subject_did = c.drone_id::TEXT;
    """



# ─────────────────────────────────────────────────────────
# Scenario별 워크로드 함수 정의
# ─────────────────────────────────────────────────────────

def scenario1_realtime_turntaking(cur, conn, cfg, params, nodes, depths, iterations, rows):
    interval = params['turn_taking']['interval_sec']
    ratio = params['turn_taking']['update_ratio']

    for num_nodes in nodes:
        print(f"\n-- Scale-up: {num_nodes} nodes (Turn-Taking) --")
        for depth in depths:
            update_count = int(cfg.num_drones * ratio)
            drones = random.sample(range(cfg.num_drones), update_count)

            # ─── moderate‐sized batch 업데이트 ───
            chunk_size = cfg.chunk_size
            for i in range(0, len(drones), chunk_size):
                chunk = drones[i:i+chunk_size]
                cur.execute(
                    "UPDATE delegation SET hq_id = %s WHERE drone_id = ANY(%s)",
                    (cfg.headquarters_id, chunk)
                )
                conn.commit()


            time.sleep(interval)
            query = get_bench_query(cfg.headquarters_id, depth)
            p50, p95, p99, tps = benchmark_query(cur, query, iterations)
            
            print(f"Depth {depth} → P50: {p50*1000:.2f} ms, P95: {p95*1000:.2f} ms, P99: {p99*1000:.2f} ms, TPS: {tps:.2f}")
            rows.append({
                'scenario': 'B-1',
                'scale_up': num_nodes,
                'depth': depth,
                'p50_ms': p50*1000,
                'p95_ms': p95*1000,
                'p99_ms': p99*1000,
                'tps': tps
            })


def scenario2_chain_churn(cur, conn, cfg, params, nodes, depths, iterations, rows):
    cycle = params['chain_churn']['depth_cycle']
    interval = params['chain_churn']['cycle_interval_sec']
    ratio = params['chain_churn']['update_ratio']

    for depth in cycle:
        print(f"\n-- Chain-Churn: depth={depth} --")
        update_count = int(cfg.num_drones * ratio)
        drones = random.sample(range(cfg.num_drones), update_count)

        # for did in drones:
        #     cur.execute(
        #         "UPDATE delegation SET hq_id = %s WHERE drone_id = %s",
        #         (cfg.headquarters_id, did)
        #     )
        # 한 번에 다중 레코드 갱신
        # cur.execute(
        #     "UPDATE delegation SET hq_id = %s WHERE drone_id = ANY(%s)",
        #     (cfg.headquarters_id, drones)
        # )
        # conn.commit()
        # ─── moderate‐sized batch 업데이트 ───
        chunk_size = cfg.chunk_size
        for i in range(0, len(drones), chunk_size):
            chunk = drones[i:i+chunk_size]
            cur.execute(
                "UPDATE delegation SET hq_id = %s WHERE drone_id = ANY(%s)",
                (cfg.headquarters_id, chunk)
            )
            conn.commit()


        time.sleep(interval)
        query = get_bench_query(cfg.headquarters_id, depth)
        p50, p95, p99, tps = benchmark_query(cur, query, iterations)
        print(f"Depth {depth} → P50: {p50*1000:.2f} ms, P95: {p95*1000:.2f} ms, P99: {p99*1000:.2f} ms, TPS: {tps:.2f}")
        rows.append({
            'scenario': 'B-2',
            'scale_up': '',
            'depth': depth,
            'p50_ms': p50*1000,
            'p95_ms': p95*1000,
            'p99_ms': p99*1000,
            'tps': tps
        })


def scenario3_partition_reconciliation(cur, conn, cfg, params, nodes, depths, iterations, rows):
    split = params['partition_reconciliation']['split_ratio']
    split_duration = params['partition_reconciliation']['split_duration_sec']
    recon_sync = params['partition_reconciliation']['post_reconcile_sync_requests']
    total = cfg.num_drones
    boundary = int(total * split[0])
    print(f"\n-- Partition: A={boundary}, B={total-boundary}, duration={split_duration}s --")

    start = time.time()
    # while time.time() - start < split_duration:
    #     for did in range(boundary):
    #         cur.execute(
    #             "UPDATE delegation SET hq_id = %s WHERE drone_id = %s",
    #             (cfg.headquarters_id, did)
    #         )
    #     for did in range(boundary, total):
    #         cur.execute(
    #             "UPDATE delegation SET hq_id = %s WHERE drone_id = %s",
    #             (cfg.headquarters_id, did)
    #         )
    first = list(range(boundary))
    second = list(range(boundary, total))
    # while time.time() - start < split_duration:
    #     cur.execute(
    #         "UPDATE delegation SET hq_id = %s WHERE drone_id = ANY(%s)",
    #         (cfg.headquarters_id, first)
    #     )
    #     cur.execute(
    #         "UPDATE delegation SET hq_id = %s WHERE drone_id = ANY(%s)",
    #         (cfg.headquarters_id, second)
    #     )
    #     conn.commit()
    # ─── moderate‐sized batch 파티션 A/B 업데이트 ───
    chunk_size = cfg.chunk_size
    while time.time() - start < split_duration:
        # Partition A
        for i in range(0, len(first), chunk_size):
            chunk = first[i:i+chunk_size]
            cur.execute(
                "UPDATE delegation SET hq_id = %s WHERE drone_id = ANY(%s)",
                (cfg.headquarters_id, chunk)
            )
        # Partition B
        for i in range(0, len(second), chunk_size):
            chunk = second[i:i+chunk_size]
            cur.execute(
                "UPDATE delegation SET hq_id = %s WHERE drone_id = ANY(%s)",
                (cfg.headquarters_id, chunk)
            )
        conn.commit()


    print(f"Partition 해제, 동기화 {recon_sync}건 --")
    query = get_bench_query(cfg.headquarters_id, depths[0])
    p50, p95, p99, tps = benchmark_query(cur, query, recon_sync)
    print(f"Reconciliation → P50: {p50*1000:.2f} ms, P95: {p95*1000:.2f} ms, P99: {p99*1000:.2f} ms, TPS: {tps:.2f}")
    rows.append({
        'scenario': 'B-3',
        'scale_up': total,
        'depth': depths[0],
        'p50_ms': p50*1000,
        'p95_ms': p95*1000,
        'p99_ms': p99*1000,
        'tps': tps
    })


# ─────────────────────────────────────────────────────────
# 메인 함수
# ─────────────────────────────────────────────────────────
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Scenario B Dynamic Topology Benchmark")
    parser.add_argument('-c', '--config', required=True, help='Path to config JSON')
    parser.add_argument('-s', '--scenario', required=True, choices=['1', '2', '3'], help='Scenario number')
    args = parser.parse_args()

    cfg = TestConfig(args.config)
    with open(args.config) as f:
        cfg_json = json.load(f)

    scale_up_nodes = cfg_json.get('scale_up_nodes', [])
    depths = cfg_json.get('depths', [])
    iterations = cfg_json.get('iterations', 100)
    params = cfg.scenario_params.get(args.scenario, {})

    conn = psycopg.connect(**cfg.db_params)
    cur = conn.cursor()
    # WAL 동기화(off) → 커밋 대기시간 제거
    cur.execute("SET synchronous_commit = ON;")
    print("› synchronous_commit ON 설정 완료")

    rows = []
    if args.scenario == '1':
        print("=== Running Scenario B-1: Real-Time Turn-Taking ===")
        scenario1_realtime_turntaking(cur, conn, cfg, params, scale_up_nodes, depths, iterations, rows)
    elif args.scenario == '2':
        print("=== Running Scenario B-2: Chain-Churn ===")
        scenario2_chain_churn(cur, conn, cfg, params, scale_up_nodes, depths, iterations, rows)
    else:
        print("=== Running Scenario B-3: Partition & Reconciliation ===")
        scenario3_partition_reconciliation(cur, conn, cfg, params, scale_up_nodes, depths, iterations, rows)

    
    # result_dir = Path(ROOT) / 'data' / 'result'
    result_dir = Path(ROOT) / cfg.data_result_path
    result_dir.mkdir(parents=True, exist_ok=True)
    
    
    output_file = result_dir / f"B_{args.scenario}_results.csv"
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile,
                                fieldnames=['scenario','scale_up','depth','p50_ms','p95_ms','p99_ms','tps'])
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    print(f"Results written to {output_file}")

    cur.close()
    conn.close()
