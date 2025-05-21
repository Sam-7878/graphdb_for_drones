#!/usr/bin/env python3
# benchmark_scenario_a.py
# Scenario A Dynamic Topology Benchmark: 비대칭키 서명 + PostgreSQL RDB

import time
import psycopg
import statistics
import json
import argparse
import random
import csv
from pathlib import Path
import sys, os

# 프로젝트 루트를 PYTHONPATH에 추가 (common 모듈 로드용)
ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))
from common.load_config import TestConfig
from common.bench_utils import get_bench_query, benchmark_query

# ─────────────────────────────────────────────────────────
# Scenario별 워크로드 함수
# ─────────────────────────────────────────────────────────

def scenario1_realtime_turntaking(cur, conn, cfg, params, nodes, depths, iterations, rows):
    interval = params['turn_taking']['interval_sec']
    ratio = params['turn_taking']['update_ratio']
    for total in nodes:
        print(f"\n-- Scale-up: {total} nodes (Turn-Taking) --")
        for depth in depths:
            # Delegation 업데이트
            update_count = int(cfg.num_drones * ratio)
            drones = random.sample(range(cfg.num_drones), update_count)
            for did in drones:
                cur.execute("UPDATE delegation SET hq_id=%s WHERE drone_id=%s", (cfg.headquarters_id, did))
            conn.commit()
            time.sleep(interval)
            # 성능 측정
            query = get_bench_query(cfg.headquarters_id, depth)
            p50, p95, p99, tps = benchmark_query(cur, query, iterations)
            print(f"Depth {depth} → P50: {p50*1000:.2f} ms, P95: {p95*1000:.2f} ms, P99: {p99*1000:.2f} ms, TPS: {tps:.2f}")
            # 결과 저장
            rows.append({
                'scenario': 'A-1', 'scale_up': total, 'depth': depth,
                'p50_ms': p50*1000, 'p95_ms': p95*1000, 'p99_ms': p99*1000, 'tps': tps
            })


def scenario2_chain_churn(cur, conn, cfg, params, nodes, depths, iterations, rows):
    cycle = params['chain_churn']['depth_cycle']
    interval = params['chain_churn']['cycle_interval_sec']
    ratio = params['chain_churn']['update_ratio']
    for depth in cycle:
        print(f"\n-- Chain-Churn: depth={depth} --")
        update_count = int(cfg.num_drones * ratio)
        drones = random.sample(range(cfg.num_drones), update_count)
        for did in drones:
            cur.execute("UPDATE delegation SET hq_id=%s WHERE drone_id=%s", (cfg.headquarters_id, did))
        conn.commit()
        time.sleep(interval)
        query = get_bench_query(cfg.headquarters_id, depth)
        p50, p95, p99, tps = benchmark_query(cur, query, iterations)
        print(f"Depth {depth} → P50: {p50*1000:.2f} ms, P95: {p95*1000:.2f} ms, P99: {p99*1000:.2f} ms, TPS: {tps:.2f}")
        rows.append({
            'scenario': 'A-2', 'scale_up': '', 'depth': depth,
            'p50_ms': p50*1000, 'p95_ms': p95*1000, 'p99_ms': p99*1000, 'tps': tps
        })


def scenario3_partition_reconciliation(cur, conn, cfg, params, nodes, depths, iterations, rows):
    split = params['partition_reconciliation']['split_ratio']
    split_dur = params['partition_reconciliation']['split_duration_sec']
    recon_dur = params['partition_reconciliation']['reconcile_duration_sec']
    total = cfg.num_drones
    boundary = int(total * split[0])
    print(f"\n-- Partition: A={boundary}, B={total-boundary}, duration={split_dur}s --")
    start = time.time()
    while time.time() - start < split_dur:
        for did in range(boundary):
            cur.execute("UPDATE delegation SET hq_id=%s WHERE drone_id=%s", (cfg.headquarters_id, did))
        for did in range(boundary, total):
            cur.execute("UPDATE delegation SET hq_id=%s WHERE drone_id=%s", (cfg.headquarters_id, did))
        conn.commit()
    # 재결합 및 동기화
    num_sync = params['partition_reconciliation']['post_reconcile_sync_requests']
    print(f"Partition 해제, 동기화 {num_sync}건, duration={recon_dur}s --")
    query = get_bench_query(cfg.headquarters_id, depths[0])
    p50, p95, p99, tps = benchmark_query(cur, query, num_sync)
    print(f"Reconciliation → P50: {p50*1000:.2f} ms, P95: {p95*1000:.2f} ms, P99: {p99*1000:.2f} ms, TPS: {tps:.2f}")
    rows.append({
        'scenario': 'A-3', 'scale_up': total, 'depth': depths[0],
        'p50_ms': p50*1000, 'p95_ms': p95*1000, 'p99_ms': p99*1000, 'tps': tps
    })

# ─────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Scenario A Dynamic Topology Benchmark")
    parser.add_argument('-c', '--config', required=True, help='Path to config JSON')
    parser.add_argument('-s', '--scenario', required=True, choices=['1', '2', '3'], help='Scenario number')
    args = parser.parse_args()

    # config 읽기
    cfg = TestConfig(args.config)
    with open(args.config) as f:
        cfg_json = json.load(f)

    # 벤치파라미터
    scale_up_nodes = cfg_json.get('scale_up_nodes', [100000, 500000, 1000000])
    depths = cfg_json.get('depths', [4, 8, 12, 16])
    iterations = cfg_json.get('iterations', 100)

    # DB 연결
    conn = psycopg.connect(**cfg.db_params)
    cur = conn.cursor()

    # 결과 저장용 리스트
    rows = []

    params = cfg.scenario_params.get(args.scenario, {})
    if args.scenario == '1':
        print("=== Running Scenario A-1: Real-Time Turn-Taking ===")
        scenario1_realtime_turntaking(cur, conn, cfg, params, scale_up_nodes, depths, iterations, rows)
    elif args.scenario == '2':
        print("=== Running Scenario A-2: Chain-Churn ===")
        scenario2_chain_churn(cur, conn, cfg, params, scale_up_nodes, depths, iterations, rows)
    else:
        print("=== Running Scenario A-3: Partition & Reconciliation ===")
        scenario3_partition_reconciliation(cur, conn, cfg, params, scale_up_nodes, depths, iterations, rows)


    # CSV로 결과 출력
    # result_dir = Path(ROOT) / 'data' / 'result'
    result_dir = Path(ROOT) / cfg.data_result_path
    result_dir.mkdir(parents=True, exist_ok=True)
    
    
    output_file = result_dir / f"A_{args.scenario}_results.csv"
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=['scenario', 'scale_up', 'depth', 'p50_ms', 'p95_ms', 'p99_ms', 'tps'])
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    print(f"Results written to {output_file}")

    cur.close()
    conn.close()
