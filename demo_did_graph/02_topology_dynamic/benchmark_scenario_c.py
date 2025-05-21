#!/usr/bin/env python3
# benchmark_scenario_c.py
# Scenario C Dynamic Topology Benchmark: DID + VC + AgensGraph

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


def get_bench_query(hq_id: str, max_depth: int) -> str:
    return f"""
MATCH (hq:HQ {{id:'{hq_id}'}})
      -[:DELEGATES*1..{max_depth}]->(d:Drone)
      <-[:ASSERTS]-(v:VC)
RETURN count(v) AS vc_count;
"""


def scenario1_realtime_turntaking(cur, conn, cfg, params, nodes, depths, iterations, rows, drones_list):
    interval = params['turn_taking']['interval_sec']
    ratio = params['turn_taking']['update_ratio']
    for total in nodes:
        print(f"\n-- Scale-up: {total} nodes (Turn-Taking) --")
        for depth in depths:
            # 실제 노드 수를 반영한 업데이트 카운트 계산 및 샘플링
            update_count = int(total * ratio)
            # print(f"Total : {total}, ratio : {ratio} --> Updating {update_count} DELEGATES edges...")
            selected = random.sample(drones_list, update_count)
            # Delegation 업데이트
            # update_count = int(total * ratio)
            # selected = random.sample(range(total), update_count)


            # DELEGATES 엣지 batch 업데이트
            chunk_size = cfg.chunk_size
            for i in range(0, len(selected), chunk_size):
                chunk = selected[i:i+chunk_size]
                ids_list = '[' + ','.join(f"'{did}'" for did in chunk) + ']'  # Cypher용 ID 리스트
                cur.execute(
                    """
                    UNWIND """ + ids_list + """ AS id
                    MATCH ()-[r:DELEGATES]->(d:Drone {id:id})
                    DELETE r
                    """
                )
                cur.execute(
                    """
                    UNWIND """ + ids_list + """ AS id
                    MATCH (hq:HQ {id:'""" + cfg.headquarters_id + """'}), (d:Drone {id:id})
                    CREATE (hq)-[:DELEGATES]->(d)
                    """
                )
                conn.commit()

            time.sleep(interval)
            query = get_bench_query(cfg.headquarters_id, depth)
            p50, p95, p99, tps = benchmark_query(cur, query, iterations)
            print(f"Depth {depth} → P50: {p50*1000:.2f} ms, P95: {p95*1000:.2f} ms, P99: {p99*1000:.2f} ms, TPS: {tps:.2f}")
            rows.append({
                'scenario': 'C-1',
                'scale_up': total,
                'depth': depth,
                'p50_ms': p50*1000,
                'p95_ms': p95*1000,
                'p99_ms': p99*1000,
                'tps': tps
            })


def scenario2_chain_churn(cur, conn, cfg, params, nodes, depths, iterations, rows, drones_list):
    cycle = params['chain_churn']['depth_cycle']
    interval = params['chain_churn']['cycle_interval_sec']
    ratio = params['chain_churn']['update_ratio']

    for depth in cycle:
        print(f"\n-- Chain-Churn: depth={depth} --")
        # 실제 존재하는 Drone ID 목록에서 샘플링
        update_count = int(len(drones_list) * ratio)
        selected = random.sample(drones_list, update_count)
        
        chunk_size = cfg.chunk_size
        for i in range(0, len(selected), chunk_size):
            chunk = selected[i:i+chunk_size]
            ids_list = '[' + ','.join(f"'{did}'" for did in chunk) + ']'  # Cypher용 ID 리스트
            # 기존 DELEGATES 엣지 삭제
            cur.execute(
                """
                UNWIND """ + ids_list + """ AS id
                MATCH ()-[r:DELEGATES]->(d:Drone {id:id})
                DELETE r
                """
            )
            # 새로운 DELEGATES 엣지 생성
            cur.execute(
                """
                UNWIND """ + ids_list + """ AS id
                MATCH (hq:HQ {id:'""" + cfg.headquarters_id + """'}), (d:Drone {id:id})
                CREATE (hq)-[:DELEGATES]->(d)
                """
            )
            conn.commit()

        time.sleep(interval)
        query = get_bench_query(cfg.headquarters_id, depth)
        p50, p95, p99, tps = benchmark_query(cur, query, iterations)
        print(f"Depth {depth} → P50: {p50*1000:.2f} ms, P95: {p95*1000:.2f} ms, P99: {p99*1000:.2f} ms, TPS: {tps:.2f}")
        rows.append({
            'scenario': 'C-2',
            'scale_up': '',
            'depth': depth,
            'p50_ms': p50*1000,
            'p95_ms': p95*1000,
            'p99_ms': p99*1000,
            'tps': tps
        })


# scenario3_partition_reconciliation remains unchanged

def scenario3_partition_reconciliation(cur, conn, cfg, params, nodes, depths, iterations, rows, drones_list):
    split = params['partition_reconciliation']['split_ratio']
    split_duration = params['partition_reconciliation']['split_duration_sec']
    recon_sync = params['partition_reconciliation']['post_reconcile_sync_requests']
    total = cfg.num_drones
    boundary = int(total * split[0])
    print(f"\n-- Partition: A={boundary}, B={total-boundary}, duration={split_duration}s --")
    start = time.time()

    # ─── moderate‐sized batch 파티션 A/B 업데이트 ───
    first_ids  = drones_list[:boundary]
    second_ids = drones_list[boundary:]
    chunk_size = 500
    while time.time() - start < split_duration:
        # Partition A
        for i in range(0, len(first_ids), chunk_size):
            chunk = first_ids[i:i+chunk_size]
            ids_list = "[" + ",".join(f"'{did}'" for did in chunk) + "]"

            cur.execute(
                """
                UNWIND """ + ids_list + """ AS id
                MATCH ()-[r:DELEGATES]->(d:Drone {id:id})
                DELETE r
                """
            )
            cur.execute(
                """
                UNWIND """ + ids_list + """ AS id
                MATCH (hq:HQ {id:'""" + cfg.headquarters_id + """'}), (d:Drone {id:id})
                CREATE (hq)-[:DELEGATES]->(d)
                """
            )
            conn.commit()

        # Partition B
        for i in range(0, len(second_ids), chunk_size):
            chunk = second_ids[i:i+chunk_size]
            ids_list = "[" + ",".join(f"'{did}'" for did in chunk) + "]"

            cur.execute(
                """
                UNWIND """ + ids_list + """ AS id
                MATCH ()-[r:DELEGATES]->(d:Drone {id:id})
                DELETE r
                """
            )
            cur.execute(
                """
                UNWIND """ + ids_list + """ AS id
                MATCH (hq:HQ {id:'""" + cfg.headquarters_id + """'}), (d:Drone {id:id})
                CREATE (hq)-[:DELEGATES]->(d)
                """
            )
            conn.commit()


    # 재결합 벤치마크
    print(f"Partition 해제, 동기화 {recon_sync}건 --")
    query = get_bench_query(cfg.headquarters_id, depths[0])
    p50, p95, p99, tps = benchmark_query(cur, query, recon_sync)
    print(f"Reconciliation → P50: {p50*1000:.2f} ms, P95: {p95*1000:.2f} ms, P99: {p99*1000:.2f} ms, TPS: {tps:.2f}")
    rows.append({
        'scenario': 'C-3',
        'scale_up': total,
        'depth': depths[0],
        'p50_ms': p50*1000,
        'p95_ms': p95*1000,
        'p99_ms': p99*1000,
        'tps': tps
    })


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Scenario C Dynamic Topology Benchmark")
    parser.add_argument('-c', '--config', required=True, help='Path to config JSON')
    parser.add_argument('-s', '--scenario', required=True, choices=['1', '2', '3'], help='Scenario number')
    args = parser.parse_args()

    cfg = TestConfig(args.config)
    with open(args.config, 'r') as f:
        cfg_json = json.load(f)

    scale_up_nodes = cfg_json.get('scale_up_nodes', [])
    depths = cfg_json.get('depths', [])
    iterations = cfg_json.get('iterations', 100)
    params = cfg.scenario_params.get(args.scenario, {})

    conn = psycopg.connect(**cfg.db_params)
    cur = conn.cursor()
    cur.execute("SET graph_path = vc_graph;")

    # 전체 Drone ID 목록 가져오기
    cur.execute("MATCH (d:Drone) RETURN d.id;")
    drones_list = [row[0] for row in cur.fetchall()]

    rows = []
    if args.scenario == '1':
        scenario1_realtime_turntaking(cur, conn, cfg, params, scale_up_nodes, depths, iterations, rows, drones_list)
    elif args.scenario == '2':
        scenario2_chain_churn(cur, conn, cfg, params, scale_up_nodes, depths, iterations, rows, drones_list)
    else:
        scenario3_partition_reconciliation(cur, conn, cfg, params, scale_up_nodes, depths, iterations, rows, drones_list)

    result_dir = Path(ROOT) / cfg.data_result_path
    result_dir.mkdir(parents=True, exist_ok=True)
    output_file = result_dir / f"C_{args.scenario}_results.csv"
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=['scenario','scale_up','depth','p50_ms','p95_ms','p99_ms','tps'])
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    print(f"Results written to {output_file}")

    cur.close()
    conn.close()
