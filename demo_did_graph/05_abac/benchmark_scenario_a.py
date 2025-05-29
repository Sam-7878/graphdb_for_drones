#!/usr/bin/env python3
# benchmark_scenario_a.py
# Scenario A Dynamic Topology Benchmark: 비대칭키 서명 + PostgreSQL RDB

import time
import psycopg
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
from common.bench_utils import benchmark_query, benchmark_query_parametric



# ─── Scenario A 전용: Drone 개수만 세는 CTE 쿼리 ──────────────────────────────
def get_bench_query(start_hq: str, depth: int) -> str:
    """
    Delegation 계층(1..depth)에서 Drone 노드만 카운트.
    depth 단계까지 c.lvl < depth 로 탐색(1..depth) 보장.
    """
    return f"""
    WITH RECURSIVE chain(drone_id, hq_id, lvl) AS (
      -- level 1
      SELECT drone_id, hq_id, 1 AS lvl
        FROM delegation
       WHERE hq_id = '{start_hq}'
    UNION ALL
      -- levels 2..depth
      SELECT d.drone_id, d.hq_id, c.lvl + 1
        FROM delegation d
        JOIN chain c ON d.hq_id = c.drone_id::TEXT
       WHERE c.lvl < {depth}
    )
    SELECT COUNT(*) AS drone_count FROM chain;
    """

def load_drones(cur):
    # delegation 테이블에서 등록된 드론 DID 목록을 로드
    cur.execute("SELECT drone_id FROM delegation;")
    rows = cur.fetchall()
    return [row[0] for row in rows]

# ─────────────────────────────────────────────────────────
# Scenario별 워크로드 함수
# ─────────────────────────────────────────────────────────
def scenario1_realtime_turntaking(cur, conn, cfg, params, nodes, depths, iterations, rows):
    interval = params['turn_taking']['interval_sec']
    ratio = params['turn_taking']['update_ratio']
    drones_list = load_drones(cur)
    chunk_size = cfg.chunk_size
    hq = cfg.headquarters_id

    for num_nodes in nodes:
        print(f"\n-- Scale-up: update_count based on {num_nodes} nodes (Turn-Taking) --")

        for depth in depths:
            # Delegation 업데이트
            update_count = int(num_nodes * ratio)
            # 그 중에서 update_count 만큼 랜덤 샘플링
            # drones = random.sample(range(num_nodes), update_count)
            drones = random.sample(drones_list, update_count)

            # ─── moderate‐sized batch 업데이트 ───
            for i in range(0, update_count, chunk_size):
                chunk = [str(d) for d in drones[i:i+chunk_size]]   # ensure they're strings
                # 위임 관계 삭제            
                cur.execute(
                    """
                    DELETE FROM delegation 
                    WHERE drone_id = ANY(%s::text[]) AND hq_id = %s
                    """, 
                    (chunk, hq)
                )

                # 위임 관계 생성
                cur.execute(
                    """
                    INSERT INTO delegation (drone_id, hq_id)
                    SELECT unnest(%s::text[]), %s
                    ON CONFLICT (drone_id) DO UPDATE
                    SET hq_id = EXCLUDED.hq_id
                    """,
                    (chunk, hq)
                )
                conn.commit()

            time.sleep(interval)

            # 성능 측정
            query = get_bench_query(cfg.headquarters_id, depth)
            # ─── 캐시 워밍업: 첫 실행으로 OS/DB 버퍼 채우기 ───
            cur.execute(query)
            cur.fetchone()

            p50, p95, p99, tps = benchmark_query(cur, query, iterations)

            print(f"Depth {depth} → P50: {p50*1000:.2f} ms, P95: {p95*1000:.2f} ms, P99: {p99*1000:.2f} ms, TPS: {tps:.2f}")
            # 결과 저장
            rows.append({
                'scenario': f'A-{args.scenario}',
                'scale_up': num_nodes, 
                'depth': depth,
                'p50_ms': p50*1000, 
                'p95_ms': p95*1000, 
                'p99_ms': p99*1000, 
                'tps': tps
            })


def get_rdb_bench_query(headquarters_id: str, depth: int) -> str:
    """
    Returns a SQL query which, starting from all drones directly
    delegated by headquarters_id, recursively follows delegation
    edges up to the given depth, and counts how many drones are
    reachable.
    """
    return f"""
    WITH RECURSIVE chain(d_id, lvl) AS (
        -- depth 1: drones directly under HQ
        SELECT d.drone_id, 1
          FROM delegation d
         WHERE d.hq_id = %s

      UNION ALL

        -- depth >1: drones delegated by any in the previous level
        SELECT d2.drone_id, c.lvl + 1
          FROM chain c
          JOIN delegation d2
            ON d2.hq_id = c.d_id
         WHERE c.lvl < {depth}
    )
    SELECT count(*) FROM chain;
    """


def scenario2_chain_churn(cfg, params, iterations, rows):
    depths           = cfg.depths
    scale_up_nodes   = cfg.scale_up_nodes
    interval         = params['chain_churn']['cycle_interval_sec']
    ratio            = params['chain_churn']['update_ratio']
    chunk_size       = cfg.chunk_size
    hq_id            = cfg.headquarters_id

    # 1) prime your drone list
    conn = psycopg.connect(**cfg.db_params)
    cur = conn.cursor()    

    drones_list = drones_list = load_drones(cur)

    for num_nodes in scale_up_nodes:
        print(f"\n-- Chain-Churn (A): num_nodes={num_nodes} --")

        for depth in depths:
            # pick a random subset to re-delegate
            update_count = int(num_nodes * ratio)
            selected     = random.sample(drones_list, update_count)

            # delete+re-create delegation in chunks
            for chunk in (selected[i:i+chunk_size] for i in range(0, len(selected), chunk_size)):
                # 2a) DELETE old edges
                cur.execute(
                    "DELETE FROM delegation WHERE drone_id = ANY(%s::text[]);",
                    (chunk,)
                )
                # 2b) INSERT new edges
                cur.execute(
                    """
                    INSERT INTO delegation (drone_id, hq_id)
                      SELECT unnest(%s::text[]), %s
                    ON CONFLICT (drone_id) DO UPDATE
                      SET hq_id = EXCLUDED.hq_id
                    """,
                    (chunk, hq_id)
                )
                conn.commit()

            # 3) wait before measuring
            time.sleep(interval)

            # 4) warm-up run
            query = get_rdb_bench_query(hq_id, depth)
            cur.execute(query, (hq_id,))
            cur.fetchone()

            # 5) actual benchmarking
            p50, p95, p99, tps = benchmark_query_parametric(cur, query, iterations, (hq_id,))

            print(f"Depth {depth} → P50: {p50*1000:.2f} ms, "
                  f"P95: {p95*1000:.2f} ms, P99: {p99*1000:.2f} ms, "
                  f"TPS: {tps:.2f}")

            rows.append({
                'scenario': f'A-{args.scenario}',
                'scale_up': num_nodes,
                'depth': depth,
                'p50_ms': p50 * 1000,
                'p95_ms': p95 * 1000,
                'p99_ms': p99 * 1000,
                'tps': tps,
            })

    cur.close()
    conn.close()




def scenario3_abac(cur, cfg, params, iterations, rows):
    depths = params['abac']['max_depths']

    cur.execute("SELECT did FROM abac_user;")
    users = [r[0] for r in cur.fetchall()]
    
    cur.execute("SELECT id FROM abac_resource;")
    resources = [r[0] for r in cur.fetchall()]

    for depth in depths:
        user = random.choice(users)
        resource = random.choice(resources)

        query = f"""
        WITH RECURSIVE groups(user_did, group_id, lvl) AS (
          SELECT user_did, group_id, 1 FROM abac_member WHERE user_did = '{user}'
          UNION ALL
          SELECT g.user_did, s.to_id, g.lvl + 1
            FROM abac_subgroup s
            JOIN groups g ON s.from_id = g.group_id
          WHERE g.lvl < '{depth}'
        )
        SELECT COUNT(*) FROM groups g
          JOIN abac_permission p ON g.group_id = p.group_id
         WHERE p.resource_id = '{resource}';
        """
        cur.execute(query)
        cur.fetchone()

        p50, p95, p99, tps = benchmark_query(cur, query, iterations)

        print(f"[ABAC depth={depth}] P50={p50*1000:.2f}ms, p95={p95*1000:.2f}ms, p99={p99*1000:.2f}ms, TPS={tps:.2f}")
        rows.append({
            'scenario': f'A-{args.scenario}', 
            'depth':depth, 
            'p50_ms':p50*1000, 
            'p95_ms':p95*1000, 
            'p99_ms':p99*1000, 
            'tps':tps
        })


def scenario4_web_of_trust(cur, cfg, params, iterations, rows):
    anchor = params['web_of_trust']['anchor_did']
    lengths = params['web_of_trust']['max_path_lengths']

    cur.execute("SELECT DISTINCT from_did FROM web_trust WHERE from_did <> %s;", (anchor,))
    candidates = [r[0] for r in cur.fetchall()]
    for length in lengths:
        client = random.choice(candidates)

        query = f"""
        WITH RECURSIVE trust_path(from_did, to_did, depth) AS (
          SELECT from_did, to_did, 1 FROM web_trust WHERE from_did = '{client}'
          UNION ALL
          SELECT wt.from_did, wt.to_did, tp.depth + 1
            FROM web_trust wt
            JOIN trust_path tp ON wt.from_did = tp.to_did
          WHERE tp.depth < '{length}'
        )
        SELECT COUNT(*) FROM trust_path WHERE to_did = '{anchor}';
        """
        cur.execute(query)
        cur.fetchone()

        p50, p95, p99, tps = benchmark_query(cur, query, iterations)

        print(f"[WebTrust len={length}] P50={p50*1000:.2f}ms, p95={p95*1000:.2f}ms, p99={p99*1000:.2f}ms, TPS={tps:.2f}")
        rows.append({
            'scenario': f'A-{args.scenario}', 
            'length':length, 
            'p50_ms':p50*1000, 
            'p95_ms':p95*1000, 
            'p99_ms':p99*1000, 
            'tps':tps
        })



def scenario5_partition_reconciliation(cur, conn, cfg, params, nodes, depths, iterations, rows):
    split = params['partition_reconciliation']['split_ratio']
    split_dur = params['partition_reconciliation']['split_duration_sec']
    recon_dur = params['partition_reconciliation']['reconcile_duration_sec']

    total = cfg.num_drones
    boundary = int(total * split[0])
    print(f"\n-- Partition: A={boundary}, B={total-boundary}, duration={split_dur}s --")
    start = time.time()

    # update_count = int(cfg.num_drones * ratio)
    # drones = random.sample(drones_list, update_count)

    first  = list(range(boundary))
    second = list(range(boundary, total))
    # 파티션별 배치 업데이트
    # ─── moderate‐sized batch 파티션 A/B 업데이트 ───
    chunk_size = cfg.chunk_size
    while time.time() - start < split_dur:
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

    # 재결합 및 동기화
    num_sync = params['partition_reconciliation']['post_reconcile_sync_requests']
    print(f"Partition 해제, 동기화 {num_sync}건, duration={recon_dur}s --")

    query = get_bench_query(cfg.headquarters_id, depths[0])
    cur.execute(query)
    cur.fetchone()

    p50, p95, p99, tps = benchmark_query(cur, query, num_sync)

    print(f"Reconciliation → P50: {p50*1000:.2f} ms, P95: {p95*1000:.2f} ms, P99: {p99*1000:.2f} ms, TPS: {tps:.2f}")
    rows.append({
        'scenario': f'A-{args.scenario}', 
        'scale_up': total, 
        'depth': depths[0],
        'p50_ms': p50*1000, 
        'p95_ms': p95*1000, 
        'p99_ms': p99*1000, 
        'tps': tps
    })



# ─────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Scenario A Dynamic Topology Benchmark")
    parser.add_argument('-c', '--config', required=True, help='Path to config JSON')
    parser.add_argument('-s', '--scenario', required=True, choices=['1', '2', '3', '4', '5'], help='Scenario number')
    args = parser.parse_args()

    # config 읽기
    cfg = TestConfig(args.config)
    with open(args.config) as f:
        cfg_json = json.load(f)

    # 벤치파라미터
    scale_up_nodes = cfg_json.get('scale_up_nodes', [1000, 5000, 10000])
    depths = cfg_json.get('depths', [2, 4, 6, 8])
    iterations = cfg_json.get('iterations', 100)
    params = cfg.scenario_params.get(args.scenario, {})

    # DB 연결
    conn = psycopg.connect(**cfg.db_params)
    cur = conn.cursor()
    # 커밋 동기화(off) 설정: COMMIT 시 디스크 동기화 대기 없이 반환
    cur.execute("SET synchronous_commit = ON;")
    print("› synchronous_commit ON 설정 완료")

    # # ★ 실제 테이블에서 드론 ID 목록을 먼저 로드
    # cur.execute("SELECT id FROM drones;")
    # drones_list = [row[0] for row in cur.fetchall()]


    rows = []
    if args.scenario == '1':
        print("=== Running Scenario A-1: Real-Time Turn-Taking ===")
        scenario1_realtime_turntaking(cur, conn, cfg, params, scale_up_nodes, depths, iterations, rows)
    elif args.scenario == '2':
        print("=== Running Scenario A-2: Chain-Churn ===")
        scenario2_chain_churn(cfg, params, iterations, rows)
    elif args.scenario == '3':
        print("=== Running Scenario A-3: ABAC (RDB) ===")
        scenario3_abac(cur, cfg, params, iterations, rows)
    elif args.scenario == '4':
        print("=== Running Scenario A-4: Web-of-Trust (RDB) ===")
        scenario4_web_of_trust(cur, cfg, params, iterations, rows)
    elif args.scenario == '5':
        print("=== Running Scenario A-5: Partition & Reconciliation ===")
        scenario5_partition_reconciliation(cur, conn, cfg, params, scale_up_nodes, depths, iterations, rows)
    else:
        raise ValueError("Unsupported scenario for security patterns")


   # 결과 저장
    result_dir = Path(ROOT) / cfg.data_result_path
    result_dir.mkdir(parents=True, exist_ok=True)
    output_file = result_dir / f"A_{args.scenario}_results.csv"
    with open(output_file, 'w', newline='') as f:
        cols = []
        if args.scenario in ['1', '2', '5']:
            cols = ['scenario', 'scale_up', 'depth', 'p50_ms', 'p95_ms', 'p99_ms', 'tps']
        elif args.scenario == '3':
            cols = ['scenario', 'scale_up', 'depth', 'p50_ms', 'p95_ms', 'p99_ms', 'tps']
        elif args.scenario == '4':
             cols = ['scenario', 'scale_up', 'length', 'p50_ms', 'p95_ms', 'p99_ms', 'tps']

        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
    print(f"Results written to {output_file}")
    
    cur.close()
    conn.close()
