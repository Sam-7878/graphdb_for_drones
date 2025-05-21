#!/usr/bin/env python3
# benchmark_scenario_b.py
# Scenario B Benchmark: DID 인증 + RDB 기반

import time
import psycopg
import json
import random
import argparse
import csv
from pathlib import Path
import sys

# 프로젝트 루트를 PYTHONPATH에 추가
ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))
from common.load_config import TestConfig
from common.did_utils import load_private_key
from common.bench_utils import benchmark_query, benchmark_query_parametric
from setup_scenario_b import setup_database


def load_drones(cur):
    # delegation 테이블에서 등록된 드론 DID 목록을 로드
    cur.execute("SELECT drone_id FROM delegation;")
    rows = cur.fetchall()
    return [row[0] for row in rows]


def scenario1_realtime_turntaking(cfg, params, iterations, rows, private_key):
    conn = psycopg.connect(**cfg.db_params)
    cur = conn.cursor()

    # 드론 목록 로드
    drones_list = load_drones(cur)
    hq = cfg.headquarters_id
    depths = cfg.depths
    chunk_size = cfg.chunk_size

    scale_nodes = cfg.scale_up_nodes
    ratio = params['turn_taking']['update_ratio']
    interval = params['turn_taking']['interval_sec']

    for num_nodes in scale_nodes:
        print(f"\n-- Scale-up: update_count based on {num_nodes} nodes (Turn-Taking) --")

        for depth in depths:
            update_count = int(num_nodes * ratio)
            # 샘플링: DB 로드된 DID 리스트 사용
            sample_dids = random.sample(drones_list, update_count)
            # 위임 관계 삭제            
            for did in sample_dids:
                cur.execute(
                    "DELETE FROM delegation WHERE drone_id = %s AND hq_id = %s;",
                    (did, hq)
                )
            # ─── moderate‐sized batch 업데이트 ───
            # for i in range(0, len(sample_dids), chunk_size):
            #     chunk = [str(d) for d in sample_dids[i:i+chunk_size]]   # ensure they're strings
            #     cur.execute(
            #         """
            #         DELETE delegation
            #         WHERE hq_id = %s AND drone_id = ANY(%s::text[])
            #         """,
            #         (cfg.headquarters_id, chunk)
            #     )
            conn.commit()

            # 위임 관계 생성
            for did in sample_dids:
                cur.execute(
                    "INSERT INTO delegation(drone_id, hq_id) VALUES(%s, %s)"
                    " ON CONFLICT (drone_id) DO UPDATE SET hq_id = EXCLUDED.hq_id;",
                    (did, hq)
                )           
            conn.commit()

            time.sleep(interval)

            # 벤치마크 위한 쿼리
            query = f"WITH RECURSIVE chain(drone_id, hq_id, lvl) AS ("
            query += f" SELECT drone_id, hq_id, 1 FROM delegation WHERE hq_id = '{hq}' "
            query += f" UNION ALL "
            query += f" SELECT d.drone_id, d.hq_id, lvl+1 FROM chain c "
            query += f" JOIN delegation d ON c.drone_id = d.hq_id "
            query += f" WHERE lvl < {depth}) SELECT count(*) FROM chain;"
            cur.execute(query)
            cur.fetchone()

            # 벤치마크
            p50, p95, p99, tps = benchmark_query(cur, query, iterations)

            print(f"Depth {depth} → P50: {p50*1000:.2f} ms, P95: {p95*1000:.2f} ms, P99: {p99*1000:.2f} ms, TPS: {tps:.2f}")
            rows.append({
                'scenario': f'B-{args.scenario}',
                'scale_up': num_nodes,
                'depth': depth,
                'p50_ms': p50*1000,
                'p95_ms': p95*1000,
                'p99_ms': p99*1000,
                'tps': tps
            })
    cur.close()
    conn.close()


def scenario2_chain_churn(cfg, params, iterations, rows, private_key):
    conn = psycopg.connect(**cfg.db_params)
    cur = conn.cursor()

    drones = load_drones(cur)
    depths = cfg.depths
    scale_nodes = cfg.scale_up_nodes
    ratio = params['2']['chain_churn']['update_ratio']

    for num_nodes in scale_nodes:
        for depth in depths:
            churn_count = int(len(drones) * ratio)
            sample_dids = random.sample(drones, churn_count)
            # 체인 분리 및 재결합
            for did in sample_dids:
                cur.execute("DELETE FROM delegation WHERE drone_id = %s;", (did,))
            for did in sample_dids:
                cur.execute(
                    "INSERT INTO delegation(drone_id, hq_id) VALUES(%s, %s)"
                    " ON CONFLICT (drone_id) DO UPDATE SET hq_id = EXCLUDED.hq_id;",
                    (did, cfg.headquarters_id)
                )
            conn.commit()

            # 벤치마크 위한 쿼리
            # 체인 깊이와 노드 수에 따라 쿼리 생성
            sql = f"WITH RECURSIVE chain(drone_id, hq_id, lvl) AS ("
            sql += f" SELECT drone_id, hq_id, 1 FROM delegation WHERE hq_id = '{cfg.headquarters_id}' "
            sql += f" UNION ALL SELECT d.drone_id, d.hq_id, lvl+1 FROM chain c "
            sql += f" JOIN delegation d ON c.drone_id = d.hq_id WHERE lvl < {depth}) SELECT count(*) FROM chain;"


            # 벤치마크
            p50, p95, p99, tps = benchmark_query(cur, sql, iterations)

            print(f"Depth {depth} → P50: {p50*1000:.2f} ms, P95: {p95*1000:.2f} ms, P99: {p99*1000:.2f} ms, TPS: {tps:.2f}")
            rows.append({
                'scenario': f'B-{args.scenario}',
                'scale_up': num_nodes,
                'depth': depth,
                'p50_ms': p50*1000,
                'p95_ms': p95*1000,
                'p99_ms': p99*1000,
                'tps': tps
            })

    cur.close()
    conn.close()


def scenario3_partition_reconciliation(cfg, params, iterations, rows, private_key):
    conn = psycopg.connect(**cfg.db_params)
    cur = conn.cursor()

    drones = load_drones(cur)
    partitions = params['3']['partition_reconciliation']['partitions']
    depths = params['3']['partition_reconciliation']['depths']
    syncs = params['3']['partition_reconciliation']['post_reconcile_sync_requests']

    for part in partitions:
        for depth in depths:
            # 파티션 왕복 업데이트
            for did in part:
                cur.execute("DELETE FROM delegation WHERE drone_id = %s;", (did,))
                cur.execute(
                    "INSERT INTO delegation(drone_id, hq_id) VALUES(%s, %s)"
                    " ON CONFLICT (drone_id) DO UPDATE SET hq_id = EXCLUDED.hq_id;",
                    (did, cfg.headquarters_id)
                )
            conn.commit()

            for _ in range(syncs):
                sql = f"WITH RECURSIVE chain(drone_id, hq_id, lvl) AS ("
                sql += f" SELECT drone_id, hq_id, 1 FROM delegation WHERE hq_id = '{cfg.headquarters_id}' "
                sql += f" UNION ALL SELECT d.drone_id, d.hq_id, lvl+1 FROM chain c "
                sql += f" JOIN delegation d ON c.drone_id = d.hq_id WHERE lvl < {depth}) SELECT count(*) FROM chain;"
                p50, p95, p99, tps = benchmark_query(cur, sql, iterations)
            rows.append(["scenario3", part, depth, p50, p95, p99, tps])

    cur.close()
    conn.close()


def scenario4_web_of_trust(cfg, params, iterations, rows):
    # 1) 연결 열기
    conn = psycopg.connect(**cfg.db_params)
    cur = conn.cursor()

    # 2) 파라미터 언패킹
    anchor  = params['web_of_trust']['anchor_did']
    lengths = params['web_of_trust']['max_path_lengths']
    interval = params['network']['link_partition']['cycle_interval_sec']

    # 3) 후보 DID 목록 추출 (entity 테이블에서)
    cur.execute("SELECT from_did FROM web_trust;")
    candidates = [r[0] for r in cur.fetchall()]

    # 4) 파라미터화된 재귀 CTE 쿼리 준비
    cte_query = """
    WITH RECURSIVE path(cn, lvl) AS (
      SELECT from_did::text, 1
        FROM web_trust
       WHERE from_did = %(client)s
      UNION ALL
      SELECT w.to_did::text, p.lvl + 1
        FROM path p
        JOIN web_trust w ON p.cn = w.from_did
       WHERE p.lvl < %(length)s
    )
    SELECT count(*) FROM path WHERE cn = %(anchor)s;
    """

    # 5) 워밍업: Plan 캐싱 및 JIT 컴파일 방지
    warm_client = random.choice(candidates)
    warm_length = lengths[0]
    cur.execute(cte_query, {
        'client': warm_client,
        'length': warm_length,
        'anchor': anchor
    })
    cur.fetchone()

    # 6) 본격 벤치마크 루프
    for length in lengths:
        client = random.choice(candidates)
        time.sleep(interval)

        p50, p95, p99, tps = benchmark_query_parametric(
            cur,
            cte_query,
            iterations,
            params={'client': client, 'length': length, 'anchor': anchor}
        )

        print(f"[WebTrust len={length}] "
              f"P50={p50*1000:.2f}ms, "
              f"P95={p95*1000:.2f}ms, "
              f"P99={p99*1000:.2f}ms, "
              f"TPS={tps:.2f}")

        rows.append({
            'scenario': f'B-{args.scenario}',
            'scale_up': '',
            'length': length,
            'p50_ms': p50*1000,
            'p95_ms': p95*1000,
            'p99_ms': p99*1000,
            'tps': tps
        })

    # 7) 정리
    cur.close()
    conn.close()


def scenario5_abac(cur, cfg, params, iterations, rows):
    conn = psycopg.connect(**cfg.db_params)
    cur = conn.cursor()

    depths = params['abac']['max_depths']
    interval = params['network']['link_partition']['cycle_interval_sec']

    # delegation과 달리 abac_user 테이블에서 load
    cur.execute("SELECT did FROM abac_user;")
    users = [r[0] for r in cur.fetchall()]

    cur.execute("SELECT id FROM abac_resource;")
    resources = [r[0] for r in cur.fetchall()]
    # resource = params['abac']['resource_id']

    for depth in depths:
        user = random.choice(users)
        resource = random.choice(resources)

        time.sleep(interval)   

        query = ("WITH RECURSIVE chain(did, lvl) AS ("
                " SELECT user_did, 1 FROM abac_member WHERE user_did = '%s' " % user +
                " UNION ALL SELECT s.to_id, lvl+1 FROM chain c JOIN abac_subgroup s ON c.did = s.from_id WHERE lvl < %d) " % depth +
                " SELECT count(*) FROM chain JOIN abac_permission p ON chain.did = p.group_id AND p.resource_id = '%s';" % resource)
        cur.execute(query)
        cur.fetchone()

        p50, p95, p99, tps = benchmark_query(cur, query, iterations)

        print(f"[ABAC depth={depth}] P50={p50*1000:.2f}ms, p95={p95*1000:.2f}ms, p99={p99*1000:.2f}ms, TPS={tps:.2f}")
        rows.append({
            'scenario': f'B-{args.scenario}',
            'scale_up':'', 
            'depth':depth, 
            'p50_ms':p50*1000, 
            'p95_ms':p95*1000, 
            'p99_ms':p99*1000, 
            'tps':tps
        })

    cur.close()
    conn.close()


def main(args):
    cfg = TestConfig(args.config)
    private_key = load_private_key(cfg.private_key_path)
    params = cfg.scenario_params.get(args.scenario, {})
    iterations = cfg.iterations

    rows = []
    setup_database(cfg, private_key, int(args.scenario))

    if args.scenario == '1':
        scenario1_realtime_turntaking(cfg, params, iterations, rows, private_key)
    elif args.scenario == '2':
        scenario2_chain_churn(cfg, params, iterations, rows, private_key)
    elif args.scenario == '3':
        scenario3_partition_reconciliation(cfg, params, iterations, rows, private_key)
    elif args.scenario == '4':
        scenario4_web_of_trust(cfg, params, iterations, rows)
    elif args.scenario == '5':
        scenario5_abac(cfg, params, iterations, rows)

    # 결과 저장
    result_dir = Path(ROOT) / cfg.data_result_path
    result_dir.mkdir(parents=True, exist_ok=True)
    output_file = result_dir / f"B_{args.scenario}_results.csv"
    with open(output_file, 'w', newline='') as f:
        cols = []
        if args.scenario in ['1', '2', '3']:
            cols = ['scenario', 'scale_up', 'depth', 'p50_ms', 'p95_ms', 'p99_ms', 'tps']
        elif args.scenario == '4':
            cols = ['scenario', 'scale_up', 'length', 'p50_ms', 'p95_ms', 'p99_ms', 'tps']
        elif args.scenario == '5':
            cols = ['scenario', 'scale_up', 'depth', 'p50_ms', 'p95_ms', 'p99_ms', 'tps']

        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)
    print(f"Results written to {output_file}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Scenario B Benchmark (RDB-DID)")
    parser.add_argument('-c', '--config', required=True)
    parser.add_argument('-s', '--scenario', required=True, choices=['1','2','3','4','5'])
    args = parser.parse_args()
  
    main(args)
