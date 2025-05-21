#!/usr/bin/env python3
# benchmark_scenario_c.py
# Scenario C Dynamic Topology Benchmark: DID 인증 + AgensGraph(GraphDB)

import time
import psycopg
import json
import random
import argparse
import csv
from pathlib import Path
import sys

# 프로젝트 루트를 PYTHONPATH에 추가 (common 모듈 로드용)
ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))
from common.load_config import TestConfig
from common.bench_utils import benchmark_query, benchmark_query_parametric
from setup_scenario_c import setup_database
from common.did_utils import load_private_key


def get_bench_query(hq_id: str, max_depth: int) -> str:
    """
    delegation*1..max_depth -> Drone <- ASSERTS- VC
    VC 개수를 카운트하는 Cypher 쿼리
    """
    return f"""
    MATCH (hq:HQ {{id:'{hq_id}'}})
      -[:DELEGATES*1..{max_depth}]->(d:Drone)
      <-[:ASSERTS]-(v:VC)
    RETURN count(v) AS vc_count;
    """


def scenario1_realtime_turntaking(cur, conn, cfg, params, scale_up_nodes, depths, iterations, rows, private_key):
    ratio = params['turn_taking']['update_ratio']
    interval = params['turn_taking']['interval_sec']

    for num_nodes in scale_up_nodes:
        # 2) 그래프 DROP/CREATE (별도 커넥션)
        print(f"\n-- Init DB : update_count based on {num_nodes} nodes (Turn-Taking) --")
        setup_database(cfg, private_key, int(args.scenario))

        # 3) 벤치마크용 커넥션 재생성 및 graph_path 재설정
        conn = psycopg.connect(**cfg.db_params)
        cur = conn.cursor()
        cur.execute("SET graph_path = vc_graph;")

        # 최신 Drone ID 목록 조회
        cur.execute("MATCH (d:Drone) RETURN d.id;")
        drones_list = [r[0] for r in cur.fetchall()]

        print(f"\n-- Scale-up: update_count based on {num_nodes} nodes (Turn-Taking) --")
        
        for depth in depths:
            # 업데이트 대상 수 계산
            update_count = int(num_nodes * ratio)
            selected = random.sample(drones_list, update_count)

            # Delegates 엣지 배치 업데이트
            for chunk in (selected[i:i+cfg.chunk_size] for i in range(0, len(selected), cfg.chunk_size)):
                ids = "[" + ",".join(f"'{did}'" for did in chunk) + "]"
                cur.execute(
                    f"UNWIND {ids} AS id MATCH ()-[r:DELEGATES]->(d:Drone {{id:id}}) DELETE r;"
                )
                cur.execute(
                    f"UNWIND {ids} AS id MATCH (hq:HQ {{id:'{cfg.headquarters_id}'}}),(d:Drone {{id:id}}) CREATE (hq)-[:DELEGATES]->(d);"
                )
                conn.commit()

            time.sleep(interval)
            # 워밍업
            query = get_bench_query(cfg.headquarters_id, depth)
            # ─── 캐시 워밍업: 첫 실행으로 OS/DB 버퍼 채우기 ───
            cur.execute(query)
            cur.fetchone()

            # 벤치마크
            p50, p95, p99, tps = benchmark_query(cur, query, iterations)

            print(f"Depth {depth} → P50: {p50*1000:.2f} ms, P95: {p95*1000:.2f} ms, P99: {p99*1000:.2f} ms, TPS: {tps:.2f}")
            rows.append({
                'scenario': f'C-{args.scenario}',
                'scale_up': num_nodes,
                'depth': depth,
                'p50_ms': p50*1000,
                'p95_ms': p95*1000,
                'p99_ms': p99*1000,
                'tps': tps
            })
        cur.close()
        conn.close()


def scenario2_chain_churn(cur, conn, cfg, params, scale_up_nodes, depths, iterations, rows, private_key):
    depths = params['chain_churn']['depth_cycle']
    interval = params['chain_churn']['cycle_interval_sec']
    ratio = params['chain_churn']['update_ratio']

    cur.execute("SET graph_path = vc_graph;")
    cur.execute("MATCH (d:Drone) RETURN d.id;")
    drones_list = [r[0] for r in cur.fetchall()]

    for depth in depths:
        print(f"\n-- Chain-Churn: depth={depth} --")

        # 2) 그래프 DROP/CREATE (별도 커넥션)
        update_count = int(len(drones_list) * ratio)
        selected = random.sample(drones_list, update_count)
        for chunk in (selected[i:i+cfg.chunk_size] for i in range(0, len(selected), cfg.chunk_size)):
            ids = "[" + ",".join(f"'{did}'" for did in chunk) + "]"
            cur.execute(f"UNWIND {ids} AS id MATCH ()-[r:DELEGATES]->(d:Drone {{id:id}}) DELETE r;")
            cur.execute(f"UNWIND {ids} AS id MATCH (hq:HQ {{id:'{cfg.headquarters_id}'}}),(d:Drone {{id:id}}) CREATE (hq)-[:DELEGATES]->(d);")
            conn.commit()

        time.sleep(interval)

        query = get_bench_query(cfg.headquarters_id, depth)
        cur.execute(query)
        cur.fetchone()

        p50, p95, p99, tps = benchmark_query(cur, query, iterations)

        print(f"Depth {depth} → P50: {p50*1000:.2f} ms, P95: {p95*1000:.2f} ms, P99: {p99*1000:.2f} ms, TPS: {tps:.2f}")
        rows.append({
            'scenario': f'C-{args.scenario}',
            'scale_up': '',
            'depth': depth,
            'p50_ms': p50*1000,
            'p95_ms': p95*1000,
            'p99_ms': p99*1000,
            'tps': tps
        })
    cur.close()
    conn.close()

def scenario3_partition_reconciliation(cur, conn, cfg, params, scale_up_nodes, depths, iterations, rows, private_key):
    # Partition & Reconciliation 시나리오도 동일하게 graph 초기화
    split_ratio = params['partition_reconciliation']['split_ratio']
    split_duration_sec = params['partition_reconciliation']['split_duration_sec']
    reconcile_duration_sec = params['partition_reconciliation']['reconcile_duration_sec']
    post_reconcile_sync_requests = params['partition_reconciliation']['post_reconcile_sync_requests']

    print(f"\n=== Reinitializing graph for Partition & Reconciliation ===")
    # setup_database(cfg, private_key, int(args.scenario))
    cur.execute("SET graph_path = vc_graph;")
    cur.execute("MATCH (d:Drone) RETURN d.id;")

    drones_list = [r[0] for r in cur.fetchall()]

    total = len(drones_list)
    boundary = int(total * split_ratio[0])
    first_ids = drones_list[:boundary]
    second_ids = drones_list[boundary:]
    chunk_size = cfg.chunk_size

    start = time.time()
    while time.time() - start < split_duration_sec:
        for ids_group in (first_ids, second_ids):
            for chunk in (ids_group[i:i+chunk_size] for i in range(0, len(ids_group), chunk_size)):
                ids = "[" + ",".join(f"'{did}'" for did in chunk) + "]"
                cur.execute(f"UNWIND {ids} AS id MATCH ()-[r:DELEGATES]->(d:Drone {{id:id}}) DELETE r;")
                cur.execute(f"UNWIND {ids} AS id MATCH (hq:HQ {{id:'{cfg.headquarters_id}'}}),(d:Drone {{id:id}}) CREATE (hq)-[:DELEGATES]->(d);")
        conn.commit()
       
    print(f"Partition 해제, 동기화 {post_reconcile_sync_requests}건 --")
    query = get_bench_query(cfg.headquarters_id, depths[0])
    cur.execute(query)
    cur.fetchone()

    p50, p95, p99, tps = benchmark_query(cur, query, post_reconcile_sync_requests)

    print(f"Reconciliation → P50: {p50*1000:.2f} ms, P95: {p95*1000:.2f} ms, P99: {p99*1000:.2f} ms, TPS: {tps:.2f}")
    rows.append({
        'scenario': f'C-{args.scenario}',
        'scale_up': total,
        'depth': depths[0],
        'p50_ms': p50*1000,
        'p95_ms': p95*1000,
        'p99_ms': p99*1000,
        'tps': tps
    })
    cur.close()
    conn.close()


def scenario4_web_of_trust(cur, cfg, params, iterations, rows):
    # use the correct graph
    cur.execute("SET graph_path = trust_graph;")
    
    anchor = params['web_of_trust']['anchor_did']
    lengths = params['web_of_trust']['max_path_lengths']
    interval = params['network']['link_partition']['cycle_interval_sec']

    # 클라이언트 후보 추출
    cur.execute("MATCH (e:Entity) WHERE e.did <> '{0}' RETURN e.did;".format(anchor))
    candidates = [r[0] for r in cur.fetchall()]

    for length in lengths:
        client = random.choice(candidates)
        newClient = random.choice(candidates)

        time.sleep(interval)
       
        query = f"""
        MATCH path=(c:Entity {{did:'{client}'}})-[:CROSSED_SIGNED*1..{length}]->(a:Entity {{did:'{anchor}'}})
        RETURN count(path) AS path_count;
        """
        # query = f"""
        #     UNWIND $mappings AS pair
        #     MATCH (d:Drone {id: pair.old})
        #     DETACH DELETE d
        #     CREATE (new:Drone {id: pair.new})
        #     WITH pair, new
        #     MATCH (hq:HQ {id: '{cfg.headquarters_id}'})
        #     CREATE (hq)-[:DELEGATES]->(new);
        # """
        cur.execute(query)
        cur.fetchone()

        p50, p95, p99, tps = benchmark_query(cur, query, iterations)

        print(f"[WebTrust len={length}] P50={p50*1000:.2f}ms, p95={p95*1000:.2f}ms, p99={p99*1000:.2f}ms, TPS={tps:.2f}")
        rows.append({
            'scenario': f'C-{args.scenario}',
            'scale_up':'', 
            'length':length, 
            'p50_ms':p50*1000, 
            'p95_ms':p95*1000, 
            'p99_ms':p99*1000, 
            'tps':tps
        })

    cur.close()
    conn.close() 


def scenario5_abac(cur, cfg, params, iterations, rows):
    depths = params['abac']['max_depths']
    interval = params['network']['link_partition']['cycle_interval_sec']

    # 반드시 abac_graph로 전환해야 함
    cur.execute("SET graph_path = abac_graph;")

    # 사용자 & 리소스 목록
    cur.execute("MATCH (u:AppUser) RETURN u.did;")
    users = [r[0] for r in cur.fetchall()]

    cur.execute("MATCH (r:Resource) RETURN r.id;")
    resources = [r[0] for r in cur.fetchall()]

    for depth in depths:
        user = random.choice(users)
        resource = random.choice(resources)

        time.sleep(interval)

        query = f"""
        MATCH (:AppUser {{did:'{user}'}})-[:MEMBER_OF*1..{depth}]->(g:AppGroup)-[:HAS_PERMISSION]->(r:Resource {{id:'{resource}'}})
        RETURN count(r) AS allowed;
        """
        cur.execute(query)
        cur.fetchone()
        
        p50, p95, p99, tps = benchmark_query(cur, query, iterations)

        print(f"[ABAC depth={depth}] P50={p50*1000:.2f}ms, p95={p95*1000:.2f}ms, p99={p99*1000:.2f}ms, TPS={tps:.2f}")
        rows.append({
            'scenario': f'C-{args.scenario}',
            'scale_up':'', 
            'depth':depth, 
            'p50_ms':p50*1000, 
            'p95_ms':p95*1000, 
            'p99_ms':p99*1000, 
            'tps':tps
        })

    cur.close()
    conn.close()



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Scenario C Dynamic Topology Benchmark")
    parser.add_argument('-c', '--config', required=True, help='Path to config JSON')
    parser.add_argument('-s', '--scenario', required=True, choices=['1','2','3','4','5'], help='Scenario number')
    args = parser.parse_args()

    cfg = TestConfig(args.config)
    with open(args.config, 'r') as f:
        cfg_json = json.load(f)

    scale_up_nodes = cfg.scale_up_nodes
    depths = cfg.depths
    iterations = cfg.iterations
    params = cfg.scenario_params.get(args.scenario, {})

    # DB 연결 및 초기 설정
    conn = psycopg.connect(**cfg.db_params)
    cur = conn.cursor()
    cur.execute("SET graph_path = vc_graph;")
    cur.execute("SET synchronous_commit = ON;")
    conn.commit()
    print("› synchronous_commit ON 설정 완료")

    # private key 로드
    private_key = load_private_key(cfg.private_key_path)

    rows = []
    if args.scenario == '1':
        print("=== Running Scenario C-1: Real-Time Turn-Taking ===")
        scenario1_realtime_turntaking(cur, conn, cfg, params, scale_up_nodes, depths, iterations, rows, private_key)
    elif args.scenario == '2':
        print("=== Running Scenario C-2: Chain-Churn ===")
        scenario2_chain_churn(cur, conn, cfg, params, scale_up_nodes, depths, iterations, rows, private_key)
    elif args.scenario == '3':
        print("=== Running Scenario C-3: Partition & Reconciliation ===")
        scenario3_partition_reconciliation(cur, conn, cfg, params, scale_up_nodes, depths, iterations, rows, private_key)
    elif args.scenario == '4':
        print("=== Running Scenario C-4: Web-of-Trust ===")
        scenario4_web_of_trust(cur, cfg, params, iterations, rows)
    elif args.scenario == '5':
        print("=== Running Scenario C-5: ABAC ===")
        scenario5_abac(cur, cfg, params, iterations, rows)
    else:
        raise ValueError("Unsupported scenario for security patterns")


    # 결과 저장
    result_dir = Path(ROOT) / cfg.data_result_path
    result_dir.mkdir(parents=True, exist_ok=True)
    output_file = result_dir / f"C_{args.scenario}_results.csv"
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
    
    cur.close()
    conn.close()
