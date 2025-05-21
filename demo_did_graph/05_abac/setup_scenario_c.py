#!/usr/bin/env python3
# setup_scenario_c.py
# Scenario C 환경 설정: DID 인증 + AgensGraph(GraphDB) 기반 다양한 보안 시나리오

import argparse
import time
import sys
import json
import random
from pathlib import Path
import psycopg

# common 모듈 로드
ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))
from common.load_config import TestConfig
from common.did_utils import load_private_key, create_prefixed_did, create_vc_simple


def parse_args():
    parser = argparse.ArgumentParser(
        description="Setup Scenario C environment with various security graph scenarios."
    )
    parser.add_argument('--config', '-c', required=True,
                        help='Path to JSON config file (e.g., config/test_small.json)')
    parser.add_argument('--scenario', '-s', required=True,
                        choices=['1','2','3','4','5'],
                        help='Scenario: 1=Turn-Taking, 2=Chain-Churn, 3=Partition, '
                             '4=Web-of-Trust, 5=ABAC')
    return parser.parse_args()


def setup_dynamic_graph(cur, cfg, private_key):
    # 기존 시나리오 1~3용 초기 그래프
    # (HQ, Drone, VC) 구조를 초기화
    cur.execute("DROP GRAPH IF EXISTS vc_graph CASCADE;")
    cur.execute("CREATE GRAPH vc_graph;")
    cur.execute("SET graph_path = vc_graph;")
    cur.connection.commit()

    # VLABEL / ELABEL / INDEX
    for lbl in ("HQ", "Drone", "VC", "VC_Issuer"): cur.execute(f"CREATE VLABEL IF NOT EXISTS {lbl};")
    for et in ("DELEGATES", "ASSERTS"): cur.execute(f"CREATE ELABEL IF NOT EXISTS {et};")
    for label, prop in [("HQ", "id"), ("Drone","id"), ("VC","vc_json")]:
        cur.execute(f"CREATE PROPERTY INDEX ON {label}({prop});")
    cur.connection.commit()

    # HQ 노드
    cfg_id = cfg.headquarters_id
    cur.execute(f"CREATE (:HQ {{id:'{cfg_id}'}});")

    # Drone 및 DELEGATES
    drones = []
    for i in range(cfg.num_drones):
        d = create_prefixed_did('drone', i)
        drones.append(d)
        cur.execute(f"CREATE (:Drone {{id:'{d}'}});")
        cur.execute(
            f"MATCH (h:HQ {{id:'{cfg_id}'}}),(d:Drone {{id:'{d}'}}) "
            f"CREATE (h)-[:DELEGATES]->(d);")
    cur.connection.commit()

    # VC_Issuer
    issuer = create_prefixed_did('issuer', 0)
    cur.execute(f"CREATE (:VC_Issuer {{did:'{issuer}'}});")
    cur.connection.commit()

    # VC 및 ASSERTS
    for idx, sub in enumerate(drones):
        vc_doc = create_vc_simple({'index':idx}, private_key, issuer)
        # bytes → hex 문자열로 변환
        vc_clean = vc_doc.copy()
        vc_clean['signature'] = vc_clean['signature'].hex()
        # 이제 JSON으로 직렬화 가능
        vjson = json.dumps(vc_clean).replace("'", "\\'")
        vid = vc_doc.get('id', create_prefixed_did('vc', idx))

        cur.execute(
            f"CREATE (:VC {{vc_id:'{vid}', issuer:'{issuer}', subject:'{sub}', vc_json:'{vjson}'}});")
        cur.execute(
            f"MATCH (v:VC {{vc_id:'{vid}'}}),(d:Drone {{id:'{sub}'}}) "
            f"CREATE (v)-[:ASSERTS]->(d);")
    cur.connection.commit()
    print(f"› Dynamic graph (scenarios 1-3) initialized: {cfg.num_drones} drones, VCs")


def setup_web_of_trust(cur, cfg):
    # Scenario 4: Web-of-Trust
    cur.execute("DROP GRAPH IF EXISTS trust_graph CASCADE;")
    cur.execute("CREATE GRAPH trust_graph;")
    cur.execute("SET graph_path = trust_graph;")
    cur.connection.commit()

    # VLABEL/ELABEL/INDEX
    cur.execute("CREATE VLABEL IF NOT EXISTS Entity;")
    cur.execute("CREATE ELABEL IF NOT EXISTS CROSSED_SIGNED;")
    cur.execute("CREATE PROPERTY INDEX ON Entity(did);")
    cur.connection.commit()

    # Anchor 엔티티
    anchor = cfg.scenario_params['4']['web_of_trust']['anchor_did']
    cur.execute(f"CREATE (:Entity {{did:'{anchor}'}});")

    # 랜덤 Entity 및 체인 생성
    entities = []
    for i in range(cfg.num_drones):
        e = create_prefixed_did('entity', i)
        entities.append(e)
        cur.execute(f"CREATE (:Entity {{did:'{e}'}});")
    cur.connection.commit()

    # 선형 체인 및 Anchor 연결
    for i in range(len(entities)-1):
        cur.execute(
            f"MATCH (a:Entity {{did:'{entities[i]}'}}),(b:Entity {{did:'{entities[i+1]}'}}) "
            f"CREATE (a)-[:CROSSED_SIGNED]->(b);")
    cur.execute(
        f"MATCH (a:Entity {{did:'{entities[-1]}'}}),(b:Entity {{did:'{anchor}'}}) "
        f"CREATE (a)-[:CROSSED_SIGNED]->(b);")
    cur.connection.commit()
    print(f"› Web-of-Trust graph initialized: {len(entities)} Entities with chain to anchor {anchor}")


def setup_abac(cur, cfg):
    # Scenario 5: 다단계 ABAC (라벨 User → AppUser 로 변경)
    cur.execute("DROP GRAPH IF EXISTS abac_graph CASCADE;")
    cur.execute("CREATE GRAPH abac_graph;")
    cur.execute("SET graph_path = abac_graph;")
    cur.connection.commit()

    # VLABEL/ELABEL/INDEX
    for lbl in ('AppUser', 'AppGroup', 'Resource'):
        cur.execute(f'CREATE VLABEL "{lbl}";')
    for et in ('MEMBER_OF', 'SUBGROUP_OF', 'HAS_PERMISSION'):
        cur.execute(f'CREATE ELABEL "{et}";')

    # 라벨명이 바뀌었으니 인덱스도 AppUser 로
    cur.execute("CREATE PROPERTY INDEX ON \"AppUser\"(did);")
    cur.execute("CREATE PROPERTY INDEX ON \"AppGroup\"(id);")
    cur.execute("CREATE PROPERTY INDEX ON \"Resource\"(id);")
    cur.connection.commit()

    # 유저, 그룹, 리소스 노드 생성
    users = [create_prefixed_did('user', i) for i in range(cfg.num_drones)]
    groups = [create_prefixed_did('group', i) for i in range(max(1, cfg.num_drones//10))]
    resources = [create_prefixed_did('resource', i) for i in range(len(groups))]
    
     # AppUser 노드 생성
    for u in users:
        cur.execute(f"CREATE (:AppUser {{did:'{u}'}});")
    for g in groups: 
        cur.execute(f"CREATE (:AppGroup {{id:'{g}'}});")
    for r in resources: 
        cur.execute(f"CREATE (:Resource {{id:'{r}'}});")
    cur.connection.commit()

    # MEMBER_OF: 사용자 -> 그룹 (round-robin)
    for idx, u in enumerate(users):
        g = groups[idx % len(groups)]
        cur.execute(
            f"MATCH (u:AppUser {{did:'{u}'}}),(g:AppGroup {{id:'{g}'}}) "
            f"CREATE (u)-[:MEMBER_OF]->(g);")
    cur.connection.commit()

    # SUBGROUP_OF: 그룹 체인
    for i in range(len(groups)-1):
        cur.execute(
            f"MATCH (a:AppGroup {{id:'{groups[i]}'}}),(b:AppGroup {{id:'{groups[i+1]}'}}) "
            f"CREATE (a)-[:SUBGROUP_OF]->(b);")
    cur.connection.commit()

    # HAS_PERMISSION: 최상위 그룹 -> 모든 리소스
    top = groups[-1]
    for r in resources:
        cur.execute(
            f"MATCH (g:AppGroup {{id:'{top}'}}),(r:Resource {{id:'{r}'}}) "
            f"CREATE (g)-[:HAS_PERMISSION]->(r);")
    cur.connection.commit()
    print(f"› ABAC graph initialized: {len(users)} Users, {len(groups)} Groups, {len(resources)} Resources")


def setup_database(cfg: TestConfig, private_key, scenario: int):
    # 새로운 연결로 락 분리
    conn = psycopg.connect(**cfg.db_params)
    cur = conn.cursor()
    # 성능 최적화
    cur.execute("SET synchronous_commit = ON;")
    conn.commit()

    if scenario in (1,2,3):
        setup_dynamic_graph(cur, cfg, private_key)
    elif scenario == 4:
        setup_web_of_trust(cur, cfg)
    elif scenario == 5:
        setup_abac(cur, cfg)
    else:
        raise ValueError(f"Unsupported scenario: {scenario}")

    cur.close()
    conn.close()
    print(f"Scenario C-{scenario} environment setup complete.")


if __name__ == '__main__':
    args = parse_args()
    cfg = TestConfig(args.config)
    private_key = load_private_key(cfg.private_key_path)
    setup_database(cfg, private_key, int(args.scenario))
