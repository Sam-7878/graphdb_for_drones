#!/usr/bin/env python3
# setup_scenario_c.py
# Scenario C 환경 설정: DID 인증 + AgensGraph(GraphDB) 기반 다양한 보안 시나리오 (Non-optimized)

import argparse
import sys
import json
from pathlib import Path
import psycopg

# common 모듈 로드
ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))
from common.load_config import TestConfig
from common.did_utils import load_private_key, create_prefixed_did, create_vc_simple


def parse_args():
    parser = argparse.ArgumentParser(
        description="Setup Scenario C environment with various security graph scenarios (non-optimized)."
    )
    parser.add_argument('--config', '-c', required=True,
                        help='Path to JSON config file (e.g., config/test_small.json)')
    parser.add_argument('--scenario', '-s', required=True,
                        choices=['1','2','3','4','5'],
                        help='Scenario: 1=Turn-Taking, 2=Chain-Churn, 3=Partition, '
                             '4=Web-of-Trust, 5=ABAC')
    return parser.parse_args()


def setup_dynamic_graph(cur, cfg, private_key):
    # Scenario 1~3: 기본 DID 인증 그래프 초기화
    cur.execute("DROP GRAPH IF EXISTS vc_graph CASCADE;")
    cur.execute("CREATE GRAPH vc_graph;")
    cur.execute("SET graph_path = vc_graph;")

    # 스키마 설정
    for lbl in ("HQ", "Drone", "VC", "VC_Issuer"):  
        cur.execute(f"CREATE VLABEL IF NOT EXISTS {lbl};")
    for et in ("DELEGATES", "ASSERTS"):  
        cur.execute(f"CREATE ELABEL IF NOT EXISTS {et};")
    for label, prop in [("HQ", "id"), ("Drone", "id"), ("VC", "vc_json")]:
        cur.execute(f"CREATE PROPERTY INDEX ON {label}({prop});")

    # HQ 노드
    hq_id = cfg.headquarters_id
    cur.execute(f"CREATE (:HQ {{id:'{hq_id}'}});")

    # Drone 및 DELEGATES
    drones = []
    for i in range(cfg.num_drones):
        did = create_prefixed_did('drone', i)
        drones.append(did)
        cur.execute(f"CREATE (:Drone {{id:'{did}'}});")
        cur.execute(
            f"MATCH (h:HQ {{id:'{hq_id}'}}),(d:Drone {{id:'{did}'}}) CREATE (h)-[:DELEGATES]->(d);")

    # VC_Issuer
    issuer = create_prefixed_did('issuer', 0)
    cur.execute(f"CREATE (:VC_Issuer {{did:'{issuer}'}});")

    # VC 및 ASSERTS
    for idx, sub in enumerate(drones):
        vc_doc = create_vc_simple({'index':idx}, private_key, issuer)
        vc_clean = vc_doc.copy()
        vc_clean['signature'] = vc_clean['signature'].hex()
        vjson = json.dumps(vc_clean).replace("'", "\\'")
        vid = vc_doc.get('id', create_prefixed_did('vc', idx))
        cur.execute(
            f"CREATE (:VC {{vc_id:'{vid}', issuer:'{issuer}', subject:'{sub}', vc_json:'{vjson}'}});")
        cur.execute(
            f"MATCH (v:VC {{vc_id:'{vid}'}}),(d:Drone {{id:'{sub}'}}) CREATE (v)-[:ASSERTS]->(d);")


def setup_web_of_trust(cur, cfg):
    # Scenario 4: Web-of-Trust 그래프 초기화
    cur.execute("DROP GRAPH IF EXISTS trust_graph CASCADE;")
    cur.execute("CREATE GRAPH trust_graph;")
    cur.execute("SET graph_path = trust_graph;")

    # 스키마 설정
    cur.execute("CREATE VLABEL IF NOT EXISTS Entity;")
    cur.execute("CREATE ELABEL IF NOT EXISTS CROSSED_SIGNED;")
    cur.execute("CREATE PROPERTY INDEX ON Entity(did);")

    # Anchor 및 체인 노드
    anchor = cfg.scenario_params['4']['web_of_trust']['anchor_did']
    cur.execute(f"CREATE (:Entity {{did:'{anchor}'}});")
    entities = []
    for i in range(cfg.num_drones):
        did = create_prefixed_did('entity', i)
        entities.append(did)
        cur.execute(f"CREATE (:Entity {{did:'{did}'}});")

    # 선형 사인 체인 생성
    for i in range(len(entities)-1):
        a, b = entities[i], entities[i+1]
        cur.execute(f"MATCH (x:Entity {{did:'{a}'}}),(y:Entity {{did:'{b}'}}) CREATE (x)-[:CROSSED_SIGNED]->(y);")
    cur.execute(
        f"MATCH (x:Entity {{did:'{entities[-1]}'}}),(y:Entity {{did:'{anchor}'}}) CREATE (x)-[:CROSSED_SIGNED]->(y);")


def setup_abac(cur, cfg):
    # Scenario 5: ABAC 그래프 초기화
    cur.execute("DROP GRAPH IF EXISTS abac_graph CASCADE;")
    cur.execute("CREATE GRAPH abac_graph;")
    cur.execute("SET graph_path = abac_graph;")

    # 스키마 설정
    for lbl in ('AppUser', 'AppGroup', 'Resource'):
        cur.execute(f"CREATE VLABEL IF NOT EXISTS \"{lbl}\";")
    for et in ('MEMBER_OF', 'SUBGROUP_OF', 'HAS_PERMISSION'):
        cur.execute(f"CREATE ELABEL IF NOT EXISTS \"{et}\";")
    cur.execute("CREATE PROPERTY INDEX ON \"AppUser\"(did);")
    cur.execute("CREATE PROPERTY INDEX ON \"AppGroup\"(id);")
    cur.execute("CREATE PROPERTY INDEX ON \"Resource\"(id);")

    # 노드 생성
    users = [create_prefixed_did('user', i) for i in range(cfg.num_drones)]
    groups = [create_prefixed_did('group', i) for i in range(max(1, cfg.num_drones//10))]
    resources = [create_prefixed_did('resource', i) for i in range(len(groups))]
    for u in users:
        cur.execute(f"CREATE (:AppUser {{did:'{u}'}});")
    for g in groups:
        cur.execute(f"CREATE (:AppGroup {{id:'{g}'}});")
    for r in resources:
        cur.execute(f"CREATE (:Resource {{id:'{r}'}});")

    # 관계 생성
    for idx, u in enumerate(users):
        g = groups[idx % len(groups)]
        cur.execute(
            f"MATCH (x:AppUser {{did:'{u}'}}),(y:AppGroup {{id:'{g}'}}) CREATE (x)-[:MEMBER_OF]->(y);")
    for i in range(len(groups)-1):
        a, b = groups[i], groups[i+1]
        cur.execute(
            f"MATCH (x:AppGroup {{id:'{a}'}}),(y:AppGroup {{id:'{b}'}}) CREATE (x)-[:SUBGROUP_OF]->(y);")
    top = groups[-1]
    for r in resources:
        cur.execute(
            f"MATCH (x:AppGroup {{id:'{top}'}}),(y:Resource {{id:'{r}'}}) CREATE (x)-[:HAS_PERMISSION]->(y);")


def setup_database(cfg: TestConfig, private_key, scenario: int):
    conn = psycopg.connect(**cfg.db_params)
    cur = conn.cursor()

    if scenario in (1, 2, 3):
        setup_dynamic_graph(cur, cfg, private_key)
    elif scenario == 4:
        setup_web_of_trust(cur, cfg)
    elif scenario == 5:
        setup_abac(cur, cfg)
    else:
        raise ValueError(f"Unsupported scenario: {scenario}")

    # 일괄 커밋 (비최적화)
    conn.commit()
    cur.close()
    conn.close()


if __name__ == '__main__':
    args = parse_args()
    cfg = TestConfig(args.config)
    private_key = load_private_key(cfg.private_key_path)
    setup_database(cfg, private_key, int(args.scenario))
