#!/usr/bin/env python3
# setup_scenario_c.py
# Scenario C 환경 설정: DID 인증 + AgensGraph(GraphDB) 기반 동적 네트워크 시나리오

import argparse
import time
import sys
import json
from pathlib import Path
import psycopg

# 프로젝트 루트 common 모듈 로드
ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))
from common.load_config import TestConfig
from common.did_utils import load_private_key, create_did, create_vc


def parse_args():
    parser = argparse.ArgumentParser(
        description="Setup Scenario C environment with dynamic network scenarios (GraphDB)."
    )
    parser.add_argument('--config', '-c', required=True,
                        help='Path to JSON config file (e.g., config/test_large.json)')
    parser.add_argument('--scenario', '-s', required=True, choices=['1','2','3'],
                        help='Scenario number: 1=Turn-Taking, 2=Chain-Churn, 3=Partition')
    return parser.parse_args()


def setup_database(cfg: TestConfig, private_key, scenario: int):
    # AgensGraph 연결 (Postgres 드라이버 사용)
    conn = psycopg.connect(**cfg.db_params)
    cur = conn.cursor()

    # 0) 성능 최적화: WAL 동기화 끄기
    cur.execute("SET synchronous_commit = ON;")
    conn.commit()
    print("› synchronous_commit ON 설정 완료")

    # 1) 그래프 초기화: 드롭 & 생성
    cur.execute("DROP GRAPH IF EXISTS vc_graph CASCADE;")
    cur.execute("CREATE GRAPH vc_graph;")
    cur.execute("SET graph_path = vc_graph;")
    conn.commit()
    print("› Graph(vc_graph) 초기화 완료")

    # 2) 라벨 및 엣지 타입 생성
    for lbl in ("HQ", "Drone", "VC", "VC_Issuer"):
        cur.execute(f"CREATE VLABEL IF NOT EXISTS {lbl};")
    for et in ("DELEGATES", "ASSERTS"):
        cur.execute(f"CREATE ELABEL IF NOT EXISTS {et};")
    conn.commit()
    print("› VLABEL 및 ELABEL 생성 완료")

    # 3) 속성 인덱스 생성
    for label, prop in [("HQ","id"), ("Drone","id"), ("VC","vc_json")]:
        # AgensGraph는 IF NOT EXISTS 옵션을 지원하지 않으므로 생략
        cur.execute(f"CREATE PROPERTY INDEX ON {label}({prop});")
    conn.commit()
    print("› Property Index 생성 완료")

    # 4) HQ 및 Drone 네트워크 생성
    HQ_ID = cfg.headquarters_id
    # HQ 노드 생성
    cur.execute(f"CREATE (:HQ {{id:'{HQ_ID}'}});")
    # Drone 노드 생성 및 DELEGATES 관계
    drones = []
    start_net = time.perf_counter()
    for _ in range(cfg.num_drones):
        drone_did = create_did()
        drones.append(drone_did)
        cur.execute(f"CREATE (:Drone {{id:'{drone_did}'}});")
        cur.execute(
            f"MATCH (h:HQ {{id:'{HQ_ID}'}}),(d:Drone {{id:'{drone_did}'}}) "
            f"CREATE (h)-[:DELEGATES]->(d);")
    conn.commit()
    print(f"› Drone 네트워크 생성 완료: {len(drones)}개 in {time.perf_counter() - start_net:.2f}s")

    # 5) VC Issuer 노드 생성
    issuer_did = create_did()
    cur.execute(f"CREATE (:VC_Issuer {{did:'{issuer_did}'}});")
    conn.commit()
    print(f"› VC_Issuer 노드 생성 완료 (issuer: {issuer_did})")

    # 6) VC 노드 및 ASSERTS 관계 생성
    start_vc = time.perf_counter()
    for idx, subject_did in enumerate(drones):
        # VC 생성 (common.did_utils 방식)
        data = {'drone_index': idx}
        vc_doc = create_vc(issuer_did, subject_did, data, private_key)
        vc_json = json.dumps(vc_doc).replace("'", "\\'")
        vc_id = vc_doc.get('id')
        # VC 노드 (vc_json 포함)
        cur.execute(
            f"CREATE (:VC {{vc_id:'{vc_id}', issuer:'{issuer_did}', subject:'{subject_did}', vc_json:'{vc_json}'}});")
        # ASSERTS 관계
        cur.execute(
            f"MATCH (v:VC {{vc_id:'{vc_id}'}}),(d:Drone {{id:'{subject_did}'}}) "
            f"CREATE (v)-[:ASSERTS]->(d);")
    conn.commit()
    print(f"› VC 및 ASSERTS 생성 완료: {len(drones)}개 in {time.perf_counter() - start_vc:.2f}s")

    cur.close()
    conn.close()
    print(f"Scenario C-{scenario} 환경 설정 완료")


if __name__ == '__main__':
    args = parse_args()
    cfg = TestConfig(args.config)
    private_key = load_private_key(cfg.private_key_path)
    setup_database(cfg, private_key, int(args.scenario))
