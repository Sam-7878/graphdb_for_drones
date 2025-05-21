#!/usr/bin/env python3
# setup_scenario_c.py
# Scenario C 환경 설정: DID + VC + AgensGraph 기반 동적 네트워크 시나리오

import json
import time
import argparse
from pathlib import Path
import psycopg
import sys

# 프로젝트 루트를 PYTHONPATH에 추가
ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))
from common.load_config import TestConfig
from common.did_utils import load_private_key, create_did, create_vc


def parse_args():
    parser = argparse.ArgumentParser(
        description="Setup Scenario C environment with dynamic network scenarios (AgensGraph)"
    )
    parser.add_argument('-c', '--config', required=True,
                        help='Path to JSON config file (e.g., config/test_large.json)')
    parser.add_argument('-s', '--scenario', required=True, choices=['1', '2', '3'],
                        help='Scenario number: 1=Real-Time Turn-Taking, 2=Chain-Churn, 3=Partition & Reconciliation')
    return parser.parse_args()


def setup_database(cfg: TestConfig, private_key, scenario: int, config_path: str):
    # AgensGraph 연결
    conn = psycopg.connect(**cfg.db_params)
    cur = conn.cursor()

    # 1) 그래프 초기화
    cur.execute("DROP GRAPH IF EXISTS vc_graph CASCADE;")
    cur.execute("CREATE GRAPH vc_graph;")
    cur.execute("SET graph_path = vc_graph;")
    
    # 2) 라벨 및 엣지 타입 생성
    labels = ["HQ", "Regional", "Unit", "Squad", "Drone", "Issuer", "Subject", "VC"]
    etypes = ["DELEGATES", "ISSUED", "ASSERTS"]
    for lbl in labels:
        cur.execute(f"CREATE VLABEL IF NOT EXISTS {lbl};")
    for et in etypes:
        cur.execute(f"CREATE ELABEL IF NOT EXISTS {et};")
    conn.commit()
    print("› Graph 및 라벨/엣지 초기화 완료")

    # 3) 인덱스 생성
    index_defs = [
        ("HQ", "id"), ("Regional", "id"), ("Unit", "id"),
        ("Squad", "id"), ("Drone", "id"), ("VC", "vc_id")
    ]
    for label, prop in index_defs:
        cur.execute(f"CREATE PROPERTY INDEX ON {label}({prop});")
    conn.commit()
    print("› 인덱스 생성 완료")

    # 4) 네트워크 계층 구조 생성 (config 기반)
    REGIONAL_COUNT   = cfg.num_regions
    UNIT_COUNT       = cfg.num_units
    SQUAD_COUNT      = cfg.num_squads
    DRONES_PER_SQUAD = cfg.num_drones_per_squad
    HQ_ID            = cfg.headquarters_id

    start_net = time.perf_counter()
    # HQ 노드
    cur.execute(f"CREATE (:HQ {{id:'{HQ_ID}'}});")

    # Regional
    regionals = [f"R{i:03d}" for i in range(1, REGIONAL_COUNT + 1)]
    for rid in regionals:
        cur.execute(f"CREATE (:Regional {{id:'{rid}'}});")
        cur.execute(f"MATCH (h:HQ {{id:'{HQ_ID}'}}),(r:Regional {{id:'{rid}'}}) CREATE (h)-[:DELEGATES]->(r);")

    # Unit
    units = [f"U{i:04d}" for i in range(1, UNIT_COUNT + 1)]
    for idx, uid in enumerate(units):
        parent = regionals[idx % REGIONAL_COUNT]
        cur.execute(f"CREATE (:Unit {{id:'{uid}'}});")
        cur.execute(f"MATCH (r:Regional {{id:'{parent}'}}),(u:Unit {{id:'{uid}'}}) CREATE (r)-[:DELEGATES]->(u);")

    # Squad
    squads = [f"S{i:05d}" for i in range(1, SQUAD_COUNT + 1)]
    for idx, sid in enumerate(squads):
        parent = units[idx % UNIT_COUNT]
        cur.execute(f"CREATE (:Squad {{id:'{sid}'}});")
        cur.execute(f"MATCH (u:Unit {{id:'{parent}'}}),(s:Squad {{id:'{sid}'}}) CREATE (u)-[:DELEGATES]->(s);")

    # Drone
    drones = []
    for idx, sid in enumerate(squads):
        for j in range(1, DRONES_PER_SQUAD + 1):
            did = f"D{idx:05d}_{j:02d}"
            drones.append(did)
            cur.execute(f"CREATE (:Drone {{id:'{did}'}});")
            cur.execute(f"MATCH (s:Squad {{id:'{sid}'}}),(d:Drone {{id:'{did}'}}) CREATE (s)-[:DELEGATES]->(d);")
    conn.commit()
    print(f"› 네트워크 생성 완료: {len(regionals)}R, {len(units)}U, {len(squads)}S, {len(drones)}D in {time.perf_counter() - start_net:.2f}s")

    # 5) Drone hqId 설정
    cur.execute(f"MATCH (d:Drone) SET d.hqId = '{HQ_ID}';")
    conn.commit()
    print(f"› Drone hqId 초기 설정 완료: {len(drones)}건 처리")

    # 6) Issuer 및 VC 노드 삽입
    priv_key = load_private_key(cfg.private_key_path)
    issuer_did = f"did:example:{HQ_ID}"
    start_vc = time.perf_counter()
    cur.execute(f"CREATE (:Issuer {{did:'{issuer_did}'}});")
    for drone_id in drones:
        subject_did = create_did()
        cur.execute(f"CREATE (:Subject {{did:'{subject_did}'}});")
        payload = {"mission_id": f"M{len(subject_did)}", "drone_id": drone_id}
        vc = create_vc(
            issuer_did=issuer_did,
            subject_did=subject_did,
            data=payload,
            private_key=priv_key
        )
        vc_json = json.dumps(vc).replace("'", "''")
        cur.execute(f"CREATE (:VC {{vc_id:'{vc['id']}', vc_json:'{vc_json}'}});")
        cur.execute(f"MATCH (i:Issuer {{did:'{issuer_did}'}}),(v:VC {{vc_id:'{vc['id']}'}}) CREATE (i)-[:ISSUED]->(v);")
        cur.execute(f"MATCH (s:Subject {{did:'{subject_did}'}}),(v:VC {{vc_id:'{vc['id']}'}}) CREATE (v)-[:ASSERTS]->(s);")
    conn.commit()
    print(f"› VC 및 관계 삽입 완료: {len(drones)}건 in {time.perf_counter() - start_vc:.2f}s")

    # 7) Scenario 완료 로깅
    print(f"› Scenario C-{scenario} 환경 설정 완료: DID/VC 및 Graph 준비 완료")

    cur.close()
    conn.close()


if __name__ == '__main__':
    args = parse_args()
    cfg = TestConfig(args.config)
    private_key = load_private_key(cfg.private_key_path)
    setup_database(cfg, private_key, int(args.scenario), args.config)
