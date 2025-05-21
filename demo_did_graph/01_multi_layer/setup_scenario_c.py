# setup_scenario_c.py
# 시나리오 C 환경 설정: DID + VC + AgensGraph

import json
import time
import random
from pathlib import Path
import psycopg
import sys, os

# 프로젝트 루트를 PYTHONPATH에 추가
ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))

from common.load_config import TestConfig
from common.did_utils import load_private_key, create_did, create_vc

# ─────────────────────────────────────────────────────────
# 1) 설정 파일 로드
# ─────────────────────────────────────────────────────────
CONFIG_JSON = ROOT / "config" / "test_large.json"
with open(CONFIG_JSON, 'r') as f:
    cfg_raw = json.load(f)
# TestConfig로 DB 연결 정보 로드
cfg = TestConfig(str(CONFIG_JSON))

# 시나리오 파라미터
sp = cfg_raw.get("scenario_parameters", {})
HQ_ID            = sp.get("HQ_ID", "HQ1")
REGIONAL_COUNT   = sp.get("REGIONAL_COUNT", 100)
UNIT_COUNT       = sp.get("UNIT_COUNT", 200)
SQUAD_COUNT      = sp.get("SQUAD_COUNT", 500)
DRONES_PER_SQUAD = sp.get("DRONES_PER_SQUAD", 5)

# ─────────────────────────────────────────────────────────
# 2) DB 연결 및 Graph 초기화
# ─────────────────────────────────────────────────────────
conn = psycopg.connect(
    host=cfg.db_host,
    port=cfg.db_port,
    dbname=cfg.db_name,
    user=cfg.db_user,
    password=cfg.db_password
)
cur = conn.cursor()

# 그래프 삭제 및 생성
cur.execute("DROP GRAPH IF EXISTS vc_graph CASCADE;")
cur.execute("CREATE GRAPH vc_graph;")
cur.execute("SET graph_path = vc_graph;")

# 라벨 및 엣지 타입 정의
for label in ["HQ", "Regional", "Unit", "Squad", "Drone", "Issuer", "Subject", "VC"]:
    cur.execute(f"CREATE VLABEL {label};")
for etype in ["DELEGATES", "ISSUED", "ASSERTS"]:
    cur.execute(f"CREATE ELABEL {etype};")
conn.commit()

# ─────────────────────────────────────────────────────────
# 3) 인덱스 최적화
# ─────────────────────────────────────────────────────────
index_defs = [
    ("HQ", "id"), ("Regional", "id"), ("Unit", "id"),
    ("Squad", "id"), ("Drone", "id"), ("VC", "vc_id")
]
for label, prop in index_defs:
    cur.execute(f"CREATE PROPERTY INDEX ON {label}({prop});")
conn.commit()

# ─────────────────────────────────────────────────────────
# 4) 계층 구조 노드 생성 (HQ → Regional → Unit → Squad → Drone)
# ─────────────────────────────────────────────────────────
start_net = time.perf_counter()
# HQ 노드
cur.execute(f"CREATE (:HQ {{id:'{HQ_ID}'}});")

# Regionals
regionals = [f"R{i:03d}" for i in range(1, REGIONAL_COUNT+1)]
for rid in regionals:
    cur.execute(f"CREATE (:Regional {{id:'{rid}'}});")
    cur.execute(f"MATCH (h:HQ {{id:'{HQ_ID}'}}),(r:Regional {{id:'{rid}'}}) CREATE (h)-[:DELEGATES]->(r);")

# Units
units = [f"U{i:04d}" for i in range(1, UNIT_COUNT+1)]
for idx, uid in enumerate(units):
    parent = regionals[idx % REGIONAL_COUNT]
    cur.execute(f"CREATE (:Unit {{id:'{uid}'}});")
    cur.execute(f"MATCH (r:Regional {{id:'{parent}'}}),(u:Unit {{id:'{uid}'}}) CREATE (r)-[:DELEGATES]->(u);")

# Squads
squads = [f"S{i:05d}" for i in range(1, SQUAD_COUNT+1)]
for idx, sid in enumerate(squads):
    parent = units[idx % UNIT_COUNT]
    cur.execute(f"CREATE (:Squad {{id:'{sid}'}});")
    cur.execute(f"MATCH (u:Unit {{id:'{parent}'}}),(s:Squad {{id:'{sid}'}}) CREATE (u)-[:DELEGATES]->(s);")

# Drones
# 각 Squad 당 DRONES_PER_SQUAD 대 생성
drones = []
for idx, sid in enumerate(squads):
    for j in range(1, DRONES_PER_SQUAD+1):
        did = f"D{idx:05d}_{j:02d}"
        drones.append(did)
        cur.execute(f"CREATE (:Drone {{id:'{did}'}});")
        cur.execute(f"MATCH (s:Squad {{id:'{sid}'}}),(d:Drone {{id:'{did}'}}) CREATE (s)-[:DELEGATES]->(d);")

conn.commit()
elapsed_net = time.perf_counter() - start_net
print(f"› 네트워크 생성: Regionals({len(regionals)}), Units({len(units)}), Squads({len(squads)}), Drones({len(drones)}) in {elapsed_net:.2f}s")

# ─────────────────────────────────────────────────────────
# 5) Issuer 및 VC 생성
# ─────────────────────────────────────────────────────────
priv_key = load_private_key("common/keys/commander_private.pem")
issuer_did = f"did:example:{HQ_ID}"
# Issuer 노드
cur.execute(f"CREATE (:Issuer {{did:'{issuer_did}'}});")
conn.commit()

start_vc = time.perf_counter()
for idx, drone_id in enumerate(drones):
    subject_did = create_did()
    cur.execute(f"CREATE (:Subject {{did:'{subject_did}'}});")

    payload = {"mission_id": f"M{idx:06d}", "drone_id": drone_id}
    vc = create_vc(issuer_did, subject_did, payload, priv_key)
    vc_json = json.dumps(vc).replace("'", "''")
    cur.execute(f"CREATE (:VC {{vc_id:'{vc['id']}', vc_json:'{vc_json}'}});")

    # 관계 생성: Issuer-[:ISSUED]->VC-[:ASSERTS]->Subject
    cur.execute(f"MATCH (i:Issuer {{did:'{issuer_did}'}}),(v:VC {{vc_id:'{vc['id']}'}}) CREATE (i)-[:ISSUED]->(v);")
    cur.execute(f"MATCH (s:Subject {{did:'{subject_did}'}}),(v:VC {{vc_id:'{vc['id']}'}}) CREATE (v)-[:ASSERTS]->(s);")

conn.commit()
elapsed_vc = time.perf_counter() - start_vc
print(f"› VC 생성 및 관계 삽입: {len(drones)}건 in {elapsed_vc:.2f}s")

# ─────────────────────────────────────────────────────────
# 6) 정리
# ─────────────────────────────────────────────────────────
cur.close()
conn.close()
print("Scenario C 환경 설정 완료.")
