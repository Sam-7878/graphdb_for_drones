# setup_scenario_b.py

import time
import random
import json
import psycopg
from psycopg import Binary

import sys, os
from pathlib import Path
# 프로젝트 루트 경로를 PYTHONPATH에 추가하여 common 모듈 로드 지원
ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))

from common.load_config import TestConfig
from common.did_utils import load_private_key, create_did, create_vc

# ─────────────────────────────────────────────────────────
# 설정
# ─────────────────────────────────────────────────────────
CONFIG_PATH       = "config/test_small.json"  # 또는 test_large.json 사용 가능
HQ_ID             = "HQ1"
REGIONAL_COUNT    = 100
UNIT_COUNT        = 200
SQUAD_COUNT       = 500
DRONES_PER_SQUAD  = 5

# ─────────────────────────────────────────────────────────
# DB 연결
# ─────────────────────────────────────────────────────────
cfg = TestConfig(CONFIG_PATH)
random.seed(cfg.random_seed)
conn = psycopg.connect(
    host=cfg.db_host,
    port=cfg.db_port,
    dbname=cfg.db_name,
    user=cfg.db_user,
    password=cfg.db_password
)
cur = conn.cursor()

# ─────────────────────────────────────────────────────────
# 스키마 초기화
# ─────────────────────────────────────────────────────────
cur.execute("DROP TABLE IF EXISTS vc_test;")
cur.execute("DROP TABLE IF EXISTS did_subject;")
cur.execute("DROP TABLE IF EXISTS did_issuer;")
cur.execute("DROP TABLE IF EXISTS delegation_relation;")
cur.execute("DROP TABLE IF EXISTS hq;")
conn.commit()

cur.execute("""
CREATE TABLE hq (
  id TEXT PRIMARY KEY
);
""")
cur.execute("""
CREATE TABLE delegation_relation (
  parent_id  TEXT NOT NULL,
  child_id   TEXT NOT NULL,
  child_type TEXT NOT NULL,
  PRIMARY KEY(parent_id, child_id)
);
""")
cur.execute("""
CREATE TABLE did_issuer (
  did TEXT PRIMARY KEY
);
""")
cur.execute("""
CREATE TABLE did_subject (
  did TEXT PRIMARY KEY
);
""")
cur.execute("""
CREATE TABLE vc_test (
  vc_id       TEXT PRIMARY KEY,
  issuer_did  TEXT REFERENCES did_issuer(did),
  subject_did TEXT REFERENCES did_subject(did),
  vc_json     JSONB NOT NULL
);
""")
conn.commit()

# ─────────────────────────────────────────────────────────
# HQ, Issuer 등록
# ─────────────────────────────────────────────────────────
cur.execute("INSERT INTO hq (id) VALUES (%s) ON CONFLICT DO NOTHING;", (HQ_ID,))
issuer_did = f"did:example:{HQ_ID}"
cur.execute("INSERT INTO did_issuer (did) VALUES (%s) ON CONFLICT DO NOTHING;", (issuer_did,))
conn.commit()

# ─────────────────────────────────────────────────────────
# 계층 네트워크 생성: Regionals, Units, Squads, Drones
# ─────────────────────────────────────────────────────────
start = time.perf_counter()

# Regionals
regionals = [f"R{i:03d}" for i in range(1, REGIONAL_COUNT+1)]
for rid in regionals:
    cur.execute(
        "INSERT INTO delegation_relation (parent_id, child_id, child_type) VALUES (%s, %s, %s)",
        (HQ_ID, rid, "Regional")
    )

# Units
units = [f"U{i:04d}" for i in range(1, UNIT_COUNT+1)]
for idx, uid in enumerate(units):
    parent = regionals[idx % REGIONAL_COUNT]
    cur.execute(
        "INSERT INTO delegation_relation (parent_id, child_id, child_type) VALUES (%s, %s, %s)",
        (parent, uid, "Unit")
    )

# Squads
squads = [f"S{i:05d}" for i in range(1, SQUAD_COUNT+1)]
for idx, sid in enumerate(squads):
    parent = units[idx % UNIT_COUNT]
    cur.execute(
        "INSERT INTO delegation_relation (parent_id, child_id, child_type) VALUES (%s, %s, %s)",
        (parent, sid, "Squad")
    )

# Drones
# 각 Squad당 DRONES_PER_SQUAD대 생성
drones = []
for idx, sid in enumerate(squads):
    for j in range(1, DRONES_PER_SQUAD+1):
        did = f"D{idx:05d}_{j:02d}"
        drones.append(did)
        cur.execute(
            "INSERT INTO delegation_relation (parent_id, child_id, child_type) VALUES (%s, %s, %s)",
            (sid, did, "Drone")
        )

conn.commit()
elapsed_net = time.perf_counter() - start
print(f"› 네트워크 생성 완료: Regionals({len(regionals)}), Units({len(units)}), Squads({len(squads)}), Drones({len(drones)}) in {elapsed_net:.2f}s")

# ─────────────────────────────────────────────────────────
# VC 생성 및 삽입
# ─────────────────────────────────────────────────────────
priv_key = load_private_key("common/keys/commander_private.pem")
start_vc = time.perf_counter()

for idx, drone_id in enumerate(drones):
    subject_did = create_did()
    cur.execute("INSERT INTO did_subject (did) VALUES (%s) ON CONFLICT DO NOTHING;", (subject_did,))

    payload = {
        "mission_id": f"M{idx:06d}",
        "drone_id": drone_id
    }
    vc = create_vc(issuer_did, subject_did, payload, priv_key)
    vc_json_str = json.dumps(vc)

    cur.execute(
        "INSERT INTO vc_test (vc_id, issuer_did, subject_did, vc_json) VALUES (%s, %s, %s, %s)",
        (vc["id"], issuer_did, subject_did, vc_json_str)
    )

conn.commit()
elapsed_vc = time.perf_counter() - start_vc
print(f"› VC 생성 및 삽입 완료: {len(drones)}건 in {elapsed_vc:.2f}s")

# ─────────────────────────────────────────────────────────
# 정리
# ─────────────────────────────────────────────────────────
cur.close()
conn.close()
print("Scenario B 환경 설정 완료.")
