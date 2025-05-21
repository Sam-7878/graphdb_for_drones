# testcases/test_b_did_pg.py

import time
import psycopg
import json
import random

from pathlib import Path
import sys, os
sys.path.append(str(Path(__file__).resolve().parent.parent))
# === 모듈 임포트 ===
from common.load_config import TestConfig
from common.did_utils import (
    generate_did, generate_key_pair, load_private_key, load_public_key,
    create_did, create_vc, verify_vc)

# === 설정 ===
PRIVATE_KEY_PATH = "common/keys/commander_private.pem"
PUBLIC_KEY_PATH = "common/keys/commander_public.pem"

def run_test_case_b(config: TestConfig):
    print("[B-Test] Running DID + VC + PostgreSQL (RDB) Test")

    random.seed(config.random_seed)
    issuer_private = load_private_key(PRIVATE_KEY_PATH)
    issuer_public = load_public_key(PUBLIC_KEY_PATH)
    issuer_did = "did:example:issuer"

    conn = psycopg.connect(
        host=config.db_host,
        port=config.db_port,
        dbname=config.db_name,
        user=config.db_user,
        password=config.db_password
    )
    cur = conn.cursor()

    # 테이블 초기화
    cur.execute("DROP TABLE IF EXISTS vc_test;")
    cur.execute("DROP TABLE IF EXISTS did_issuer;")
    cur.execute("DROP TABLE IF EXISTS did_subject;")
    conn.commit()

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
            vc_id TEXT PRIMARY KEY,
            issuer_did TEXT REFERENCES did_issuer(did),
            subject_did TEXT REFERENCES did_subject(did),
            vc_json JSONB
        );
    """)
    conn.commit()

    cur.execute("INSERT INTO did_issuer (did) VALUES (%s)", (issuer_did,))

    inserted = 0
    insert_start = time.perf_counter()

    for i in range(config.num_mission):
        subject_did = create_did()
        cur.execute("INSERT INTO did_subject (did) VALUES (%s)", (subject_did,))

        payload = {
            "mission_id": f"M{i:04d}",
            "content": f"Mission content {i}"
        }

        vc = create_vc(issuer_did, subject_did, payload, issuer_private)
        vc_id = vc["id"]

        cur.execute("""
            INSERT INTO vc_test (vc_id, issuer_did, subject_did, vc_json)
            VALUES (%s, %s, %s, %s)
        """, (vc_id, issuer_did, subject_did, json.dumps(vc)))

        inserted += 1


    conn.commit()
    insert_time = time.perf_counter() - insert_start

    print(f"[B-Test] Inserted {config.num_mission} VCs in {insert_time:.4f} seconds")

    # VC 검증 테스트
    verify_start = time.perf_counter()
    cur.execute("SELECT vc_json FROM vc_test")
    rows = cur.fetchall()

    verified_count = 0
    for row in rows:
        vc = row[0]
        if verify_vc(vc, issuer_public):
            verified_count += 1

    verify_time = time.perf_counter() - verify_start
    print(f"[B-Test] Verified {verified_count} VCs in {verify_time:.4f} seconds")

    cur.close()
    conn.close()

    return {
        "insert_time": insert_time,
        "verify_time": verify_time,
        "total": config.num_mission
    }

if __name__ == "__main__":
    #cfg = TestConfig("config/test_small.json")
    cfg = TestConfig("config/test_large.json")
    
    result = run_test_case_b(cfg)
    print("[B-Test] Result:", result)
