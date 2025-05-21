# scripts/traversal_verify.py

import time
import psycopg
import json

from pathlib import Path
import sys, os
sys.path.append(str(Path(__file__).resolve().parent.parent))
# === 모듈 임포트 ===
from common.load_config import TestConfig
from common.sign_verify import verify_signature, load_public_key
from common.did_utils import verify_vc

# === Traversal 검증: 시나리오 A ===
def verify_traversal_a(config: TestConfig) -> dict:
    conn = psycopg.connect(
        host=config.db_host,
        port=config.db_port,
        dbname=config.db_name,
        user=config.db_user,
        password=config.db_password
    )
    cur = conn.cursor()

    pub_key = load_public_key("common/keys/commander_public.pem")

    start = time.perf_counter()

    cur.execute("""
        SELECT m.mission_id, m.payload, m.signature, c.cid, e.eid
        FROM mission_test m
        JOIN commander c ON m.cid = c.cid
        JOIN executor e ON m.eid = e.eid;
    """)
    rows = cur.fetchall()

    verified = 0
    for mission_id, payload, signature, cid, eid in rows:
        try:
            if verify_signature(pub_key, signature, payload):
                verified += 1
        except Exception as e:
            print(f"[Error] Mission {mission_id} verification failed: {e}")

    elapsed = time.perf_counter() - start
    cur.close()
    conn.close()

    return {"verified": verified, "elapsed": elapsed}


# === Traversal 검증: 시나리오 B ===
def verify_traversal_b(config: TestConfig) -> dict:
    conn = psycopg.connect(
        host=config.db_host,
        port=config.db_port,
        dbname=config.db_name,
        user=config.db_user,
        password=config.db_password
    )
    cur = conn.cursor()

    issuer_pub = load_public_key("common/keys/commander_public.pem")

    start = time.perf_counter()

    cur.execute("""
        SELECT v.vc_json, v.issuer_did, v.subject_did
        FROM vc_test v
        JOIN did_issuer i ON v.issuer_did = i.did
        JOIN did_subject s ON v.subject_did = s.did;
    """)
    rows = cur.fetchall()

    verified = 0
    for vc_json, issuer_did, subject_did in rows:
        try:
            #vc = json.loads(vc_json)
            vc = vc_json

            # 구조 검증: issuer/subject DID 일치 여부
            if vc.get("issuer") != issuer_did:
                continue
            if vc.get("credentialSubject", {}).get("id") != subject_did:
                continue

            # 서명 검증
            if verify_vc(vc, issuer_pub):
                verified += 1
                
        except Exception as e:
            print("[Verify Error]", e)
            continue

    elapsed = time.perf_counter() - start
    cur.close()
    conn.close()

    return {"verified": verified, "elapsed": elapsed}


# === Traversal 검증: 시나리오 C ===
def verify_traversal_c(config: TestConfig) -> dict:
    conn = psycopg.connect(
        host=config.db_host,
        port=config.db_port,
        dbname=config.db_name,
        user=config.db_user,
        password=config.db_password
    )
    cur = conn.cursor()
    cur.execute("SET graph_path = vc_graph;")
    issuer_pub = load_public_key("common/keys/commander_public.pem")

    start = time.perf_counter()

    # VC, Issuer, Subject 간의 관계를 따라가며 검증
    cur.execute("""
        MATCH (i:Issuer)-[:ISSUED]->(v:VC)-[:ASSERTS]->(s:Subject)
        RETURN i.did, v.vc_json, s.did;
    """)
    rows = cur.fetchall()

    verified = 0
    for issuer_did, vc_json, subject_did in rows:
        try:
            vc = json.loads(vc_json)

            # 1. VC 내의 issuer와 그래프 Issuer 노드 비교
            if vc.get("issuer") != issuer_did:
                continue

            # 2. VC 내의 credentialSubject.id와 그래프 Subject 노드 비교
            subject_in_vc = vc.get("credentialSubject", {}).get("id")
            if subject_in_vc != subject_did:
                continue

            # 3. VC 서명 검증
            if verify_vc(vc, issuer_pub):
                verified += 1
        except Exception as e:
            print("Error verifying VC:", e)
            continue

    elapsed = time.perf_counter() - start
    cur.close()
    conn.close()

    return {"verified": verified, "elapsed": elapsed}


# === 엔트리 포인트 ===
if __name__ == "__main__":
    #cfg = TestConfig("config/test_small.json")
    cfg = TestConfig("config/test_large.json")
    
    result_a = verify_traversal_a(cfg)
    result_b = verify_traversal_b(cfg)
    result_c = verify_traversal_c(cfg)

    print("[Verify A]", result_a)
    print("[Verify B]", result_b)
    print("[Verify C]", result_c)
