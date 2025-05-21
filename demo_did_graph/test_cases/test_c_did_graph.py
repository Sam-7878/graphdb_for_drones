# testcases/test_c_did_graph.py

import time
import json
import random
import psycopg

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

def run_test_case_c(config: TestConfig):
    print("[C-Test] Running DID + VC + AgensGraph Test")

    random.seed(config.random_seed)

    # DB 연결 (AgensGraph)
    conn = psycopg.connect(
        host=config.db_host,
        port=config.db_port,
        dbname=config.db_name,
        user=config.db_user,
        password=config.db_password
    )
    cur = conn.cursor()

    # Graph 생성 및 초기화
    cur.execute("DROP GRAPH IF EXISTS vc_graph CASCADE;")
    cur.execute("CREATE GRAPH vc_graph;")
    cur.execute("SET graph_path = vc_graph;")

    cur.execute("""
        CREATE (:Label {name:'DID'})
    """)  # Dummy node for label registration

    cur.execute("""
        CREATE VLABEL Issuer;
        CREATE VLABEL Subject;
        CREATE VLABEL VC;
        CREATE ELABEL ISSUED;
        CREATE ELABEL ASSERTS;
    """)

    conn.commit()

    # VC 생성 및 Graph 삽입
    # issuer_private, issuer_public = generate_key_pair()

    insert_start = time.perf_counter()


    # 2025-04-30 modified by su.
    # 고정된 키 불러오기
    issuer_private = load_private_key(PRIVATE_KEY_PATH)
    issuer_public  = load_public_key(PUBLIC_KEY_PATH)
    issuer_did = "did:example:issuer"

    inserted = 0
    start = time.perf_counter()

    # for 루프 밖에서 Issuer 노드 생성
    cur.execute(f"CREATE (:Issuer {{did: '{issuer_did}'}});")

    # 루프 안에서는 중복 INSERT 방지 또는 존재 여부 확인 후 관계만 생성:
    for i in range(config.num_mission):
        subject_did = create_did()
        #subject_did = 'did:example:subject1'
        payload = {
            "mission_id": f"M{i:04d}",
            "content": f"Mission content {i}"
        }

        vc = create_vc(issuer_did, subject_did, payload, issuer_private)
        vc_id = vc["id"]
        # 노드 및 관계 삽입
        cur.execute(f"""
            CREATE (:Subject {{did: '{subject_did}'}});

            CREATE (:VC {{vc_id: '{vc_id}', vc_json: '{json.dumps(vc).replace("'", "''")}'}});
            
            MATCH (i:Issuer), (v:VC)
            WHERE i.did = '{issuer_did}' AND v.vc_id = '{vc_id}'
            CREATE (i)-[:ISSUED]->(v);
            
            MATCH (s:Subject), (v:VC)
            WHERE s.did = '{subject_did}' AND v.vc_id = '{vc_id}'
            CREATE (v)-[:ASSERTS]->(s);
        """)
            # MATCH (i:Issuer), (v:VC) WHERE i.did = '{issuer_did}' AND v.vc_id = '{vc_id}'
            #     CREATE (i)-[:ISSUED]->(v);
            # MATCH (s:Subject), (v:VC) WHERE s.did = '{subject_did}' AND v.vc_id = '{vc_id}'
            #     CREATE (v)-[:ASSERTS]->(s);

        # cur.execute("""
        #     CREATE (:VC {vc_id: %s, issuer_did: %s, subject_did: %s, vc_json: %s});
        # """, (vc_id, issuer_did, subject_did, json.dumps(vc).replace("'", "''")))

        inserted += 1

    
    conn.commit()
    insert_time = time.perf_counter() - insert_start
    print(f"[C-Test] Inserted {config.num_mission} VCs in {insert_time:.4f} seconds")

    # Traversal + 검증
    verify_start = time.perf_counter()
    cur.execute("SET graph_path = vc_graph;")
    cur.execute("MATCH (v:VC) RETURN v.vc_json;")
    rows = cur.fetchall()

    verified_count = 0
    for row in rows:
        vc = json.loads(row[0])
        if verify_vc(vc, issuer_public):
            verified_count += 1

    verify_time = time.perf_counter() - verify_start
    print(f"[C-Test] Verified {verified_count} VCs in {verify_time:.4f} seconds")

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
    
    result = run_test_case_c(cfg)
    print("[C-Test] Result:", result)
