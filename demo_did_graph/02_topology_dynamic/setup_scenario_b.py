#!/usr/bin/env python3
# setup_scenario_b.py

"""
Scenario B 환경 설정 스크립트
  - DID 인증 + RDB 기반
  - 시나리오 1, 2, 3(동적 네트워크 변화) 적용 준비
Usage:
  python setup_scenario_b.py --config config/test_large.json --scenario {1,2,3}
"""

import time
import json
import random
import argparse
from pathlib import Path
import psycopg
from psycopg import Binary
import sys

# 프로젝트 루트를 PYTHONPATH에 추가 (common 모듈 로드용)
ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))
from common.load_config import TestConfig
from common.did_utils import load_private_key, create_did, create_vc


def parse_args():
    parser = argparse.ArgumentParser(description="Setup Scenario B environment with dynamic network scenarios.")
    parser.add_argument('--config', '-c', required=True,
                        help='Path to JSON config file (e.g., config/test_large.json)')
    parser.add_argument('--scenario', '-s', required=True, choices=['1', '2', '3'],
                        help='Dynamic scenario number: 1=Real-Time Turn-Taking, 2=Chain-Churn, 3=Partition & Reconciliation')
    return parser.parse_args()


def setup_database(cfg: TestConfig, private_key, scenario: int):
    conn = psycopg.connect(**cfg.db_params)
    cur = conn.cursor()

    # 1) 테이블 생성 (없으면) 및 스키마 업그레이드
    cur.execute("""
    CREATE TABLE IF NOT EXISTS did_issuer (
      did TEXT PRIMARY KEY
    );
    """)
    # public_key 컬럼이 없으면 추가 (기존 데이터를 유지하면서)
    cur.execute("""
    ALTER TABLE did_issuer
      ADD COLUMN IF NOT EXISTS public_key JSON NOT NULL
        DEFAULT '{}'::JSON;
    """)
    conn.commit()

    # (1) DDL: ensure did_subject exists
    cur.execute("""
    CREATE TABLE IF NOT EXISTS did_subject (
    did TEXT PRIMARY KEY,
    public_key JSON NOT NULL
    );
    """)
    # public_key 컬럼이 없으면 추가 (기존 데이터를 유지하면서)
    cur.execute("""
    ALTER TABLE did_subject
      ADD COLUMN IF NOT EXISTS public_key JSON NOT NULL
        DEFAULT '{}'::JSON;
    """)
    conn.commit()

    print("› DID 및 VC 테이블 생성 또는 확인 완료")


    # 2) 테이블 초기화 (외래키 참조를 고려하여 한 번에 TRUNCATE)
    cur.execute("TRUNCATE TABLE did_issuer, vc_test;")
    conn.commit()
    print(f"› did_issuer, vc_test 테이블 초기화 완료 (scenario {scenario})")


    # 3) DID 생성 및 삽입
    start = time.perf_counter()
    for _ in range(cfg.num_drones):
        # create_did()는 문자열 반환
        did = create_did()
        # public_key 정보가 필요하면 private_key에서 추출하도록 구현하세요
        public_key_dict = {}
        cur.execute(
            "INSERT INTO did_issuer (did, public_key) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
            (did, json.dumps(public_key_dict))
        )
        # **also** insert as a subject
        cur.execute(
        "INSERT INTO did_subject (did, public_key) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
        (did, json.dumps(public_key_dict))
        )

    conn.commit()
    print(f"› DID 삽입 완료: {cfg.num_drones}건 in {time.perf_counter() - start:.2f}s")


    # 4) VC 생성 및 삽입
    priv_key = load_private_key("common/keys/commander_private.pem")
    start2 = time.perf_counter()
    cur.execute("SELECT did FROM did_issuer;")
    all_dids = [r[0] for r in cur.fetchall()]
    for i in range(cfg.num_mission):
        issuer = random.choice(all_dids)
        subject = random.choice(all_dids)
        vc_doc = create_vc(issuer, subject, {"mission": i}, priv_key)
        cur.execute(
            "INSERT INTO vc_test (vc_id, issuer_did, subject_did, vc_json) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING;",
            (vc_doc['id'], issuer, subject, json.dumps(vc_doc))
        )
    conn.commit()
    print(f"› VC 삽입 완료: {cfg.num_mission}건 in {time.perf_counter() - start2:.2f}s")



    # 두 번째 VC 생성 루프: 반드시 did_issuer에서 DID를 가져와야 외래키 제약을 만족합니다
    start_vc2 = time.perf_counter()
    # did_issuer 에서 다시 한 번 전체 DID 목록을 가져옵니다
    cur.execute("SELECT did FROM did_issuer;")
    all_dids = [row[0] for row in cur.fetchall()]

    vc_count2 = 0
    for i in range(cfg.num_mission):
        issuer2 = random.choice(all_dids)
        subject2 = random.choice(all_dids)
        # create_vc 호출도 키워드 인자로 확실히 맞춰주세요
        vc_doc2 = create_vc(
            issuer_did=issuer2,
            subject_did=subject2,
            data={"mission": i},
            private_key=private_key
        )
        cur.execute(
            "INSERT INTO vc_test (vc_id, issuer_did, subject_did, vc_json) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING;",
            (vc_doc2["id"], issuer2, subject2, json.dumps(vc_doc2))
        )
        vc_count2 += 1
    conn.commit()
    print(f"› 두 번째 VC 생성 및 삽입 완료: {vc_count2}건 in {time.perf_counter() - start_vc2:.2f}s")




    # 5) 시나리오별 안내
    print(f"› Scenario B-{scenario} 데이터 로드 완료: DID 및 VC 준비 완료")

    # 정리
    cur.close()
    conn.close()
    print(f"Scenario B 환경 설정 완료 (scenario {scenario}).")


if __name__ == '__main__':
    args = parse_args()
    cfg = TestConfig(args.config)
    private_key = load_private_key(cfg.private_key_path)
    setup_database(cfg, private_key, int(args.scenario))
