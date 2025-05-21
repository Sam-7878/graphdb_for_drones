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
import sys
from pathlib import Path
import psycopg
from psycopg import Binary
# 프로젝트 루트를 PYTHONPATH에 추가 (common 모듈 로드용)
ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))
from common.load_config import TestConfig
from common.did_utils import load_private_key, create_did, create_vc



def parse_args():
    parser = argparse.ArgumentParser(
        description="Setup Scenario B environment with dynamic network scenarios."
    )
    parser.add_argument(
        '--config', '-c', required=True,
        help='Path to JSON config file (e.g., config/test_large.json)'
    )
    parser.add_argument(
        '--scenario', '-s', required=True, choices=['1', '2', '3'],
        help='Dynamic scenario number: 1=Real-Time Turn-Taking, 2=Chain-Churn, 3=Partition & Reconciliation'
    )
    return parser.parse_args()


def setup_database(cfg: TestConfig, private_key, scenario: int):
    conn = psycopg.connect(**cfg.db_params)
    cur = conn.cursor()

    # 0) 성능 최적화: WAL 동기화 비활성화
    cur.execute("SET synchronous_commit = ON;")
    conn.commit()
    print("› synchronous_commit 설정 OFF")

    # 1) delegation 테이블 생성 및 인덱스
    cur.execute("DROP TABLE IF EXISTS delegation;")
    cur.execute("""
        CREATE UNLOGGED TABLE delegation (
            drone_id INTEGER PRIMARY KEY,
            hq_id    TEXT    NOT NULL
        );
    """
    )
    conn.commit()
    cur.execute("CREATE INDEX IF NOT EXISTS idx_delegation_hq_id ON delegation(hq_id);")
    conn.commit()
    print("› delegation 테이블 및 인덱스 생성 완료")

    # 2) DID 발급용 테이블 생성
    cur.execute("""
        CREATE TABLE IF NOT EXISTS did_issuer (
            did TEXT PRIMARY KEY,
            public_key JSON NOT NULL DEFAULT '{}'::JSON
        );
    """
    )
    cur.execute("""
        CREATE TABLE IF NOT EXISTS did_subject (
            did TEXT PRIMARY KEY,
            public_key JSON NOT NULL DEFAULT '{}'::JSON
        );
    """
    )
    conn.commit()
    print("› did_issuer, did_subject 테이블 생성 완료")

    # 3) VC 저장용 테이블 생성 및 인덱스
    cur.execute("""
        CREATE TABLE IF NOT EXISTS vc_test (
            vc_id TEXT PRIMARY KEY,
            issuer_did TEXT NOT NULL REFERENCES did_issuer(did),
            subject_did TEXT NOT NULL REFERENCES did_subject(did),
            vc_json JSON NOT NULL
        );
    """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_vc_subject ON vc_test(subject_did);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_vc_issuer ON vc_test(issuer_did);")
    conn.commit()
    print("› vc_test 테이블 및 인덱스 생성 완료")

    # 4) 테이블 초기화
    cur.execute("TRUNCATE TABLE delegation, did_issuer, did_subject, vc_test;")
    conn.commit()
    print(f"› delegation, did_issuer, did_subject, vc_test 초기화 완료 (scenario {scenario})")

    # 5) delegation 데이터 삽입
    start = time.perf_counter()
    for drone_id in range(cfg.num_drones):
        cur.execute(
            """
            INSERT INTO delegation (drone_id, hq_id)
            VALUES (%s, %s)
            ON CONFLICT (drone_id) DO UPDATE SET hq_id = EXCLUDED.hq_id;
            """,
            (drone_id, cfg.headquarters_id)
        )
    conn.commit()
    print(f"› delegation 삽입 완료: {cfg.num_drones} edges in {time.perf_counter() - start:.2f}s")
    cur.execute("ANALYZE delegation;")
    conn.commit()
    print("› delegation ANALYZE 완료")

    # 6) DID 발급 및 삽입
    start = time.perf_counter()
    for _ in range(cfg.num_drones):
        did = create_did()
        public_key_dict = {}  # TODO: private_key에서 공개키 추출
        cur.execute(
            "INSERT INTO did_issuer (did, public_key) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
            (did, json.dumps(public_key_dict))
        )
        cur.execute(
            "INSERT INTO did_subject (did, public_key) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
            (did, json.dumps(public_key_dict))
        )
    conn.commit()
    print(f"› DID 삽입 완료: {cfg.num_drones} items in {time.perf_counter() - start:.2f}s")
    cur.execute("ANALYZE did_issuer;")
    cur.execute("ANALYZE did_subject;")
    conn.commit()
    print("› did_issuer, did_subject ANALYZE 완료")

    # 7) VC 생성 및 삽입
    start = time.perf_counter()
    cur.execute("SELECT did FROM did_issuer;")
    all_dids = [row[0] for row in cur.fetchall()]
    for i in range(cfg.num_mission):
        issuer = random.choice(all_dids)
        subject = random.choice(all_dids)
        vc_doc = create_vc(issuer, subject, {"mission": i}, private_key)
        cur.execute(
            "INSERT INTO vc_test (vc_id, issuer_did, subject_did, vc_json) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING;",
            (vc_doc['id'], issuer, subject, json.dumps(vc_doc))
        )
    conn.commit()
    print(f"› VC 삽입 완료: {cfg.num_mission} items in {time.perf_counter() - start:.2f}s")
    cur.execute("ANALYZE vc_test;")
    conn.commit()
    print("› vc_test ANALYZE 완료")

    # 8) 정리
    cur.close()
    conn.close()
    print(f"Scenario B 환경 설정 완료 (scenario {scenario}).")


if __name__ == '__main__':
    args = parse_args()
    cfg = TestConfig(args.config)
    private_key = load_private_key(cfg.private_key_path)
    setup_database(cfg, private_key, int(args.scenario))
