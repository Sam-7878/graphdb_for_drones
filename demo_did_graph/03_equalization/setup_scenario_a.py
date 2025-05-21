#!/usr/bin/env python3
# setup_scenario_a.py

"""
Scenario A 환경 설정 스크립트
  - Asymmetric-key 인증 + RDB 기반
  - 시나리오 1, 2, 3(동적 네트워크 변화) 적용 준비
Usage:
  python setup_scenario_a.py --config config/test_large.json --scenario {1,2,3}
"""

import time
import argparse
import random
import json
import sys
from pathlib import Path
import psycopg
from psycopg import Binary
# 프로젝트 루트를 PYTHONPATH에 추가 (common 모듈 로드용)
ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))
from common.load_config import TestConfig
from common.did_utils import load_private_key
from common.sign_verify import sign_data


def parse_args():
    parser = argparse.ArgumentParser(
        description="Setup Scenario A environment with dynamic network scenarios."
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


def setup_database(cfg, private_key, scenario):
    conn = psycopg.connect(**cfg.db_params)
    cur = conn.cursor()

    # (옵션) 세션 단위로 WAL 동기화를 끄면 로드 성능이 향상될 수 있습니다.
    cur.execute("SET synchronous_commit = ON;")
    conn.commit()
    print("› synchronous_commit 설정 OFF")

    # 1) mission_test 테이블 초기화
    cur.execute("TRUNCATE TABLE mission_test;")
    conn.commit()
    print(f"› mission_test 테이블 초기화 완료 (scenario {scenario})")

    # 2) delegation 테이블 생성 및 인덱스
    cur.execute("""
        CREATE TABLE IF NOT EXISTS delegation (
            drone_id INTEGER PRIMARY KEY,
            hq_id    TEXT    NOT NULL
        );
    """
    )
    conn.commit()
    print("› delegation 테이블 생성 또는 확인 완료")

    # delegation.hq_id 컬럼에 인덱스 생성
    cur.execute("CREATE INDEX IF NOT EXISTS idx_delegation_hq_id ON delegation(hq_id);")
    conn.commit()
    print("› delegation(hq_id) 인덱스 생성 완료")

    # 3) 초기 delegation 엣지 삽입 (모든 드론을 HQ로 위임)
    start_time = time.perf_counter()
    for drone_id in range(cfg.num_drones):
        HQ_ID = cfg.headquarters_id
        cur.execute(
            """
            INSERT INTO delegation (drone_id, hq_id)
            VALUES (%s, %s)
            ON CONFLICT (drone_id) DO UPDATE SET hq_id = EXCLUDED.hq_id;
            """,
            (drone_id, HQ_ID)
        )
    conn.commit()
    elapsed = time.perf_counter() - start_time
    print(f"› delegation 엣지 삽입 완료: {cfg.num_drones} edges in {elapsed:.2f}s")

    # 4) 분석 정보 갱신 (쿼리 플래너 사용을 위해)
    cur.execute("ANALYZE delegation;")
    conn.commit()
    print("› delegation 테이블 분석(ANALYZE) 완료")

    # 5) mission_test 데이터 삽입 (시나리오별 payload + signature)
    start = time.perf_counter()
    for i in range(cfg.num_mission):
        mission_id = f"m{i}"
        drone_id   = random.randrange(cfg.num_drones)
        payload    = json.dumps({"step": i})
        signature  = sign_data(private_key, payload.encode())
        cur.execute(
            """
            INSERT INTO mission_test (mission_id, drone_id, cid, payload, signature)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (mission_id) DO NOTHING;
            """,
            (mission_id, drone_id, cfg.headquarters_id, payload, Binary(signature))
        )
    conn.commit()
    elapsed = time.perf_counter() - start
    print(f"› mission_test 삽입 완료: {cfg.num_mission} rows in {elapsed:.2f}s")

    # 6) 정리
    cur.close()
    conn.close()
    print(f"Scenario A 환경 설정 완료 (scenario {scenario}).")


if __name__ == '__main__':
    args = parse_args()
    cfg = TestConfig(args.config)

    # 개인키 로드 (비대칭 서명 키)
    private_key = load_private_key(cfg.private_key_path)

    # DB에 초기 데이터 삽입
    setup_database(cfg, private_key, int(args.scenario))
