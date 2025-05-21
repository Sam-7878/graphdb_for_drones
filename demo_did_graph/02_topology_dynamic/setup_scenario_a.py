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
from pathlib import Path
import psycopg
from psycopg import sql, Binary
import json
import random
import sys

# 프로젝트 루트를 PYTHONPATH에 추가 (common 모듈 로드용)
ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))
from common.load_config import TestConfig
from common.sign_verify import load_private_key, sign_data


def parse_args():
    parser = argparse.ArgumentParser(description="Setup Scenario A environment with dynamic network scenarios.")
    parser.add_argument('--config', '-c', required=True,
                        help='Path to JSON config file (e.g., config/test_large.json)')
    parser.add_argument('--scenario', '-s', required=True, choices=['1', '2', '3'],
                        help='Dynamic scenario number: 1=Real-Time Turn-Taking, 2=Chain-Churn, 3=Partition & Reconciliation')
    return parser.parse_args()


def setup_database(cfg, private_key, scenario):
    conn = psycopg.connect(**cfg.db_params)
    cur = conn.cursor()

    # 1) 테이블 초기화
    cur.execute("TRUNCATE TABLE mission_test;")
    conn.commit()
    print(f"› mission_test 테이블 초기화 완료 (scenario {scenario})")

    # 2) 드론 노드와 초기 delegation 엣지 삽입
    start = time.perf_counter()
    inserted = 0

    # setup_database() 맨 위에 추가
    cur.execute("""
        CREATE TABLE IF NOT EXISTS delegation (
        drone_id   INTEGER     PRIMARY KEY,
        hq_id      TEXT        NOT NULL
        );
        """)
    conn.commit()
    print("› delegation 테이블 생성 또는 확인 완료")
    # 드론 노드와 HQ_ID를 기반으로 기본 delegation 엣지 삽입

    for drone_id in range(cfg.num_drones):
        # 기본 위임: 모두 HQ_ID로 위임
        HQ_ID = cfg.headquarters_id
        cur.execute(
            """
            INSERT INTO delegation (drone_id, hq_id)
            VALUES (%s, %s)
            ON CONFLICT (drone_id) DO UPDATE SET hq_id = EXCLUDED.hq_id
            """,
            (drone_id, HQ_ID)
        )
        inserted += 1
    conn.commit()
    elapsed = time.perf_counter() - start
    print(f"› 기본 delegation 삽입 완료: {inserted}건 in {elapsed:.2f}s")

    # 3) 미션 테스트 데이터 준비
    start2 = time.perf_counter()
    inserted = 0
    for i in range(cfg.num_mission):
        mission_id = f"m{{i}}"
        drone_id = random.randrange(cfg.num_drones)
        HQ_ID = cfg.headquarters_id
        payload = json.dumps({"step": i})
        signature = sign_data(private_key, payload.encode())
        cur.execute(
            """
            INSERT INTO mission_test (mission_id, drone_id, cid, payload, signature) 
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (mission_id) DO NOTHING;
            """,
            (mission_id, drone_id, HQ_ID, payload, Binary(signature))
        )
        inserted += 1
    conn.commit()
    elapsed2 = time.perf_counter() - start2
    print(f"› 미션 및 서명 삽입 완료: {inserted}건 in {elapsed2:.2f}s")

    # 4) 시나리오별 초기 안내
    if scenario == 1:
        print("› Scenario 1 (Real-Time Turn-Taking) 준비 완료: 기본 delegation + mission 데이터 로드")
    elif scenario == 2:
        print("› Scenario 2 (Chain-Churn) 준비 완료: 기본 delegation + mission 데이터 로드")
    else:
        print("› Scenario 3 (Partition & Reconciliation) 준비 완료: 기본 delegation + mission 데이터 로드")

    # 5) 정리
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
