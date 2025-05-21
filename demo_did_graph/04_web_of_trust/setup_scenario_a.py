#!/usr/bin/env python3
# setup_scenario_a.py

"""
Scenario A 환경 설정 스크립트
  - Asymmetric-key 인증 + RDB 기반
  - 시나리오 1, 2, 3, 4(Web-of-Trust), 5(ABAC) 적용 준비
Usage:
  python setup_scenario_a.py --config config/test_large.json --scenario {1,2,3,4,5}
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
from common.did_utils import load_private_key, create_did
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
        '--scenario', '-s', required=True, choices=['1', '2', '3', '4', '5'],
        help='Scenario number (1-5)'
    )
    return parser.parse_args()

def setup_database(cfg, private_key, scenario):
    conn = psycopg.connect(**cfg.db_params)
    cur = conn.cursor()

    cur.execute("SET synchronous_commit = ON;")
    conn.commit()
    print("› synchronous_commit 설정 ON")

    if scenario in (1, 2, 3):
        cur.execute("TRUNCATE TABLE mission_test;")
        cur.execute("CREATE TABLE IF NOT EXISTS delegation (drone_id INTEGER PRIMARY KEY, hq_id TEXT NOT NULL);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_delegation_hq_id ON delegation(hq_id);")
        conn.commit()

        for drone_id in range(cfg.num_drones):
            cur.execute("INSERT INTO delegation (drone_id, hq_id) VALUES (%s, %s) ON CONFLICT (drone_id) DO UPDATE SET hq_id = EXCLUDED.hq_id;",
                        (drone_id, cfg.headquarters_id))
        conn.commit()

        for i in range(cfg.num_mission):
            mission_id = f"m{i}"
            drone_id   = random.randrange(cfg.num_drones)
            payload    = json.dumps({"step": i})
            signature  = sign_data(private_key, payload.encode())
            cur.execute("""
                INSERT INTO mission_test (mission_id, drone_id, cid, payload, signature)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (mission_id) DO NOTHING;
            """, (mission_id, drone_id, cfg.headquarters_id, payload, Binary(signature)))
        conn.commit()
        print(f"› Scenario A-{scenario}: 기본 위임 및 미션 생성 완료")

    elif scenario == 4:
        cur.execute("DROP TABLE IF EXISTS web_trust;")
        cur.execute("""
            CREATE UNLOGGED TABLE web_trust (
                from_did TEXT NOT NULL,
                to_did TEXT NOT NULL
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_from_did ON web_trust(from_did);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_to_did ON web_trust(to_did);")
        conn.commit()

        anchor = cfg.scenario_params['4']['web_of_trust']['anchor_did']
        entities = [create_did() for _ in range(cfg.num_drones)]
        for i in range(len(entities)-1):
            cur.execute("INSERT INTO web_trust (from_did, to_did) VALUES (%s, %s);", (entities[i], entities[i+1]))
        cur.execute("INSERT INTO web_trust (from_did, to_did) VALUES (%s, %s);", (entities[-1], anchor))
        conn.commit()
        print(f"› Scenario A-4 Web-of-Trust 생성 완료 → {anchor}")

    elif scenario == 5:
        cur.execute("DROP TABLE IF EXISTS abac_user, abac_group, abac_resource, abac_member, abac_subgroup, abac_permission;")
        cur.execute("CREATE TABLE abac_user (did TEXT PRIMARY KEY);")
        cur.execute("CREATE TABLE abac_group (id TEXT PRIMARY KEY);")
        cur.execute("CREATE TABLE abac_resource (id TEXT PRIMARY KEY);")
        cur.execute("CREATE TABLE abac_member (user_did TEXT, group_id TEXT);")
        cur.execute("CREATE TABLE abac_subgroup (from_id TEXT, to_id TEXT);")
        cur.execute("CREATE TABLE abac_permission (group_id TEXT, resource_id TEXT);")
        conn.commit()

        users = [create_did() for _ in range(cfg.num_drones)]
        groups = [create_did() for _ in range(max(1, cfg.num_drones // 10))]
        resources = [create_did() for _ in range(len(groups))]

        for u in users:
            cur.execute("INSERT INTO abac_user (did) VALUES (%s);", (u,))
        for g in groups:
            cur.execute("INSERT INTO abac_group (id) VALUES (%s);", (g,))
        for r in resources:
            cur.execute("INSERT INTO abac_resource (id) VALUES (%s);", (r,))

        for idx, u in enumerate(users):
            g = groups[idx % len(groups)]
            cur.execute("INSERT INTO abac_member (user_did, group_id) VALUES (%s, %s);", (u, g))
        for i in range(len(groups)-1):
            cur.execute("INSERT INTO abac_subgroup (from_id, to_id) VALUES (%s, %s);", (groups[i], groups[i+1]))
        for r in resources:
            cur.execute("INSERT INTO abac_permission (group_id, resource_id) VALUES (%s, %s);", (groups[-1], r))
        conn.commit()
        print(f"› Scenario A-5 ABAC 삽입 완료: 사용자={len(users)}, 그룹={len(groups)}, 리소스={len(resources)}")

    cur.close()
    conn.close()
    print(f"Scenario A-{scenario} 환경 설정 완료.")


if __name__ == '__main__':
    args = parse_args()
    cfg = TestConfig(args.config)
    private_key = load_private_key(cfg.private_key_path)
    setup_database(cfg, private_key, int(args.scenario))