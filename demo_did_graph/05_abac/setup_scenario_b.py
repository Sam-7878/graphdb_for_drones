#!/usr/bin/env python3
# setup_scenario_b.py

"""
Scenario B 환경 설정 스크립트
  - DID 인증 + RDB 기반
  - 시나리오 1, 2, 3, 4(Web-of-Trust), 5(ABAC) 적용 준비
Usage:
  python setup_scenario_b.py --config config/test_large.json --scenario {1,2,3,4,5}
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
    parser.add_argument('--config', '-c', required=True,
        help='Path to JSON config file (e.g., config/test_large.json)')
    parser.add_argument('--scenario', '-s', required=True, choices=['1', '2', '3', '4', '5'],
        help='Scenario number (1-5)')
    return parser.parse_args()


def setup_database(cfg: TestConfig, private_key, scenario: int):
    conn = psycopg.connect(**cfg.db_params)
    cur = conn.cursor()

    # WAL 동기화 제거로 커밋 지연 최소화
    cur.execute("SET synchronous_commit = ON;")
    conn.commit()
    print("› synchronous_commit ON 설정 완료")

    if scenario in (1, 2, 5):
        # delegation 테이블: drone_id TEXT, hq_id TEXT
        cur.execute("DROP TABLE IF EXISTS delegation;")
        cur.execute("""
            CREATE UNLOGGED TABLE delegation (
                drone_id TEXT PRIMARY KEY,
                hq_id    TEXT NOT NULL
            );
        """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_delegation_hq_id ON delegation(hq_id);")

        # DID 테이블
        cur.execute("DROP TABLE IF EXISTS vc_test;")
        cur.execute("DROP TABLE IF EXISTS did_subject;")
        cur.execute("DROP TABLE IF EXISTS did_issuer;")
        cur.execute("DROP TABLE IF EXISTS did_subject;")
        cur.execute("""
            CREATE TABLE did_issuer (
                did TEXT PRIMARY KEY,
                public_key JSON NOT NULL DEFAULT '{}'::JSON
            );
        """
        )
        cur.execute("""
            CREATE TABLE did_subject (
                did TEXT PRIMARY KEY,
                public_key JSON NOT NULL DEFAULT '{}'::JSON
            );
        """
        )

        # VC 저장소
        cur.execute("DROP TABLE IF EXISTS vc_test;")
        cur.execute("""
            CREATE TABLE vc_test (
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

        # Drone DID 목록 생성 및 등록
        drones = []
        for i in range(cfg.num_drones):
            did = create_did()
            drones.append(did)
            # subject 및 issuer 등록
            cur.execute(
                "INSERT INTO did_subject (did, public_key) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
                (did, json.dumps({}))
            )
            cur.execute(
                "INSERT INTO did_issuer (did, public_key) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
                (did, json.dumps({}))
            )
        conn.commit()

        # delegation 테이블에 HQ 위임 정보 삽입
        for did in drones:
            cur.execute(
                "INSERT INTO delegation (drone_id, hq_id) VALUES (%s, %s) "
                "ON CONFLICT (drone_id) DO UPDATE SET hq_id = EXCLUDED.hq_id;",
                (did, cfg.headquarters_id)
            )
        conn.commit()
        print(f"› delegation 및 DID 등록 완료: {len(drones)} drones")

        # Verifiable Credential 생성 및 등록
        for idx, subject in enumerate(drones):
            issuer = random.choice(drones)
            vc_doc = create_vc(issuer, subject, {"index": idx}, private_key)
            vc_id = vc_doc.get('id')
            cur.execute(
                "INSERT INTO vc_test (vc_id, issuer_did, subject_did, vc_json) VALUES (%s, %s, %s, %s) "
                "ON CONFLICT (vc_id) DO NOTHING;",
                (vc_id, vc_doc.get('issuer', issuer), subject, json.dumps(vc_doc))
            )
        conn.commit()
        print(f"› VC 등록 완료: {cfg.num_drones} credentials")
        print(f"› VC 등록 완료: {cfg.num_drones} credentials")
        print(f"› VC 등록 완료: {cfg.num_drones} credentials")

    elif scenario == 3:
        # ABAC 테이블 생성 및 초기화
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
            cur.execute("INSERT INTO abac_user (did) VALUES (%s) ON CONFLICT DO NOTHING;", (u,))
        for g in groups:
            cur.execute("INSERT INTO abac_group (id) VALUES (%s) ON CONFLICT DO NOTHING;", (g,))
        for r in resources:
            cur.execute("INSERT INTO abac_resource (id) VALUES (%s) ON CONFLICT DO NOTHING;", (r,))
        conn.commit()

        # 그룹 관계 및 권한 설정
        for idx, u in enumerate(users):
            g = groups[idx % len(groups)]
            cur.execute("INSERT INTO abac_member (user_did, group_id) VALUES (%s, %s);", (u, g))
        for i in range(len(groups)-1):
            cur.execute("INSERT INTO abac_subgroup (from_id, to_id) VALUES (%s, %s);", (groups[i], groups[i+1]))
        for r in resources:
            cur.execute("INSERT INTO abac_permission (group_id, resource_id) VALUES (%s, %s);", (groups[-1], r))
        conn.commit()
        print(f"› ABAC 사용자 {len(users)}, 그룹 {len(groups)}, 리소스 {len(resources)} 삽입 완료")

    elif scenario == 4:
        # Web-of-Trust RDB 구현
        cur.execute("DROP TABLE IF EXISTS web_trust;")
        cur.execute("""
            CREATE UNLOGGED TABLE web_trust (
                from_did TEXT NOT NULL,
                to_did TEXT NOT NULL
            );
        """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_from_did ON web_trust(from_did);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_to_did ON web_trust(to_did);")
        conn.commit()

        anchor = cfg.scenario_params['4']['web_of_trust']['anchor_did']
        entities = [create_did() for _ in range(cfg.num_drones)]
        # subject table 등록
        for e in entities:
            cur.execute(
                "INSERT INTO did_subject (did, public_key) VALUES (%s, %s) ON CONFLICT DO NOTHING;",
                (e, json.dumps({}))
            )
        # 웹 트러스트 체인 생성
        for i in range(len(entities)-1):
            cur.execute(
                "INSERT INTO web_trust (from_did, to_did) VALUES (%s, %s);",
                (entities[i], entities[i+1])
            )
        cur.execute(
            "INSERT INTO web_trust (from_did, to_did) VALUES (%s, %s);",
            (entities[-1], anchor)
        )
        conn.commit()
        print(f"› Web-of-Trust 생성 완료: {len(entities)} chain → {anchor}")

    else:
        raise ValueError(f"Unsupported scenario: {scenario}")

    cur.close()
    conn.close()
    print(f"Scenario B-{scenario} 환경 설정 완료.")


if __name__ == '__main__':
    args = parse_args()
    cfg = TestConfig(args.config)
    private_key = load_private_key(cfg.private_key_path)
    setup_database(cfg, private_key, int(args.scenario))
