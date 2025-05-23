#!/usr/bin/env python3
# postgres_cdc_sink.py

import os
import json
import psycopg2
from kafka import KafkaConsumer
# 프로젝트 루트를 PYTHONPATH에 추가 (common 모듈 로드용)
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT))
from common.load_config import TestConfig

# — 환경 변수 또는 하드코딩된 기본값
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "sqlite.cdc.items")
PG_DSN          = os.getenv("PG_DSN", "dbname=testdb user=sam password=dooley host=localhost")

def init_postgres(conn):
    """Postgres에 타겟 테이블 생성"""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS items (
                id   INTEGER PRIMARY KEY,
                name TEXT
            );
        """)
    conn.commit()
    print("✅ Postgres 테이블 준비 완료: items")

def apply_event(conn, payload):
    """
    Debezium 페이로드(python dict)에서 'before','after','op'를 꺼내
    Postgres에 INSERT/UPDATE/DELETE 수행
    """
    op     = payload.get("op")      # c,r=insert, u=update, d=delete
    before = payload.get("before")  # 삭제/업데이트 전 값
    after  = payload.get("after")   # 삽입/업데이트 후 값

    with conn.cursor() as cur:
        if op in ("c", "r"):
            # upsert
            cur.execute(
                "INSERT INTO items(id, name) VALUES(%s, %s) "
                "ON CONFLICT(id) DO UPDATE SET name = EXCLUDED.name",
                (after["id"], after["name"])
            )
            print(f"[PG] UPSERT id={after['id']} name={after['name']}")
        elif op == "u":
            cur.execute(
                "UPDATE items SET name=%s WHERE id=%s",
                (after["name"], after["id"])
            )
            print(f"[PG] UPDATE id={after['id']} name={after['name']}")
        elif op == "d":
            cur.execute(
                "DELETE FROM items WHERE id=%s",
                (before["id"],)
            )
            print(f"[PG] DELETE id={before['id']}")
        else:
            print(f"[PG] Unknown op: {op}")
    conn.commit()

def main():
    # 1) Postgres 연결 및 테이블 준비
    pg_conn = psycopg2.connect(PG_DSN)

    # cfg = TestConfig("config/test_small.json")
    # pg_conn = psycopg2.connect(**cfg.db_params)

    init_postgres(pg_conn)

    # 2) Kafka 소비자 설정
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
        auto_offset_reset='earliest',
        group_id='cdc-sink-group',
        value_deserializer=lambda m: json.loads(m.decode())
    )
    print(f"▶ Listening on Kafka topic: {KAFKA_TOPIC}")

    # 3) 메시지 수신 → Postgres 반영
    try:
        for msg in consumer:
            payload = msg.value.get("payload")
            if payload:
                apply_event(pg_conn, payload)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        pg_conn.close()

if __name__ == "__main__":
    main()
