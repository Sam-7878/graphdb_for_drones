#!/usr/bin/env python3
# test_sqlite.py

import sqlite3
import os

DB_PATH = "test.db"

def init_db(path):
    # 기존 파일이 있으면 삭제
    if os.path.exists(path):
        os.remove(path)
    # DB 연결
    conn = sqlite3.connect(path)
    print("Opened database at", path)
    return conn

def setup_schema(conn):
    cur = conn.cursor()
    # 간단한 테이블 생성 (id INTEGER, data BLOB)
    cur.execute("""
        CREATE TABLE sample (
            id   INTEGER PRIMARY KEY,
            data BLOB
        )""")
    conn.commit()
    print("Table 'sample' created.")

def insert_blob(conn, record_id, blob_bytes):
    cur = conn.cursor()
    cur.execute("INSERT INTO sample(id, data) VALUES(?, ?)", (record_id, blob_bytes))
    conn.commit()
    print(f"Inserted record id={record_id}, {len(blob_bytes)} bytes of BLOB.")

def query_blob(conn, record_id):
    cur = conn.cursor()
    cur.execute("SELECT data FROM sample WHERE id = ?", (record_id,))
    row = cur.fetchone()
    if row:
        blob = row[0]
        print(f"Queried id={record_id}, BLOB size={len(blob)} bytes.")
    else:
        print(f"No record found with id={record_id}.")

def main():
    # 1) DB 초기화
    conn = init_db(DB_PATH)

    # 2) 스키마 준비
    setup_schema(conn)

    # 3) BLOB 삽입 (예: DID/VC JSON을 bytes로)
    example_json = b'{"did":"did:example:alice","vc":{"type":"TestCredential"}}'
    insert_blob(conn, record_id=1, blob_bytes=example_json)

    # 4) BLOB 조회 확인
    query_blob(conn, record_id=1)

    conn.close()
    print("Done.")

if __name__ == "__main__":
    main()
