#!/usr/bin/env python3
# trigger_cdc_test_app.py

import sqlite3, os, threading, time
from random import choice, randint

DB_PATH = "test_cdc.db"
POLL_INTERVAL = 1.0  # 초 단위

def init_db(path):
    if os.path.exists(path):
        os.remove(path)
    conn = sqlite3.connect(path, check_same_thread=False)
    cursor = conn.cursor()
    # items 테이블
    cursor.execute("""
        CREATE TABLE items (
            id   INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT
        )
    """)
    # change_log 테이블
    cursor.execute("""
        CREATE TABLE change_log (
            change_id  INTEGER PRIMARY KEY AUTOINCREMENT,
            operation  TEXT,
            table_name TEXT,
            row_id     INTEGER,
            old_name   TEXT,
            new_name   TEXT,
            timestamp  TEXT DEFAULT (datetime('now'))
        )
    """)
    # 트리거들
    cursor.executescript("""
        CREATE TRIGGER log_insert AFTER INSERT ON items
        BEGIN
            INSERT INTO change_log(operation, table_name, row_id, new_name)
            VALUES('INSERT','items',NEW.id,NEW.name);
        END;
        CREATE TRIGGER log_update AFTER UPDATE ON items
        BEGIN
            INSERT INTO change_log(operation, table_name, row_id, old_name, new_name)
            VALUES('UPDATE','items',NEW.id,OLD.name,NEW.name);
        END;
        CREATE TRIGGER log_delete AFTER DELETE ON items
        BEGIN
            INSERT INTO change_log(operation, table_name, row_id, old_name)
            VALUES('DELETE','items',OLD.id,OLD.name);
        END;
    """)
    conn.commit()
    return conn

def simulate_changes(conn):
    """무작위로 아이템을 삽입·수정·삭제"""
    names = ["Alpha","Beta","Gamma","Delta","Epsilon"]
    cur = conn.cursor()
    while True:
        op = choice(["INSERT","UPDATE","DELETE"])
        if op == "INSERT":
            name = choice(names)
            cur.execute("INSERT INTO items(name) VALUES(?)", (name,))
        elif op == "UPDATE":
            # 1~max_id 사이 랜덤 선택
            cur.execute("SELECT id FROM items")
            ids = [r[0] for r in cur.fetchall()]
            if ids:
                rid = choice(ids)
                new_name = choice(names)
                cur.execute("UPDATE items SET name=? WHERE id=?", (new_name, rid))
        else:  # DELETE
            cur.execute("SELECT id FROM items")
            ids = [r[0] for r in cur.fetchall()]
            if ids:
                rid = choice(ids)
                cur.execute("DELETE FROM items WHERE id=?", (rid,))
        conn.commit()
        time.sleep(0.5)

def mock_cdc_module(event):
    """CDC 모듈 모의 호출: 실제론 Kafka/HTTP 전송 등으로 대체"""
    print(f"[CDC] {event['timestamp']} | {event['operation']} on {event['table_name']} (row {event['row_id']}) "
          f"old={event.get('old_name')} new={event.get('new_name')}")

def poll_change_log(conn):
    """change_log 테이블의 새 레코드를 폴링해 CDC 모듈 호출"""
    cur = conn.cursor()
    last_id = 0
    while True:
        cur.execute("""
            SELECT change_id, operation, table_name, row_id, old_name, new_name, timestamp
            FROM change_log
            WHERE change_id > ?
            ORDER BY change_id ASC
        """, (last_id,))
        rows = cur.fetchall()
        for rid, op, tbl, row_id, old, new, ts in rows:
            event = {
                "change_id": rid, "operation": op,
                "table_name": tbl, "row_id": row_id,
                "old_name": old, "new_name": new,
                "timestamp": ts
            }
            mock_cdc_module(event)
            last_id = rid
        time.sleep(POLL_INTERVAL)

def main():
    conn = init_db(DB_PATH)

    # 1) 변경 시뮬레이터 쓰레드
    t1 = threading.Thread(target=simulate_changes, args=(conn,), daemon=True)
    t1.start()

    # 2) CDC 폴링 쓰레드
    t2 = threading.Thread(target=poll_change_log, args=(conn,), daemon=True)
    t2.start()

    # 메인 스레드는 무한 대기
    print("=== Test App Running ===\n(CTRL+C to exit)")
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        print("\nExiting...")

if __name__ == "__main__":
    main()
