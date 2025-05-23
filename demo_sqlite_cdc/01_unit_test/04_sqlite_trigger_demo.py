#!/usr/bin/env python3
# sqlite_trigger_demo.py

import sqlite3
import os
from datetime import datetime

DB_PATH = "sqlite_trigger_test.db"

def init_db(path):
    if os.path.exists(path):
        os.remove(path)
    conn = sqlite3.connect(path)
    print("Opened SQLite database at", path)
    return conn

def setup_schema_and_triggers(conn):
    cursor = conn.cursor()
    # Create main table
    cursor.execute("""
        CREATE TABLE items (
            id   INTEGER PRIMARY KEY,
            name TEXT
        )
    """)
    # Create log table
    cursor.execute("""
        CREATE TABLE change_log (
            change_id INTEGER PRIMARY KEY AUTOINCREMENT,
            operation TEXT,
            table_name TEXT,
            row_id INTEGER,
            old_name TEXT,
            new_name TEXT,
            timestamp TEXT DEFAULT (datetime('now'))
        )
    """)
    # Trigger for INSERT
    cursor.execute("""
        CREATE TRIGGER log_insert
        AFTER INSERT ON items
        BEGIN
            INSERT INTO change_log(operation, table_name, row_id, new_name)
            VALUES('INSERT','items',NEW.id,NEW.name);
        END;
    """)
    # Trigger for UPDATE
    cursor.execute("""
        CREATE TRIGGER log_update
        AFTER UPDATE ON items
        BEGIN
            INSERT INTO change_log(operation, table_name, row_id, old_name, new_name)
            VALUES('UPDATE','items',NEW.id,OLD.name,NEW.name);
        END;
    """)
    # Trigger for DELETE
    cursor.execute("""
        CREATE TRIGGER log_delete
        AFTER DELETE ON items
        BEGIN
            INSERT INTO change_log(operation, table_name, row_id, old_name)
            VALUES('DELETE','items',OLD.id,OLD.name);
        END;
    """)
    conn.commit()
    print("Schema and triggers created.")

def generate_changes(conn):
    cursor = conn.cursor()
    # Insert items
    cursor.execute("INSERT INTO items(name) VALUES(?)", ("Alpha",))
    cursor.execute("INSERT INTO items(name) VALUES(?)", ("Beta",))
    # Update item
    cursor.execute("UPDATE items SET name = ? WHERE id = ?", ("Gamma", 1))
    # Delete item
    cursor.execute("DELETE FROM items WHERE id = ?", (2,))
    conn.commit()
    print("Sample changes applied to 'items' table.")

def show_change_log(conn):
    cursor = conn.cursor()
    print("\nChange Log Entries:")
    cursor.execute("SELECT change_id, operation, table_name, row_id, old_name, new_name, timestamp FROM change_log")
    rows = cursor.fetchall()
    for row in rows:
        print(row)

def main():
    conn = init_db(DB_PATH)
    setup_schema_and_triggers(conn)
    generate_changes(conn)
    show_change_log(conn)
    print("\nTrigger demonstration completed.")

if __name__ == "__main__":
    main()

