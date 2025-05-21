from pysqlcipher3 import dbapi2 as sqlite

# 데이터베이스 초기화
def init_db():
    conn = sqlite.connect("../wallet.db")  # SQLite 데이터베이스 파일
    cursor = conn.cursor()

    # 암호화 키 설정
    cursor.execute("PRAGMA key = 'securepassword'")  # 데이터베이스 암호화 키 설정

    # DID Document 테이블 생성
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS did_document (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        did TEXT UNIQUE NOT NULL,
        public_key TEXT NOT NULL,
        service_endpoint TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # Verifiable Credential 테이블 생성
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS verifiable_credential (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        vc_id TEXT UNIQUE NOT NULL,
        issuer TEXT NOT NULL,
        holder TEXT NOT NULL,
        credential_data TEXT NOT NULL,
        signature TEXT NOT NULL,
        issued_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # Private Key 테이블 생성
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS private_keys (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        did TEXT UNIQUE NOT NULL,
        private_key TEXT NOT NULL
    )
    """)

    conn.commit()
    conn.close()
    print("테이블이 성공적으로 생성되었습니다.")

if __name__ == "__main__":
    init_db()