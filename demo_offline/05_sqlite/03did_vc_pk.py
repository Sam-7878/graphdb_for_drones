from pysqlcipher3 import dbapi2 as sqlite

import hashlib
from datetime import datetime

#######################################################
# DID Document 데이터 삽입
# DID Document 삽입
def insert_did(did, public_key, service_endpoint=None):
    conn = sqlite.connect("../wallet.db")
    cursor = conn.cursor()
    # 암호화 키 설정
    cursor.execute("PRAGMA key = 'securepassword'")  # 데이터베이스 암호화 키 설정

    try:
        cursor.execute("""
        INSERT INTO did_document (did, public_key, service_endpoint)
        VALUES (?, ?, ?)
        """, (did, public_key, service_endpoint))
        conn.commit()
        print(f"DID {did}이(가) 성공적으로 추가되었습니다.")
    except sqlite.IntegrityError as e:
        print(f"오류: {e}")
    finally:
        conn.close()

# DID 생성 및 삽입
def create_did():
    # DID와 Public Key 생성
    did = f"did:example:{hashlib.sha256(str(datetime.now()).encode()).hexdigest()[:10]}"
    public_key = hashlib.sha256(did.encode()).hexdigest()
    insert_did(did, public_key)
    return did, public_key

#######################################################
# Verifiable Credential 삽입
def insert_vc(vc_id, issuer, holder, credential_data, signature):
    conn = sqlite.connect("../wallet.db")
    cursor = conn.cursor()
    # 암호화 키 설정
    cursor.execute("PRAGMA key = 'securepassword'")  # 데이터베이스 암호화 키 설정

    try:
        cursor.execute("""
        INSERT INTO verifiable_credential (vc_id, issuer, holder, credential_data, signature)
        VALUES (?, ?, ?, ?, ?)
        """, (vc_id, issuer, holder, credential_data, signature))
        conn.commit()
        print(f"VC {vc_id}이(가) 성공적으로 추가되었습니다.")
    except sqlite.IntegrityError as e:
        print(f"오류: {e}")
    finally:
        conn.close()

# VC 생성 및 삽입
def create_vc(issuer, holder, credential_data):
    vc_id = hashlib.sha256((issuer + holder + credential_data).encode()).hexdigest()
    signature = hashlib.sha256(vc_id.encode()).hexdigest()
    insert_vc(vc_id, issuer, holder, credential_data, signature)
    return vc_id, signature


#######################################################
# Private Key 삽입
def insert_private_key(did, private_key):
    conn = sqlite.connect("../wallet.db")
    cursor = conn.cursor()
    # 암호화 키 설정
    cursor.execute("PRAGMA key = 'securepassword'")  # 데이터베이스 암호화 키 설정

    try:
        cursor.execute("""
        INSERT INTO private_keys (did, private_key)
        VALUES (?, ?)
        """, (did, private_key))
        conn.commit()
        print(f"Private Key가 DID {did}에 성공적으로 추가되었습니다.")
    except sqlite.IntegrityError as e:
        print(f"오류: {e}")
    finally:
        conn.close()

# Private Key 생성 및 삽입
def create_private_key(did):
    private_key = hashlib.sha256(did.encode()).hexdigest()
    insert_private_key(did, private_key)
    return private_key

#######################################################
## 데이터 조회
# DID 조회
def get_did(did):
    conn = sqlite.connect("../wallet.db")
    cursor = conn.cursor()
    # 암호화 키 설정
    cursor.execute("PRAGMA key = 'securepassword'")  # 데이터베이스 암호화 키 설정

    cursor.execute("SELECT * FROM did_document WHERE did = ?", (did,))
    result = cursor.fetchone()
    conn.close()
    return result

# VC 조회
def get_vc(vc_id):
    conn = sqlite.connect("../wallet.db")
    cursor = conn.cursor()
    # 암호화 키 설정
    cursor.execute("PRAGMA key = 'securepassword'")  # 데이터베이스 암호화 키 설정

    cursor.execute("SELECT * FROM verifiable_credential WHERE vc_id = ?", (vc_id,))
    result = cursor.fetchone()
    conn.close()
    return result

# Private Key 조회
def get_private_key(did):
    conn = sqlite.connect("../wallet.db")
    cursor = conn.cursor()
    # 암호화 키 설정
    cursor.execute("PRAGMA key = 'securepassword'")  # 데이터베이스 암호화 키 설정

    cursor.execute("SELECT private_key FROM private_keys WHERE did = ?", (did,))
    result = cursor.fetchone()
    conn.close()
    return result


#######################################################
## 실행 예시
if __name__ == "__main__":
    # 데이터베이스 초기화
    #init_db()

    # DID 생성 및 저장
    did, public_key = create_did()

    # Private Key 생성 및 저장
    private_key = create_private_key(did)

    # Verifiable Credential 생성 및 저장
    issuer = "did:example:issuer123"
    holder = did
    credential_data = '{"name": "Alice", "degree": "BSc Computer Science"}'
    vc_id, signature = create_vc(issuer, holder, credential_data)

    # 데이터 조회
    print("DID 조회:", get_did(did))
    print("VC 조회:", get_vc(vc_id))
    print("Private Key 조회:", get_private_key(did))