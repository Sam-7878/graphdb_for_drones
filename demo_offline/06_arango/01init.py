from arango import ArangoClient

# ArangoDB 클라이언트 초기화
client = ArangoClient()

# Root 사용자로 ArangoDB 시스템 데이터베이스에 연결
sys_db = client.db("_system", username="root", password="dooley")

# 데이터베이스가 없으면 생성
if not sys_db.has_database("test"):
    sys_db.create_database("test")

# 새로 생성한 데이터베이스에 연결
db = client.db("test", username="root", password="dooley")

print("ArangoDB 연결 및 데이터베이스 설정 성공!")

# Vertex 컬렉션 생성
if not db.has_collection("users"):
    users = db.create_collection("users")
else:
    users = db.collection("users")

# Edge 컬렉션 생성
if not db.has_collection("relationships"):
    relationships = db.create_collection("relationships", edge=True)
else:
    relationships = db.collection("relationships")

# Vertex 데이터 삽입
alice = users.insert({"_key": "alice", "name": "Alice"})
bob = users.insert({"_key": "bob", "name": "Bob"})

# Edge 데이터 삽입
relationships.insert({"_from": "users/alice", "_to": "users/bob", "type": "KNOWS"})

query = """
FOR user IN users
    RETURN user
"""
cursor = db.aql.execute(query)
for user in cursor:
    print(user)


