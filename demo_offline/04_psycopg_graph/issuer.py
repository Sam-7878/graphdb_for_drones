import psycopg
import json
import hashlib

# Function to compute a hash for a data structure
def compute_hash(data):
    serialized_data = json.dumps(data, sort_keys=True).encode("utf-8")
    return hashlib.sha256(serialized_data).hexdigest()

# Step 1: Create Issuer DID Document
def create_did_document():
    did_document = {
        "@context": "https://w3id.org/did/v1",
        "id": "did:example:issuer12345",
        "verificationMethod": [{
            "id": "did:example:issuer12345#keys-1",
            "type": "Ed25519VerificationKey2018",
            "controller": "did:example:issuer12345",
            "publicKeyBase58": "Base58PublicKeyHere"
        }]
    }
    did_document["hash"] = compute_hash(did_document)
    print("Generated DID Document:", json.dumps(did_document, indent=4))
    return did_document

# Step 2: Create Verifiable Credential (VC)
def create_verifiable_credential(issuer_did):
    verifiable_credential = {
        "@context": ["https://www.w3.org/2018/credentials/v1"],
        "id": "http://example.edu/credentials/3732",
        "type": ["VerifiableCredential", "MilitaryControlCredential"],
        "issuer": issuer_did,
        "credentialSubject": {
            "id": "did:example:abcdef1234567",
            "permission": {
                "type": "ControlPermissions",
                "name": "Drone Control Permissions"
            }
        },
        "issuanceDate": "2025-01-13T21:19:10Z"
    }
    verifiable_credential["hash"] = compute_hash(verifiable_credential)
    print("Generated Verifiable Credential:", json.dumps(verifiable_credential, indent=4))
    return verifiable_credential

# Function to save DID Document and VC to AgensGraph
def save_to_agensgraph(did_document, verifiable_credential):
    connection_string = "host=localhost port=5432 dbname=test user=sam"

    try:
        with psycopg.connect(connection_string) as conn:
            with conn.cursor() as cur:
                # Create tables if not exist
                cur.execute("CREATE TABLE IF NOT EXISTS did_documents (id SERIAL PRIMARY KEY, did JSONB, hash TEXT)")
                cur.execute("CREATE TABLE IF NOT EXISTS verifiable_credentials (id SERIAL PRIMARY KEY, vc JSONB, hash TEXT)")

                # Insert DID Document
                cur.execute(
                    "INSERT INTO did_documents (did, hash) VALUES (%s, %s) RETURNING id", 
                    (json.dumps(did_document), did_document["hash"])
                )
                did_doc_id = cur.fetchone()
                print(f"Inserted DID Document with ID: {did_doc_id}")

                # Insert Verifiable Credential
                cur.execute(
                    "INSERT INTO verifiable_credentials (vc, hash) VALUES (%s, %s) RETURNING id", 
                    (json.dumps(verifiable_credential), verifiable_credential["hash"])
                )
                vc_id = cur.fetchone()
                print(f"Inserted Verifiable Credential with ID: {vc_id}")

                # Fetch and display all records
                cur.execute("SELECT * FROM did_documents")
                did_docs = cur.fetchall()
                print("DID Documents:", json.dumps(did_docs, indent=4))

                cur.execute("SELECT * FROM verifiable_credentials")
                vcs = cur.fetchall()
                print("Verifiable Credentials:", json.dumps(vcs, indent=4))

                conn.commit()
                print("DID Document and Verifiable Credential saved successfully.")

    except Exception as e:
        print("Failed to save data to AgensGraph:", e)

# Generate DID Document and VC
did_document = create_did_document()
verifiable_credential = create_verifiable_credential(did_document["id"])

# Save them to AgensGraph
save_to_agensgraph(did_document, verifiable_credential)
