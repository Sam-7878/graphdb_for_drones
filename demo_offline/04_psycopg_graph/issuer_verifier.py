import psycopg
import json
import hashlib

# Function to compute a hash for a data structure
def compute_hash(data):
    serialized_data = json.dumps(data, sort_keys=True).encode("utf-8")
    return hashlib.sha256(serialized_data).hexdigest()

# Function to verify issuer from AgensGraph
def verify_issuer_from_db(vc_hash):
    connection_string = "host=localhost port=5432 dbname=test user=sam"

    try:
        with psycopg.connect(connection_string) as conn:
            with conn.cursor() as cur:
                # Retrieve Verifiable Credential by hash
                cur.execute("SELECT vc FROM verifiable_credentials WHERE hash = %s", (vc_hash,))
                vc_record = cur.fetchone()

                if not vc_record:
                    print("Verifiable Credential not found.")
                    return

                vc = vc_record[0]
                print("Loaded Verifiable Credential:", json.dumps(vc, indent=4))

                # Retrieve DID Document by ID
                issuer_id = vc["issuer"]
                cur.execute("SELECT did FROM did_documents WHERE did->>'id' = %s", (issuer_id,))
                did_record = cur.fetchone()

                if not did_record:
                    print("Issuer DID Document not found.")
                    return

                did_document = did_record[0]
                print("Loaded Issuer DID Document:", json.dumps(did_document, indent=4))

                # Verify if the issuer in the VC matches the DID Document ID
                if vc["issuer"] == did_document["id"]:
                    print("Issuer verification succeeded!")
                else:
                    print("Issuer verification failed!")

    except Exception as e:
        print("Failed to verify data from AgensGraph:", e)

# Example usage
vc_hash_to_verify = "cb34128ad8a80bcf04a0e26bcfae17b5c654a794ecb77d92d7879241b104bb58"  # Replace with actual hash
verify_issuer_from_db(vc_hash_to_verify)