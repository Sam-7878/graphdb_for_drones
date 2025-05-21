import json
import hashlib

# Function to compute a hash for a data structure
def compute_hash(data):
    serialized_data = json.dumps(data, sort_keys=True).encode('utf-8')
    return hashlib.sha256(serialized_data).hexdigest()

# Step 1: Issuer verification function
# Verify the Issuer
def verify_issuer(vc_path, issuer_path):
    with open(vc_path, "r") as vc_file:
        vc = json.load(vc_file)

    with open(issuer_path, "r") as issuer_file:
        issuer = json.load(issuer_file)

    print("\nStep 1: Loaded Verifiable Credential:")
    print(json.dumps(vc, indent=4))

    print("\nStep 2: Loaded Issuer DID Document:")
    print(json.dumps(issuer, indent=4))

    if vc["issuer"] == issuer["id"]:
        print("\nStep 3: Issuer verification succeeded!")
    else:
        print("\nStep 3: Issuer verification failed!")

# Verify the Issuer in a Verifiable Credential
vc_path = "updated_verifiable_credential.json"
issuer_path = "issuer_did_document.json"
verify_issuer(vc_path, issuer_path)