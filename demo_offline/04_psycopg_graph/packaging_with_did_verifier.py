import json
import base64
import hashlib
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.serialization import load_pem_public_key

# Function to compute a hash for a data structure
def compute_hash(data):
    serialized_data = json.dumps(data, sort_keys=True).encode('utf-8')
    return hashlib.sha256(serialized_data).hexdigest()

# Step 1: Load the Combined Package
with open('packaging_with_did.json', 'r') as f:
    received_package = json.load(f)

print("\nStep 1: Loaded Combined Package:")
print(json.dumps(received_package, indent=4))

# Step 2: Verify Hashes for DID Document and Verifiable Credential
did_document = received_package["did_document"]
verifiable_credential = received_package["verifiable_credential"]

print("\nStep 2: Verifying Hashes...")
if did_document["hash"] != compute_hash(did_document):
    print("DID Document hash verification failed!")
else:
    print("DID Document hash verification succeeded!")

if verifiable_credential["hash"] != compute_hash(verifiable_credential):
    print("Verifiable Credential hash verification failed!")
else:
    print("Verifiable Credential hash verification succeeded!")

# Step 3: Extract Proof and Public Key
proof = received_package.get("signature")
if not proof:
    print("\nStep 3: Signature missing in the package!")
else:
    print("\nStep 3: Signature Found:")
    print(proof)

# Step 4: Load the Public Key
with open('public_key.pem', 'rb') as f:
    public_key = load_pem_public_key(f.read())
    print("\nStep 4: Loaded Public Key:")
    print(public_key)

# Step 5: Verify the Signature
def verify_signature(public_key, package, signature):
    serialized_data = json.dumps(
        {key: value for key, value in package.items() if key != "signature"},
        sort_keys=True
    ).encode('utf-8')
    try:
        public_key.verify(
            base64.urlsafe_b64decode(signature),
            serialized_data,
            ec.ECDSA(hashes.SHA256())
        )
        return True
    except Exception as e:
        print("Signature verification failed:", e)
        return False

if proof and verify_signature(public_key, received_package, proof):
    print("\nStep 5: Signature verification succeeded!")
else:
    print("\nStep 5: Signature verification failed!")
