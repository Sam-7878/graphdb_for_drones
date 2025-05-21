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

# Step 1: Load the Verifiable Credential
with open("self_contained_vc.json", "r") as f:
    verifiable_credential = json.load(f)

print("\nStep 1: Loaded Verifiable Credential:")
print(json.dumps(verifiable_credential, indent=4))

# Step 2: Verify the Hash
computed_hash = compute_hash({key: value for key, value in verifiable_credential.items() if key != "proof"})
print("\nStep 2: Verifying Hash...")
if verifiable_credential["hash"] != computed_hash:
    print("Hash verification failed!")
else:
    print("Hash verification succeeded!")

# Step 3: Extract Proof and Public Key
proof = verifiable_credential["proof"]
signature = base64.urlsafe_b64decode(proof["signature"])
public_key_pem = proof["verificationMethod"]["publicKeyPem"]

print("\nStep 3: Extracted Proof and Public Key:")
print("Public Key (PEM):")
print(public_key_pem)

# Step 4: Load Public Key
public_key = load_pem_public_key(public_key_pem.encode("utf-8"))
print("\nStep 4: Loaded Public Key Successfully")

# Step 5: Verify the Signature
def verify_signature(public_key, vc, signature):
    vc_copy = {key: value for key, value in vc.items() if key != "proof"}
    serialized_vc = json.dumps(vc_copy, sort_keys=True).encode("utf-8")
    try:
        public_key.verify(
            signature,
            serialized_vc,
            ec.ECDSA(hashes.SHA256())
        )
        return True
    except Exception as e:
        print("Signature verification failed:", e)
        return False

print("\nStep 5: Verifying Signature...")
if verify_signature(public_key, verifiable_credential, signature):
    print("Signature verification succeeded!")
else:
    print("Signature verification failed!")
