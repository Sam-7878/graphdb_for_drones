import json
import base64
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.serialization import load_pem_public_key

# Step 1: Load the Self-contained Verifiable Credential
with open("self_contained_vc.json", "r") as f:
    verifiable_credential = json.load(f)
print("\nStep 1: Loaded Self-contained Verifiable Credential:")
print(json.dumps(verifiable_credential, indent=4))

# Step 2: Extract Proof and Public Key
proof = verifiable_credential["proof"]
signature = base64.urlsafe_b64decode(proof["signature"])
public_key_pem = proof["verificationMethod"]["publicKeyPem"]
print("\nStep 2: Extracted Proof and Public Key:")
print("Public Key (PEM):")
print(public_key_pem)

# Step 3: Load Public Key
public_key = load_pem_public_key(public_key_pem.encode("utf-8"))
print("\nStep 3: Loaded Public Key Successfully")

# Step 4: Verify the Signature
def verify_signature(public_key, vc, signature):
    vc_copy = vc.copy()
    del vc_copy["proof"]
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

is_valid = verify_signature(public_key, verifiable_credential, signature)
if is_valid:
    print("\nStep 4: Signature Verification Succeeded!")
else:
    print("\nStep 4: Signature Verification Failed!")
