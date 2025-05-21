import json
import base64
import hashlib
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import serialization, hashes

# Function to compute a hash for a data structure
def compute_hash(data):
    serialized_data = json.dumps(data, sort_keys=True).encode('utf-8')
    return hashlib.sha256(serialized_data).hexdigest()

# Step 1: Prepare Verifiable Credential (VC) with hash codes
def create_verifiable_credential():
    verifiable_credential = {
        "@context": ["https://www.w3.org/2018/credentials/v1"],
        "id": "http://example.edu/credentials/3732",
        "type": ["VerifiableCredential", "MilitaryControlCredential"],
        "issuer": {
            "id": "did:example:issuer12345",
            "name": "Example Army"
        },
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
    print("Step 1: Generated Verifiable Credential:")
    print(json.dumps(verifiable_credential, indent=4))
    return verifiable_credential

# Step 2: Generate Key Pair
private_key = ec.generate_private_key(ec.SECP256R1())
public_key = private_key.public_key()
public_key_pem = public_key.public_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PublicFormat.SubjectPublicKeyInfo
).decode('utf-8')
print("\nStep 2: Generated Key Pair")
print("Public Key:")
print(public_key_pem)

# Step 3: Sign the Verifiable Credential
def sign_vc(private_key, vc):
    serialized_vc = json.dumps(vc, sort_keys=True).encode("utf-8")
    signature = private_key.sign(
        serialized_vc,
        ec.ECDSA(hashes.SHA256())
    )
    return base64.urlsafe_b64encode(signature).decode("utf-8")

verifiable_credential = create_verifiable_credential()
verifiable_credential["proof"] = {
    "type": "EcdsaSecp256r1Signature2019",
    "created": "2025-01-13T21:19:10Z",
    "verificationMethod": {
        "id": "did:example:issuer12345#keys-1",
        "type": "EcdsaSecp256r1",
        "publicKeyPem": public_key_pem,
    },
    "signature": sign_vc(private_key, verifiable_credential)
}

print("\nStep 3: Verifiable Credential with proof:")
print(json.dumps(verifiable_credential, indent=4))

# Step 4: Save Results
with open("self_contained_vc.json", "w") as f:
    json.dump(verifiable_credential, f, indent=4)

with open("public_key.pem", "wb") as f:
    f.write(public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    ))

print("\nStep 4: Verifiable Credential and Public Key saved successfully.")