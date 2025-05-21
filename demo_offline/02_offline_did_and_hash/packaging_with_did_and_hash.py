import json
import base64
import hashlib
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import serialization, hashes

# Function to compute a hash for a data structure
def compute_hash(data):
    serialized_data = json.dumps(data, sort_keys=True).encode('utf-8')
    return hashlib.sha256(serialized_data).hexdigest()

# Step 1: Prepare the DID Document with hash codes
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
    print("Step 1: Generated DID Document:")
    print(json.dumps(did_document, indent=4))
    return did_document

# Step 2: Prepare the Verifiable Credential with hash codes
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
    print("\nStep 2: Generated Verifiable Credential:")
    print(json.dumps(verifiable_credential, indent=4))
    return verifiable_credential

# Step 3: Generate Key Pair
private_key = ec.generate_private_key(ec.SECP256R1())
public_key = private_key.public_key()
public_key_pem = public_key.public_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PublicFormat.SubjectPublicKeyInfo
).decode('utf-8')
print("\nStep 3: Generated Key Pair")
print("Public Key:")
print(public_key_pem)

# Step 4: Create Combined Package
did_document = create_did_document()
verifiable_credential = create_verifiable_credential(did_document["id"])

combined_package = {
    "did_document": did_document,
    "verifiable_credential": verifiable_credential
}

def sign_data(private_key, data):
    serialized_data = json.dumps(data, sort_keys=True).encode('utf-8')
    signature = private_key.sign(
        serialized_data,
        ec.ECDSA(hashes.SHA256())
    )
    return base64.urlsafe_b64encode(signature).decode('utf-8')

combined_package['signature'] = sign_data(private_key, combined_package)
print("\nStep 4: Combined Package Created and Signed:")
print(json.dumps(combined_package, indent=4))

# Step 5: Save Results
with open('packaging_with_did.json', 'w') as f:
    json.dump(combined_package, f, indent=4)

with open('public_key.pem', 'wb') as f:
    f.write(public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    ))

print("\nStep 5: Combined package and public key saved successfully.")