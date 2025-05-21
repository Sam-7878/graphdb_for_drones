import json
import base64
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import serialization

# Step 1: Prepare the DID Document and Verifiable Credential
did_document = {
    "@context": "https://w3id.org/did/v1",
    "id": "did:example:123456789abcdefghi",
    "verificationMethod": [{
        "id": "did:example:123456789abcdefghi#keys-1",
        "type": "Ed25519VerificationKey2018",
        "controller": "did:example:123456789abcdefghi",
        "publicKeyBase58": "2jKdmc..."
    }]
}
print("\nStep 1: Prepared DID Document:")
print(json.dumps(did_document, indent=4))

verifiable_credential = {
    "@context": ["https://www.w3.org/2018/credentials/v1"],
    "id": "http://example.edu/credentials/3732",
    "type": ["VerifiableCredential", "MilitaryControlCredential"],
    "issuer": "did:example:123456789abcdefghi",
    "credentialSubject": {
        "id": "did:example:abcdef1234567",
        "permission": {
            "type": "ControlPermissions",
            "name": "Drone Control Permissions"
        }
    },
    "issuanceDate": "2025-01-13T21:19:10Z"
}
print("\nStep 1: Prepared Verifiable Credential:")
print(json.dumps(verifiable_credential, indent=4))

# Step 2: Generate Key Pair
private_key = ec.generate_private_key(ec.SECP256R1())
public_key = private_key.public_key()
print("\nStep 2: Generated Key Pair:")
print("Public Key (PEM):")
public_key_pem = public_key.public_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PublicFormat.SubjectPublicKeyInfo
)
print(public_key_pem.decode())

# Step 3: Create Combined Package
combined_package = {
    "did_document": did_document,
    "verifiable_credential": verifiable_credential
}
print("\nStep 3: Created Combined Package (Before Signing):")
print(json.dumps(combined_package, indent=4))

# Step 4: Sign the Package
def sign_data(private_key, data):
    serialized_data = json.dumps(data, sort_keys=True).encode('utf-8')
    signature = private_key.sign(
        serialized_data,
        ec.ECDSA(hashes.SHA256())
    )
    return base64.urlsafe_b64encode(signature).decode('utf-8')

combined_package['signature'] = sign_data(private_key, combined_package)
print("\nStep 4: Signed the Package:")
print(json.dumps(combined_package, indent=4))

# Step 5: Save the Results
with open('packaging_with_did.json', 'w') as f:
    json.dump(combined_package, f, indent=4)
print("\nStep 5: Saved Combined Package as 'packaging_with_did.json'")

with open('public_key.pem', 'wb') as f:
    f.write(public_key_pem)
print("Step 5: Saved Public Key as 'public_key.pem'")
