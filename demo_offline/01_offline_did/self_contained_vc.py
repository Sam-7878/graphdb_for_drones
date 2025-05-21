import json
import base64
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import serialization

# Step 1: Prepare Verifiable Credential (VC)
verifiable_credential = {
    "@context": ["https://www.w3.org/2018/credentials/v1"],
    "id": "http://example.edu/credentials/3732",
    "type": ["VerifiableCredential", "MilitaryControlCredential"],
    "issuer": {
        "id": "did:example:123456789abcdefghi",
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

# Step 3: Sign the Verifiable Credential
def sign_vc(private_key, vc):
    serialized_vc = json.dumps(vc, sort_keys=True).encode("utf-8")
    signature = private_key.sign(
        serialized_vc,
        ec.ECDSA(hashes.SHA256())
    )
    return base64.urlsafe_b64encode(signature).decode("utf-8")

verifiable_credential["proof"] = {
    "type": "EcdsaSecp256r1Signature2019",
    "created": "2025-01-13T21:19:10Z",
    "verificationMethod": {
        "id": "did:example:123456789abcdefghi#keys-1",
        "type": "EcdsaSecp256r1",
        "publicKeyPem": public_key_pem.decode("utf-8"),
    },
    "signature": sign_vc(private_key, verifiable_credential)
}
print("\nStep 3: Signed Verifiable Credential with Proof:")
print(json.dumps(verifiable_credential, indent=4))

# Step 4: Save Results
with open("self_contained_vc.json", "w") as f:
    json.dump(verifiable_credential, f, indent=4)
print("\nStep 4: Saved Self-contained Verifiable Credential as 'self_contained_vc.json'")
