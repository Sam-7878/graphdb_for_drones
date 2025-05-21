import json
import hashlib

# Function to compute a hash for a data structure
def compute_hash(data):
    serialized_data = json.dumps(data, sort_keys=True).encode('utf-8')
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
    print("Step 1: Generated DID Document:")
    print(json.dumps(did_document, indent=4))
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
    print("\nStep 2: Generated Verifiable Credential:")
    print(json.dumps(verifiable_credential, indent=4))
    return verifiable_credential

# Generate and save DID Document and VC
stored_did_document = create_did_document()
verifiable_credential = create_verifiable_credential(stored_did_document["id"])

# Save the DID Document and VC as JSON files
with open("updated_did_document.json", "w") as did_file:
    json.dump(stored_did_document, did_file, indent=4)

with open("updated_verifiable_credential.json", "w") as vc_file:
    json.dump(verifiable_credential, vc_file, indent=4)

print("\nStep 3: Saved DID Document and Verifiable Credential to JSON files.")