import json
import base64
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.serialization import load_pem_public_key

# Step 1: Load the Combined Package
with open('packaging_with_did.json', 'r') as f:
    received_package = json.load(f)
print("\nStep 1: Loaded Combined Package:")
print(json.dumps(received_package, indent=4))

# Step 2: Load the Public Key
with open('public_key.pem', 'rb') as f:
    public_key = load_pem_public_key(f.read())
print("\nStep 2: Loaded Public Key:")

# Step 3: Extract the Data and Signature
data_to_verify = {
    "did_document": received_package['did_document'],
    "verifiable_credential": received_package['verifiable_credential']
}
signature = base64.urlsafe_b64decode(received_package['signature'])
print("\nStep 3: Extracted Data to Verify and Signature:")
print("Data to Verify:")
print(json.dumps(data_to_verify, indent=4))
print("Signature (Base64):", received_package['signature'])

# Step 4: Verify the Signature
def verify_signature(public_key, data, signature):
    serialized_data = json.dumps(data, sort_keys=True).encode('utf-8')
    try:
        public_key.verify(
            signature,
            serialized_data,
            ec.ECDSA(hashes.SHA256())
        )
        return True
    except Exception as e:
        print("Signature verification failed:", e)
        return False

is_valid = verify_signature(public_key, data_to_verify, signature)
if is_valid:
    print("\nStep 4: Signature Verification Succeeded!")
else:
    print("\nStep 4: Signature Verification Failed!")
