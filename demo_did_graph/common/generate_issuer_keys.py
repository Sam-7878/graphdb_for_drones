# scripts/generate_issuer_keys.py

from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization
from pathlib import Path

KEY_DIR = Path("common/keys")
KEY_DIR.mkdir(exist_ok=True)

private_key = ed25519.Ed25519PrivateKey.generate()
public_key = private_key.public_key()

# Save private key
with open(KEY_DIR / "issuer_private.pem", "wb") as f:
    f.write(private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    ))

# Save public key
with open(KEY_DIR / "issuer_public.pem", "wb") as f:
    f.write(public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    ))

print("[OK] Issuer keypair saved to 'keys/issuer_private.pem' and 'keys/issuer_public.pem'")
