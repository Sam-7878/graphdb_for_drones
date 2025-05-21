# generate_keys.py

from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

def generate_key_pair():
    private_key = ed25519.Ed25519PrivateKey.generate()
    public_key = private_key.public_key()
    return private_key, public_key

def save_private_key(private_key, filename):
    pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    with open(filename, 'wb') as f:
        f.write(pem)

def save_public_key(public_key, filename):
    pem = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    )
    with open(filename, 'wb') as f:
        f.write(pem)

def load_private_key(filename):
    with open(filename, 'rb') as f:
        return serialization.load_pem_private_key(f.read(), password=None)

def load_public_key(filename):
    with open(filename, 'rb') as f:
        return serialization.load_pem_public_key(f.read())

if __name__ == "__main__":
    commander_private, commander_public = generate_key_pair()
    drone_private, drone_public = generate_key_pair()
    verifier_private, verifier_public = generate_key_pair()

    save_private_key(commander_private, "common/keys/commander_private.pem")
    save_public_key(commander_public, "common/keys/commander_public.pem")

    save_private_key(drone_private, "common/keys/drone_private.pem")
    save_public_key(drone_public, "common/keys/drone_public.pem")

    save_private_key(verifier_private, "common/keys/verifier_private.pem")
    save_public_key(verifier_public, "common/keys/verifier_public.pem")

    print("Keys generated and saved successfully.")
