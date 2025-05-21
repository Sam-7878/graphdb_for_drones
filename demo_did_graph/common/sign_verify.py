# sign_verify.py

from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.hazmat.primitives import serialization

def load_private_key(filename):
    print("Loading private key from:", filename)
    with open(filename, 'rb') as f:
        return serialization.load_pem_private_key(f.read(), password=None)

def load_public_key(filename):
    with open(filename, 'rb') as f:
        return serialization.load_pem_public_key(f.read())

def sign_data(private_key, data):
    # 데이터(payload)가 문자열(str)이든 바이트(bytes)이든
    # 모두 처리할 수 있도록 수정   
    if isinstance(data, bytes):
        msg = data
    else:
        msg = data.encode('utf-8')
    signature = private_key.sign(msg)
    return signature


def verify_signature(public_key, signature: bytes, data: str) -> bool:
    try:
        public_key.verify(signature, data.encode('utf-8'))
        return True
    except Exception:
        return False

if __name__ == "__main__":
    # Example usage
    commander_private = load_private_key("common/keys/commander_private.pem")
    commander_public = load_public_key("common/keys/commander_public.pem")

    message = "Test Mission Data"
    signature = sign_data(commander_private, message)

    result = verify_signature(commander_public, signature, message)
    print("Signature valid:", result)
