import sqlite3
import os
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa, padding as asym_padding
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

# Generate RSA key pair (server/drone could load from secure storage in real use)
private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
public_key = private_key.public_key()

# Initialize SQLite database
db_path = 'secure_vc.db'
conn = sqlite3.connect(db_path)
cur = conn.cursor()
cur.execute('''
CREATE TABLE IF NOT EXISTS encrypted_vc (
    id TEXT PRIMARY KEY,
    enc_key BLOB,
    nonce BLOB,
    ciphertext BLOB,
    tag BLOB,
    signature BLOB
)
''')
conn.commit()

def encrypt_and_sign(vcid, data: bytes):
    # 1. Generate AES key and encrypt data
    aes_key = AESGCM.generate_key(bit_length=256)
    aesgcm = AESGCM(aes_key)
    nonce = os.urandom(12)
    ciphertext_with_tag = aesgcm.encrypt(nonce, data, None)
    # Separate ciphertext and tag (last 16 bytes)
    ciphertext, tag = ciphertext_with_tag[:-16], ciphertext_with_tag[-16:]
    
    # 2. Encrypt AES key with RSA-OAEP
    enc_key = public_key.encrypt(
        aes_key,
        asym_padding.OAEP(
            mgf=asym_padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None
        )
    )
    
    # 3. Sign the package (nonce + enc_key + ciphertext + tag)
    signer_data = nonce + enc_key + ciphertext + tag
    signature = private_key.sign(
        signer_data,
        asym_padding.PSS(
            mgf=asym_padding.MGF1(hashes.SHA256()),
            salt_length=asym_padding.PSS.MAX_LENGTH
        ),
        hashes.SHA256()
    )
    
    # 4. Store in SQLite
    cur.execute('''
        INSERT OR REPLACE INTO encrypted_vc (id, enc_key, nonce, ciphertext, tag, signature)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (vcid, enc_key, nonce, ciphertext, tag, signature))
    conn.commit()

def decrypt_and_verify(vcid):
    # 1. Retrieve package from SQLite
    cur.execute('SELECT enc_key, nonce, ciphertext, tag, signature FROM encrypted_vc WHERE id = ?', (vcid,))
    row = cur.fetchone()
    if not row:
        raise ValueError("VC ID not found")
    enc_key, nonce, ciphertext, tag, signature = row
    
    # 2. Verify signature
    verifier_data = nonce + enc_key + ciphertext + tag
    public_key.verify(
        signature,
        verifier_data,
        asym_padding.PSS(
            mgf=asym_padding.MGF1(hashes.SHA256()),
            salt_length=asym_padding.PSS.MAX_LENGTH
        ),
        hashes.SHA256()
    )
    
    # 3. Decrypt AES key with RSA-OAEP
    aes_key = private_key.decrypt(
        enc_key,
        asym_padding.OAEP(
            mgf=asym_padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None
        )
    )
    aesgcm = AESGCM(aes_key)
    
    # 4. Decrypt ciphertext and verify tag
    plaintext = aesgcm.decrypt(nonce, ciphertext + tag, None)
    return plaintext


if __name__ == '__main__':
    # Demonstration
    vc_document = b'{"@context":"https://www.w3.org/2018/credentials/v1","id":"vc1","issuer":"did:example:123","credentialSubject":{"id":"did:example:456"}}'
    encrypt_and_sign("vc1", vc_document)
    retrieved = decrypt_and_verify("vc1")
    print("Decrypted VC Document:", retrieved.decode())

    # Close connection
    conn.close()
