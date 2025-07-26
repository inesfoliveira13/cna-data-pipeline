# Bibliotecas
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import hashes


def gerar_chaves_RSA():
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public_key = private_key.public_key()
    return private_key, public_key

def guardar_chave_privada(private_key, caminho):
    with open(caminho, "wb") as f:
        f.write(
            private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
        )

def guardar_chave_publica(public_key, caminho):
    with open(caminho, "wb") as f:
        f.write(
            public_key.public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo,
            )
        )

# Uso
priv, pub = gerar_chaves_RSA()
guardar_chave_privada(priv, "private_key.pem")
guardar_chave_publica(pub, "public_key.pem")
print("Chaves geradas e guardadas.")

