# Bibliotecas
import hashlib
import json
import os
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.exceptions import InvalidSignature

def calcular_hash(arquivo: str) -> bytes:
    """Calcula o hash SHA-256 de um arquivo."""
    sha256 = hashlib.sha256()
    try:
        with open(arquivo, "rb") as f:
            while chunk := f.read(8192):
                sha256.update(chunk)
        return sha256.digest()
    except FileNotFoundError:
        print(f" Arquivo não encontrado: {arquivo}")
        return b""

def carregar_chave_privada(arquivo_chave: str, senha: bytes = None):
    """Carrega uma chave privada em formato PEM."""
    try:
        with open(arquivo_chave, "rb") as f:
            return serialization.load_pem_private_key(f.read(), password=senha)
    except Exception as e:
        print(f" Erro ao carregar a chave privada: {e}")
        raise

def assinar_hash(hash_valor: bytes, chave_privada) -> bytes:
    """Assina o hash usando a chave privada RSA com PSS."""
    return chave_privada.sign(
        hash_valor,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.MAX_LENGTH
        ),
        hashes.SHA256()
    )

def processar_arquivos(arquivos: list[str], chave_privada_path: str, saida_json: str = "assinaturas.json"):
    chave_privada = carregar_chave_privada(chave_privada_path)
    assinaturas = {}

    for arquivo in arquivos:
        print(f" Processando o arquivo: {arquivo}")
        hash_valor = calcular_hash(arquivo)
        if not hash_valor:
            continue
        assinatura = assinar_hash(hash_valor, chave_privada)
        assinaturas[arquivo] = {
            "assinatura": assinatura.hex()
        }

    with open(saida_json, "w", encoding="utf-8") as f:
        json.dump(assinaturas, f, indent=4, ensure_ascii=False)
    print(f" Assinaturas guardadas no arquivo '{saida_json}'.")

# --------- Uso ---------
if __name__ == "__main__":
    arquivos_csv = ["nota_ultimo_colocado.csv", "colocados.csv", "candidatos.csv"]
    arquivo_chave_privada = "private_key.pem"
    processar_arquivos(arquivos_csv, arquivo_chave_privada)
