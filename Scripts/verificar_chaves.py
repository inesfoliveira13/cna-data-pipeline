# Bibliotecas
import hashlib
import json
from cryptography.hazmat.primitives.asymmetric import padding
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

def carregar_chave_publica(caminho_chave: str):
    """Carrega uma chave pública de um arquivo PEM."""
    try:
        with open(caminho_chave, "rb") as f:
            return serialization.load_pem_public_key(f.read())
    except Exception as e:
        print(f" Erro ao carregar a chave pública: {e}")
        raise

def verificar_assinatura(hash_valor: bytes, assinatura_hex: str, chave_publica) -> bool:
    """Verifica a assinatura digital de um hash com a chave pública."""
    try:
        chave_publica.verify(
            bytes.fromhex(assinatura_hex),
            hash_valor,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH
            ),
            hashes.SHA256()
        )
        return True
    except InvalidSignature:
        return False
    except Exception as e:
        print(f" Erro ao verificar a assinatura: {e}")
        return False

def verificar_arquivos(arquivos: list[str], chave_publica_path: str, assinaturas_path: str):
    chave_publica = carregar_chave_publica(chave_publica_path)

    try:
        with open(assinaturas_path, "r", encoding="utf-8") as f:
            assinaturas = json.load(f)
    except Exception as e:
        print(f" Erro ao carregar arquivo de assinaturas: {e}")
        return

    for arquivo in arquivos:
        print(f"\n Verificando: {arquivo}")
        if arquivo not in assinaturas:
            print(" Assinatura não encontrada.")
            continue

        hash_valor = calcular_hash(arquivo)
        if not hash_valor:
            continue

        assinatura = assinaturas[arquivo].get("assinatura")
        if not assinatura:
            print(" Assinatura ausente ou mal formatada.")
            continue

        if verificar_assinatura(hash_valor, assinatura, chave_publica):
            print(" Assinatura válida.")
        else:
            print(" Assinatura inválida.")

# --------- Uso ---------
if __name__ == "__main__":
    arquivos_csv = ["nota_ultimo_colocado.csv", "colocados.csv", "candidatos.csv"]
    chave_publica = "public_key.pem"
    json_assinaturas = "assinaturas.json"
    verificar_arquivos(arquivos_csv, chave_publica, json_assinaturas)
