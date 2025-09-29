import os
from cryptography.fernet import Fernet
from src.utils.load_env import load_env
load_env()

class Criptografia:
    def __init__(self):
        """
        Inicializa a classe com uma chave Fernet.
        """
        self.path = os.getenv('PATH_FERNET_KEY')
        chave = self.ler_key()
        if not chave:
            chave = Fernet.generate_key()
            self.chave = chave
            self.salvar_key()
        self.chave = chave
        self.fernet = Fernet(self.chave)

    def get_chave(self) -> bytes:
        """
        Retorna a chave usada na criptografia.
        """
        return self.chave

    def criptografar(self, mensagem: str) -> bytes:
        """
        Criptografa uma string e retorna em bytes.
        """
        if isinstance(mensagem,bytes):
            return self.fernet.encrypt(mensagem)
        else:
            return self.fernet.encrypt(mensagem.encode())

    def descriptografar(self, mensagem: bytes) -> str:
        """
        Descriptografa a mensagem e retorna como string.
        """
        return self.fernet.decrypt(mensagem).decode()
    
    def salvar_key(self) -> bool:
        """
        Salva a chave em um arquivo.
        """
        try:
            with open(self.path, "wb") as arquivo:
                arquivo.write(self.chave)
            return True
        except Exception as e:
            print(f"Erro ao salvar a chave: {e}")
            return False
        
    def ler_key(self) -> bytes:
        """
        Lê a chave de um arquivo.
        """
        try:
            with open(self.path, "rb") as arquivo:
                chave = arquivo.read()
            return chave
        except Exception as e:
            print(f"Erro ao ler a chave: {e}")
            return None