import re
import requests
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

class Viacep:

    def __init__(self):
        self.regex = r"\D"

    async def busca_ibge_pelo_cep(self,cep:str) -> dict:
        if not cep:
            return False
        cep = re.sub(self.regex,"",cep)
        if len(cep) != 8:
            logger.error("CEP inválido: %s",cep)
            return False
        res = requests.get(f"https://viacep.com.br/ws/{cep}/json/")
        if res.status_code == 200:            
            return int(res.json().get('ibge'))
        logger.error("CEP %s não encontrado. Erro %s. %s",cep,res.status_code,res.text)
        return False