import re
import requests
import os
import logging
from dotenv import load_dotenv
from src.utils.log import Log

load_dotenv('keys/.env')
logger = logging.getLogger(__name__)
logging.basicConfig(filename=Log().buscar_path(),
                    encoding='utf-8',
                    format=os.getenv('LOGGER_FORMAT'),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)
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