import os
import asyncio
import logging
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta
from cryptography.fernet import Fernet
from database.crud import token_sankhya as crud
from datetime import datetime, timedelta
from src.utils.log import Log

load_dotenv('keys/.env')
logger = logging.getLogger(__name__)
logging.basicConfig(filename=Log().buscar_path(),
                    encoding='utf-8',
                    format=os.getenv('LOGGER_FORMAT'),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

class Connect(object):

    def __init__(self):
        self.fernet = Fernet(os.getenv('SANKHYA_FERNET_KEY').encode())
        self.url = os.getenv('SANKHYA_URL_TOKEN')
        self.timeout_token = int(os.getenv('SANKHYA_TIMEOUT_TOKEN'))
        self.headers = {
            'token':os.getenv('SANKHYA_TOKEN'),
            'appkey':os.getenv('SANKHYA_APPKEY'),
            'username':os.getenv('SANKHYA_USERNAME'),
            'password':os.getenv('SANKHYA_PASSWORD')
        }

    async def request_token(self) -> dict:
        res = requests.post(
            url=self.url,
            headers=self.headers)
        if res.status_code != 200:
            logger.error("Erro %s ao obter token: %s", res.status_code, res.text)
            print(f"Erro {res.status_code} ao obter token: {res.text}")
            return {}
        if not res.json().get('bearerToken'):
            logger.error("Token de acesso não encontrado na resposta")
            return {}
        return res.json().get("bearerToken")
    
    def save_token(self, token: str) -> bool:
        try:
            encrypted_token = self.fernet.encrypt(token.encode("utf-8")).decode()
            expire_date = datetime.now() + timedelta(minutes=self.timeout_token)            
            crud.criar(token_criptografado=encrypted_token,
                       dh_expiracao_token=expire_date)
            return True
        except Exception as e:
            logger.error("Erro ao salvar token criptografado: %s",e)
            return False
        
    def get_token(self) -> str:
        try:
            token_data = crud.buscar()
            if token_data and token_data.dh_expiracao_token > datetime.now():
                # print("Token salvo ainda é válido")
                decrypted_token = self.fernet.decrypt(token_data.token_criptografado.encode()).decode()
                return f"Bearer {decrypted_token}"
            else:
                # print("Token salvo não é válido ou não existe")
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    coro = self.request_token()
                    new_token = asyncio.ensure_future(coro)               
                    import nest_asyncio
                    nest_asyncio.apply()
                    new_token = loop.run_until_complete(new_token)
                else:                    
                    new_token = loop.run_until_complete(self.request_token())
                if not new_token:
                    logger.error("Erro ao recuperar ou renovar token")
                    print("Erro ao recuperar ou renovar token")
                    return ""
                self.save_token(token=new_token)
                return f"Bearer {new_token}"
        except Exception as e:
            logger.error("Erro ao recuperar ou renovar token: %s",e)
            return ""
