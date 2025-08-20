import os
import json
import asyncio
import logging
import requests
from datetime import datetime,timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib.parse import urlparse, parse_qs
from cryptography.fernet import Fernet
from dotenv import load_dotenv
from database.crud import token_olist as crud
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
        self.fernet = Fernet(os.getenv('OLIST_FERNET_KEY'))
        self.auth_url = os.getenv('OLIST_AUTH_URL')
        self.endpoint_token = os.getenv('OLIST_ENDPOINT_TOKEN')
        self.client_id = os.getenv('OLIST_CLIENT_ID')
        self.client_secret = os.getenv('OLIST_CLIENT_SECRET')
        self.redirect_uri = os.getenv('OLIST_REDIRECT_URI')
        self.username = os.getenv('OLIST_USERNAME')
        self.password = os.getenv('OLIST_PASSWORD')
        self.path_token = os.getenv('OLIST_PATH_TOKENS')        
        self.access_token  = ''   
        self.refresh_token = ''   

    async def request_auth_code(self) -> str:
        url = self.auth_url+f'/auth?scope=openid&response_type=code&client_id={self.client_id}&redirect_uri={self.redirect_uri}'
        try:
            driver = webdriver.Firefox()
            driver.get(url)

            login_input = driver.find_element(By.ID, "username")
            next_button = driver.find_element(By.XPATH, "//button[@class='sc-dAlyuH biayZs sc-dAbbOL ddEnAE']")
            login_input.clear()
            login_input.send_keys(self.username)
            next_button.click()
            
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "password")))
            pass_input = driver.find_element(By.ID, "password")
            submit_button = driver.find_element(By.XPATH, "//button[@class='sc-dAlyuH biayZs sc-dAbbOL ddEnAE']")
            pass_input.clear()
            pass_input.send_keys(self.password)
            submit_button.click()
            
            res_url = driver.current_url
            parsed_url = urlparse(res_url)
            auth_code = parse_qs(parsed_url.query).get('code', [''])[0]
            return auth_code
        except Exception as e:
            logger.error("Erro durante a autenticação via navegador: %s", e)
        finally:
            driver.quit()

    async def request_token(self, authorization_code: str = None, refresh_token: str = None) -> dict:
        if not authorization_code and not refresh_token:
            logger.error("authorization_code ou refresh_token não informado")
            return {"erro":"authorization_code ou refresh_token não informado"}
        header = {
            "Content-Type": "application/x-www-form-urlencoded"
        }

        payload = {
            "grant_type": "authorization_code",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "redirect_uri": self.redirect_uri,
            "code": authorization_code
        } if authorization_code else {
            "grant_type": "refresh_token",
            "client_id": self.client_id,
            "client_secret": self.client_secret,            
            "refresh_token": refresh_token
        }

        try:
            res = requests.post(
                url=self.auth_url + self.endpoint_token,
                headers=header,
                data=payload
            )
            if res.status_code == 200:
                return res.json()
            else:
                error = res.json().get("error_description", "Erro desconhecido")
                logger.error("Erro ao obter token: %s",error)
                return {"erro":error}
        except requests.exceptions.RequestException as e:
            logger.error("Erro de conexão: %s",e)
            return {"erro":e}

    def save_token(self, token_data: dict) -> bool:
        try:
            access_token = json.dumps(token_data['access_token']).encode("utf-8")
            refresh_token = json.dumps(token_data['refresh_token']).encode("utf-8")
            id_token = json.dumps(token_data['id_token']).encode("utf-8")
            encrypted_access_token = self.fernet.encrypt(access_token).decode()
            encrypted_refresh_token = self.fernet.encrypt(refresh_token).decode()
            encrypted_id_token = self.fernet.encrypt(id_token).decode()
            expire_date = datetime.now()+timedelta(0,token_data['expires_in'])
            expire_date_refresh = datetime.now()+timedelta(0,token_data['refresh_expires_in'])
            ack = crud.criar(token_criptografado=encrypted_access_token,
                             dh_expiracao_token=expire_date,
                             refresh_token_criptografado=encrypted_refresh_token,
                             dh_expiracao_refresh_token=expire_date_refresh,
                             id_token_criptografado=encrypted_id_token)
            if not ack:
                logger.error("Erro ao salvar token criptografado")
                return False            
            return True
        except Exception as e:
            logger.error("Erro ao salvar token criptografado: %s",e)
            return False
        
    def get_token(self) -> str:
        try:
            token_data = crud.buscar()
            if token_data:
                if token_data.dh_expiracao_token > datetime.now():
                    token_descriptografado = self.fernet.decrypt(token_data.token_criptografado.encode()).decode()
                    return json.loads(token_descriptografado)
                elif token_data.dh_expiracao_refresh_token > datetime.now():
                    refresh_token_descriptografado = self.fernet.decrypt(token_data.refresh_token_criptografado.encode()).decode()
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        coro = self.request_token(refresh_token=json.loads(refresh_token_descriptografado))
                        new_token = asyncio.ensure_future(coro)
                        import nest_asyncio
                        nest_asyncio.apply()
                        new_token = loop.run_until_complete(new_token)     
                    else:
                        new_token = loop.run_until_complete(self.request_token(refresh_token=json.loads(refresh_token_descriptografado)))                
                    if new_token.get("erro"):
                        logger.error("Retorno do token de acesso invalido.")
                        return ''               
                    else:
                        self.save_token(new_token)                
                        logger.info("Token de acesso atualizado.")
                        return new_token["access_token"]
                else:
                    loop = asyncio.get_event_loop()
                    if loop.is_running():
                        coro = self.login()
                        new_token = asyncio.ensure_future(coro)
                        import nest_asyncio
                        nest_asyncio.apply()
                        new_token = loop.run_until_complete(new_token)
                        return new_token            
                    else:
                        new_token = loop.run_until_complete(self.login())
                        return new_token
            else:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    coro = self.login()
                    new_token = asyncio.ensure_future(coro)
                    import nest_asyncio
                    nest_asyncio.apply()
                    new_token = loop.run_until_complete(new_token)
                    return new_token            
                else:
                    new_token = loop.run_until_complete(self.login())
                    return new_token                
        except Exception as e:
            logger.error("Erro ao recuperar ou renovar token: %s",e)
            return ''

    async def login(self) -> str:
        try:
            authcode = await self.request_auth_code()
            token = await self.request_token(authorization_code=authcode)
            self.save_token(token)
            logger.info("Token de acesso recuperado via login.")
            return token["access_token"]
        except Exception as e:
            logger.error("Erro no login: %s",e)            
            return ''

