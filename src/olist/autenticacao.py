import os
import json
import logging
import requests
from dotenv   import load_dotenv
from datetime import datetime,timedelta,timezone

from selenium                      import webdriver
from selenium.webdriver.common.by  import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support    import expected_conditions as EC
from urllib.parse                  import urlparse, parse_qs

from database.crud               import olist as crud
from src.utils.log               import Log
from src.utils.decorador.empresa import ensure_dados_empresa

load_dotenv('keys/.env')
logger = logging.getLogger(__name__)
logging.basicConfig(filename=Log().buscar_path(),
                    encoding='utf-8',
                    format=os.getenv('LOGGER_FORMAT'),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

class Autenticacao:

    def __init__(self, codemp:int):
        self.codemp = codemp
        self.auth_url = os.getenv('OLIST_AUTH_URL')
        self.endpoint_token = os.getenv('OLIST_ENDPOINT_TOKEN')
        self.redirect_uri = os.getenv('OLIST_REDIRECT_URI')
        self.path_token = os.getenv('OLIST_PATH_TOKENS')
        self.dados_empresa = None
        
    @ensure_dados_empresa
    async def solicitar_auth_code(self) -> str:
        url = self.auth_url+f'/auth?scope=openid&response_type=code&client_id={self.dados_empresa.get('client_id')}&redirect_uri={self.redirect_uri}'
        try:
            driver = webdriver.Firefox()
            driver.get(url)

            login_input = driver.find_element(By.ID, "username")
            next_button = driver.find_element(By.XPATH, "//button[@class='sc-dAlyuH biayZs sc-dAbbOL ddEnAE']")
            login_input.clear()
            login_input.send_keys(self.dados_empresa.get('olist_admin_email'))
            next_button.click()
            
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "password")))
            pass_input = driver.find_element(By.ID, "password")
            submit_button = driver.find_element(By.XPATH, "//button[@class='sc-dAlyuH biayZs sc-dAbbOL ddEnAE']")
            pass_input.clear()
            pass_input.send_keys(self.dados_empresa.get('olist_admin_senha'))
            submit_button.click()
            
            res_url = driver.current_url
            parsed_url = urlparse(res_url)
            auth_code = parse_qs(parsed_url.query).get('code', [''])[0]
            return auth_code
        except Exception as e:
            logger.error("Erro durante a autenticação via navegador: %s", e)
        finally:
            driver.quit()

    @ensure_dados_empresa
    async def solicitar_token(self,authorization_code:str) -> dict:
        
        header = {
            "Content-Type": "application/x-www-form-urlencoded"
        }

        payload = {
            "grant_type": "authorization_code",
            "client_id": self.dados_empresa.get('client_id'),
            "client_secret": self.dados_empresa.get('client_secret'),
            "redirect_uri": self.redirect_uri,
            "code": authorization_code
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
                return False
        except requests.exceptions.RequestException as e:
            logger.error("Erro de conexão: %s",e)
            return False

    @ensure_dados_empresa
    async def solicitar_atualizacao_token(self,refresh_token:str) -> dict:

        header = {
            "Content-Type": "application/x-www-form-urlencoded"
        }

        payload = {
            "grant_type": "refresh_token",
            "client_id": self.dados_empresa.get('client_id'),
            "client_secret": self.dados_empresa.get('client_secret'),            
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
                return False
        except requests.exceptions.RequestException as e:
            logger.error("Erro de conexão: %s",e)
            return False
    
    @ensure_dados_empresa
    async def salvar_token(self, dados_token: dict) -> bool:
        try:
            access_token = json.dumps(dados_token['access_token']).encode("utf-8")
            refresh_token = json.dumps(dados_token['refresh_token']).encode("utf-8")
            id_token = json.dumps(dados_token['id_token']).encode("utf-8")
            expire_date = datetime.now()+timedelta(0,dados_token['expires_in'])
            expire_date_refresh = datetime.now()+timedelta(0,dados_token['refresh_expires_in'])
        except Exception as e:
            logger.error("Erro ao formatar dados do token: %s",e)
            return False
        
        ack = await crud.criar(empresa_id=self.dados_empresa.get('id'),
                                     token=access_token,
                                     dh_expiracao_token=expire_date,
                                     refresh_token=refresh_token,
                                     dh_expiracao_refresh_token=expire_date_refresh,
                                     id_token=id_token)
        
        if not ack:
            logger.error("Erro ao salvar token")
            return False
        
        return True

    @ensure_dados_empresa        
    async def atualizar_token(self, refresh_token:str) -> str:
        
        novo_token = await self.solicitar_atualizacao_token(refresh_token=refresh_token)
        if not novo_token:
            return False
        
        ack = await self.salvar_token(novo_token)
        if not ack:
            return False
        
        return novo_token.get('access_token')        
         
    @ensure_dados_empresa        
    async def buscar_token_salvo(self) -> str:
        
        # Busca o token mais recente na base
        dados_token = await crud.buscar(self.dados_empresa.get('id'))

        if not dados_token:
            logger.error(f"Token não encontrado para a empresa {self.codemp}")
            print(f"Token não encontrado para a empresa {self.codemp}")
            return None

        if dados_token.get('dh_expiracao_token') > datetime.now(timezone.utc):            
            return dados_token.get('token')
        
        if dados_token.get('dh_expiracao_refresh_token') > datetime.now(timezone.utc):
            return [dados_token.get('refresh_token')]

        if dados_token.get('dh_expiracao_refresh_token') < datetime.now(timezone.utc):
            logger.warning(f"Refresh token expirado para a empresa {self.codemp}")            
            return None     

    @ensure_dados_empresa
    async def primeiro_login(self) -> str:
        
        authcode = await self.solicitar_auth_code()
        if not authcode:
            return ''
        
        token = await self.solicitar_token(authorization_code=authcode)
        if not token:
            return ''
        
        ack = await self.salvar_token(token)
        if not ack:
            return ''
        
        return token.get('access_token')

    @ensure_dados_empresa
    async def autenticar(self) -> str:
        try:
            token = await self.buscar_token_salvo()
            if isinstance(token,str):
                # Token válido salvo na base
                return token
            
            if isinstance(token,list):
                # Token expirado. Solicita novo token
                novo_token = await self.atualizar_token(refresh_token=token[0])
                return novo_token

            if not token:
                # Token não existe ou expirou o refresh token
                token_login = await self.primeiro_login()
                return token_login
        except Exception as e:
            logger.error("Erro na autenticacao: %s",e)
            return ''



