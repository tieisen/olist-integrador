import os
import json
import requests
from datetime import datetime,timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib.parse import urlparse, parse_qs
from database.crud import olist as crud
from src.utils.decorador import carrega_dados_empresa
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

class Autenticacao:

    def __init__(
            self,
            codemp:int=None,
            empresa_id:int=None
        ):
        self.codemp = codemp
        self.empresa_id = empresa_id
        self.auth_url = os.getenv('OLIST_AUTH_URL')
        self.endpoint_token = os.getenv('OLIST_ENDPOINT_TOKEN')
        self.redirect_uri = os.getenv('OLIST_REDIRECT_URI')
        self.path_token = os.getenv('OLIST_PATH_TOKENS')
        self.dados_empresa = None
        
    @carrega_dados_empresa
    async def solicitar_auth_code(self) -> str:
        """
        Realiza login com usuário Admin e solicita o código de autorização
            :return str: código de autorização
        """
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

    @carrega_dados_empresa
    async def solicitar_token(self,authorization_code:str) -> dict:
        """
        Solicita token de acesso
            :param authorization_code: código de autorização
            :return dict: dicionário com os dados do token vigente
        """
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

    @carrega_dados_empresa
    async def solicitar_atualizacao_token(self,refresh_token:str) -> dict:
        """
        Solicita novo token de acesso, se expirado
            :param refresh_token: refresh token adquirido na solicitação anterior
            :return dict: dicionário com os dados do novo token vigente
        """        
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
    
    @carrega_dados_empresa
    async def salvar_token(self, dados_token: dict) -> bool:
        """
        Salva o token no banco de dados
            :param dados_token: dicionário com os dados do token
            :return bool: status da operação
        """
        try:
            access_token = dados_token['access_token']
            refresh_token = dados_token['refresh_token']
            id_token = dados_token['id_token']
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

    @carrega_dados_empresa        
    async def atualizar_token(self, refresh_token:str) -> str:
        """
        Atualiza token expirado
            :param refresh_token: refresh token
            :return str: token de acesso vigente
        """
        
        novo_token = await self.solicitar_atualizacao_token(refresh_token=refresh_token)
        if not novo_token:
            return False
        
        ack = await self.salvar_token(novo_token)
        if not ack:
            return False
        
        return novo_token.get('access_token')        
         
    @carrega_dados_empresa        
    async def buscar_token_salvo(self) -> str:
        """
        Busca último token salvo no banco de dados
            :return str: último token de acesso
        """        
        
        # Busca o token mais recente na base
        dados_token = await crud.buscar(self.dados_empresa.get('id'))

        if not dados_token:
            logger.error(f"Token não encontrado para a empresa {self.codemp or self.empresa_id}")
            return None

        if dados_token.get('dh_expiracao_token') > datetime.now():            
            return dados_token.get('token')
        
        if dados_token.get('dh_expiracao_refresh_token') > datetime.now():
            return [dados_token.get('refresh_token')]

        if dados_token.get('dh_expiracao_refresh_token') < datetime.now():            
            return None     

    @carrega_dados_empresa
    async def primeiro_login(self) -> str:
        """
        Realiza o fluxo de primeiro login
            :return str: token de acesso válido
        """         
        
        authcode = await self.solicitar_auth_code()
        if not authcode:
            return ''
        
        token = await self.solicitar_token(authorization_code=authcode)
        if not token:
            return ''
        
        ack = await self.salvar_token(token)
        if not ack:
            return ''
        
        logger.info("Login success")
        return token.get('access_token')

    @carrega_dados_empresa
    async def autenticar(self) -> str:
        """
        Executa rotina de autenticação
            :return str: token de acesso válido
        """         
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



