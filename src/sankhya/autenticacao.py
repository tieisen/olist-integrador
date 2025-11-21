import os
import json
import requests
from datetime import datetime, timedelta
from src.utils.decorador import carrega_dados_empresa, interno
from database.crud import sankhya as crud
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

class Autenticacao:

    def __init__(self, codemp:int=None, empresa_id:int=None):
        self.codemp:int = codemp
        self.empresa_id:int = empresa_id
        self.dados_empresa:dict = None        
        self.headers:dict = None
        self.url:str = os.getenv('SANKHYA_URL_TOKEN')

    @interno
    def formatar_header(self):
        """ Formata o header da requisição. """
        self.headers = {
            'token':self.dados_empresa.get('snk_token'),
            'appkey':self.dados_empresa.get('snk_appkey'),
            'username':self.dados_empresa.get('snk_admin_email'),
            'password':self.dados_empresa.get('snk_admin_senha')
        }

    @interno
    @carrega_dados_empresa
    async def solicitar_token(self) -> dict:
        """ Solicita um novo token. """

        self.formatar_header()

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
        
        return res.json()
    
    @interno
    @carrega_dados_empresa
    async def salvar_token(self,dados_token:dict) -> bool:
        """
        Salva o token no banco de dados.
            :param dados_token: dicionário de retorno da API de autenticação.
        """

        try:
            token = json.dumps(dados_token.get('bearerToken'))
            expire_date = datetime.now() + timedelta(minutes=self.dados_empresa.get('snk_timeout_token_min'))
        except Exception as e:
            logger.error("Erro ao formatar dados do token: %s",e)
            return False
                
        ack = await crud.criar(empresa_id=self.dados_empresa.get('id'),
                               token=token,
                               dh_expiracao_token=expire_date)
        
        if not ack:
            logger.error("Erro ao salvar token criptografado")
            return False
        
        return True

    @interno
    @carrega_dados_empresa
    async def buscar_token_salvo(self) -> str:
        """ Busca o último token salvo no banco de dados. """        
        dados_token = await crud.buscar(empresa_id=self.dados_empresa.get('id'))

        if not dados_token:
            logger.error(f"Token não encontrado para a empresa {self.codemp}")
            print(f"Token não encontrado para a empresa {self.codemp}")
            return None

        if dados_token.get('dh_expiracao_token') > datetime.now():            
            return dados_token.get('token')
        else:            
            return None

    @interno
    @carrega_dados_empresa
    async def login(self) -> str:
        """ Executa rotina de autenticação. """
        token = await self.solicitar_token()
        if not token:
            return ''
        
        ack = await self.salvar_token(token)
        if not ack:
            return ''
        
        return token.get('bearerToken')

    @carrega_dados_empresa
    async def autenticar(self) -> str:
        """ Busca último token salvo ou executa a rotina de autenticação. """
        try:
            token = await self.buscar_token_salvo()
            if token:
                # Token válido salvo na base
                return token
            else:
                # Token não existe ou expirou
                token_login = await self.login()
                return token_login
        except Exception as e:
            logger.error("Erro na autenticacao: %s",e)
            return ''
