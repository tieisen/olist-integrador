import os, requests, asyncio
from datetime import datetime, timedelta
from src.utils.decorador import carrega_dados_snk
from database.crud import sankhya as crud
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)
_lock_autenticacao = asyncio.Lock()

class Autenticacao:

    def __init__(self, app_id:int=None):
        self.app_id:int = app_id or int(os.getenv('SANKHYA_APP_ID'))
        self.url:str = os.getenv('SANKHYA_URL_AUTH')
        self.dados_snk:dict = {}

    @carrega_dados_snk
    async def solicitar_token(self) -> dict:
        """ 
        Solicita um novo token. 
            :return dict: dicionário de retorno da API de autenticação.
        """
        retorno:dict={}
        
        try:
            x_token:str = self.dados_snk.get('x_token')
            app_id:int = self.dados_snk.get('app_id')
            
            if not all([x_token, app_id]):
                msg = "xToken ou App Id não encontrado"
                raise ValueError(msg)

            header = {
                'xToken': x_token
            }

            res = requests.post(
                url=self.url+f"/{app_id}",
                headers=header
            )
            
            if res.status_code != 200:
                msg = f"Erro {res.status_code} ao obter token: {res.text}"
                raise ValueError(msg)
                    
            if not res.json().get('token'):
                msg = "Token de acesso não encontrado na resposta"
                raise ValueError(msg)
            
            retorno = res.json()
        except Exception as e:
            logger.error("Erro ao solicitar token: %s",e)
        finally:
            pass
        
        return retorno
    
    @carrega_dados_snk
    async def salvar_token(self,dados_token:dict) -> bool:
        """
        Salva o token no banco de dados.
            :param dados_token: dicionário de retorno da API de autenticação.
            :return bool: status da operação.
        """

        token:str = dados_token.get('token')
        request_date:datetime = datetime.now()
        expire_date:datetime = datetime.strptime(dados_token.get('dhExpiracaoToken',''), "%Y-%m-%dT%H:%M:%S.%f")                
        ack = await crud.atualizar(app_id=self.dados_snk.get('app_id'),
                                   token=token,
                                   dh_solicitacao=request_date,
                                   dh_expiracao_token=expire_date)
        
        if not ack:
            logger.error("Erro ao salvar token criptografado")
            return False
        
        return True

    async def buscar_token_salvo(self) -> str:
        """
        Busca o último token salvo no banco de dados.
            :return str: token descriptografado.
        """
        dados_token = await crud.buscar(app_id=self.app_id)

        if not dados_token:
            logger.error(f"Token não encontrado")
            return None

        if (not dados_token.get('token')) or (not dados_token.get('dh_expiracao_token')):
            logger.error(f"Token não encontrado")
            return None
    
        if dados_token.get('dh_expiracao_token') <= (datetime.now() - timedelta(seconds=60)):
            return None
        
        return dados_token.get('token')

    async def login(self) -> str:
        """
        Executa rotina de autenticação.
            :return str: token descriptografado.        
        """

        token = await self.solicitar_token()
        if not token:
            return ''
        
        ack = await self.salvar_token(token)
        if not ack:
            return ''
        
        return token.get('token')
        
    async def autenticar(self) -> str:
        """
        Busca último token salvo ou executa a rotina de autenticação.
            :return str: token descriptografado.
        """
                
        try:
            token = await self.buscar_token_salvo()
            if token:
                return token

            async with _lock_autenticacao:

                token = await self.buscar_token_salvo()
                if token:
                    return token

                token_login = await self.login()
                return token_login

        except Exception as e:
            logger.error("Erro na autenticacao: %s", e)
            return ''        
