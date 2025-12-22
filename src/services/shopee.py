import os, time, json, requests, hmac, hashlib
from database.crud import shopee as crud
from datetime import datetime, timedelta
from src.utils.decorador import carrega_dados_shopee
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

HOST_URL = os.getenv('SHOPEE_HOST_URL')
PATH_AUTH = os.getenv('SHOPEE_PATH_AUTH')
PATH_TOKEN = os.getenv('SHOPEE_PATH_TOKEN')
PATH_INCOME_DETAIL = os.getenv('SHOPEE_PATH_INCOME_DETAIL')
REDIRECT_URL = os.getenv('OLIST_REDIRECT_URI')

class Autenticacao:

    def __init__(self,ecommerce_id:int=None,empresa_id:int=None):
        self.ecommerce_id = ecommerce_id
        self.empresa_id = empresa_id
        self.dados_empresa:dict={}
        self.dados_shopee:dict={}
        self.access_token:str=None

    @carrega_dados_shopee
    async def shop_auth(self):
        timest:int = int(time.time())
        partner_id:int = self.dados_shopee.get("partner_id")
        partner_key:str = self.dados_shopee.get("partner_key")
        partner_key_encoded = partner_key.encode()
        base_string = "%s%s%s" % (partner_id, PATH_AUTH, timest)
        base_string_encoded = base_string.encode()
        sign = hmac.new(partner_key_encoded, base_string_encoded, hashlib.sha256).hexdigest()
        ##generate api
        url = HOST_URL + PATH_AUTH + "?partner_id=%s&timestamp=%s&sign=%s&redirect=%s" % (partner_id, timest, sign, REDIRECT_URL)
        print(url)

    @carrega_dados_shopee
    async def get_token_shop_level(self,code:str):
        timest:int = int(time.time())
        partner_id:int = self.dados_shopee.get("partner_id")
        shop_id:int = self.dados_shopee.get("shop_id")
        partner_key:str = self.dados_shopee.get("partner_key")        
        body = {"code": code, "shop_id": shop_id, "partner_id": partner_id}
        base_string = "%s%s%s" % (partner_id, PATH_TOKEN, timest)
        base_string_encoded = base_string.encode()
        partner_key_encoded = partner_key.encode()
        sign = hmac.new(partner_key_encoded, base_string_encoded, hashlib.sha256).hexdigest()
        url = HOST_URL + PATH_TOKEN + "?partner_id=%s&timestamp=%s&sign=%s" % (partner_id, timest, sign)
        headers = {"Content-Type": "application/json"}
        resp = requests.post(url, json=body, headers=headers)
        if resp.ok:
            ret = json.loads(resp.content)
            return ret
        else:
            return False

    @carrega_dados_shopee
    async def refresh_token_shop_level(self,refresh_token:str):
        timest:int = int(time.time())
        partner_id:int = self.dados_shopee.get("partner_id")
        shop_id:int = self.dados_shopee.get("shop_id")
        partner_key:str = self.dados_shopee.get("partner_key")        
        body = {"shop_id": shop_id, "refresh_token": refresh_token,"partner_id":partner_id}
        base_string = "%s%s%s" % (partner_id, PATH_TOKEN, timest)
        base_string_encoded = base_string.encode()
        partner_key_encoded = partner_key.encode()
        sign = hmac.new(partner_key_encoded, base_string_encoded, hashlib.sha256).hexdigest()
        url = HOST_URL + PATH_TOKEN + "?partner_id=%s&timestamp=%s&sign=%s" % (partner_id, timest, sign)
        headers = {"Content-Type": "application/json"}
        resp = requests.post(url, json=body, headers=headers)
        if resp.ok:
            ret = json.loads(resp.content)
            return ret
        else:
            return False

    @carrega_dados_shopee
    async def salvar_token(self, dados_token: dict) -> bool:
        """
        Salva o token no banco de dados
            :param dados_token: dicionário com os dados do token
            :return bool: status da operação
        """
        try:
            access_token = dados_token['access_token']
            refresh_token = dados_token['refresh_token']
            dh_solicitacao = datetime.now()
            expire_date = dh_solicitacao+timedelta(seconds=dados_token['expire_in'])
            expire_date_refresh = dh_solicitacao+timedelta(days=30)
        except Exception as e:
            logger.error("Erro ao formatar dados do token: %s",e)
            return False
        ack = await crud.atualizar(ecommerce_id=self.dados_shopee.get('ecommerce_id'),
                                   access_token = access_token,
                                   dh_solicitacao = dh_solicitacao,
                                   dh_expiracao_token = expire_date,
                                   refresh_token = refresh_token,
                                   dh_expiracao_refresh_token = expire_date_refresh)
        if not ack:
            logger.error("Erro ao salvar token")
            return False
        return True
    
    @carrega_dados_shopee
    async def buscar_token_salvo(self):
        """
        Busca último token salvo no banco de dados
            :return str: último token de acesso
        """ 
        if not self.dados_shopee:
            msg = f"Loja não encontrada no banco de dados para a empresa {self.ecommerce_id or self.empresa_id}"
            print(msg)
            logger.error(msg)
            return None
        elif self.dados_shopee.get('access_token') == '0':
            msg = f"Token não encontrado para a loja da empresa {self.ecommerce_id or self.empresa_id}"
            print(msg)
            logger.error(msg)
            return -1
        elif self.dados_shopee.get('dh_expiracao_token') > datetime.now():            
            return self.dados_shopee.get('access_token')
        elif self.dados_shopee.get('dh_expiracao_refresh_token') > datetime.now():
            return [self.dados_shopee.get('refresh_token')]
        elif self.dados_shopee.get('dh_expiracao_refresh_token') < datetime.now():            
            msg = f"Refresh token expirado para a loja da empresa {self.ecommerce_id or self.empresa_id}"
            print(msg)
            logger.error(msg)
            return -1
        else:
            # print("Nenhuma condição encontrada buscar_token_salvo")
            return None
        
    @carrega_dados_shopee
    async def autenticar(self,code:str=None) -> str:
        """
        Executa rotina de autenticação
            :return str: token de acesso válido
        """         
        try:
            if code:
                new_token = await self.get_token_shop_level(code)
                if new_token:
                    await self.salvar_token(new_token)
                    return new_token['access_token']
            
            # print("Buscando token salvo")
            tnk = await self.buscar_token_salvo()
            # print(f"tnk: {tnk}")
            if not tnk:
                # print("Nenhum token encontrado")
                return False
            elif isinstance(tnk,str):
                # print("Token encontrado")
                return tnk
            elif isinstance(tnk,list):
                # print("Refresh token encontrado")
                new_token = await self.refresh_token_shop_level(tnk[0])
                if new_token:
                    await self.salvar_token(new_token)
                    return new_token['access_token']
            elif tnk == -1:
                # print("Refresh token expirado")
                await self.shop_auth()
                print(f"Acesse o link para autorizar o acesso. Após, execute novamente a função autenticar informando o código de autorização.")
                return True
            else:
                # print("Nenhuma condição encontrada")
                return False
        except Exception as e:
            msg = f"Erro na autenticação: {e}"
            print(msg)
            logger.error(msg)
            return False

class Pagamento:

    from src.utils.autenticador import token_shopee
    def __init__(self, auth_instance: 'Autenticacao' = None, ecommerce_id:int=None,empresa_id:int=None):
        self.ecommerce_id = auth_instance.ecommerce_id if auth_instance else  ecommerce_id
        self.empresa_id = auth_instance.empresa_id if auth_instance else  empresa_id
        self.dados_shopee = auth_instance.dados_shopee if auth_instance else None
        self.access_token = auth_instance.access_token if auth_instance else None

    @carrega_dados_shopee
    @token_shopee
    async def get_income_detail(self, date_from:str, date_to:str, income_status:int=1,cursor:str='',page_size:int=100) -> tuple[list[dict],list[dict]]:
        timest:int = int(time.time())
        partner_id:int = self.dados_shopee.get("partner_id")
        shop_id:int = self.dados_shopee.get("shop_id")
        partner_key:str = self.dados_shopee.get("partner_key")
        self.access_token = self.dados_shopee.get("access_token")
        base_string = "%s%s%s%s%s" % (partner_id, PATH_INCOME_DETAIL, timest, self.access_token, shop_id)
        base_string_encoded = base_string.encode()
        partner_key_encoded = partner_key.encode()
        sign = hmac.new(partner_key_encoded, base_string_encoded, hashlib.sha256).hexdigest()
        common_params = "partner_id=%s&timestamp=%s&access_token=%s&shop_id=%s&sign=%s" % (partner_id,timest,self.access_token,shop_id,sign)
        req_params = "date_from=%s&date_to=%s&income_status=%s&cursor=%s&page_size=%s" % (date_from,date_to,income_status,cursor,page_size)
        url = HOST_URL + PATH_INCOME_DETAIL + "?" + common_params + "&" + req_params
        headers = {"Content-Type": "application/json"}
        resp = requests.post(url, headers=headers)
        if resp.ok:
            ret = json.loads(resp.content)
            return ret['response'].get('next_page'), ret['response'].get('list')
        else:
            return False