import os
import requests
from datetime import datetime
from src.utils.decorador import carrega_dados_ecommerce, carrega_dados_empresa
from src.utils.autenticador import token_olist
from src.utils.log import set_logger
from src.utils.load_env import load_env
from src.utils.busca_paginada import paginar_olist
load_env()
logger = set_logger(__name__)

class Financeiro:

    def __init__(self, id_loja:int, empresa_id:int):
        self.id_loja = id_loja
        self.empresa_id = empresa_id
        self.codemp = None
        self.dados_ecommerce:dict = None
        self.dados_empresa:dict = None
        self.token = None
        self.endpoint_receber = os.getenv('OLIST_API_URL')+os.getenv('OLIST_ENDPOINT_FINANCEIRO_RECEBER')
        self.endpoint_pagar = os.getenv('OLIST_API_URL')+os.getenv('OLIST_ENDPOINT_FINANCEIRO_PAGAR')

    @token_olist
    async def buscar_receber(self,id:int=None,serieNf:str=None,numeroNf:str=None) -> dict:
        """
        Busca o registro de contas a receber
            :param id: ID do lançamento
            :param serieNf: série da NF
            :param numeroNf: número da NF            
            :return dict: dicionário com os dados do contas a receber
        """

        if id:
            url = self.endpoint_receber+f"/{id}"
        elif all([serieNf, numeroNf]):
            url = self.endpoint_receber+f"?numeroDocumento={serieNf}{numeroNf}/01"
        else:
            return False
        
        res = requests.get(
            url = url,            
            headers = {
                "Authorization":f"Bearer {self.token}",
                "Content-Type":"application/json",
                "Accept":"application/json"
            }
        )

        if res.status_code != 200:
            logger.error("Erro %s: %s fin %s", res.status_code, res.text, f"{serieNf}{numeroNf}/01")
            print(f"Erro {res.status_code}: {res.text} fin {serieNf}{numeroNf}/01")
            return False
        
        if id:
            return res.json()
        elif all([serieNf, numeroNf]):
            return res.json().get('itens')[0]
        else:
            return res.json().get('itens')

    @carrega_dados_ecommerce
    @token_olist
    async def buscar_pagar(self,id:int=None,numeroNf:str=None) -> dict:
        """
        Busca o registro de contas a pagar
            :param id: ID do lançamento
            :param numeroNf: número da NF            
            :return dict: dicionário com os dados do contas a pagar
        """

        if id:
            url = self.endpoint_receber+f"/{id}"
        elif numeroNf:
            url = self.endpoint_receber+f"?numeroDocumento={numeroNf}"
        else:
            return False
        
        res = requests.get(
            url = url,            
            headers = {
                "Authorization":f"Bearer {self.token}",
                "Content-Type":"application/json",
                "Accept":"application/json"
            }
        )

        if res.status_code != 200:
            logger.error("Erro %s: %s fin %s", res.status_code, res.text, f"{numeroNf}")
            print(f"Erro {res.status_code}: {res.text} fin {numeroNf}")
            return False
        
        if id:
            return res.json()
        elif numeroNf:
            return res.json().get('itens')[0]
        else:
            return res.json().get('itens')

    @token_olist
    async def buscar_lista_receber_aberto(self,dt_emissao:str=None) -> list[dict]:
        """
        Busca a lista de contas a receber em aberto pela data
            :param dt_emissao: data da geração do título no Olist no padrão YYYY-MM-DD. data atual se nulo
            :return list[dict]: lista de dicionários com os dados das contas a receber do dia
        """
        if not dt_emissao:
            dt_emissao = datetime.today().strftime('%Y-%m-%d')
        url = self.endpoint_receber+f"/?situacao=aberto&dataInicialEmissao={dt_emissao}&dataFinalEmissao={dt_emissao}"
        return await paginar_olist(token=self.token,url=url)

    @carrega_dados_ecommerce
    @token_olist
    async def baixar_receber(self,id:int,payload:dict=None,valor:float=None) -> bool:
        """
        Realiza o recebimento/baixa do registro de contas a receber gerado pela NF
            :param id: ID do registro de contas a receber
            :param valor: valor do recebimento
            :return payload: dicionário com os dados do contas a receber
            :return bool: status da operação            
        """        

        url = self.endpoint_receber+f"/{id}/baixar"
        if not url:
            logger.error("Erro relacionado à url. %s",url)
            return False 
        
        if all([payload,valor]):
            payload = {
                "contaDestino": {
                    "id": self.dados_ecommerce.get('id_conta_destino')
                },
                "data": None,
                "categoria": {
                    "id": self.dados_ecommerce.get('id_categoria_financeiro')
                },
                "historico": None,
                "taxa": None,
                "juros": None,
                "desconto": None,
                "valorPago": valor,
                "acrescimo": None
            }

        res = requests.post(
            url = url,            
            headers = {
                "Authorization":f"Bearer {self.token}",
                "Content-Type":"application/json",
                "Accept":"application/json"
            },
            json=payload
        )
        
        # Financeiro baixado com sucesso (204) ou
        # Financeiro da nota já foi baixado (409)
        if res.status_code not in (409,204):
            logger.error("Erro %s: %s fin %s", res.status_code, res.text, id)            
            logger.error("payload: %s", payload)
            return False       

        return True        

    @carrega_dados_ecommerce
    @carrega_dados_empresa
    @token_olist
    async def lancar_pagar(self,payload:dict) -> bool:
        """
        Cria um registro de contas a pagar
            :return payload: dicionário com os dados
            :return bool: status da operação            
        """

        url = self.endpoint_pagar
        if not url:
            logger.error("Erro relacionado à url. %s",url)
            return False 

        res = requests.post(
            url = url,            
            headers = {
                "Authorization":f"Bearer {self.token}",
                "Content-Type":"application/json",
                "Accept":"application/json"
            },
            json=payload
        )
        
        if not res.ok:
            logger.error("Erro %s: %s", res.status_code, res.text)
            logger.error("payload: %s", payload)
            return False       

        return True        