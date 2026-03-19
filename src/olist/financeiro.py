import os, time, requests
from datetime import datetime
from src.olist.autenticacao import tokenOlist
from src.utils.log import set_logger
from src.utils.load_env import load_env
from src.utils.busca_paginada import paginar_olist
load_env()
logger = set_logger(__name__)

class Receita:

    def __init__(self, empresa_id:int):
        self.empresa_id = empresa_id
        self.codemp = None
        self.token = None
        self.endpoint = os.getenv('OLIST_API_URL')+os.getenv('OLIST_ENDPOINT_FINANCEIRO_RECEBER')
        self.req_time_sleep = float(os.getenv('REQ_TIME_SLEEP',1.5))        

    @tokenOlist
    async def buscar(self,id:int=None,serieNf:str=None,numeroNf:str=None,id_nota:int=None) -> dict:
        """
        Busca o registro de contas a receber
            :param id: ID do lançamento
            :param serieNf: série da NF
            :param numeroNf: número da NF            
            :return dict: dicionário com os dados do contas a receber
        """

        if id:
            url = self.endpoint+f"/{id}"
        elif id_nota:
            url = self.endpoint+f"?idNota={id_nota}"
        elif all([serieNf, numeroNf]):
            url = self.endpoint+f"?numeroDocumento={serieNf}{numeroNf}/01"
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
        elif id_nota:
            try:
                res_ = res.json().get('itens')[0]
                return res_
            except IndexError:
                return None
        elif all([serieNf, numeroNf]):
            try:
                res_ = res.json().get('itens')[0]
                return res_
            except IndexError:
                return None            
        else:
            return res.json().get('itens')

    @tokenOlist
    async def buscarMarcadores(self,id:int) -> bool:
        """
        Lista marcadores do título de recebimento.
            :param id: ID do título de recebimento (Olist)
            :return bool: status da operação
        """
        
        dados_marcadores:list[dict] = []

        url = self.endpoint+f"/{id}/marcadores"
        if not url:
            logger.error("Erro relacionado à url. %s",url)
            return False 

        res = requests.get(
            url=url,
            headers={
                "Authorization":f"Bearer {self.token}",
                "Content-Type":"application/json",
                "Accept":"application/json"
            }       
        )

        if not res.ok:
            logger.error("Erro %s: %s. Conta receber %s", res.status_code, res.text, id)
        else:
            dados_marcadores = res.json()

        return dados_marcadores

    @tokenOlist
    async def listarReceberAberto(self,dt_emissao:str=None) -> list[dict]:
        """
        Busca a lista de contas a receber em aberto pela data
            :param dt_emissao: data da geração do título no Olist no padrão YYYY-MM-DD. data atual se nulo
            :return list[dict]: lista de dicionários com os dados das contas a receber do dia
        """
        if not dt_emissao:
            dt_emissao = datetime.today().strftime('%Y-%m-%d')
        url = self.endpoint+f"/?situacao=aberto&dataInicialEmissao={dt_emissao}&dataFinalEmissao={dt_emissao}"
        return await paginar_olist(token=self.token,url=url)

    @tokenOlist
    async def marcarDevolvido(self,id:int) -> bool:
        """
        Adiciona um marcador no título de recebimento.
            :param id: ID do título de recebimento (Olist)
            :return bool: status da operação
        """
        
        url = self.endpoint+f"/{id}/marcadores"
        if not url:
            logger.error("Erro relacionado à url. %s",url)
            return False 
        
        payload = [
            {
                "descricao": "devolvido"
            }
        ]

        res = requests.post(
            url=url,
            headers={
                "Authorization":f"Bearer {self.token}",
                "Content-Type":"application/json",
                "Accept":"application/json"
            },
            json=payload        
        )

        if not res.ok:
            logger.error("Erro %s: %s. Conta receber %s", res.status_code, res.text, id)
            return False
        
        return True

    @tokenOlist
    async def desmarcarDevolvido(self,id:int) -> bool:
        """
        Remove um marcador no título de recebimento.
            :param id: ID do título de recebimento (Olist)
            :return bool: status da operação
        """

        url = self.endpoint+f"/{id}/marcadores"
        if not url:
            logger.error("Erro relacionado à url. %s",url)
            return False 
        
        time.sleep(self.req_time_sleep)
        dados_marcadores:list[dict] = await self.buscarMarcadores(id=id)
        if not dados_marcadores:            
            return True
        
        for marcador in dados_marcadores:
            if marcador.get('descricao') == 'devolvido':
                dados_marcadores.remove(marcador)
                break

        res = requests.put(
            url=url,
            headers={
                "Authorization":f"Bearer {self.token}",
                "Content-Type":"application/json",
                "Accept":"application/json"
            },
            json=dados_marcadores     
        )

        if not res.ok:
            logger.error("Erro %s: %s. Conta receber %s", res.status_code, res.text, id)
            return False
        
        return True

    @tokenOlist
    async def baixar(self,id:int,payload:dict) -> bool:
        """
        Realiza o recebimento/baixa do registro de contas a receber gerado pela NF
            :param id: ID do registro de contas a receber
            :param valor: valor do recebimento
            :return payload: dicionário com os dados do contas a receber
            :return bool: status da operação            
        """        

        url = self.endpoint+f"/{id}/baixar"
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
        
        # Financeiro baixado com sucesso (204) ou
        # Financeiro da nota já foi baixado (409)
        if res.status_code not in (409,204):
            logger.error("Erro %s: %s fin %s", res.status_code, res.text, id)            
            logger.error("payload: %s", payload)
            return False       

        return True        

    @tokenOlist
    async def lancar(self,payload:dict) -> int:
        """
        Cria um registro de contas a receber
            :return payload: dicionário com os dados
            :return bool: status da operação            
        """
        
        id:int=None

        url = self.endpoint
        if not url:
            logger.error("Erro relacionado à url. %s",url)
            return id

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
            return id
        
        id = res.json().get('id')

        return id

class Despesa:

    def __init__(self, empresa_id:int):
        self.empresa_id = empresa_id
        self.codemp = None
        self.token = None
        self.endpoint = os.getenv('OLIST_API_URL')+os.getenv('OLIST_ENDPOINT_FINANCEIRO_PAGAR')
        self.req_time_sleep = float(os.getenv('REQ_TIME_SLEEP',1.5))        

    @tokenOlist
    async def buscar(self,id:int) -> dict:
        """
        Busca o registro de contas a pagar
            :param id: ID do lançamento
            :param numeroNf: número da NF            
            :return dict: dicionário com os dados do contas a pagar
        """

        retorno:dict={}
        url = self.endpoint+f"/{id}"
        
        res = requests.get(
            url = url,            
            headers = {
                "Authorization":f"Bearer {self.token}",
                "Content-Type":"application/json",
                "Accept":"application/json"
            }
        )

        if res.status_code != 200:
            logger.error("Erro %s: %s", res.status_code, res.text)
            print(f"Erro {res.status_code}: {res.text}")
            return retorno
        
        retorno = res.json()

        return retorno

    @tokenOlist
    async def lancar(self,payload:dict) -> int:
        """
        Cria um registro de contas a pagar
            :return payload: dicionário com os dados
            :return bool: status da operação            
        """
        id:int=None

        url = self.endpoint
        if not url:
            logger.error("Erro relacionado à url. %s",url)
            return id 

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
            return id
        
        id = res.json().get('id')

        return id

    @tokenOlist
    async def baixar(self,id:int,payload:dict) -> bool:
        """
        Realiza o recebimento/baixa do registro de contas a pagar gerado pela NF
            :param id: ID do registro de contas a receber
            :param valor: valor do recebimento
            :return payload: dicionário com os dados do contas a receber
            :return bool: status da operação            
        """        

        url = self.endpoint+f"/{id}/baixar"
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
        
        # Financeiro baixado com sucesso (204) ou
        # Financeiro da nota já foi baixado (409)
        if res.status_code not in (409,204):
            logger.error("Erro %s: %s fin %s", res.status_code, res.text, id)            
            logger.error("payload: %s", payload)
            return False       

        return True  