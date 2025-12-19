import os
import time
import json
import requests
from datetime import datetime, timedelta
from src.utils.autenticador import token_olist
from src.utils.log import set_logger
from src.utils.load_env import load_env
from src.utils.busca_paginada import paginar_olist
load_env()
logger = set_logger(__name__)

class Produto:

    def __init__(self, codemp:int=None, empresa_id:int=None):  
        self.codemp = codemp
        self.empresa_id = empresa_id
        self.token = None
        self.req_sleep = float(os.getenv('REQ_TIME_SLEEP',1.5))
        self.endpoint = os.getenv('OLIST_API_URL') + os.getenv('OLIST_ENDPOINT_PRODUTOS')
        self.tempo_busca_alter_prod = int(os.getenv('OLIST_TEMPO_BUSCA_ALTER_PROD',30))

    @token_olist
    async def buscar(self,id:int=None,sku:int=None) -> dict:
        """
        Busca os dados do produto no Olist.
            :param id: ID do produto (Olist)
            :param sku: Código do produto (Sankhya)
            :return dict: dicionário com os dados do produto
        """

        if not any([id, sku]):
            logger.error("Produto não informado.")
            return False
        
        if id:
            url = self.endpoint+f"/{id}"

        if sku:
            url = self.endpoint+f"/?codigo={sku}"
        
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

        if res.status_code == 200:
            # Busca por sku precisa fazer outra chamada com o ID para ter todos os dados
            if sku:
                try:
                    id = res.json()['itens'][0].get('id')
                    url = self.endpoint+f"/{id}"
                    res = requests.get(
                        url=url,
                        headers={
                            "Authorization":f"Bearer {self.token}",
                            "Content-Type":"application/json",
                            "Accept":"application/json"
                        }
                    )
                except:
                    return False                
            return res.json()
        else:
            if id and not sku:
                msg = f"Erro {res.status_code}: {res.text} cod {id}"
                print(msg)
                logger.error(msg)
            if sku:
                msg = f"Erro {res.status_code}: {res.text} sku {sku}"
                print(msg)
                logger.error(msg)
            return False

    @token_olist
    async def incluir(self,data:dict) -> tuple[bool,dict]:
        """
        Inclui um novo produto no Olist.
            :param data: dicionário com os dados do produto
            :return bool: status da operação
            :return dict: dicionário com os dados de confirmação do cadastro
        """        
        
        if not isinstance(data, dict):
            logger.error("Dados do produto em formato inválido.")
            return False, {}
        
        url = self.endpoint
        if not url:
            logger.error("Erro relacionado à url. %s",url)
            return False, {}

        try:
            res = requests.post(url=url,
                                headers={
                                    "Authorization":f"Bearer {self.token}",
                                    "Content-Type":"application/json",
                                    "Accept":"application/json"
                                },
                                json=data)
        except Exception as e:
            erro = f"Produto {data.get('sku')}. Erro relacionado à requisição. {e}"
            logger.error(erro)
            return False, {erro}
        
        if res.status_code in [200,201]:                    
            return True, res.json()
        else:
            erro = f"Erro {res.status_code}: {res.json()} cod {data.get('sku')}"
            logger.error(erro)
            return False, {erro}

    @token_olist
    async def atualizar(self,id:int=None,idprodpai:int=None,data:dict=None) -> bool:
        """
        Atualiza um produto no Olist.
            :param id: ID do produto (Olist)
            :param idprodpai: ID do produto pai, se variação (Olist)
            :param data: dicionário com os dados do produto
            :return bool: status da operação
        """           

        if not all([id, data]):
            logger.error("Dados do produto e ID não informados.")
            return False

        url = self.endpoint+f"/{idprodpai}/variacoes/{id}" if idprodpai else self.endpoint+f"/{id}"

        if not url:
            logger.error("Erro relacionado à url. %s",url)
            return False
        
        res = requests.put(url=url,
                           headers={
                               "Authorization":f"Bearer {self.token}",
                               "Content-Type":"application/json",
                               "Accept":"application/json"
                           },
                           json=data)
        
        if res.status_code == 204:                    
            return True
        if res.status_code in [404,409]:
            logger.warning("Erro %s: %s ID %s", res.status_code, res.json(), id)
            logger.info(json.dumps(data))
            return True      
        else:
            logger.error("Erro %s: %s ID %s", res.status_code, res.json(), id)
            logger.info(json.dumps(data))
            return False

    @token_olist
    async def buscar_todos(self) -> list[dict]:
        """
        Busca os dados de todos os produtos no Olist.
            :return list[dict]: lista de dicionários com os dados dos produtos
        """
        return await paginar_olist(token=self.token,url=self.endpoint)
    
    @token_olist
    async def buscar_alteracoes(self,todo_historico:bool=False) -> list[dict]:
        """
        Busca lista de produtos com alteração no cadastro no último período.
            :param todo_historico: se busca todo o histórico ou considera a janela de tempo padrão em minutos
            :return list[dict]: lista de dicionários com os dados resumidos dos produtos alterados
        """
        
        data_alteracao = (datetime.now()-timedelta(minutes=self.tempo_busca_alter_prod)).strftime("%Y-%m-%d %H:%M:00")
        url = self.endpoint if todo_historico else self.endpoint+f"?dataAlteracao={data_alteracao}"

        itens = await paginar_olist(token=self.token,url=url)
      
        if itens:
            itens.sort(key=lambda i: i['dataAlteracao'])
            lista_itens = []
            for i in itens:
                if i["tipo"] == "S":
                    lista_itens.append({
                        "id":i.get("id"),
                        "sku":i.get("sku"),
                        "dh_alter":i.get("dataAlteracao"),
                        "situacao":i.get("situacao")
                    })
            return lista_itens
        else:
            return []    