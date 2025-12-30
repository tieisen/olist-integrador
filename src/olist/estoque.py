import os, time, requests
from src.utils.autenticador import token_olist
from src.parser.estoque import Estoque as ParserEstoque
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

class Estoque:

    def __init__(self,codemp:int=None,empresa_id:int=None):  
        self.codemp = codemp
        self.empresa_id = empresa_id
        self.token = None
        self.endpoint = os.getenv('OLIST_API_URL')+os.getenv('OLIST_ENDPOINT_ESTOQUES')
        self.req_time_sleep = float(os.getenv('REQ_TIME_SLEEP',1.5))
        
    @token_olist
    async def buscar(self,id:int=None,lista_produtos:list[int]=None) -> list[dict]:
        """
        Busca estoque atual dos produtos
            :param id: ID do produto (Olist)
            :param lista_produtos: lista de IDs dos produtos
            :return list[dict]: lista de dicionários com os dados de estoque dos produtos
        """        
        
        if not any([id,lista_produtos]):
            logger.error("Produto não informado.")
            return False

        result = []

        if id:
            url = self.endpoint+f"/{id}"
            res = requests.get(
                url=url,
                headers={
                    "Authorization":f"Bearer {self.token}",
                    "Content-Type":"application/json",
                    "Accept":"application/json"
                }
            )

            if res.status_code == 200:
                result.append(res.json())                
            else:                      
                logger.error("Erro %s: %s cod %s", res.status_code, res.json().get("mensagem","Erro desconhecido"), id)
                return result            

        if lista_produtos:
            for produto in lista_produtos:
                url = self.endpoint+f"/{produto}"
                time.sleep(self.req_time_sleep)
                res = requests.get(
                    url=url,
                    headers={
                        "Authorization":f"Bearer {self.token}",
                        "Content-Type":"application/json",
                        "Accept":"application/json"
                    }
                )

                if res.status_code == 200:
                    result.append(res.json())
                else: 
                    logger.error("Erro %s: %s cod %s", res.status_code, res.json().get("mensagem","Erro desconhecido"), id)
                    return result
        
        return result
    
    @token_olist
    async def enviar_saldo(self,id:int=None,data:dict=None,lista_dados:list=None) -> list[dict]:
        """
        Atualiza o estoque do produto no Olist
            :param id: ID do produto (Olist)
            :param data: dicionário com os dados do estoque atualizado
            :param lista_dados: lista de dicionários com os dados do estoque atualizado
            :return list[dict]: lista de dicionários com os dados de confirmação da atualização de estoque
        """
        
        if not all([id,data]) and not lista_dados:
            return False

        url = None

        if id and data:
            url = self.endpoint+f"/{id}"
            res = requests.post(url=url,
                                headers={"Authorization":f"Bearer {self.token}",
                                         "Content-Type":"application/json",
                                         "Accept":"application/json"},
                                json=data)
            if res.status_code == 200:
                data['sucesso'] = True
            else:
                logger.error("Erro %s: %s cod %s", res.status_code, res.json(), id)
                data['sucesso'] = False
            return data
        
        if lista_dados:
            parser = ParserEstoque()
            for dados in lista_dados:
                if dados.get('variacao') == 0:
                    dados['sucesso'] = True
                    continue

                id_produto, payload = parser.to_olist(dados.get('ajuste_estoque'))
                if not all([id_produto, payload]):
                    dados['sucesso'] = False
                    logger.error("Erro ao preparar dados para o produto cod %s", dados['ajuste_estoque'].get('id'))
                    continue

                url = self.endpoint+f"/{id_produto}"
                time.sleep(self.req_time_sleep)
                res = requests.post(url=url,
                                    headers={
                                        "Authorization":f"Bearer {self.token}",
                                        "Content-Type":"application/json",
                                        "Accept":"application/json"
                                    },
                                    json=payload)                
                if res.status_code == 200:
                    dados['sucesso'] = True
                else:
                    dados['sucesso'] = False
                    logger.error("Erro %s: cod %s", res.status_code, id_produto)
            return lista_dados
