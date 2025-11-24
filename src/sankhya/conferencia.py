import os
import requests
from datetime import datetime
from src.utils.decorador import carrega_dados_empresa, interno
from src.utils.autenticador import token_snk
from src.utils.formatter import Formatter
from src.utils.buscar_script import buscar_script
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

class Conferencia:

    def __init__(self, codemp:int=None, empresa_id:int=None):
        self.token = None
        self.codemp = codemp
        self.empresa_id = empresa_id
        self.dados_empresa:dict=None
        self.formatter = Formatter()
        self.campos_cabecalho = [
            "NUCONF", "NUNOTAORIG", "STATUS", "DHINICONF",
            "DHFINCONF", "CODUSUCONF", "QTDVOL"
        ]
        self.campos_item = [
            "NUCONF", "SEQCONF", "CODBARRA", "CODPROD",
            "CODVOL", "CONTROLE", "QTDCONFVOLPAD", "QTDCONF"
        ]
        self.nuconf = None

    @interno
    def extrai_nuconf(self,payload:dict=None) -> int:
        """
        Extrai o número da conferência.
            :param payload: retorno da API do Sankhya em JSON
            :return int: número da conferência
        """

        return int(payload['responseBody']['result'][0][0])

    @token_snk
    async def buscar_aguardando_conferencia(self,id_loja:int=None) -> list[dict]:
        """
        Busca pedidos de venda com status de aguardando conferência.
            :param id_loja: ID do E-commerce no Olist
            :return list[dict]: dados da nota de venda
        """

        url = os.getenv('SANKHYA_URL_DBEXPLORER')
        if not url:
            logger.error("Erro relacionado à url. %s",url)
            return False  

        parametero = 'SANKHYA_PATH_SCRIPT_AGUARDANDO_CONFERENCIA'
        script = buscar_script(parametro=parametero)
        query = script.format_map({"id_loja":id_loja})        

        res = requests.get(
            url=url,
            headers={ 'Authorization':f"Bearer {self.token}" },
            json={
                "serviceName": "DbExplorerSP.executeQuery",
                "requestBody": {
                    "sql":query
                }
            })

        if res.status_code in (200,201) and res.json().get('status')=='1':
            return self.formatter.return_format(res.json())
        else:
            logger.error("Erro ao buscar status dos pedidos. %s",res.text)
            return False
    
    @token_snk
    async def buscar(self,nunota:int) -> list[dict]:
        """
        Busca conferência pelo número único do pedido de venda.
            :param nunota: número único do pedido de venda (Sankhya)
            :return list[dict]: dados da conferência do pedido
        """        

        url = os.getenv('SANKHYA_URL_LOAD_RECORDS')
        if not url:
            logger.error("Erro relacionado à url. %s",url)
            return False         

        res = requests.get(
            url=url,
            headers={ 'Authorization':f"Bearer {self.token}" },
            json={
                "serviceName": "CRUDServiceProvider.loadRecords",
                "requestBody": {
                    "dataSet": {
                        "rootEntity": "CabecalhoConferencia",
                        "includePresentationFields": "N",
                        "offsetPage": "0",
                        "criteria": {
                            "expression": {
                                "$": "this.NUNOTAORIG = ?"
                            },
                            "parameter": [
                                {
                                    "$": f"{nunota}",
                                    "type": "I"
                                }
                            ]
                        },
                        "entity": {
                            "fieldset": {
                                "list": ','.join(self.campos_cabecalho)
                            }
                        }
                    }
                }
            })
        
        if res.status_code in (200,201):
            if res.json().get('status') in ['0', '2']:
                return 0
            if res.json().get('status')=='1':
                dados_conferencia = self.formatter.return_format(res.json())[0]
                self.nuconf = dados_conferencia.get('nuconf')
                return dados_conferencia
        else:
            logger.error("Erro ao buscar dados da conferência do pedido %s. %s",nunota,res.json().get('statusMessage'))
            return False

    @token_snk
    async def criar(self,nunota:int) -> bool:
        """
        Cria uma conferência vinculada ao pedido de venda.
            :param nunota: número único do pedido de venda (Sankhya)
            :return bool: status da operação
        """        
        
        url = os.getenv('SANKHYA_URL_SAVE')
        if not url:
            logger.error("Erro relacionado à url. %s",url)
            return False        

        payload = {
            "serviceName":"DatasetSP.save",
            "requestBody":{
                "entityName":"CabecalhoConferencia",
                "standAlone":False,
                "fields":[
                    "NUCONF",
                    "NUNOTAORIG",
                    "STATUS",
                    "DHINICONF"
                ],
                "records":[
                    {
                        "values":{
                        "1":f"{nunota}",
                        "2":"A",
                        "3":datetime.now().strftime('%d/%m/%Y %H:%M:%S')
                        }
                    }
                ]
            }
            }

        res = requests.get(
            url=url,
            headers={ 'Authorization':f"Bearer {self.token}" },
            json=payload
        )
        
        if res.status_code in (200,201):
            if res.json().get('status') in ['0', '2']:
                return False
            if res.json().get('status')=='1':
                self.nuconf = int(self.extrai_nuconf(res.json()))
                return True
        else:
            logger.error("Erro ao criar conferência do pedido %s. %s",nunota,res.json().get('statusMessage'))
            return False

    @token_snk
    async def vincular_pedido(self,nunota:int,nuconf:int) -> bool:
        """
        Vincula a conferência criada ao pedido de venda.            
            :param nunota: número único do pedido de venda (Sankhya)
            :param nuconf: número da conferência
            :return bool: status da operação
        """             
        
        url = os.getenv('SANKHYA_URL_SAVE')
        if not url:
            logger.error("Erro relacionado à url. %s",url)
            return False 

        payload = {
            "serviceName": "DatasetSP.save",
            "requestBody": {
                "entityName": "CabecalhoNota",
                "standAlone": False,
                "fields": [ "NUCONFATUAL" ],
                "records": [
                    {
                        "pk": {
                            "NUNOTA": f"{nunota}"
                        },
                        "values": {
                            "0": f"{nuconf}"
                        }
                    }
                ]
            }
        }

        res = requests.get(
            url=url,
            headers={ 'Authorization':f"Bearer {self.token}" },
            json=payload
        )
        
        if res.status_code in (200,201):
            if res.json().get('status')=='0':
                return False
            if res.json().get('status')=='1':
                return True
            if res.json().get('status')=='2':
                return True
        else:
            logger.error("Erro ao vincular conferência ao pedido #%s. %s",nunota,res.text)
            return False

    @token_snk
    async def insere_itens(self,dados_item:list[dict]) -> bool:
        """
        Insere itens na conferência.
            :param dados_item: dados dos itens a serem inseridos na conferência
            :return bool: status da operação
        """          

        url = os.getenv('SANKHYA_URL_SAVE')
        if not url:
            logger.error("Erro relacionado à url. %s",url)
            return False    

        payload = {
            "serviceName":"DatasetSP.save",
            "requestBody":{
                "entityName":"DetalhesConferencia",
                "standAlone":False,
                "fields":self.campos_item,
                "records":dados_item
            }
        }

        res = requests.post(
            url=url,
            headers={ 'Authorization':f"Bearer {self.token}" },
            json=payload
        )

        if res.status_code in (200,201) and res.json().get('status')=='1':
            return True
        else:
            logger.error("Erro ao inserir item(ns) na conferência. %s",res.text)
            return False

    @token_snk
    @carrega_dados_empresa
    async def concluir(self,nuconf:int) -> bool:
        """
        Atualiza o status da conferência para Finalizada
            :param nuconf: número da conferência
            :return bool: status da operação
        """                

        url = os.getenv('SANKHYA_URL_SAVE')
        if not url:
            logger.error("Erro relacionado à url. %s",url)
            return False          

        payload = {
            "serviceName": "DatasetSP.save",
            "requestBody": {
                "entityName": "CabecalhoConferencia",
                "standAlone": False,
                "fields": [
                    "STATUS",
                    "DHFINCONF",
                    "CODUSUCONF",
                    "QTDVOL"
                ],
                "records": [
                    {
                        "pk": {
                            "NUCONF": f"{nuconf}"
                        },
                        "values": {
                            "0": "F",
                            "1": datetime.now().strftime('%d/%m/%Y %H:%M:%S'),
                            "2": self.dados_empresa.get('snk_codusu_integracao'),
                            "3": "1"
                        }
                    }
                ]
            }
        }

        res = requests.get(
            url=url,
            headers={ 'Authorization':f"Bearer {self.token}" },
            json=payload
        )
        
        if res.status_code in (200,201):
            if res.json().get('status') in ['0', '2']:
                return False
            if res.json().get('status')=='1':
                return True
        else:
            logger.error("Erro ao concluir conferência do pedido. %s",res.json().get('statusMessage'))
            return False
       