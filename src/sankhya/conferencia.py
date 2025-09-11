import os
import logging
import requests
from datetime import datetime
from dotenv import load_dotenv

from src.utils.decorador.sankhya import ensure_token
from src.utils.formatter import Formatter
from src.utils.buscar_script import buscar_script

load_dotenv('keys/.env')
logger = logging.getLogger(__name__)
logging.basicConfig(filename=os.getenv('PATH_LOGS'),
                    encoding='utf-8',
                    format=os.getenv('LOGGER_FORMAT'),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

class Conferencia:

    def __init__(self, codemp:int):
        self.token = None
        self.codemp = codemp
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

    def extrai_nuconf(self,payload:dict=None):
        return int(payload['responseBody']['result'][0][0])

    async def buscar_aguardando_conferencia(self):

        url = os.getenv('SANKHYA_URL_DBEXPLORER')
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False  

        parametero = 'SANKHYA_PATH_SCRIPT_AGUARDANDO_CONFERENCIA'
        script = buscar_script(parametro=parametero)

        res = requests.get(
            url=url,
            headers={ 'Authorization':f"Bearer {self.token}" },
            json={
                "serviceName": "DbExplorerSP.executeQuery",
                "requestBody": {
                    "sql":script
                }
            })

        if res.status_code in (200,201) and res.json().get('status')=='1':
            return self.formatter.return_format(res.json())
        else:
            print(f"Erro ao buscar status dos pedidos. {res.text}")
            logger.error("Erro ao buscar status dos pedidos. %s",res.text)
            return False
    
    @ensure_token
    async def buscar(
            self,
            nunota:int
        ) -> dict:

        url = os.getenv('SANKHYA_URL_LOAD_RECORDS')
        if not url:
            print(f"Erro relacionado à url. {url}")
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
            print(res.json().get('statusMessage'))
            return False

    @ensure_token
    async def criar(
            self,
            nunota:int
        ) -> dict:
        
        url = os.getenv('SANKHYA_URL_SAVE')
        if not url:
            print(f"Erro relacionado à url. {url}")
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
                print(res.json().get('statusMessage'))
                return False
            if res.json().get('status')=='1':
                self.nuconf = int(self.extrai_nuconf(res.json()))
                return True
        else:
            logger.error("Erro ao criar conferência do pedido %s. %s",nunota,res.json().get('statusMessage'))
            print(res.json().get('statusMessage'))
            return False

    @ensure_token
    async def vincular_pedido(
            self,
            nunota:int,
            nuconf:int
        ) -> int:
        
        url = os.getenv('SANKHYA_URL_SAVE')
        if not url:
            print(f"Erro relacionado à url. {url}")
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
                print(res.json().get('statusMessage'))
                return False
            if res.json().get('status')=='1':
                return True
            if res.json().get('status')=='2':
                print(res.json().get('statusMessage'))
                return True
        else:
            logger.error("Erro ao vincular conferência ao pedido #%s. %s",nunota,res.text)
            print(res.text)
            return False

    @ensure_token
    async def insere_itens(
            self,
            dados_item:list
        ) -> bool:

        url = os.getenv('SANKHYA_URL_SAVE')
        if not url:
            print(f"Erro relacionado à url. {url}")
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

    @ensure_token
    async def concluir(
            self,
            nuconf:int
        ) -> dict:

        url = os.getenv('SANKHYA_URL_SAVE')
        if not url:
            print(f"Erro relacionado à url. {url}")
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
                            "2": os.getenv('SANKHYA_CODUSU_INTEGRACAO'),
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
                print(res.json().get('statusMessage'))
                return False
            if res.json().get('status')=='1':
                return True
        else:
            logger.error("Erro ao concluir conferência do pedido. %s",res.json().get('statusMessage'))
            print(res.json().get('statusMessage'))
            return False
       