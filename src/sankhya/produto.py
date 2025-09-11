import os
import logging
import requests
from dotenv import load_dotenv

from src.utils.decorador.sankhya import ensure_token
from src.utils.formatter import Formatter
from src.utils.log import Log

load_dotenv('keys/.env')
logger = logging.getLogger(__name__)
logging.basicConfig(filename=Log().buscar_path(),
                    encoding='utf-8',
                    format=os.getenv('LOGGER_FORMAT'),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

class Produto:

    def __init__(self, codemp:int):
        self.token = None
        self.codemp = codemp
        self.formatter = Formatter()        
        self.campos_lista = [
            "AD_MKP_CATEGORIA","AD_MKP_DESCRICAO","AD_MKP_DHATUALIZADO","AD_MKP_ESTPOL",
            "AD_MKP_ESTREGBAR","AD_MKP_ESTREGBARTIP","AD_MKP_ESTREGBARVAL","AD_MKP_IDPROD",
            "AD_MKP_IDPRODPAI","AD_MKP_INTEGRADO","AD_MKP_MARCA","AD_MKP_NOME","ALTURA",
            "CODESPECST","CODPROD","CODVOL","DESCRPROD","ESPESSURA","ESTMAX","ESTMIN","LARGURA",
            "NCM","ORIGPROD","PESOBRUTO","PESOLIQ","QTDEMB","REFERENCIA","REFFORN","TIPCONTEST"
        ]

    @ensure_token
    async def buscar(
            self,
            codprod:int=None,
            idprod:int=None
        ) -> dict:

        if not any([codprod, idprod]):
            logger.error("Nenhum critério de busca fornecido. Deve ser informado 'codprod' ou 'idprod'.")
            print("Nenhum critério de busca fornecido. Deve ser informado 'codprod' ou 'idprod'.")
            return False

        url = os.getenv('SANKHYA_URL_LOAD_RECORDS')
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False   

        if codprod:
            criteria = {
                "expression": {
                    "$": "this.CODPROD = ?"
                },
                "parameter": [
                    {
                        "$": f"{codprod}",
                        "type": "I"
                    }
                ]
            }

        if idprod:
            criteria = {
                "expression": {
                    "$": "this.AD_MKP_IDPROD = ?"
                },
                "parameter": [
                    {
                        "$": f"{idprod}",
                        "type": "I"
                    }
                ]
            }       

        res = requests.get(
            url=url,
            headers={ 'Authorization':f"Bearer {self.token}" },
            json={
                "serviceName": "CRUDServiceProvider.loadRecords",
                "requestBody": {
                    "dataSet": {
                        "rootEntity": "Produto",
                        "includePresentationFields": "N",
                        "offsetPage": "0",
                        "criteria": criteria,
                        "entity": {
                            "fieldset": {
                                "list": ','.join(self.campos_lista)
                            }
                        }
                    }
                }
            })

        if res.status_code in (200,201) and res.json().get('status')=='1':
            try:
                return self.formatter.return_format(res.json())[0]
            except:
                return []
        else:
            if codprod:
                logger.error("Erro ao buscar produto. Cód. %s. %s",codprod,res.text)
                print(f"Erro ao buscar produto. Cód. {codprod}. {res.text}")
            if idprod:
                logger.error("Erro ao buscar produto. ID %s. %s",idprod,res.text)
                print(f"Erro ao buscar produto. ID. {idprod}. {res.text}")
            return False

    def prepapar_dados(
            self,
            payload:dict
        ):
        
        if not isinstance(payload, dict):
            logger.error("O payload deve ser um dicionário.")
            print("O payload deve ser um dicionário.")
            return False

        dados = {}
        for i in payload:
            dados[f'{self.campos_lista.index(str.upper(i))}'] = f'{payload.get(i)}'
        return dados

    @ensure_token
    async def atualizar(
            self,
            codprod:int,
            payload:dict
        ) -> bool:

        if not isinstance(payload, dict):
            logger.error("O payload deve ser um dicionário.")
            print("O payload deve ser um dicionário.")
            return False

        url = os.getenv('SANKHYA_URL_SAVE')
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False

        res = requests.post(
            url=url,
            headers={ 'Authorization':f"Bearer {self.token}" },
            json={
                "serviceName":"DatasetSP.save",
                "requestBody":{
                    "entityName":"Produto",
                    "standAlone":False,
                    "fields":self.campos_lista,
                    "records":[
                        {
                            "pk": {
                                "CODPROD": str(codprod)
                            },
                            "values": payload
                        }
                    ]
                }
            }
        )

        if res.status_code in (200,201) and res.json().get('status')=='1':
            return True
        else:
            logger.error("Erro ao atualizar produto. Cód. %s. %s",codprod,res.text)
            print(f"Erro ao atualizar produto. Cód. {codprod}. {res.text}")
            return False        

    @ensure_token
    async def buscar_alteracoes(self) -> dict:
        
        url = os.getenv('SANKHYA_URL_LOAD_RECORDS')
        if not url:
            erro = f"Parâmetro da URL não encontrado"
            print(erro)
            logger.error(erro)
            return False
        
        tabela = os.getenv('SAKNHYA_TABELA_RASTRO_PRODUTO')
        if not tabela:
            erro = f"Parâmetro da tabela de rastro de produto não encontrado"
            print(erro)
            logger.error(erro)
            return False

        res = requests.get(
            url=url,
            headers={ 'Authorization':f"Bearer {self.token}" },
            json={
                "serviceName": "CRUDServiceProvider.loadRecords",
                "requestBody": {
                    "dataSet": {
                        "rootEntity": tabela,
                        "includePresentationFields": "N",
                        "offsetPage": "0",
                        "entity": {
                            "fieldset": {
                                "list": "*"
                            }
                        }
                    }
                }
            })
        if res.status_code != 200:
            print(f"Erro ao buscar produtos com alterações. {res.text}")
            logger.error("Erro ao buscar produtos com alterações. %s",res.text)
            return False
        else:                
            return self.formatter.return_format(res.json())

    @ensure_token
    async def excluir_alteracoes(
            self,
            codprod:int=None,
            lista_produtos:list=None
        ) -> bool:

        if not any([codprod, lista_produtos]):
            print("Código do produto não informado ou lista de produtos vazia")
            logger.error("Código do produto não informado ou lista de produtos vazia")
            return False

        url = os.getenv('SANKHYA_URL_DELETE')
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False

        tabela = os.getenv('SAKNHYA_TABELA_RASTRO_PRODUTO')
        if not tabela:
            erro = f"Parâmetro da tabela de rastro de produto não encontrado"
            print(erro)
            logger.error(erro)
            return False          
        
        if codprod:
            filter = [{"CODPROD": f"{codprod}"}]
            
        if lista_produtos:
            filter = []
            for produto in lista_produtos:
                if isinstance(produto, dict):
                    if produto.get('sucesso'):
                        filter.append({"CODPROD": f"{produto.get('codprod')}"})
                else:
                    try:
                        aux = produto.__dict__
                        if aux.get('sucesso'):
                            filter.append({"CODPROD": f"{aux.get('cod_snk')}"})
                    except:
                        logger.error("Erro ao extrair dados do objeto sqlalchemy.")
                        print("Erro ao extrair dados do objeto sqlalchemy.")
                        return False

        res = requests.get(
            url=url,
            headers={ 'Authorization':f"Bearer {self.token}" },
            json={
                "serviceName": "DatasetSP.removeRecord",
                "requestBody": {
                    "entityName": tabela,
                    "standAlone": False,
                    "pks": filter
                }
            })

        if res.status_code in (200,201) and res.json().get('status') in ('0','1'):
            return True
        else:
            logger.error("Erro ao remover alterações pendentes. %s",res.json())
            print(f"Erro ao remover alterações pendentes. {res.json()}")
            return False