import os
import requests
import time
from src.utils.decorador import interno
from src.utils.buscar_script import buscar_script
from src.utils.autenticador import token_snk
from src.utils.formatter import Formatter
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

class Produto:

    def __init__(self, codemp:int, empresa_id:int=None) -> None:
        self.token = None
        self.codemp = codemp
        self.empresa_id = empresa_id
        self.formatter = Formatter()
        self.req_time_sleep = float(os.getenv('REQ_TIME_SLEEP',1.5))
        self.campos_atualiza_snk = [ "ID", "IDPRODPAI" ]

    @token_snk
    async def buscar(
            self,
            codprod:int=None,
            idprod:int=None
        ) -> dict:

        if not any([codprod, idprod]):
            logger.error("Nenhum critério de busca fornecido. Deve ser informado 'codprod' ou 'idprod'.")
            print("Nenhum critério de busca fornecido. Deve ser informado 'codprod' ou 'idprod'.")
            return False

        url = os.getenv('SANKHYA_URL_DBEXPLORER')
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False  

        parametero = 'SANKHYA_PATH_SCRIPT_PRODUTO'
        script = buscar_script(parametro=parametero)
        query = script.format_map({"codemp":self.codemp,
                                   "codprod":codprod or "NULL",
                                   "idprod":idprod or "NULL"})

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
            try:
                return self.formatter.return_format(res.json())
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

    def preparar_dados(
            self,
            payload:dict
        ):        
        if not isinstance(payload, dict):
            logger.error("O payload deve ser um dicionário.")
            print("O payload deve ser um dicionário.")
            return False
        dados = {}
        for i in payload:
            dados[f'{self.campos_atualiza_snk.index(str.upper(i))}'] = f'{payload.get(i)}'
        return dados

    @token_snk
    async def atualizar(
            self,
            codprod:int,
            payload:dict,
            seq:int=None
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
        
        payload = {
                "serviceName":"DatasetSP.save",
                "requestBody":{
                    "entityName":"AD_OLISTPRODUTO",
                    "standAlone":False,
                    "fields":self.campos_atualiza_snk,
                    "records":[
                        {
                            "pk": {
                                "CODPROD": codprod,
                                "SEQ": seq
                            },
                            "values": payload
                        }
                    ]
                }
            }
        
        print("payload")
        print(payload)

        res = requests.post(
            url=url,
            headers={ 'Authorization':f"Bearer {self.token}" },
            json=payload
        )

        if res.status_code in (200,201) and res.json().get('status')=='1':
            return True
        else:
            logger.error("Erro ao atualizar produto. Cód. %s. %s",codprod,res.text)
            print(f"Erro ao atualizar produto. Cód. {codprod}. {res.text}")
            return False        

    @token_snk
    async def buscar_alteracoes(self) -> dict:
        
        url = os.getenv('SANKHYA_URL_LOAD_RECORDS')
        if not url:
            erro = f"Parâmetro da URL não encontrado"
            print(erro)
            logger.error(erro)
            return False
        
        tabela = os.getenv('SANKHYA_TABELA_RASTRO_PRODUTO')
        if not tabela:
            erro = f"Parâmetro da tabela de rastro de produto não encontrado"
            print(erro)
            logger.error(erro)
            return False

        offset = 0
        limite_alcancado = False
        todos_resultados = []

        while not limite_alcancado:
            time.sleep(self.req_time_sleep)
            payload = {
                    "serviceName": "CRUDServiceProvider.loadRecords",
                    "requestBody": {
                        "dataSet": {
                            "rootEntity": tabela,
                            "includePresentationFields": "N",
                            "offsetPage": offset,
                            "entity": {
                                "fieldset": {
                                    "list": "*"
                                }
                            }
                        }
                    }
                }
            
            res = requests.get(
                url=url,
                headers={ 'Authorization':f"Bearer {self.token}" },
                json=payload)
            
            if res.status_code != 200:
                print(f"Erro ao buscar produtos com alterações. {res.text}")
                logger.error("Erro ao buscar produtos com alterações. %s",res.text)
                return False
            
            if res.json().get('status') == '1':
                todos_resultados.extend(self.formatter.return_format(res.json()))
                if res.json()['responseBody']['entities'].get('hasMoreResult') == 'true':
                    offset += 1
                else:   
                    limite_alcancado = True
        
        return todos_resultados

    @token_snk
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

        tabela = os.getenv('SANKHYA_TABELA_RASTRO_PRODUTO')
        if not tabela:
            erro = f"Parâmetro da tabela de rastro de produto não encontrado"
            print(erro)
            logger.error(erro)
            return False          
        
        if codprod:
            filter = [{"CODPROD": f"{codprod}","CODEMP": f"{self.codemp}"}]
            
        if lista_produtos:
            filter = []
            for produto in lista_produtos:
                if isinstance(produto, dict):
                    if produto.get('sucesso'):
                        filter.append({"CODPROD": f"{codprod}","CODEMP": f"{self.codemp}"})
                else:
                    try:
                        aux = produto.__dict__
                        if aux.get('sucesso'):
                            filter.append({"CODPROD": f"{aux.get('cod_snk')}","CODEMP": f"{self.codemp}"})
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