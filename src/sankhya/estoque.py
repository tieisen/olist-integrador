import os
import time
import requests
from src.utils.decorador import carrega_dados_empresa, interno
from src.utils.autenticador import token_snk
from src.utils.formatter import Formatter
from src.utils.buscar_script import buscar_script
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

class Estoque:

    def __init__(self, codemp:int):
        self.token = None
        self.codemp = codemp
        self.empresa_id = None
        self.dados_empresa = None
        self.req_time_sleep = float(os.getenv('REQ_TIME_SLEEP',1.5))        
        self.formatter = Formatter()
    
    @token_snk
    @carrega_dados_empresa
    async def buscar(
            self,
            codprod:int=None,
            lista_produtos:list[int]=None
        ) -> dict:

        if not any([codprod, lista_produtos]):
            print("Código do produto não informado ou lista de produtos vazia")
            return False
        
        url = os.getenv('SANKHYA_URL_DBEXPLORER')
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False
        
        filtro_produtos:str=''
        if codprod:
            filtro_produtos = str(codprod)
        if lista_produtos:   
            filtro_produtos = ','.join(map(str,lista_produtos))

        # Considera estoque da empresa ecommerce e da unidade física do estado
        cod_empresas_estoque:str = ','.join(map(str,[self.dados_empresa.get('snk_codemp_fornecedor'),self.dados_empresa.get('snk_codemp')]))

        parametero = 'SANKHYA_PATH_SCRIPT_SALDO_ESTOQUE'
        script = buscar_script(parametro=parametero)
        query = script.format_map({"codlocais": self.dados_empresa.get('snk_codlocal_estoque'),
                                   "codemp": self.codemp,
                                   "codempresas": cod_empresas_estoque,
                                   "lista_produtos": filtro_produtos
                                })

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
            logger.error("Erro ao buscar saldo de estoque do item %s. %s",codprod,res.json())
            print(f"Erro ao buscar saldo de estoque do item {codprod}. {res.json()}")
            return False
    
    @token_snk
    async def buscar_alteracoes(self, codemp:int) -> dict:

        url = os.getenv('SANKHYA_URL_LOAD_RECORDS')
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False

        tabela = os.getenv('SANKHYA_TABELA_RASTRO_ESTOQUE')
        if not tabela:
            erro = f"Parâmetro da tabela de rastro de estoque não encontrado"
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
                        "criteria": {
                            "expression": {
                                "$": "this.CODEMP = ?"
                            },
                            "parameter": [
                                {
                                    "$": f"{codemp}",
                                    "type": "I"
                                }
                            ]
                        },                        
                        "entity": {
                            "fieldset": {
                                "list": '*'
                            }
                        }
                    }
                }
            }
            res = requests.get(
                url=url,
                headers={ 'Authorization':f"Bearer {self.token}" },
                json=payload
            )
            if res.status_code != 200:
                print(f"Erro ao buscar alterações pendentes. {res.text}")
                logger.error("Erro ao buscar alterações pendentes. %s",res.text)
                return False
            
            if res.json().get('status') == '1':
                todos_resultados.extend(self.formatter.return_format(res.json()))
                if res.json()['responseBody']['entities'].get('hasMoreResult') == 'true':
                    offset += 1
                else:   
                    limite_alcancado = True

        return todos_resultados
    
    @token_snk
    async def remover_alteracoes(
            self,
            codprod:int=None,
            lista_produtos:list=None
        ) -> dict:

        if not any([codprod, lista_produtos]):
            print("Código do produto não informado ou lista de produtos vazia")
            logger.error("Código do produto não informado ou lista de produtos vazia")
            return False
        
        url = os.getenv('SANKHYA_URL_DELETE')
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False
        
        tabela = os.getenv('SANKHYA_TABELA_RASTRO_ESTOQUE')
        if not tabela:
            erro = f"Parâmetro da tabela de rastro de estoque não encontrado"
            print(erro)
            logger.error(erro)
            return False

        if codprod:
            filter = [
                {
                    "CODPROD": f"{codprod}",
                    "CODEMP": f"{self.codemp}"
                }
            ]

        if lista_produtos:
            filter = []
            for produto in lista_produtos:
                if produto.get('sucesso'):
                    filter.append({
                        "CODPROD": f"{produto['ajuste_estoque'].get('codprod')}",
                        "CODEMP": f"{self.codemp}"
                    })
        
        payload = {
            "serviceName": "DatasetSP.removeRecord",
            "requestBody": {
                "entityName": tabela,
                "standAlone": False,
                "pks": filter
            }
        }

        res = requests.get(
            url=url,
            headers={ 'Authorization':f"Bearer {self.token}" },
            json=payload
        )

        if res.status_code in (200,201) and res.json().get('status')=='1':
            return True
        else:
            logger.error("Erro ao remover alterações pendentes. %s",res.json())
            print(f"Erro ao remover alterações pendentes. {res.json()}")
            return False
    
    @interno
    @carrega_dados_empresa
    async def formatar_query_busca_saldo_lote(
            self,
            codprod:int=None,
            controle:str=None,
            lista_produtos:list=None
        ):
            
        if not lista_produtos and not all([codprod, controle]):
            print("Código do produto, controle de lote e quantidade não informados")
            logger.error("Código do produto, controle de lote e quantidade não informados")
            return False
        
        parametro = 'SANKHYA_PATH_SCRIPT_ESTOQUE_LOTE_LISTA' if lista_produtos else 'SANKHYA_PATH_SCRIPT_ESTOQUE_LOTE_ITEM'
        script = buscar_script(parametro=parametro)

        try:
            if lista_produtos:
                produtos = [produto.get('codprod') for produto in lista_produtos]
                query = script.format_map({
                                    "codemp":self.codemp,
                                    "codemp_matriz":self.dados_empresa.get('snk_codemp_fornecedor'),
                                    "codlocais":self.dados_empresa.get('snk_codlocal_estoque'),
                                    "lista_produtos":','.join(map(str,produtos))
                                })
            else:
                query = script.format_map({
                                    "codemp":self.codemp,
                                    "codemp_matriz":self.dados_empresa.get('snk_codemp_fornecedor'),
                                    "codlocais":self.dados_empresa.get('snk_codlocal_estoque'),
                                    "codprod":codprod,
                                    "controle":controle
                                })
        except Exception as e:
            erro = f"Falha ao formatar query do saldo de estoque por lote. {e}"
            print(erro)
            logger.error(erro)
            return False

        return query

    @token_snk
    async def buscar_saldo_por_lote(
            self,
            codprod:int=None,
            controle:str=None,
            lista_produtos:list=None
        ) -> dict:
        
        url = os.getenv('SANKHYA_URL_DBEXPLORER')
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False
        
        query = await self.formatar_query_busca_saldo_lote(codprod=codprod,
                                                           controle=controle,
                                                           lista_produtos=lista_produtos)

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
            erro = f"Erro ao validar estoque do(s) item(ns) na empresa {self.codemp}. {res.json()}" 
            logger.error(erro)
            print(erro)
            return False