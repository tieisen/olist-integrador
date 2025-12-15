import os
import requests
from src.utils.decorador import carrega_dados_empresa, interno
from src.utils.autenticador import token_snk
from src.utils.formatter import Formatter
from src.utils.buscar_arquivo import buscar_script
from src.utils.log import set_logger
from src.utils.load_env import load_env
from src.utils.busca_paginada import paginar_snk
load_env()
logger = set_logger(__name__)

class Estoque:

    def __init__(self, codemp:int=None, empresa_id:int=None):
        self.token = None
        self.codemp = codemp
        self.empresa_id = empresa_id
        self.dados_empresa = None
        self.req_time_sleep = float(os.getenv('REQ_TIME_SLEEP',1.5))        
        self.formatter = Formatter()
    
    @token_snk
    @carrega_dados_empresa
    async def buscar(self,codprod:int=None,lista_produtos:list[int]=None) -> dict | list[dict]:
        """
        Busca estoque atual do(s) item(ns) no Sankhya.
            :param codprod: Código do produto no Sankhya.
            :param lista_produtos: Lista de códigos de produto no Sankhya.
            :return dict: dados do estoque
        """

        if not any([codprod, lista_produtos]):
            return False
        
        url = os.getenv('SANKHYA_URL_DBEXPLORER')
        if not url:
            logger.error("Erro relacionado à url. %s",url)
            return False

        filtro_produtos:str=''
        if codprod:
            filtro_produtos = str(codprod)
        if lista_produtos:   
            filtro_produtos = ','.join(map(str,lista_produtos))                

        parametero = 'SANKHYA_PATH_SCRIPT_SALDO_ESTOQUE'
        script = buscar_script(parametro=parametero)
        query = script.format_map({"codlocais": self.dados_empresa.get('snk_codlocal_estoque')+','+str(self.dados_empresa.get('snk_codlocal_ecommerce')),
                                   "codparc": self.dados_empresa.get('snk_codparc'),
                                   "codemp_fornecedor": self.dados_empresa.get('snk_codemp_fornecedor'),
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
            return False
    
    @carrega_dados_empresa
    @token_snk
    async def buscar_alteracoes(self) -> list[dict]:
        """
        Busca a lista de produtos com movimentação de estoque.
            :return list[dict]: lista de alterações
        """

        url = os.getenv('SANKHYA_URL_LOAD_RECORDS')
        if not url:
            logger.error("Erro relacionado à url. %s",url)
            return False

        tabela = os.getenv('SANKHYA_TABELA_RASTRO_ESTOQUE')
        if not tabela:
            erro = f"Parâmetro da tabela de rastro de estoque não encontrado"
            logger.error(erro)
            return False
        
        payload = {
                "serviceName": "CRUDServiceProvider.loadRecords",
                "requestBody": {
                    "dataSet": {
                        "rootEntity": tabela,
                        "includePresentationFields": "N",
                        "offsetPage": "0",
                        "criteria": {
                            "expression": {
                                "$": "this.CODEMP = ?"
                            },
                            "parameter": [
                                {
                                    "$": f"{self.dados_empresa.get('snk_codemp_fornecedor')}",
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

        res = await paginar_snk(token=self.token,url=url,payload=payload)
        return res
    
    @carrega_dados_empresa
    @token_snk
    async def remover_alteracoes(self,codprod:int=None,lista_produtos:list=None) -> bool:
        """
        Remove os produtos atualizados da fila de atualização.
            :param codprod: Código do produto no Sankhya.
            :param lista_produtos: Lista dos códigos de produto.
            :return bool: status da operação
        """

        if not any([codprod, lista_produtos]):
            logger.error("Código do produto não informado ou lista de produtos vazia")
            return False
        
        url = os.getenv('SANKHYA_URL_DELETE')
        if not url:
            logger.error("Erro relacionado à url. %s",url)
            return False
        
        tabela = os.getenv('SANKHYA_TABELA_RASTRO_ESTOQUE')
        if not tabela:
            erro = f"Parâmetro da tabela de rastro de estoque não encontrado"
            logger.error(erro)
            return False

        if codprod:
            filter = [
                {
                    "CODPROD": f"{codprod}",
                    "CODEMP": f"{self.dados_empresa.get('snk_codemp_fornecedor')}"
                }
            ]

        if lista_produtos:
            filter = []
            for produto in lista_produtos:
                if produto.get('sucesso'):
                    filter.append({
                        "CODPROD": f"{produto['ajuste_estoque'].get('codprod')}",
                        "CODEMP": f"{self.dados_empresa.get('snk_codemp_fornecedor')}"
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
            return False
    
    @interno
    @carrega_dados_empresa
    async def formatar_query_busca_saldo_lote(self,codprod:int=None,controle:str=None,lista_produtos:list=None) -> str:
        """
        Formata a query de busca de saldo de estoque por lote.
            :param codprod: Código do produto no Sankhya.
            :param controle: Lote do produto
            :param lista_produtos: Lista de códigos de produto e lotes no Sankhya.
            :return str: query formatada
        """
        if not lista_produtos and not all([codprod, controle]):
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
            logger.error(erro)
            return False

        return query

    @token_snk
    async def buscar_saldo_por_lote(self,codprod:int=None,controle:str=None,lista_produtos:list[dict]=None) -> list[dict]:
        """
        Busca saldo de estoque atual desmembrado por lote.
            :param codprod: Código do produto no Sankhya.
            :param controle: Lote do produto
            :param lista_produtos: Lista de códigos de produto e lotes no Sankhya.
            :return list[dict]: lista de dicionários com os dados do estoque            
        """
        
        url = os.getenv('SANKHYA_URL_DBEXPLORER')
        if not url:
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
            return False
    
    @interno
    @carrega_dados_empresa
    async def formatar_query_busca_saldo_local(self,codprod:int=None,lista_produtos:list[int]=None) -> str:

        """
        Formata a query de busca de saldo de estoque por local.
            :param codprod: Código do produto no Sankhya.
            :param lista_produtos: Lista de códigos de produto no Sankhya.
            :return str: query formatada
        """        
            
        if not any([lista_produtos,codprod]):
            return False
        
        parametro = 'SANKHYA_PATH_SCRIPT_ESTOQUE_LOCAL'
        script = buscar_script(parametro=parametro)

        try:
            query = script.format_map({
                                "codemp_fornecedor":self.dados_empresa.get('snk_codemp_fornecedor'),
                                "local_matriz":self.dados_empresa.get('snk_codlocal_venda'),
                                "local_ecommerce":self.dados_empresa.get('snk_codlocal_ecommerce'),
                                "produtos": codprod or ','.join(map(str,lista_produtos))
                            })
        except Exception as e:
            erro = f"Falha ao formatar query do saldo de estoque por local. {e}"
            logger.error(erro)
            return False

        return query
    
    @token_snk
    async def buscar_saldo_por_local(self,codprod:int=None,lista_produtos:list[int]=None) -> dict:
        """
        Busca saldo de estoque atual por local.
            :param codprod: Código do produto no Sankhya.
            :param lista_produtos: Lista de códigos de produto no Sankhya.
            :return list[dict]: lista de dicionários com os dados do estoque            
        """

        url = os.getenv('SANKHYA_URL_DBEXPLORER')
        if not url:
            logger.error("Erro relacionado à url. %s",url)
            return False
        
        query = await self.formatar_query_busca_saldo_local(codprod=codprod,
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
            erro = f"Erro ao validar estoque do(s) item(ns) na empresa {self.dados_empresa.get('snk_codemp_fornecedor')}. {res.json()}" 
            logger.error(erro)
            return False
    
    @interno
    @carrega_dados_empresa
    async def formatar_query_busca_saldo_ecommerce(self,lista_produtos:list[int]) -> str:
        """
        Formata a query de busca de saldo de estoque no local ECOMMERCE.
            :param lista_produtos: Lista de códigos de produto no Sankhya.
            :return str: query formatada
        """        
        
        parametro = 'SANKHYA_PATH_SCRIPT_ESTOQUE_ECOMMERCE_LOTE'
        script = buscar_script(parametro=parametro)

        try:
            query = script.format_map({
                                "codemp_fornecedor":self.dados_empresa.get('snk_codemp_fornecedor'),
                                "local_ecommerce":self.dados_empresa.get('snk_codlocal_ecommerce'),
                                "produtos": ','.join(map(str,lista_produtos))
                            })
        except Exception as e:
            erro = f"Falha ao formatar query do saldo de estoque por local. {e}"
            logger.error(erro)
            return False

        return query
    
    @token_snk
    async def buscar_saldo_ecommerce_por_lote(self,lista_produtos:list[int]) -> list[dict]:
        """
        Busca saldo de estoque atual no local ECOMMERCE, desmembrado por lote.
            :param lista_produtos: Lista de códigos de produto no Sankhya.
            :return list[dict]: lista de dicionários com os dados do estoque            
        """

        url = os.getenv('SANKHYA_URL_DBEXPLORER')
        if not url:
            logger.error("Erro relacionado à url. %s",url)
            return False
        
        query = await self.formatar_query_busca_saldo_ecommerce(lista_produtos=lista_produtos)

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
            erro = f"Erro ao validar estoque do(s) item(ns) de e-commerce na empresa {self.dados_empresa.get('snk_codemp_fornecedor')}. {res.json()}" 
            logger.error(erro)
            return False