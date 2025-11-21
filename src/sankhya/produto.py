import os
import requests
import time
from src.utils.decorador import carrega_dados_empresa
from src.utils.buscar_script import buscar_script
from src.utils.autenticador import token_snk
from src.utils.formatter import Formatter
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

class Produto:

    def __init__(self, codemp:int, empresa_id:int=None) -> None:
        self.token:str = None
        self.codemp:int = codemp
        self.empresa_id:int = empresa_id
        self.formatter = Formatter()
        self.dados_empresa:dict = {}
        self.req_time_sleep:float = float(os.getenv('REQ_TIME_SLEEP',1.5))
        self.tabela:str = os.getenv('SANKHYA_TABELA_PRODUTO')
        self.campos_atualiza_snk:list[str] = [ "ID", "IDPRODPAI", "ATIVO" ]

    @token_snk
    @carrega_dados_empresa
    async def buscar(
            self,
            codprod:int=None,
            idprod:int=None
        ) -> dict:
        """
        Busca os dados do produto no Sankhya.
            :param codprod: Código do produto no Sankhya.
            :param idprod: ID do produto no Olist.
        """

        if not any([codprod, idprod]):
            logger.error("Nenhum critério de busca fornecido. Deve ser informado 'codprod' ou 'idprod'.")
            return False

        url = os.getenv('SANKHYA_URL_DBEXPLORER')
        if not url:
            logger.error("Erro relacionado à url. %s",url)
            return False  

        parametero = 'SANKHYA_PATH_SCRIPT_PRODUTO'
        script = buscar_script(parametro=parametero)
        query = script.format_map({"codparc":self.dados_empresa.get('snk_codparc'),
                                   "codprod":codprod or 0,
                                   "idprod":idprod or 0})

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
            if idprod:
                logger.error("Erro ao buscar produto. ID %s. %s",idprod,res.text)                
            return False

    def preparar_dados(
            self,
            payload:dict
        ) -> dict:
        """
        Prepara os dados para atualização do produto no Sankhya.
            :param payload: Dicionário com os dados a serem atualizados no Sankhya.
        """        
        if not isinstance(payload, dict):
            logger.error("O payload deve ser um dicionário.")
            return False
        dados = {}
        for i in payload:
            dados[f'{self.campos_atualiza_snk.index(str.upper(i))}'] = f'{payload.get(i)}'
        return dados

    @token_snk
    @carrega_dados_empresa
    async def atualizar(
            self,
            codprod:int,
            payload:dict
        ) -> bool:
        """
        Atualiza os dados do produto no Sankhya.
            :param codprod: Código do produto no Sankhya.
            :param payload: Dicionário com os dados a serem atualizados no Sankhya.
        """
        if not isinstance(payload, dict):
            logger.error("O payload deve ser um dicionário.")
            return False

        url = os.getenv('SANKHYA_URL_SAVE')
        if not url:
            logger.error("Erro relacionado à url. %s",url)
            return False
        
        _payload = {
                "serviceName":"DatasetSP.save",
                "requestBody":{
                    "entityName":self.tabela,
                    "standAlone":False,
                    "fields":self.campos_atualiza_snk,
                    "records":[
                        {
                            "pk": {
                                "CODPROD": codprod,
                                "CODPARC": self.dados_empresa.get('snk_codparc')
                            },
                            "values": payload
                        }
                    ]
                }
            }

        res = requests.post(
            url=url,
            headers={ 'Authorization' : f"Bearer {self.token}" },
            json=_payload
        )

        if res.status_code in (200,201) and res.json().get('status')=='1':
            return True
        else:
            logger.error("Erro ao atualizar produto. Cód. %s. %s",codprod,res.text)
            return False        

    @token_snk
    @carrega_dados_empresa
    async def buscar_alteracoes(self) -> dict:
        """ Busca a lista de alterações nos cadastros de produtos integrados. """        

        url = os.getenv('SANKHYA_URL_LOAD_RECORDS')
        if not url:
            erro = f"Parâmetro da URL não encontrado"
            logger.error(erro)
            return False
        
        tabela = os.getenv('SANKHYA_TABELA_RASTRO_PRODUTO')
        if not tabela:
            erro = f"Parâmetro da tabela de rastro de produto não encontrado"
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
                                        "$": f"{self.dados_empresa.get('snk_codemp_fornecedor')}",
                                        "type": "I"
                                    }
                                ]
                            },                              
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
    @carrega_dados_empresa
    async def excluir_alteracoes(
            self,
            codprod:int=None,
            lista_produtos:list=None
        ) -> bool:
        """
        Remove os produtos atualizados da fila de atualização.
            :param codprod: Código do produto no Sankhya.
            :param lista_produtos: Lista dos códigos de produto.
        """

        if not any([codprod, lista_produtos]):
            logger.error("Código do produto não informado ou lista de produtos vazia")
            return False

        url = os.getenv('SANKHYA_URL_DELETE')
        if not url:
            logger.error("Erro relacionado à url. %s",url)
            return False

        tabela = os.getenv('SANKHYA_TABELA_RASTRO_PRODUTO')
        if not tabela:
            erro = f"Parâmetro da tabela de rastro de produto não encontrado"
            logger.error(erro)
            return False          
        
        if codprod:
            filter = [{"CODPROD": f"{codprod}","CODEMP": f"{self.dados_empresa.get('snk_codemp_fornecedor')}"}]
            
        if lista_produtos:
            filter = []
            for produto in lista_produtos:
                if isinstance(produto, dict):
                    if produto.get('sucesso'):
                        filter.append({"CODPROD": f"{produto.get('codprod')}","CODEMP": f"{self.dados_empresa.get('snk_codemp_fornecedor')}"})
                else:
                    try:
                        aux:dict=produto.__dict__
                        if aux.get('sucesso'):
                            filter.append({"CODPROD": f"{aux.get('codprod')}","CODEMP": f"{self.dados_empresa.get('snk_codemp_fornecedor')}"})
                    except:
                        logger.error("Erro ao extrair dados do objeto sqlalchemy.")
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
            return False