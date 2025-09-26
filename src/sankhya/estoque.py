import os
import logging
import requests
from dotenv import load_dotenv
from src.sankhya.connect import Connect
from src.utils.formatter import Formatter
from src.utils.log import Log

load_dotenv('keys/.env')
logger = logging.getLogger(__name__)
logging.basicConfig(filename=Log().buscar_path(),
                    encoding='utf-8',
                    format=os.getenv('LOGGER_FORMAT'),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

class Estoque:

    def __init__(self):   
        self.con = Connect()  
        self.formatter = Formatter()
        
    async def buscar(self, codprod:int=None, lista_produtos:list=None) -> dict:
        if not any([codprod, lista_produtos]):
            print("Código do produto não informado ou lista de produtos vazia")
            return False
        
        url = os.getenv('SANKHYA_URL_LOAD_VIEW')
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False  
              
        try:
            token = self.con.get_token()
        except Exception as e:
            print(f"Erro relacionado ao token de acesso. {e}")
            logger.error("Erro relacionado ao token de acesso. %s",e)
            return False        

        if codprod:
            filter = f"CODPROD = {codprod}"

        if lista_produtos:
            if len(lista_produtos) == 1:
                filter = f"CODPROD = {lista_produtos[0]}"
            else:
                filter = f"CODPROD IN ({','.join(map(str,lista_produtos))})"

        res = requests.get(
            url=url,
            headers={ 'Authorization': token },
            json={
                "serviceName": "CRUDServiceProvider.loadView",
                "requestBody": {
                    "query": {
                        "viewName": "AD_OLISTESTOQUE",
                        "where": {
                            "$": filter
                        },
                        "fields": {
                            "field": {
                                "$": "*"
                            }
                        }
                    }
                }
            })

        if res.status_code in (200,201) and res.json().get('status')=='1':
            return self.formatter.return_format(res.json())
        else:
            logger.error("Erro ao buscar saldo de estoque do item %s. %s",codprod,res.json())
            print(f"Erro ao buscar saldo de estoque do item {codprod}. {res.json()}")
            return False
        
    async def buscar_alteracoes(self) -> dict:

        url = os.getenv('SANKHYA_URL_LOAD_RECORDS')
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False 
         
        try:
            token = self.con.get_token()
        except Exception as e:
            logger.error("Erro relacionado ao token de acesso. %s",e)
            return False      

        res = requests.get(
            url=url,
            headers={ 'Authorization': token },
            json={
                "serviceName": "CRUDServiceProvider.loadRecords",
                "requestBody": {
                    "dataSet": {
                        "rootEntity": "AD_OLISTRASTEST",
                        "includePresentationFields": "N",
                        "offsetPage": "0",
                        "entity": {
                            "fieldset": {
                                "list": '*'
                            }
                        }
                    }
                }
            })

        if res.status_code in (200,201) and res.json().get('status')=='1':
            return self.formatter.return_format(res.json())
        else:
            logger.error("Erro ao buscar alterações pendentes. %s",res.json())
            print(f"Erro ao buscar alterações pendentes. {res.json()}")
            return False
        
    async def remover_alteracoes(self, codprod:int=None, lista_produtos:list=None) -> dict:

        if not any([codprod, lista_produtos]):
            print("Código do produto não informado ou lista de produtos vazia")
            logger.error("Código do produto não informado ou lista de produtos vazia")
            return False
        
        url = os.getenv('SANKHYA_URL_DELETE')
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False
        
        try:
            token = self.con.get_token()
        except Exception as e:
            print(f"Erro relacionado ao token de acesso. {e}")
            logger.error("Erro relacionado ao token de acesso. %s",e)
            return False      

        if codprod:
            filter = [{"CODPROD": f"{codprod}"}]

        if lista_produtos:
            filter = []
            for produto in lista_produtos:
                if produto.get('sucesso'):
                    filter.append({"CODPROD": f"{produto['ajuste_estoque'].get('codprod')}"})
        
        print("Enviando dados para remoção")
        res = requests.get(
            url=url,
            headers={ 'Authorization': token },
            json={
                "serviceName": "DatasetSP.removeRecord",
                "requestBody": {
                    "entityName": "AD_OLISTRASTEST",
                    "standAlone": False,
                    "pks": filter
                }
            })

        if res.status_code in (200,201) and res.json().get('status')=='1':
            return True
        else:
            logger.error("Erro ao remover alterações pendentes. %s",res.json())
            print(f"Erro ao remover alterações pendentes. {res.json()}")
            return False
        
    async def buscar_saldo_por_lote(self, codprod:int=None, controle:str=None, qtd:int=None, lista_produtos:list=None) -> dict:       

        if not lista_produtos and not all([codprod, controle, qtd]):
            print("Código do produto, controle de lote e quantidade não informados")
            logger.error("Código do produto, controle de lote e quantidade não informados")
            return False
        
        url = os.getenv('SANKHYA_URL_DBEXPLORER')
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False

        try:
            token = self.con.get_token()
        except Exception as e:
            logger.error("Erro relacionado ao token de acesso. %s",e)
            return False        

        query = f'''
            SELECT PRO.CODPROD, EST.CONTROLE, NVL(SUM(EST.ESTOQUE),0) QTD, NVL(PRO.AGRUPMIN,1) AGRUPMIN
            FROM TGFPRO PRO
                LEFT JOIN TGFEST EST ON EST.CODPROD = PRO.CODPROD
                                    AND EST.CODEMP = 31
                                    AND EST.CODLOCAL IN (101,911)
                                    AND TRIM(EST.CONTROLE) IS NOT NULL
            WHERE EST.CODPROD = {codprod}
                AND EST.CONTROLE = '{controle}'
            GROUP BY PRO.CODPROD, EST.CONTROLE, PRO.AGRUPMIN
        '''

        if lista_produtos:
            produtos = [produto.get('codprod') for produto in lista_produtos]
            query = f'''
                SELECT PRO.CODPROD, EST.CONTROLE, NVL(SUM(EST.ESTOQUE),0) QTD, NVL(PRO.AGRUPMIN,1) AGRUPMIN
                FROM TGFPRO PRO
                    LEFT JOIN TGFEST EST ON EST.CODPROD = PRO.CODPROD
                                        AND EST.CODEMP = 31
                                        AND EST.CODLOCAL IN (101,911)
                                        AND TRIM(EST.CONTROLE) IS NOT NULL
                WHERE PRO.CODPROD IN ({','.join(map(str,produtos))})
                GROUP BY PRO.CODPROD, EST.CONTROLE, PRO.AGRUPMIN
            '''

        res = requests.get(
            url=url,
            headers={ 'Authorization': token },
            json={
                "serviceName": "DbExplorerSP.executeQuery",
                "requestBody": {
                    "sql":query
                }
            })
        
        if res.status_code in (200,201) and res.json().get('status')=='1':
            return self.formatter.return_format(res.json())
        else:
            logger.error("Erro validar estoque do item %s na empresa 31. %s",codprod,res.json())
            print(f"Erro ao validar estoque do item {codprod} na empresa 31. {res.json()}")
            return False