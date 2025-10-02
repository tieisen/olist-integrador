import os
import time
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
        self.req_time_sleep = int(os.getenv('REQ_TIME_SLEEP', 1))

        
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

        offset = 0
        limite_alcancado = False
        todos_resultados = []

        while not limite_alcancado:
            time.sleep(self.req_time_sleep)
            payload = {
                "serviceName": "CRUDServiceProvider.loadRecords",
                "requestBody": {
                    "dataSet": {
                        "rootEntity": "AD_OLISTRASTEST",
                        "includePresentationFields": "N",
                        "offsetPage": offset,
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
                headers={ 'Authorization':f"{token}" },
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
            SELECT PRO.CODPROD, TRIM(EST.CONTROLE) CONTROLE, EST.QTD, NVL(PRO.AGRUPMIN,1) AGRUPMIN, EST2.QTDMATRIZ
            FROM TGFPRO PRO
                LEFT JOIN (SELECT CODPROD, CONTROLE, SUM(ESTOQUE) QTD
                           FROM TGFEST EST
                           WHERE EST.CODEMP = 31
                               AND EST.CODLOCAL IN (101,911)
                               AND TRIM(EST.CONTROLE) = '{controle}'
                           GROUP BY CODPROD, CONTROLE
                        ) EST ON EST.CODPROD = PRO.CODPROD
                LEFT JOIN (SELECT CODPROD, SUM(ESTOQUE-RESERVADO) QTDMATRIZ
                           FROM TGFEST EST
                           WHERE EST.CODEMP = 1
                               AND EST.CODLOCAL IN (101,911)
                               AND TRIM(EST.CONTROLE) = '{controle}'
                           GROUP BY CODPROD
                        ) EST2 ON EST2.CODPROD = PRO.CODPROD                                  
            WHERE EST.CODPROD = {codprod}
        '''

        if lista_produtos:
            produtos = [produto.get('codprod') for produto in lista_produtos]
            query = f'''
                SELECT PRO.CODPROD, TRIM(EST.CONTROLE) CONTROLE, EST.QTD, NVL(PRO.AGRUPMIN,1) AGRUPMIN, EST2.QTDMATRIZ
                FROM TGFPRO PRO
                    LEFT JOIN (SELECT CODPROD, CONTROLE, SUM(ESTOQUE) QTD
                                FROM TGFEST EST
                                WHERE EST.CODEMP = 31
                                    AND EST.CODLOCAL IN (101,911)
                                    AND TRIM(EST.CONTROLE) IS NOT NULL
                                GROUP BY CODPROD, CONTROLE
                            ) EST ON EST.CODPROD = PRO.CODPROD
                    LEFT JOIN (SELECT CODPROD, SUM(ESTOQUE-RESERVADO) QTDMATRIZ
                                FROM TGFEST EST
                                WHERE EST.CODEMP = 1
                                    AND EST.CODLOCAL IN (101,911)
                                    AND TRIM(EST.CONTROLE) IS NOT NULL
                                GROUP BY CODPROD
                            ) EST2 ON EST2.CODPROD = PRO.CODPROD
                WHERE PRO.CODPROD IN ({','.join(map(str,produtos))})
                    AND PRO.TIPCONTEST != 'N'

                UNION ALL

                SELECT PRO.CODPROD, '' CONTROLE, EST.QTD, NVL(PRO.AGRUPMIN,1) AGRUPMIN, EST2.QTDMATRIZ
                FROM TGFPRO PRO
                    LEFT JOIN (SELECT CODPROD, CONTROLE, SUM(ESTOQUE) QTD
                                FROM TGFEST EST
                                WHERE EST.CODEMP = 31
                                    AND EST.CODLOCAL IN (101,911)
                                    AND EST.CONTROLE IS NOT NULL
                                GROUP BY CODPROD, CONTROLE
                            ) EST ON EST.CODPROD = PRO.CODPROD
                    LEFT JOIN (SELECT CODPROD, SUM(ESTOQUE-RESERVADO) QTDMATRIZ
                                FROM TGFEST EST
                                WHERE EST.CODEMP = 1
                                    AND EST.CODLOCAL IN (101,911)
                                    AND EST.CONTROLE IS NOT NULL
                                GROUP BY CODPROD
                            ) EST2 ON EST2.CODPROD = PRO.CODPROD
                WHERE PRO.CODPROD IN ({','.join(map(str,produtos))})
                    AND PRO.TIPCONTEST = 'N'
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