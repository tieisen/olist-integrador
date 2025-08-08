import os
import logging
import requests
from datetime import datetime
from dotenv import load_dotenv
from src.utils.formatter import Formatter
from src.sankhya.connect import Connect
from src.utils.log import Log

load_dotenv('keys/.env')
logger = logging.getLogger(__name__)
logging.basicConfig(filename=Log().buscar_path(),
                    encoding='utf-8',
                    format=os.getenv('LOGGER_FORMAT'),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

class Transferencia:

    def __init__(self):
        self.nunota = None
        self.con = Connect()
        self.formatter = Formatter()
        self.campos_cabecalho_transferencia = ["NUNOTA","DTNEG","STATUSNOTA"]
        self.criterios_nota_transferencia = "this.STATUSNOTA = 'A' AND TIPMOV = 'T' AND CODEMP = 1 AND CODEMPNEGOC = 31 AND TRUNC(DTNEG) = ?"
        self.campos_cabecalho = [
                "AD_IDSHOPEE", "AD_MKP_CODPED", "AD_MKP_DESTINO", "AD_MKP_DHCHECKOUT", "AD_MKP_ID",
                "AD_MKP_IDNFE", "AD_MKP_NUMPED", "AD_MKP_ORIGEM", "AD_TAXASHOPEE", "APROVADO",
                "BASEICMS", "BASEIPI", "BASEIRF", "BASEISS", "BASESUBSTIT", "CIF_FOB", "CLASSIFICMS",
                "CODCENCUS", "CODCIDORIGEM", "CODEMP", "CODEMPNEGOC", "CODNAT", "CODPARC", "CODTIPOPER",
                "CODTIPVENDA", "CODUFDESTINO", "CODUFENTREGA", "CODUSU", "CODUSUINC",
                "CODVEND", "CONFIRMADA", "DHTIPOPER", "DHTIPVENDA", "DTMOV", "DTNEG", "NUNOTA",
                "NUMNOTA", "OBSERVACAO", "PENDENTE", "PESO", "PESOBRUTO", "QTDVOL", "TIPMOV",
                "VLRDESCTOT", "VLRDESCTOTITEM", "VLRFRETE", "VLRICMS", "VLRICMSDIFALDEST",
                "VLRICMSDIFALREM", "VLRICMSFCP", "VLRICMSFCPINT", "VLRIPI", "VLRIRF", "VLRISS",
                "VLRNOTA", "VLRSUBST", "VLRSTFCPINTANT", "VOLUME"
            ]

    def extrai_nunota(self,payload:dict=None):
        return int(payload.get('responseBody').get('pk').get('NUNOTA').get('$'))

    async def criar(self, cabecalho:dict=None, itens:list=None) -> tuple:

        if not all([cabecalho, itens]):
            print("Dados não informados")
            logger.error("Dados não informados")
            return False

        url = os.getenv('SANKHYA_URL_PEDIDO')
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

        payload = {
            "serviceName":"CACSP.incluirNota",
            "requestBody":{
                "nota":{
                    "cabecalho":cabecalho,
                    "itens":{
                        "INFORMARPRECO":"True",
                        "item":itens
                    }
                }
            }
        }

        res = requests.get(
            url=url,
            headers={ 'Authorization': token },
            json=payload
        )
        
        if res.status_code in (200,201):
            if res.json().get('status')=='0':
                logger.error("Erro ao lançar nota de transferência. %s",res.json())
                print(f"Erro ao lançar nota de transferência. {res.json()}")    
                return False, None
            if res.json().get('status') in ['1','2']:
                return True, self.extrai_nunota(res.json())
        else:
            logger.error("Erro ao lançar nota de transferência. %s",res.json())
            print(f"Erro ao lançar nota de transferência. {res.json()}")
            return False, None

    async def buscar(self, itens:bool=False) -> dict:

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
                        "rootEntity": "CabecalhoNota",
                        "includePresentationFields": "N",
                        "offsetPage": "0",
                        "criteria": {
                            "expression": {
                                "$": self.criterios_nota_transferencia
                            },
                            "parameter": [
                                {
                                    "$": f"{datetime.now().strftime('%d/%m/%Y')}",
                                    "type": "D"
                                }
                            ]
                        },
                        "entity": {
                            "fieldset": {
                                "list": ','.join(self.campos_cabecalho_transferencia)                            
                            }
                        }
                    }
                }
            })
        
        if res.status_code in (200,201):
            if res.json().get('status') in ['0', '2']:
                print(res.json())
                return False, {}
            if res.json().get('status')=='1':
                dados_nota = self.formatter.return_format(res.json())
                if isinstance(dados_nota, dict):
                    try:
                        self.nunota = int(dados_nota.get('nunota'))
                    except:
                        pass
                    finally:
                        return True, dados_nota
                if isinstance(dados_nota, list):
                    dados_nota = dados_nota[0]
                    self.nunota = int(dados_nota.get('nunota'))
                    if itens:
                        dados_itens = await Itens().buscar(token=token, nunota=int(dados_nota.get('nunota')))
                        dados_nota['itens'] = dados_itens
                    return True, dados_nota
        else:
            logger.error("Erro ao buscar dados da nota de transferência vigente. %s",res.json())
            print(f"Erro ao buscar dados da nota de transferência vigente. {res.json()}")
            return False

    async def confirmar(self, nunota:int=None) -> bool:

        if not nunota:
            nunota = self.nunota
            if not nunota:
                print("Número único da nota não informado")
                return False
            
        url = os.getenv('SANKHYA_URL_CONFIRMA_PEDIDO')
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
                "serviceName": "ServicosNfeSP.confirmarNota",
                "requestBody": {
                    "nota": {
                        "compensarNotaAutomaticamente": "false",
                        "NUNOTA": {
                            "$": f"{nunota}"
                        }
                    }
                }
            })
        
        if res.status_code in (200,201) and res.json().get('status')=='1':
            return True
        else:
            logger.error("Erro ao confirmar nota. Nunota %s. %s",nunota,res.json())
            print(f"Erro ao confirmar nota. Nunota {nunota}. {res.json()}")
            return False
        
class Itens(Transferencia):

    def __init__(self):
        super().__init__()
        self.codtab_transferencia = int(os.getenv('SANKHYA_CODTAB_TRANSFERENCIA'))
        self.formatter = Formatter()
        self.campos_item = [
                "ATUALESTOQUE", "CODANTECIPST", "CODEMP", "CODLOCALORIG", "CODPROD", "CONTROLE",
                "CODTRIB","CODVEND", "CODVOL", "NUNOTA", "QTDNEG", "RESERVA", "SEQUENCIA",
                "STATUSNOTA", "USOPROD", "VLRDESC", "VLRTOT", "VLRUNIT"
            ]            


    async def lancar(self, nunota:int=None, dados_item:dict=None ):

        if not nunota:
            nunota = self.nunota
            if not nunota:
                print("Número único do pedido não informado")
                logger.error("Número único do pedido não informado")
                return False
        
        if not dados_item:
            print("Dados dos itens não informados")
            logger.error("Dados dos itens não informados")
            return False
        
        if not isinstance(dados_item, dict):
            print("Dados dos itens devem ser um dicionário")
            logger.error("Dados dos itens devem ser um dicionário")
            return False
        
        url = os.getenv('SANKHYA_URL_PEDIDO_ALTERA_ITEM')
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False
        
        try:
            token = self.con.get_token()
        except Exception as e:
            logger.error("Erro relacionado ao token de acesso. %s",e)
            return False          

        payload = {
            "serviceName": "CACSP.incluirAlterarItemNota",
            "requestBody": {
                "nota": {
                    "NUNOTA": f"{nunota}",
                    "itens": {
                        "item": dados_item
                    }
                }
            }
        }

        res = requests.get(
            url=url,
            headers={ 'Authorization': token },
            json=payload)
        
        if res.status_code in (200,201) and res.json().get('status')=='1':
            return True
        if res.status_code in (200,201) and res.json().get('status')=='2':
            print(f"Obs: Nunota {nunota}. {res.json()}")      
            return True
        else:
            print(f"Erro ao lançar item na nota de transferência. Nunota {nunota}. {res.json()}")
            logger.error("Erro ao lançar item da nota de transferência. Nunota %s. %s",nunota,res.json())      
            return False

    async def buscar(self, nunota:int=None) -> dict:

        if not nunota:
            nunota = self.nunota
            if not nunota:
                print("Número único do pedido não informado")
                logger.error("Número único do pedido não informado")
                return False
    
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

        criteria = {
            "expression": {
                "$": "this.NUNOTA = ? AND SEQUENCIA > 0"
            },
            "parameter": [
                {
                    "$": f"{nunota or self.nunota}",
                    "type": "I"
                }
            ]
        }

        res = requests.get(
            url=url,
            headers={ 'Authorization': token },
            json={
                "serviceName": "CRUDServiceProvider.loadRecords",
                "requestBody": {
                    "dataSet": {
                        "rootEntity": "ItemNota",
                        "includePresentationFields": "N",
                        "offsetPage": "0",
                        "criteria": criteria,
                        "entity": {
                            "fieldset": {
                                "list": ','.join(self.campos_item)
                            }
                        }
                    }
                }
            })
        
        if res.status_code in (200,201) and res.json().get('status')=='1':            
            return self.formatter.return_format(res.json())
        else:
            print(f"Erro ao buscar itens do pedido. Nunota {nunota}. {res.json()}")
            logger.error("Erro ao buscar itens do pedido. Nunota %s. %s",nunota,res.json())      
            return False

    async def busca_valor_transferencia(self, codprod:int=None, lista_itens:list=None) -> float:
        if not any([codprod, lista_itens]):
            print("Produto não informado.")
            logger.error("Produto não informado.")
            return False

        url = os.getenv('SANKHYA_URL_DBEXPLORER')
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
            query = f'''
                SELECT 
                    EXC.CODPROD,
                    FIRST_VALUE(EXC.VLRVENDA) 
                          OVER (ORDER BY TAB.DTVIGOR DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) VALOR 
                FROM TGFNTA NTA
                    INNER JOIN TGFTAB TAB ON NTA.CODTAB = TAB.CODTAB
                    INNER JOIN TGFEXC EXC ON TAB.NUTAB = EXC.NUTAB
                WHERE NTA.CODTAB = {self.codtab_transferencia}
                    AND ROWNUM = 1
                    AND EXC.CODPROD = {codprod}
            '''

        if lista_itens:            
            query = f'''
                SELECT DISTINCT
                    EXC.CODPROD,
                    FIRST_VALUE(EXC.VLRVENDA)
                          OVER (PARTITION BY EXC.CODPROD ORDER BY TAB.DTVIGOR DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) VALOR
                FROM TGFNTA NTA
                    INNER JOIN TGFTAB TAB ON NTA.CODTAB = TAB.CODTAB
                    INNER JOIN TGFEXC EXC ON TAB.NUTAB = EXC.NUTAB
                WHERE NTA.CODTAB = {self.codtab_transferencia}
                    AND EXC.CODPROD IN ({','.join([str(i) for i in lista_itens])})
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
            logger.error("Erro ao buscar valor(es) do(s) item(ns) na tabela de transferências. %s",res.json())
            print(f"Erro ao buscar valor(es) do(s) item(ns) na tabela de transferências. {res.json()}")
            return False