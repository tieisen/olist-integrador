import os
import logging
import requests
from datetime import datetime
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

class Pedido:

    def __init__(self):   
        self.con = Connect()  
        self.formatter = Formatter()
        self.campos_cabecalho = [
                "AD_IDSHOPEE", "AD_MKP_CODPED", "AD_MKP_DESTINO", "AD_MKP_DHCHECKOUT", "AD_MKP_ID",
                "AD_MKP_IDNFE", "AD_MKP_NUMPED", "AD_MKP_ORIGEM", "AD_TAXASHOPEE", "APROVADO",
                "BASEICMS", "BASEIPI", "BASEIRF", "BASEISS", "BASESUBSTIT", "CIF_FOB", "CLASSIFICMS",
                "CODCENCUS", "CODCIDORIGEM", "CODEMP", "CODEMPNEGOC", "CODNAT", "CODPARC", "CODTIPOPER",
                "CODTIPVENDA", "CODUFDESTINO", "CODUFENTREGA", "CODUSU", "CODUSUINC",
                "CODVEND", "CONFIRMADA", "DHTIPOPER", "DHTIPVENDA", "DTMOV", "DTNEG", "NUNOTA",
                "NUMNOTA", "OBSERVACAO", "PENDENTE", "PESO", "PESOBRUTO", "QTDVOL", "STATUSNOTA", "TIPMOV",
                "VLRDESCTOT", "VLRDESCTOTITEM", "VLRFRETE", "VLRICMS", "VLRICMSDIFALDEST",
                "VLRICMSDIFALREM", "VLRICMSFCP", "VLRICMSFCPINT", "VLRIPI", "VLRIRF", "VLRISS",
                "VLRNOTA", "VLRSUBST", "VLRSTFCPINTANT", "VOLUME"
            ]

    def extrai_nunota(self,payload:dict=None):
        return int(payload.get('responseBody').get('pk').get('NUNOTA').get('$'))
    
    async def buscar(self, nunota:int=None, id_olist:int=None, codpedido:str=None, itens:bool=False) -> dict:
        
        if not any([nunota, id_olist, codpedido]):
            print("Nenhum critério de busca informado. Informe nunota, id_olist ou codpedido.")
            logger.error("Nenhum critério de busca informado. Informe nunota, id_olist ou codpedido.")
            return False

        url = os.getenv('SANKHYA_URL_LOAD_RECORDS')
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False
        
        try:
            # print("Buscando token...")
            token = self.con.get_token()
            # print(f"Token encontrado.")
        except Exception as e:
            logger.error("Erro relacionado ao token de acesso. %s",e)
            return False

        if nunota:
            criteria = {
                "expression": {
                    "$": "this.NUNOTA = ? AND TIPMOV = 'P'"
                },
                "parameter": [
                    {
                        "$": f"{nunota}",
                        "type": "I"
                    }
                ]
            }

        if id_olist:
            criteria = {
                "expression": {
                    "$": "this.AD_MKP_ID = ? AND TIPMOV = 'P'"
                },
                "parameter": [
                    {
                        "$": f"{id_olist}",
                        "type": "I"
                    }
                ]
            }

        if codpedido:
            criteria = {
                "expression": {
                    "$": "this.AD_MKP_CODPED = ? AND TIPMOV = 'P'"
                },
                "parameter": [
                    {
                        "$": f"{codpedido}",
                        "type": "S"
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
                        "rootEntity": "CabecalhoNota",
                        "includePresentationFields": "N",
                        "offsetPage": "0",
                        "criteria": criteria,
                        "entity": {
                            "fieldset": {
                                "list": ','.join(self.campos_cabecalho)
                            }
                        }
                    }
                }
            })
        
        if res.status_code in (200,201) and res.json().get('status')=='1':
            try:
                dados_pedido = self.formatter.return_format(res.json())[0]
                if itens:
                    dados_itens = await Itens().buscar(token=token, nunota=int(dados_pedido.get('nunota')))
                    dados_pedido['itens'] = dados_itens
                return dados_pedido
            except:
                return False
          
        else:
            if nunota:
                print(f"Erro ao buscar pedido. Nunota {nunota}. {res.text}")
                logger.error("Erro ao buscar pedido. Nunota %s. %s",nunota,res.text)
            if id_olist:
                print(f"Erro ao buscar pedido. ID {id_olist}. {res.text}")
                logger.error("Erro ao buscar pedido. ID %s. %s",id_olist,res.text)
            if codpedido:
                print(f"Erro ao buscar pedido. Pedido {codpedido}. {res.text}")        
                logger.error("Erro ao buscar pedido. Pedido %s. %s",codpedido,res.text)        
            return False
    
    async def buscar_nunota_nota(self, nunota:int) -> dict:
        
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
            SELECT VAR.NUNOTA
            FROM TGFVAR VAR
            WHERE VAR.NUNOTAORIG = {nunota} AND ROWNUM = 1
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
            logger.error("Erro ao buscar número da nota. %s",res.json())
            print(f"Erro ao buscar número da nota. {res.json()}")
            return False

    async def buscar_cidade(self, ibge:int=None) -> dict:

        if not ibge:
            print("Nenhum código IBGE informado.")
            logger.error("Nenhum código IBGE informado.")
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

        payload = {
            "serviceName": "CRUDServiceProvider.loadRecords",
            "requestBody": {
                "dataSet": {
                    "rootEntity": "Cidade",
                    "includePresentationFields": "S",
                    "offsetPage": "0",
                    "criteria": {
                        "expression": {
                            "$": "this.CODMUNFIS = ?"
                        },
                        "parameter": [
                            {
                                "$": f"{ibge}",
                                "type": "I"
                            }
                        ]
                    },
                    "entity": {
                        "fieldset": {
                            "list": "CODCID, UF"
                        }
                    }
                }
            }
        }

        res = requests.get(
            url=url,
            headers={ 'Authorization': token },
            json=payload)
        
        if res.status_code in (200,201) and res.json().get('status')=='1':
            return self.formatter.return_format(res.json())[0]
        else:
            logger.error("Erro ao buscar dados de localização da cidade %s. %s",ibge,res.text)
            print(f"Erro ao buscar dados de localização da cidade {ibge}. {res.text}")
            return False

    async def lancar(self, dados_cabecalho:dict=None, dados_itens:list=None) -> int:
        
        if not all([dados_cabecalho, dados_itens]):
            print("Dados do cabeçalho ou dos itens não informados.")
            logger.error("Dados do cabeçalho ou dos itens não informados.")
            return False
        
        url = os.getenv('SANKHYA_URL_PEDIDO')
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
                "serviceName":"CACSP.incluirNota",
                "requestBody":{
                    "nota":{
                        "cabecalho":dados_cabecalho,
                        "itens":{
                            "INFORMARPRECO":"True",
                            "item": dados_itens
                        }
                    }
                }
            })
        if res.status_code in (200,201):
            if res.json().get('status')=='0':                
                logger.error("Erro ao lançar pedido. %s",res.text)
                return 0
            if res.json().get('status')=='1':
                return self.extrai_nunota(res.json())
            if res.json().get('status')=='2':
                logger.error("Pedido %s lançado com erro. %s",self.extrai_nunota(res.json()),res.text)                
                return self.extrai_nunota(res.json())
        else:
            logger.error("Erro ao lançar pedido #%s. %s",dados_cabecalho.get('AD_MKP_NUMPED'),res.text)
            print(f"Erro ao lançar pedido #{dados_cabecalho.get('AD_MKP_NUMPED')}. {res.text}")
            return False

    async def confirmar(self, nunota:int=None) -> bool:

        if not nunota:
            print("Número único do pedido não informado.")
            logger.error("Número único do pedido não informado.")
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
        if res.status_code in (200,201) and res.json().get('status')=='0' and res.json().get('statusMessage') == f"A nota {nunota} já foi confirmada.":
            return True        
        print(f"Erro ao confirmar pedido. Nunota {nunota}. {res.text}")
        logger.error("Erro ao confirmar pedido. Nunota %s. %s",nunota,res.text)
        return False

    async def faturar(self, nunota:int=None, dt_fatur:str=None) -> tuple[bool,int]:

        if not nunota:
            print("Número único do pedido não informado.")
            logger.error("Número único do pedido não informado.")
            return False, None
        
        url = os.getenv('SANKHYA_URL_FATURA_PEDIDO')
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False, None
        
        try:
            token = self.con.get_token()
        except Exception as e:
            logger.error("Erro relacionado ao token de acesso. %s",e)
            return False, None

        data_faturamento = datetime.strptime(dt_fatur,'%Y-%m-%d').strftime('%d/%m/%Y') if dt_fatur else datetime.now().strftime('%d/%m/%Y')
        if not data_faturamento:
            print("Data de faturamento não informada.")
            logger.error("Data de faturamento não informada.")
            return False, None

        body = {
            "serviceName":"SelecaoDocumentoSP.faturar",
            "requestBody":{
                "notas":{
                    "codTipOper":os.getenv('SANKHYA_CODTIPOPER_NOTA'),
                    "serie":"2",
                    "dtFaturamento":data_faturamento,
                    "tipoFaturamento":"FaturamentoNormal",
                    "dataValidada":"true",
                    "notasComMoeda":{
                        
                    },
                    "nota":[
                        {
                            "$":nunota
                        }
                    ],
                    "codLocalDestino":"",
                    "faturarTodosItens":"true",
                    "umaNotaParaCada":"false",
                    "ehWizardFaturamento":"true",
                    "dtFixaVenc":"",
                    "ehPedidoWeb":"false",
                    "nfeDevolucaoViaRecusa":"false"
                }                
            }
        }

        res = requests.post(
            url=url,
            headers={ 'Authorization': token },
            json=body)
        
        if res.status_code in (200,201) and res.json().get('status') in ['1', '2']:
            return True, int(res.json().get('responseBody').get('notas').get('nota').get('$'))
        else:
            print(f"Erro ao faturar pedido. Nunota {nunota}. {res.text}")
            logger.error("Erro ao faturar pedido. Nunota %s. %s",nunota,res.text)
            return False, None

    async def devolver(self, nunota:int, itens:list) -> bool:

        url = os.getenv('SANKHYA_URL_FATURA_PEDIDO')
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False
        
        try:
            token = self.con.get_token()
        except Exception as e:
            logger.error("Erro relacionado ao token de acesso. %s",e)
            return False

        body = {
            "serviceName": "SelecaoDocumentoSP.faturar",
            "requestBody": {
                "notas": {
                    "codTipOper": int(os.getenv('SANKHYA_CODTIPOPER_DEVOLUCAO')),
                    "serie": os.getenv('SANKHYA_SERIE_NF'),
                    "tipoFaturamento": "FaturamentoNormal",
                    "dataValidada": True,
                    "faturarTodosItens": False,
                    "notasComMoeda": {},
                    "nota": [
                        {
                            "NUNOTA":nunota,
                            "itens":{
                                "item":itens
                            }
                        }
                    ]
                }
            }
        }

        res = requests.post(
            url=url,
            headers={ 'Authorization': token },
            json=body)
        
        if res.status_code in (200,201) and res.json().get('status') in ['1', '2']:
            return True, int(res.json().get('responseBody').get('notas').get('nota').get('$'))
        else:
            print(f"Erro ao devolver pedidos. Nunota {nunota}. {res.text}")
            logger.error("Erro ao devolver pedidos. Nunota %s. %s",nunota,res.text)
            return False, None

class Itens(Pedido):
    def __init__(self):
        self.formatter = Formatter()
        self.con = Connect() 
        self.campos_item = [
                "ATUALESTOQUE", "CODANTECIPST", "CODEMP", "CODLOCALORIG", "CODPROD", "CODTRIB","CODVEND", "CODVOL",
                "CONTROLE", "NUNOTA", "QTDNEG", "RESERVA", "SEQUENCIA", "STATUSNOTA", "USOPROD", "VLRDESC", "VLRTOT", "VLRUNIT"
            ]            

    async def buscar(self, nunota:int=None, pedido_ecommerce:str=None) -> dict:

        if not any([nunota, pedido_ecommerce]):
            print("Nenhum critério de busca informado. Informe nunota ou pedido_ecommerce.")
            logger.error("Nenhum critério de busca informado. Informe nunota ou pedido_ecommerce.")
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
        
        if nunota:
            criteria = {
                "expression": {
                    "$": "this.NUNOTA = ?"
                },
                "parameter": [
                    {
                        "$": f"{nunota}",
                        "type": "I"
                    }
                ]
            }

        if pedido_ecommerce:
            criteria = {
                "expression": {
                    "$": "CabecalhoNota.AD_MKP_NUMPED = ? AND CabecalhoNota.TIPMOV = 'P'"
                },
                "parameter": [
                    {
                        "$": pedido_ecommerce,
                        "type": "S"
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
            if nunota:
                print(f"Erro ao buscar itens do pedido. Nunota {nunota}. {res.text}")      
                logger.error("Erro ao buscar itens do pedido. Nunota %s. %s",nunota,res.text)      
            if pedido_ecommerce:
                print(f"Erro ao buscar itens do pedido. Pedido {pedido_ecommerce}. {res.text}")      
                logger.error("Erro ao buscar itens do pedido. Nunota %s. %s",nunota,res.text)      
            return False
