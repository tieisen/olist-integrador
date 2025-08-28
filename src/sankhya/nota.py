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

class Nota:

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
                "NUMNOTA", "OBSERVACAO", "PENDENTE", "PESO", "PESOBRUTO", "QTDVOL", "TIPMOV",
                "VLRDESCTOT", "VLRDESCTOTITEM", "VLRFRETE", "VLRICMS", "VLRICMSDIFALDEST",
                "VLRICMSDIFALREM", "VLRICMSFCP", "VLRICMSFCPINT", "VLRIPI", "VLRIRF", "VLRISS",
                "VLRNOTA", "VLRSUBST", "VLRSTFCPINTANT", "VOLUME"
            ]

    def extrai_nunota(self,payload:dict=None):
        return int(payload.get('responseBody').get('pk').get('NUNOTA').get('$'))

    async def buscar(self, nunota:int=None, id_olist:int=None, codpedido:str=None, pendentes:bool=False, offset:int=0, itens:bool=False) -> dict:

        if not any([nunota, id_olist, codpedido, pendentes]):
            print("Nenhum critério de busca informado. Nenhum dado será retornado.")
            logger.error("Nenhum critério de busca informado. Nenhum dado será retornado.")
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

        fieldset_list = ','.join(self.campos_cabecalho)
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

        if id_olist:
            criteria = {
                "expression": {
                    "$": "this.AD_MKP_ID = ?"
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
                    "$": "this.AD_MKP_CODPED = ? AND TIPMOV = 'V'"
                },
                "parameter": [
                    {
                        "$": f"{codpedido}",
                        "type": "S"
                    }
                ]
            }

        if pendentes:
            criteria = {
                "expression": {
                    "$": "this.STATUSNOTA = 'A' AND TIPMOV = 'V' AND CODEMP = 31 AND CODTIPOPER = 3229"
                }
            }
            fieldset_list = 'NUNOTA'

        if not pendentes and not any([nunota, id_olist, codpedido]):
            criteria = {
                "expression": {
                    "$": "this.TIPMOV = 'V' AND CODEMP = 31 AND CODTIPOPER = 3229"
                }
            }
            fieldset_list = 'AD_MKP_CODPED, NUNOTA, NUMNOTA, AD_MKP_IDNFE'

        res = requests.get(
            url=url,
            headers={ 'Authorization': token },
            json={
                "serviceName": "CRUDServiceProvider.loadRecords",
                "requestBody": {
                    "dataSet": {
                        "rootEntity": "CabecalhoNota",
                        "includePresentationFields": "N",
                        "offsetPage": f"{offset}",
                        "criteria": criteria,
                        "entity": {
                            "fieldset": {
                                "list": fieldset_list
                            }
                        }
                    }
                }
            })
        
        if res.status_code in (200,201) and res.json().get('status')=='1':
            retorno_formatado = self.formatter.return_format(res.json())
            if isinstance(retorno_formatado, list):
                dados_nota = retorno_formatado[0]            
            if itens:
                dados_itens = await Itens().buscar(nunota=int(dados_nota.get('nunota')))
                dados_nota['itens'] = dados_itens
            return dados_nota                

            #return self.formatter.return_format(res.json())
        else:
            if nunota:
                print(f"Erro ao buscar nota. Nunota {nunota}. {res.json()}")
                logger.error("Erro ao buscar nota. Nunota %s. %s",nunota,res.json())
            if id_olist:
                print(f"Erro ao buscar nota. ID {id_olist}. {res.json()}")
                logger.error("Erro ao buscar nota. ID %s. %s",id_olist,res.json())
            if codpedido:
                print(f"Erro ao buscar nota. Pedido {codpedido}. {res.json()}")
                logger.error("Erro ao buscar nota. Pedido %s. %s",codpedido,res.json())
            if pendentes:
                print(f"Erro ao buscar notas pendentes. {res.json()}")
                logger.error("Erro ao buscar notas pendentes. %s",res.json())
            return False

    async def confirmar(self, nunota:int=None) -> bool:

        if not nunota:
            print("Número único da nota não informado")
            logger.error("Número único da nota não informado")
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
            if res.json().get('statusMessage') == f'A nota {nunota} já foi confirmada.':
                print(res.json().get('statusMessage'))
                return True
            logger.error("Erro ao confirmar nota. Nunota %s. %s",nunota,res.json())
            print(f"Erro ao confirmar nota. Nunota {nunota}. {res.json()}")
            return False

    async def informar_numero_e_chavenfe(self, nunota:int=None, chavenfe:str=None, numero:str=None, id_nota:int=None) -> bool:
        
        if not all([nunota, chavenfe, numero, id_nota]):
            print("Número único da nota, chave NFe, número e ID da nota não informados.")
            logger.error("Número único da nota, chave NFe, número e ID da nota não informados.")
            return False

        url = os.getenv('SANKHYA_URL_SAVE')
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
            "serviceName": "DatasetSP.save",
            "requestBody": {
                "entityName": "CabecalhoNota",
                "standAlone": False,
                "fields": [
                    "AD_MKP_IDNFE",
                    "CHAVENFE",
                    "NUMNOTA"
                ],
                "records": [
                    {
                        "pk": {
                            "NUNOTA": f"{nunota}"
                        },
                        "values": {
                            "0": f"{id_nota}",
                            "1": f"{chavenfe}",
                            "2": f"{int(numero)}"
                        }
                    }
                ]
            }
        }

        res = requests.get(
            url=url,
            headers={ 'Authorization': token },
            json=payload
        )
        if res.status_code in (200,201) and res.json().get('status')=='1':
            return True
        else:
            logger.error("Erro informar dados da NFe na nota de venda do Sankhya. Nunota %s. Nota %s. %s",nunota,numero,res.json())
            print(f"Erro informar dados da NFe na nota de venda do Sankhya. Nunota {nunota}. Nota {numero}. {res.json()}")
            return False

    async def devolver(self, nunota:int, itens:list) -> bool:

        url = os.getenv('SANKHYA_URL_FATURA_PEDIDO')
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

class Itens(Nota):

    def __init__(self):
        super().__init__()
        self.formatter = Formatter()
        self.campos_item = [
            "ATUALESTOQUE", "CODANTECIPST", "CODEMP", "CODLOCALORIG", "CODPROD", "CONTROLE",
            "CODTRIB","CODVEND", "CODVOL", "NUNOTA", "QTDNEG", "RESERVA", "SEQUENCIA",
            "STATUSNOTA", "USOPROD", "VLRDESC", "VLRTOT", "VLRUNIT"
        ]            

    async def buscar(self, nunota:int=None) -> dict:

        if not nunota:
            print("Número único da nota não informado")
            logger.error("Número único da nota não informado")
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
                    "$": f"{nunota}",
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
