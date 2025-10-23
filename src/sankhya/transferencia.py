import os
import requests
from datetime import datetime
from src.utils.formatter import Formatter
from src.utils.decorador import interno, carrega_dados_empresa
from src.utils.autenticador import token_snk
from src.utils.buscar_script import buscar_script
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

class Transferencia:

    def __init__(self, codemp:int):
        self.token = None
        self.codemp = codemp
        self.empresa_id = None
        self.dados_empresa = None
        self.formatter = Formatter()
        self.campos_cabecalho_transferencia = ["NUNOTA","DTNEG","STATUSNOTA"]
        self.criterios_nota_transferencia = os.getenv('SANKHYA_CRITERIOS_NOTA_TRANSFERENCIA')
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

    @interno
    def extrai_nunota(self,payload:dict=None):
        return int(payload.get('responseBody').get('pk').get('NUNOTA').get('$'))

    @token_snk
    async def criar(
            self,
            cabecalho:dict,
            itens:list=None
        ) -> tuple:

        if cabecalho and not itens:
            payload = {
                "serviceName":"CACSP.incluirNota",
                "requestBody":{
                    "nota":{
                        "cabecalho":cabecalho
                    }
                }
            }

        elif cabecalho and itens:
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
        
        else:
            print("Dados não informados")
            logger.error("Dados não informados")
            return False

        url = os.getenv('SANKHYA_URL_PEDIDO')
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False

        res = requests.get(
            url=url,
            headers={ 'Authorization':f"Bearer {self.token}" },
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

    @token_snk
    async def buscar(
            self,
            nunota:int=None,
            itens:bool=False
        ) -> tuple[bool,dict]:

        url = os.getenv('SANKHYA_URL_LOAD_RECORDS')
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False, {}

        if nunota:
            criteria = {
                "expression": {
                    "$": f"this.NUNOTA = ?"
                },
                "parameter": [
                    {
                        "$": f"{nunota}",
                        "type": "I"
                    }
                ]
            }
        else:
            criteria = {
                "expression": {
                    "$": self.criterios_nota_transferencia
                },
                "parameter": [
                    {
                        "$": f"{datetime.now().strftime('%d/%m/%Y')}",
                        "type": "D"
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
                        "rootEntity": "CabecalhoNota",
                        "includePresentationFields": "N",
                        "offsetPage": "0",
                        "criteria": criteria,
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
                    return True, dados_nota
                if isinstance(dados_nota, list):
                    dados_nota = dados_nota[0]
                    if itens:
                        itens_handler = Itens(self)
                        dados_itens = await itens_handler.buscar(nunota=int(dados_nota.get('nunota')))
                        dados_nota['itens'] = dados_itens
                    return True, dados_nota
        else:
            logger.error("Erro ao buscar dados da nota de transferência vigente. %s",res.json())
            print(f"Erro ao buscar dados da nota de transferência vigente. {res.json()}")
            return False, {}

    @token_snk
    async def confirmar(
            self,
            nunota:int
        ) -> bool:
            
        url = os.getenv('SANKHYA_URL_CONFIRMA_PEDIDO')
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False
        
        res = requests.get(
            url=url,
            headers={ 'Authorization':f"Bearer {self.token}" },
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

    def __init__(self, transferencia_instance: 'Transferencia'=None, codemp:int=None):
        self.token = transferencia_instance.token if transferencia_instance else None
        self.codemp = transferencia_instance.codemp if transferencia_instance else codemp
        self.empresa_id = transferencia_instance.empresa_id if transferencia_instance else None
        self.dados_empresa = None
        # self.codtab_transferencia = int(os.getenv('SANKHYA_CODTAB_TRANSFERENCIA'))
        self.formatter = Formatter()
        self.campos_item = [
            "ATUALESTOQUE", "CODANTECIPST", "CODEMP", "CODLOCALORIG", "CODPROD",
            "CONTROLE", "CODTRIB","CODVEND", "CODVOL", "NUNOTA", "QTDNEG", "RESERVA",
            "SEQUENCIA", "STATUSNOTA", "USOPROD", "VLRDESC", "VLRTOT", "VLRUNIT"
        ]            

    @token_snk
    async def lancar(
            self,
            nunota:int,
            dados_item:dict
        ):
        
        if not isinstance(dados_item, dict):
            print("Dados dos itens devem ser um dicionário")
            logger.error("Dados dos itens devem ser um dicionário")
            return False
        
        url = os.getenv('SANKHYA_URL_PEDIDO_ALTERA_ITEM')
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
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
            headers={ 'Authorization':f"Bearer {self.token}" },
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

    @token_snk
    async def buscar(
            self,
            nunota:int
        ) -> dict:
    
        url = os.getenv('SANKHYA_URL_LOAD_RECORDS')
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
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
            headers={ 'Authorization':f"Bearer {self.token}" },
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

    @token_snk
    @carrega_dados_empresa
    async def busca_valor_transferencia(
            self,
            codprod:int=None,
            lista_itens:list=None
        ) -> float:

        if not any([codprod, lista_itens]):
            print("Produto não informado.")
            logger.error("Produto não informado.")
            return False

        url = os.getenv('SANKHYA_URL_DBEXPLORER')
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False

        parametro = 'SANKHYA_PATH_SCRIPT_VLR_TRANSF_LISTA' if lista_itens else 'SANKHYA_PATH_SCRIPT_VLR_TRANSF_ITEM'
        script = buscar_script(parametro=parametro)            

        try:
            if lista_itens:                
                query = script.format_map({
                                    "codtab":self.dados_empresa.get('snk_codtab_transf'),
                                    "lista_itens":','.join([str(i) for i in lista_itens])
                                })
            else:
                query = script.format_map({
                                    "codtab":self.dados_empresa.get('snk_codtab_transf'),
                                    "codprod":codprod
                                })
        except Exception as e:
            erro = f"Falha ao formatar query do saldo de estoque por lote. {e}"
            print(erro)
            logger.error(erro)
            return False

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
            logger.error("Erro ao buscar valor(es) do(s) item(ns) na tabela de transferências. %s",res.json())
            print(f"Erro ao buscar valor(es) do(s) item(ns) na tabela de transferências. {res.json()}")
            return False