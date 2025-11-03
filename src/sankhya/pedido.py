import os
import requests
from datetime import datetime
from src.sankhya.nota import Nota
from src.utils.decorador import interno, carrega_dados_empresa
from src.utils.autenticador import token_snk
from src.utils.formatter import Formatter
from src.utils.buscar_script import buscar_script
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

class Pedido:

    def __init__(self, codemp:int=None, empresa_id:int=None):
        self.token = None
        self.codemp = codemp
        self.empresa_id = empresa_id
        self.dados_empresa = None
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

    @interno
    def extrai_nunota(self,payload:dict=None):
        return int(payload.get('responseBody').get('pk').get('NUNOTA').get('$'))
    
    @token_snk
    async def buscar(
            self,
            nunota:int=None,
            id_olist:int=None,
            codpedido:str=None,
            itens:bool=False
        ) -> dict:

        if not any([nunota, id_olist, codpedido]):
            print("Nenhum critério de busca informado. Informe nunota, id_olist ou codpedido.")
            logger.error("Nenhum critério de busca informado. Informe nunota, id_olist ou codpedido.")
            return False

        url = os.getenv('SANKHYA_URL_LOAD_RECORDS')
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
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
                                "list": ','.join(self.campos_cabecalho)
                            }
                        }
                    }
                }
            })
        
        if res.status_code in (200,201) and res.json().get('status')=='1':
            try:
                dados_pedido = self.formatter.return_format(res.json())
                if isinstance(dados_pedido,list) and not dados_pedido:
                    # Pedido não encontrado
                    return 0
                
                dados_pedido = dados_pedido[0]
                if itens:
                    itens_handler = Itens(self)
                    dados_itens = await itens_handler.buscar(nunota=int(dados_pedido.get('nunota')))
                    dados_pedido['itens'] = dados_itens
                return dados_pedido
            except Exception as e:
                print(f"Erro ao formatar retorno da API para o pedido {nunota}. {e}")
                logger.error("Erro ao formatar retorno da API para o pedido %s. %s",nunota,e)                
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
    
    @token_snk
    async def buscar_nunota_nota(
            self,
            nunota:int
        ) -> dict:
        
        url = os.getenv('SANKHYA_URL_DBEXPLORER')
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False

        script = buscar_script(parametro='SANKHYA_PATH_SCRIPT_PEDIDO_TGFVAR')
        query = script.format_map({"nunota":nunota})

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
            logger.error("Erro ao buscar número da nota. %s",res.json())
            print(f"Erro ao buscar número da nota. {res.json()}")
            return False

    async def buscar_nota_do_pedido(
            self,
            nunota:int
        ) -> dict:
        
        nunota_nota = await self.buscar_nunota_nota(nunota=nunota)
        if not nunota_nota:            
            logger.warning("Pedido %s sem Nota vinculada",nunota_nota)
            return 0
        nunota_nota = nunota_nota[0].get('nunota')
        
        nota = Nota(self.codemp)
        dados_nota = await nota.buscar(nunota=nunota_nota,
                                       itens=True)
        
        if not dados_nota:
            print(f"Erro ao buscar dados da nota {nunota_nota} vinculada ao pedido {nunota}")
            logger.error("Erro ao buscar dados da nota %s vinculada ao pedido %s", nunota_nota, nunota)
            return False
        
        return dados_nota

    @token_snk
    async def buscar_cidade(
            self,
            ibge:int
        ) -> dict:

        url = os.getenv('SANKHYA_URL_LOAD_RECORDS')
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
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
            headers={ 'Authorization':f"Bearer {self.token}" },
            json=payload)
        
        if res.status_code in (200,201) and res.json().get('status')=='1':
            return self.formatter.return_format(res.json())[0]
        else:
            logger.error("Erro ao buscar dados de localização da cidade %s. %s",ibge,res.text)
            print(f"Erro ao buscar dados de localização da cidade {ibge}. {res.text}")
            return False

    @token_snk
    async def lancar(
            self,
            dados_cabecalho:dict,
            dados_itens:list
        ) -> int:
                
        url = os.getenv('SANKHYA_URL_PEDIDO')
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False

        res = requests.get(
            url=url,
            headers={ 'Authorization':f"Bearer {self.token}" },
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
                return False
            if res.json().get('status')=='1':
                return self.extrai_nunota(res.json())
            if res.json().get('status')=='2':
                logger.error("Pedido %s lançado com erro. %s",self.extrai_nunota(res.json()),res.text)                
                return self.extrai_nunota(res.json())
        else:
            logger.error("Erro ao lançar pedido #%s. %s",dados_cabecalho.get('AD_MKP_NUMPED'),res.text)
            print(f"Erro ao lançar pedido #{dados_cabecalho.get('AD_MKP_NUMPED')}. {res.text}")
            return False

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
        if res.status_code in (200,201) and res.json().get('status')=='0' and res.json().get('statusMessage') == f"A nota {nunota} já foi confirmada.":
            return True        
        print(f"Erro ao confirmar pedido. Nunota {nunota}. {res.text}")
        logger.error("Erro ao confirmar pedido. Nunota %s. %s",nunota,res.text)
        return False

    @token_snk
    @carrega_dados_empresa
    async def faturar(
            self,
            nunota:int,
            dt_fatur:str=None
        ) -> tuple[bool,int]:
        
        url = os.getenv('SANKHYA_URL_FATURA_PEDIDO')
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False, None
        
        top_faturamento = self.dados_empresa.get('snk_top_venda')
        serie_nf = self.dados_empresa.get('serie_nfe')
        if not all([top_faturamento,serie_nf]):
            erro = f"Parâmetros da TOP de faturamento ou série da NF não encontados"
            logger.error(erro)
            print(erro)
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
                    "codTipOper":top_faturamento,
                    "serie":serie_nf,
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
            headers={ 'Authorization':f"Bearer {self.token}" },
            json=body)
        
        if res.status_code in (200,201) and res.json().get('status') in ['1', '2']:
            return True, int(res.json().get('responseBody').get('notas').get('nota').get('$'))
        else:
            print(f"Erro ao faturar pedido. Nunota {nunota}. {res.text}")
            logger.error("Erro ao faturar pedido. Nunota %s. %s",nunota,res.text)
            return False, None

    @token_snk
    async def excluir(
            self,
            nunota:int
        ) -> bool:
        
        url = os.getenv('SANKHYA_URL_DELETE')
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False, None

        body = {
            "serviceName": "DatasetSP.removeRecord",
            "requestBody": {
                "entityName": "CabecalhoNota",
                "standAlone": False,
                "pks": [{"NUNOTA": f"{nunota}"}]
            }
        }

        res = requests.get(
            url=url,
            headers={ 'Authorization':f"Bearer {self.token}" },
            json=body)
        
        if res.status_code in (200,201) and res.json().get('status') in ('0','1'):
            return True
        else:
            logger.error("Erro ao excluir pedido. %s",res.json())
            print(f"Erro ao excluir pedido. {res.json()}")
            return False

class Itens(Pedido):
    def __init__(self, pedido_instance: 'Pedido'=None, codemp:int=None):
        self.token = pedido_instance.token if pedido_instance else None
        self.codemp = codemp or pedido_instance.codemp
        self.formatter = Formatter()
        self.campos_item = [
            "ATUALESTOQUE", "CODANTECIPST", "CODEMP", "CODLOCALORIG",
            "CODPROD", "CODTRIB","CODVEND", "CODVOL", "CONTROLE",
            "NUNOTA", "QTDNEG", "RESERVA", "SEQUENCIA", "STATUSNOTA",
            "USOPROD", "VLRDESC", "VLRTOT", "VLRUNIT"
        ]            

    @token_snk
    async def buscar(
            self,
            nunota:int=None,
            pedido_ecommerce:str=None
        ) -> dict:

        if not any([nunota, pedido_ecommerce]):
            print("Nenhum critério de busca informado. Informe nunota ou pedido_ecommerce.")
            logger.error("Nenhum critério de busca informado. Informe nunota ou pedido_ecommerce.")
            return False
        
        url = os.getenv('SANKHYA_URL_LOAD_RECORDS')
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
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
            if nunota:
                print(f"Erro ao buscar itens do pedido. Nunota {nunota}. {res.text}")      
                logger.error("Erro ao buscar itens do pedido. Nunota %s. %s",nunota,res.text)      
            if pedido_ecommerce:
                print(f"Erro ao buscar itens do pedido. Pedido {pedido_ecommerce}. {res.text}")      
                logger.error("Erro ao buscar itens do pedido. Nunota %s. %s",nunota,res.text)      
            return False
