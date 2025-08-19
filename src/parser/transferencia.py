import os
import logging
from datetime import datetime
from dotenv import load_dotenv
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
        self.codparc = os.getenv('SANKHYA_CODPARC')

    def cabecalho(self):
        return {
                "NUNOTA":  {"$": ""},
                "SERIENOTA":  {"$": "1"},
                "CODEMP":  {"$": "1"},
                "CODEMPNEGOC":  {"$": "31"},
                "CODPARC":  {"$": self.codparc},
                "CODNAT":  {"$": "1010101"},
                "CODTIPVENDA":  {"$": "0"},
                "CODTIPOPER":  {"$": "1419"},
                "CODVEND":  {"$": "1"},
                "CODOBSPADRAO":  {"$": "0"},
                "DTNEG": {"$":datetime.now().strftime('%d/%m/%Y')},
                "TIPMOV":  {"$": "T"},
                "CIF_FOB":  {"$": "C"},
                "TIPFRETE":  {"$": "N"},
                "OBSERVACAO":  {"$": "Transferência entre empresas para ressuprimento e atendimento de pedidos Shopee."}
            }
    
    def itens(self, nunota:int=None, itens_transferencia:list=None, itens_transferidos:list=None):
        if not itens_transferencia:
            print("Dados não informados.")
            logger.error("Dados não informados.")
            return False

        dados_item = {}
        res = []

        if not itens_transferidos:
            for item in itens_transferencia:
                vlrtot = round(item.get('valor') * item.get('quantidade'),3)
                dados_item['NUNOTA'] = {"$":nunota or ""}
                dados_item['SEQUENCIA'] = {"$":""}
                dados_item['CODPROD'] = {"$":item.get('codprod')}
                dados_item['QTDNEG'] = {"$":item.get('quantidade')}
                dados_item['VLRUNIT'] = {"$":item.get('valor')}
                dados_item['VLRTOT'] = {"$":vlrtot}
                dados_item['PERCDESC'] = {"$": "0"}
                dados_item['VLRDESC'] = {"$": "0"}
                dados_item['CODVOL'] = {"$": 'UN'}
                dados_item['CODLOCALORIG'] = {"$": '101'}
                dados_item['CODLOCALDEST'] = {"$": '101'}
                dados_item['CONTROLE'] = {"$": item.get('controle')}
                res.append(dados_item)
                dados_item = {}            
            return res
        
        lista_itens_transferidos = [item.get('codprod') for item in itens_transferidos]
        if item.get('codigo') in itens_transferidos:
            if not item.get('lotes'):
                print(f"Item {item.get('codigo')} sem controle informado na nota")
                return []
            dado = {}

            for i in item.get('lotes'):
                for j in lista_itens_transferidos:
                    if int(j.get('codprod')) == int(item.get('codigo')) and j.get('controle') == i.get('lote'):
                        dado = j
                if not dado:
                    print(f"Erro ao buscar dado {j}")
                    return []                                
                dados_item['NUNOTA'] = {"$":nunota}
                dados_item['QTDNEG'] = {"$":int(i.get('quantidade'))+int(dado.get('qtdneg'))}
                dados_item['SEQUENCIA'] = {"$":dado.get('sequencia')}
                res.append(dados_item)
                dados_item = {}
        else:
            for i in item.get('lotes'):
                vlrtot = round(item.get('valor') * item.get('quantidade'),3)
                dados_item['NUNOTA'] = {"$":nunota}
                dados_item['SEQUENCIA'] = {"$":""}
                dados_item['CODPROD'] = {"$":item.get('codigo')}
                dados_item['QTDNEG'] = {"$":i.get('quantidade')}
                dados_item['VLRUNIT'] = {"$":item.get('valor')}
                dados_item['VLRTOT'] = {"$":vlrtot}
                dados_item['PERCDESC'] = {"$": "0"}
                dados_item['VLRDESC'] = {"$": "0"}
                dados_item['CODVOL'] = {"$": item.get('unidade')}
                dados_item['CODLOCALORIG'] = {"$": '101'}
                dados_item['CODLOCALDEST'] = {"$": '101'}
                dados_item['CONTROLE'] = {"$": i.get('lote')}
                res.append(dados_item)
                dados_item = {}
        return res

    def to_sankhya(self, objeto:str=None, nunota:int=None, itens_transferencia:list=None, itens_transferidos:list=None):

        if not objeto:
            print("Objeto não informado.")
            logger.error("Objeto não informado.")
            return False

        if objeto == 'cabecalho':
            return self.cabecalho()
        
        if objeto == 'item':
            return self.itens(nunota=nunota,
                              itens_transferencia=itens_transferencia,
                              itens_transferidos=itens_transferidos)
        
        if objeto == 'nota':
            return self.cabecalho(), self.itens(nunota=nunota,
                                                itens_transferencia=itens_transferencia)
        