import os
import re
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

class Pedido:

    def __init__(self):
        self.codemp = int(os.getenv('SANKHYA_CODEMP'))
        self.codnat = int(os.getenv('SANKHYA_CODNAT'))
        self.codparc = int(os.getenv('SANKHYA_CODPARC'))
        self.codtipoper = int(os.getenv('SANKHYA_CODTIPOPER'))
        self.codtipvenda = int(os.getenv('SANKHYA_CODTIPVENDA'))
        self.codvend = int(os.getenv('SANKHYA_CODVEND'))
        self.codlocalorig = int(os.getenv('SANKHYA_CODLOCAL'))
        
    def to_sankhya(self, dados_olist:dict=None, dados_cidade:list=None) -> tuple[dict,list]:
        dados_sankhya = {}
        lista_itens = []

        if not all([dados_olist,dados_cidade]):
            return dados_sankhya, lista_itens

        dados_sankhya['AD_MKP_CODPED'] = {"$":str(dados_olist['ecommerce'].get('numeroPedidoEcommerce'))}
        dados_sankhya['AD_MKP_DESTINO'] = {"$": dados_cidade.get('codcid')} # ID DA CIDADE NO SAKNHYA
        dados_sankhya['AD_MKP_ID'] = {"$":dados_olist.get('id')}
        dados_sankhya['AD_MKP_NUMPED'] = {"$":dados_olist.get('numeroPedido')}
        dados_sankhya['AD_MKP_ORIGEM'] = {"$":dados_olist['ecommerce'].get('id')}
        dados_sankhya['CIF_FOB'] = {"$":'C'}
        dados_sankhya['CODCENCUS'] = {"$":'0'}
        dados_sankhya['CODCIDENTREGA'] = {"$":dados_cidade.get('codcid')}
        dados_sankhya['CODUFENTREGA'] = {"$":dados_cidade.get('uf')}
        dados_sankhya['CODCIDDESTINO'] = {"$":dados_cidade.get('codcid')}
        dados_sankhya['CODUFDESTINO'] = {"$":dados_cidade.get('uf')}
        dados_sankhya['CODEMP'] = {"$":self.codemp}
        dados_sankhya['CODNAT'] = {"$":self.codnat}
        dados_sankhya['CODPARC'] = {"$":self.codparc}
        dados_sankhya['CODTIPOPER'] = {"$":self.codtipoper}
        dados_sankhya['CODTIPVENDA'] = {"$":self.codtipvenda}
        dados_sankhya['CODVEND'] = {"$":self.codvend}
        dados_sankhya['DTNEG'] = {"$":datetime.strptime(dados_olist.get('data'),'%Y-%m-%d').strftime('%d/%m/%Y')}
        dados_sankhya['NUNOTA'] = {},
        dados_sankhya['TIPMOV'] = {"$":"P"}
        dados_sankhya['OBSERVACAO'] = {"$":f"Pedido #{dados_olist.get('numeroPedido')} importado do Olist."}

        for item in dados_olist.get('itens'):            
            dados_item = {}
            codprod = re.search(r'^\d{8}', item['produto'].get('sku'))
            dados_item['NUNOTA'] = {},
            dados_item['CODPROD'] = {"$":codprod.group()}
            dados_item['QTDNEG'] = {"$":item.get('quantidade')}
            dados_item['VLRUNIT'] = {"$":item.get('valorUnitario') if item.get('valorUnitario') > 0 else 0.01}
            dados_item['PERCDESC'] = {"$":'0'}
            dados_item['IGNOREDESCPROMOQTD'] = {"$": "True"}
            dados_item['CODVOL'] = {"$":"UN"}
            dados_item['CODLOCALORIG'] = {"$":self.codlocalorig}
            lista_itens.append(dados_item)
            
        return dados_sankhya, lista_itens
