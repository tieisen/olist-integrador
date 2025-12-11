import re
from datetime import datetime
from src.utils.decorador import carrega_dados_ecommerce, carrega_dados_empresa
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

class Financeiro:

    def __init__(self, id_loja:int, empresa_id:int):
        self.id_loja = id_loja
        self.empresa_id = empresa_id
        self.codemp = None
        self.dados_ecommerce:dict={}
        self.dados_empresa:dict={}
        self.campos_snk = [
            "CODEMP", "NUMNOTA", "SERIENOTA", "DTNEG", "DESDOBRAMENTO", "DHMOV",
            "DTVENC", "CODPARC", "CODTIPOPER", "DHTIPOPER", "CODBCO", "CODCTABCOINT",
            "CODNAT", "CODTIPTIT", "HISTORICO", "VLRDESDOB", "TIPMULTA", "TIPJURO",
            "AUTORIZADO", "RECDESP", "PROVISAO","ORIGEM", "NUNOTA"
        ]        
    
    @carrega_dados_ecommerce
    @carrega_dados_empresa
    async def sankhya(self,dados:dict) -> dict:
        """
        Converte os dados no formato da API do Sankhya.
            :param dados: dados do financeiro
            :return dict: dicionário com os dados no padrão Sankhya
        """
        pass
    
    @carrega_dados_ecommerce
    @carrega_dados_empresa
    async def olist(self,dtNeg:str,dtVcto:str,valor:float,numDocumento:int,historico:str) -> dict:
        """
        Converte os dados no formato da API do Olist.
            :param dtNeg: data da negociação no formato YYYY-MM-DD            
            :param dtVcto: data do vencimento no formato YYYY-MM-DD
            :param valor: valor do título
            :param numDocumento: número da NF
            :param historico: texto da observação do título
            :return dict: dicionário com os dados no padrão Olist
        """

        payload:dict={}

        if self.dados_empresa.get('id') == 5:
            categoria = 746666005
        elif self.dados_empresa.get('id') == 1:
            categoria = 367785383
        else:
            categoria = None

        payload = {
            "data": dtNeg,
            "dataVencimento": dtVcto,
            "valor": valor,
            "numeroDocumento": str(numDocumento),
            "contato": {
                "id": self.dados_empresa.get('olist_id_fornecedor_padrao')
            },
            "historico": historico,
            "categoria": {
                "id": categoria
            },
            "dataCompetencia": None,
            "ocorrencia": "U",
            "formaPagamento": 15,
            "diaVencimento": None,
            "quantidadeParcelas": 1,
            "diaSemanaVencimento": None
        }

        return payload