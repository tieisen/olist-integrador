import asyncio
from src.sankhya.produto import Produto
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

class Conferencia:

    def __init__(self, codemp:int):
        self.codemp = codemp
        
    def to_sankhya_itens(self, nuconf:int=None, dados_olist:list=None) -> list:
        new_dados_sankhya = []

        if not dados_olist or not nuconf:
            print("Dados não informados.")
            logger.error("Dados não informados.")
            return new_dados_sankhya
        
        produto = Produto(self.codemp)

        seq = 0
        for item in dados_olist:
            dados_produto = asyncio.run(produto.buscar(codprod=item.get('codigo')))
            for controle in item.get('lotes'):
                seq += 1
                new_dados_sankhya.append({
                    'values':{
                        '0': f"{nuconf}",
                        '1': f"{seq}",
                        '2': f"{dados_produto.get('referencia')}",
                        '3': f"{item.get('codigo')}",
                        '4': f"{item.get('unidade')}",
                        '5': f"{controle.get('lote')}",
                        '6': f"{controle.get('quantidade')}",
                        '7': f"{controle.get('quantidade')}"
                    }
                })
        
        return new_dados_sankhya