import asyncio
from src.sankhya.produto import Produto
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

class Conferencia:

    def __init__(self, codemp:int):
        self.codemp = codemp
        
    def to_sankhya_itens(
            self,
            nuconf:int,
            dados_olist:list
        ) -> list[dict]:
        """
        Converte os dados dos pedidos no formato da API do Sankhya.
            :param nuconf: número da conferência
            :param dados_olist: lista de pedidos da API do Olist
            :return list[dict]: lista de dicionários com as dados dos itens da conferência
        """        
        
        new_dados_sankhya:list[dict] = []        
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