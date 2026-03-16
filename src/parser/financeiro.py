import os, re, unicodedata
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

class Receita:

    def __init__(self):
        self.taxa_blz_envios:float = float(os.getenv('BLZWEB_TAXA_ENVIO'))
        self.comissao_blz:float = float(os.getenv('BLZWEB_TAXA_COMISSAO'))
        
    def normalizaTexto(self, texto: str) -> str:
        if not texto:
            return texto

        texto = unicodedata.normalize('NFKD', texto)
        texto = ''.join(c for c in texto if not unicodedata.combining(c))
        texto = re.sub(r'[^a-zA-Z0-9\s]', '', texto)
        texto = re.sub(r'\s+', ' ', texto).strip()
        texto = texto.replace(' ', '_')
        texto = texto.lower()

        return texto

    def formataVlr(self,vlr:float) -> str:
        return vlr.__format__('.2f').replace('.',',')
    
    async def calculaComissaoBlzWeb(self,vlrPedido:float) -> float:
        """ Calcula o valor da comissão do BLZ na Web (taxa fixa de envio + percentual sobre o valor do pedido) """
        return round(vlrPedido*self.comissao_blz+self.taxa_blz_envios,2)

    async def lancamento(self,dtNf:str,dtVenc:str,vlrTitulo:float,numDocumento:str,numNf:str,codPedido:str,idCliente:int,idCategoriaFinanceiro:int,idFormaRecebimento:int) -> dict:
        """_summary_

        Args:
            dtNf (str): _description_
            dtVenc (str): _description_
            vlrTitulo (float): _description_
            numDocumento (str): _description_
            numNf (str): _description_
            codPedido (str): _description_
            idCliente (int): _description_
            idCategoriaFinanceiro (int): _description_

        Returns:
            dict: _description_
        """
        
        return {
            "data": dtNf,
            "dataVencimento": dtVenc,
            "valor": vlrTitulo,
            "numeroDocumento": numDocumento,
            "contato": {
                "id": idCliente
            },
            "historico": f"Ref. a NF nº {numNf}, Pedido #{codPedido}",
            "categoria": {
                "id": idCategoriaFinanceiro
            },
            "dataCompetencia": None,
            "formaRecebimento": idFormaRecebimento,
            "ocorrencia": "U",
            "diaVencimento": None,
            "diaSemanaVencimento": None,
            "quantidadeParcelas": 0
        }

    async def baixa(self,idContaDestino:int,dtRecebimento:str,vlrPago:float,idCategoriaFinanceiro:int,historico:str=None) -> dict:
        """_summary_

        Args:
            idContaDestino (int): _description_
            dtRecebimento (str): _description_
            vlrPago (float): _description_
            historico (str): _description_
            idCategoriaFinanceiro (int): _description_

        Returns:
            dict: _description_
        """

        return {
            "contaDestino": {
                "id": idContaDestino
            },
            "data": dtRecebimento,
            "categoria": {
                "id": idCategoriaFinanceiro
            },
            "historico": historico,
            "taxa": '',
            "juros": '',
            "desconto": '',
            "valorPago": vlrPago,
            "acrescimo": ''
        }

class Despesa:

    def __init__(self):
        pass

    def normalizar_texto(self, texto: str) -> str:
        if not texto:
            return texto

        texto = unicodedata.normalize('NFKD', texto)
        texto = ''.join(c for c in texto if not unicodedata.combining(c))
        texto = re.sub(r'[^a-zA-Z0-9\s]', '', texto)
        texto = re.sub(r'\s+', ' ', texto).strip()
        texto = texto.replace(' ', '_')
        texto = texto.lower()

        return texto
    
    async def lancamento(self,dtNeg:str,dtVcto:str,valor:float,numDocumento:str,idFornecedor:int,idCategoriaDespesa:int,historico:str,idFormaPgto:int) -> dict:
        """
        Converte os dados no formato da API de Contas a Pagar do Olist.
            :param dtNeg: data da negociação no formato YYYY-MM-DD            
            :param dtVcto: data do vencimento no formato YYYY-MM-DD
            :param valor: valor do título
            :param numDocumento: número da NF
            :param historico: texto da observação do título
            :return dict: dicionário com os dados no padrão Olist
        """

        return {
            "data": dtNeg,
            "dataVencimento": dtVcto,
            "valor": valor,
            "numeroDocumento": str(numDocumento),
            "contato": {
                "id": idFornecedor
            },
            "historico": historico,
            "categoria": {
                "id": idCategoriaDespesa
            },
            "dataCompetencia": None,
            "ocorrencia": "U",
            "formaPagamento": idFormaPgto,
            "diaVencimento": None,
            "quantidadeParcelas": 1,
            "diaSemanaVencimento": None
        }
    
    async def baixa(self,idContaDestino:int,dtRecebimento:str,vlrPago:float,idCategoriaDespesa:int,historico:str=None) -> dict:
        """_summary_

        Args:
            idContaDestino (int): _description_
            dtRecebimento (str): _description_
            vlrPago (float): _description_
            historico (str): _description_
            idCategoriaDespesa (int): _description_

        Returns:
            dict: _description_
        """
            
        return {
            "contaDestino": {
                "id": idContaDestino
            },
            "data": dtRecebimento,
            "categoria": {
                "id": idCategoriaDespesa
            },
            "historico": historico,
            "taxa": '',
            "juros": '',
            "desconto": '',
            "valorPago": vlrPago,
            "acrescimo": ''
        }