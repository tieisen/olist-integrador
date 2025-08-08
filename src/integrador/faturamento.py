import logging
import os
from dotenv import load_dotenv
from src.sankhya.faturamento import Faturamento as FaturamentoSnk
from src.sankhya.estoque import Estoque as EstoqueSnk
from src.sankhya.transferencia import Transferencia as TransferenciaSnk
from src.sankhya.transferencia import Itens as ItemTransfSnk
from src.parser.transferencia import Transferencia as ParserTransferencia
from src.utils.log import Log

load_dotenv('keys/.env')
logger = logging.getLogger(__name__)
logging.basicConfig(filename=Log().buscar_path(),
                    encoding='utf-8',
                    format=os.getenv('LOGGER_FORMAT'),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

CONTEXTO = 'faturamento'

class Faturamento:

    async def venda_entre_empresas(self):

        faturamento = FaturamentoSnk()
        estoque = EstoqueSnk()
        transferencia = TransferenciaSnk()
        item_transf = ItemTransfSnk()
        parser = ParserTransferencia()

        saldo_pedidos = await faturamento.buscar_itens()
        if not saldo_pedidos:
            logger.error("Erro ao buscar itens conferidos no dia.")
            print("Erro ao buscar itens conferidos no dia.")
            return False

        saldo_estoque = await estoque.buscar_saldo_por_lote(lista_produtos=saldo_pedidos)
        if not saldo_estoque:
            logger.error("Erro ao buscar saldo de estoque.")
            print("Erro ao buscar saldo de estoque.")
            return False

        itens_venda_interna = await faturamento.compara_saldos(saldo_estoque=saldo_estoque,
                                                               saldo_pedidos=saldo_pedidos)

        if not itens_venda_interna:
            logger.info("Nenhum item para lançar venda interna.")
            print("Nenhum item para lançar venda interna.")
            return True

        codigos_produtos = [item.get('codprod') for item in itens_venda_interna]
        valores_produtos = await item_transf.busca_valor_transferencia(lista_itens=codigos_produtos)
        if not valores_produtos:
            logger.error("Erro ao buscar valores de transferência.")
            print("Erro ao buscar valores de transferência.")
            return False

        for item in itens_venda_interna:
            for valor in valores_produtos:
                if item.get('codprod') == valor.get('codprod'):
                    item['valor'] = float(valor.get('valor')) if valor.get('valor') else 0.1
                    break

        cabecalho, itens = parser.to_sankhya(objeto='nota',
                                             itens_transferencia=itens_venda_interna)
        
        #return cabecalho, itens
        if not all([cabecalho, itens]):
            logger.error("Erro ao preparar dados da nota de transferência.")
            print("Erro ao preparar dados da nota de transferência.")
            return False
        
        ack, nunota = await transferencia.criar(cabecalho=cabecalho,
                                                itens=itens)
        
        if not ack:
            logger.error("Erro ao lançar nota de transferência.")
            print("Erro ao lançar nota de transferência.")
            return False
        
        print("Nota de venda entre empresas lançada com sucesso.")
        logger.info("Nota de venda entre empresas lançada com sucesso.")
        
        ack = await transferencia.confirmar(nunota=nunota)
        if not ack:
            logger.error("Erro ao confirmar nota de transferência.")
            print("Erro ao confirmar nota de transferência.")
            return False
        
        print("Nota de venda entre empresas confirmada com sucesso.")
        logger.info("Nota de venda entre empresas confirmada com sucesso.")        
        
        return True