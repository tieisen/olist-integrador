import os
import time
import logging
from dotenv import load_dotenv
from src.olist.separacao import Separacao as SeparacaoOlist
from database.schemas import log as SchemaLog
from database.crud import venda, log, log_pedido
from src.utils.log import Log

load_dotenv('keys/.env')
logger = logging.getLogger(__name__)
logging.basicConfig(filename=Log().buscar_path(),
                    encoding='utf-8',
                    format=os.getenv('LOGGER_FORMAT'),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

class Separacao:

    def __init__(self):
        self.req_time_sleep = float(os.getenv('REQ_TIME_SLEEP', 1.5))

    async def receber(self) -> bool:

        print("Recebendo separacoes pendentes...")

        separacao = SeparacaoOlist()
        lista_separacoes = await separacao.listar()
        if not lista_separacoes:
            print("Nenhuma separacao pendente encontrada.")
            logger.info("Nenhuma separacao pendente encontrada.")
            return True
        
        print(f"Separacoes pendentes: {len(lista_separacoes)}")

        for item in lista_separacoes:
            if venda.read_separacao_pendente(id_pedido=item.get('id_pedido')):
                if venda.update_separacao(id_pedido=item.get('id_pedido'), id_separacao=item.get('id_separacao')):
                    print("Separacao atualizada com sucesso!")
                else:
                    print(f"Erro ao atualizar separacao do pedido ID {item.get('id_pedido')}.")
                    logger.error("Erro ao atualizar separacao do pedido ID %s.",item.get('id_pedido'))

        print("Recebimento de separações concluída!")

        return True

    async def checkout(self) -> bool:

        print("Buscando pedidos para checkout...")

        lista_checkout = venda.read_separacao_checkout()

        if not lista_checkout:
            print("Nenhum checkout pendente encontrado.")
            logger.info("Nenhum checkout pendente encontrado.")
            return True
        
        print(f"Checkouts pendentes: {len(lista_checkout)}")
        separacao = SeparacaoOlist()
        for item in lista_checkout:
            time.sleep(self.req_time_sleep)  # Evita rate limit
            if not await separacao.concluir(id=item.id_separacao):
                print(f"Erro ao concluir checkout do pedido ID {item.id_pedido}.")
                logger.error("Erro ao concluir checkout do pedido ID %s.",item.id_pedido)
                continue

        print("Checkout dos pedidos concluído!")

        return True
