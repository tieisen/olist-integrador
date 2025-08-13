from src.integrador.pedido import Pedido
from src.integrador.faturamento import Faturamento
import asyncio

# ROTINA A SER EXECUTADA EM DIAS ÚTEIS, ÀS 12H

if __name__=="__main__":    
    pedido = Pedido()
    faturamento = Faturamento()
    ack = asyncio.run(faturamento.venda_entre_empresas())
    if ack:
        asyncio.run(pedido.faturar())
        