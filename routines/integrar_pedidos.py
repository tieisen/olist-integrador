import asyncio
from src.integrador.pedido import Pedido

# ROTINA A SER EXECUTADA EM DIAS ÚTEIS, ÀS 8H E ÀS 11H

if __name__=="__main__":    
    pedido = Pedido()
    asyncio.run(pedido.importar_novos())
    asyncio.run(pedido.confirmar())