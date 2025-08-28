import asyncio
from src.integrador.pedido import Pedido

# ROTINA A SER EXECUTADA DIARIAMENTE

if __name__=="__main__":    
    pedido = Pedido()
    asyncio.run(pedido.devolver_lote())