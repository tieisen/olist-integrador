import asyncio
from src.integrador.pedido import Pedido

if __name__=="__main__":    
    pedido = Pedido()
    asyncio.run(pedido.receber())
    asyncio.run(pedido.validar_cancelamentos())