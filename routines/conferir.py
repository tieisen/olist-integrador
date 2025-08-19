from src.integrador.pedido import Pedido
import asyncio

if __name__=="__main__":
    pedido = Pedido()
    asyncio.run(pedido.conferir())    