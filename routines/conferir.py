from src.integrador.pedido import Pedido
import asyncio
import nest_asyncio

if __name__=="__main__":
    nest_asyncio.apply()
    pedido = Pedido()
    asyncio.run(pedido.conferir())    