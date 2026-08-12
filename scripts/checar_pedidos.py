import asyncio

from src.olist.pedido import Pedido

async def checar_pedidos():
    pedidos = await Pedido().buscar_pedidos()
    print(pedidos)

if __name__ == "__main__":
    asyncio.run(checar_pedidos())