import asyncio

from src.olist.pedido import Pedido

async def checar_pedidos():
    # pedidos = await Pedido().buscar_pedidos()
    pedido = Pedido(codemp=21)
    resultado = await pedido.buscar(numero=6633)
    print(resultado)

if __name__ == "__main__":
    asyncio.run(checar_pedidos())