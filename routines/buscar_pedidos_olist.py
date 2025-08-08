from src.integrador.pedido import Pedido
import asyncio

# ROTINA A SER EXECUTADA DIARIAMENTE, A CADA 30 MINUTOS, ENTRE ÀS 7H E 11H

if __name__=="__main__":    
    pedido = Pedido()
    asyncio.run(pedido.receber_pedidos())
    asyncio.run(pedido.validar_cancelamentos())