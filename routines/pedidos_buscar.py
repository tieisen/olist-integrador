import asyncio
from src.integrador.pedido import Pedido
from src.integrador.separacao import Separacao

# ROTINA A SER EXECUTADA DIARIAMENTE, A CADA 30 MINUTOS

if __name__=="__main__":    
    pedido = Pedido()
    separacao = Separacao()
    asyncio.run(pedido.receber())
    asyncio.run(pedido.validar_cancelamentos())
    asyncio.run(separacao.receber())    