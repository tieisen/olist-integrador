from src.integrador.pedido import Pedido
from src.integrador.separacao import Separacao
import asyncio

# ROTINA A SER EXECUTADA DIARIAMENTE, A CADA 30 MINUTOS, ENTRE ÀS 7H E 11H

if __name__=="__main__":    
    pedido = Pedido()
    separacao = Separacao()
    asyncio.run(pedido.receber())
    asyncio.run(pedido.validar_cancelamentos())
    asyncio.run(separacao.receber())    