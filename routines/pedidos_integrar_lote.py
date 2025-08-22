import asyncio
from src.integrador.pedido import Pedido
from src.integrador.separacao import Separacao

# ROTINA A SER EXECUTADA EM DIAS ÚTEIS, ÀS 7H30 E ÀS 13H

if __name__=="__main__":    
    pedido = Pedido()
    separacao = Separacao()
    asyncio.run(separacao.receber())
    asyncio.run(pedido.importar_lote())
    asyncio.run(pedido.confirmar_lote())