from src.integrador.produto import Produto
import asyncio

# ROTINA A SER EXECUTADA EM DIAS ÚTEIS, A CADA 30 MINUTOS, ENTRE ÀS 7H E 18H

if __name__=="__main__":    
    produto = Produto()
    asyncio.run(produto.recebe_alteracoes_pendentes())