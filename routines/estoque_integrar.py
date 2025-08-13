import asyncio
from src.integrador.estoque import Estoque

# ROTINA A SER EXECUTADA EM DIAS ÚTEIS, A CADA 10 MINUTOS

if __name__=="__main__":    
    estoque = Estoque()
    asyncio.run(estoque.atualizar_olist())