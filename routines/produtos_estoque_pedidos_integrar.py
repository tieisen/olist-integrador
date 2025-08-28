import asyncio
from src.integrador.estoque   import Estoque
from src.integrador.produto   import Produto
from src.integrador.pedido    import Pedido

# ROTINA A SER EXECUTADA EM DIAS ÚTEIS, A CADA 10 MINUTOS

if __name__=="__main__":    
    
    produto = Produto()    
    estoque = Estoque()
    pedido = Pedido()

    asyncio.run(produto.receber_alteracoes())
    asyncio.run(produto.atualizar_olist())
    asyncio.run(produto.atualizar_snk())
    asyncio.run(estoque.atualizar_olist())
    asyncio.run(pedido.receber())
    asyncio.run(pedido.validar_cancelamentos())