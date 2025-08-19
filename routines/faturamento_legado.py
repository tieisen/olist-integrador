from src.integrador.pedido import Pedido
from src.integrador.faturamento import Faturamento
from src.integrador.nota import Nota
import asyncio

# ROTINA A SER EXECUTADA EM DIAS ÚTEIS, ÀS 11H30 E 14H30

def faturar_pedidos_snk():
    pedido = Pedido()
    faturamento = Faturamento()    
    nota = Nota()    
    asyncio.run(faturamento.venda_entre_empresas_em_lote())
    asyncio.run(pedido.faturar_legado())
    asyncio.run(nota.confirmar_legado())

if __name__=="__main__":
    faturar_pedidos_snk()