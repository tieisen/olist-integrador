from src.integrador.pedido import Pedido
from src.integrador.faturamento import Faturamento
from src.integrador.nota import Nota
from src.integrador.separacao import Separacao
import asyncio

# ROTINA A SER EXECUTADA EM DIAS ÚTEIS, ÀS 12H

def faturar_pedidos_snk():
    pedido = Pedido()
    faturamento = Faturamento()
    separacao = Separacao()
    nota = Nota()    
    if not asyncio.run(faturamento.venda_entre_empresas_em_lote()):
        return False
    if not asyncio.run(pedido.faturar_legado()):
        return False
    if not asyncio.run(nota.emitir()):
        return False
    if not asyncio.run(separacao.checkout()):
        return False
    if not asyncio.run(nota.confirmar()):
        return False
    if not asyncio.run(nota.baixar_financeiro()):
        return False
    return True

if __name__=="__main__":
    faturar_pedidos_snk()