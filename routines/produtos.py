import asyncio
from database.crud import empresa
from src.integrador.produto import Produto

# ROTINA A SER EXECUTADA DIARIAMENTE, A CADA 15 MINUTOS
# ANTES DA ROTINA DE ESTOQUE

if __name__=="__main__":

    empresas:list[dict]=[]

    empresas = asyncio.run(empresa.buscar())

    print("===================: INTEGRAÇÃO DE PRODUTOS :===================")

    for i, emp in enumerate(empresas):
        print(f"\nEmpresa {emp.get('nome')} ({i+1}/{len(emp)})".upper())
        produto = Produto(codemp=emp.get('snk_codemp'))        
        asyncio.run(produto.receber_alteracoes())
        asyncio.run(produto.integrar_olist())
        asyncio.run(produto.integrar_snk())