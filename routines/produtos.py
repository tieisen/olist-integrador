import asyncio
from database.crud import empresa
from src.integrador.produto import Produto

# ROTINA A SER EXECUTADA DIARIAMENTE, A CADA 15 MINUTOS
# ANTES DA ROTINA DE ESTOQUE

async def integrar_produtos(codemp:int=None):

    empresas:list[dict]=[]

    empresas = await empresa.buscar(codemp=codemp)

    print("===================: INTEGRAÇÃO DE PRODUTOS :===================")

    for i, emp in enumerate(empresas):
        print(f"\nEmpresa {emp.get('nome')} ({i+1}/{len(emp)})".upper())
        produto = Produto(codemp=emp.get('snk_codemp'))        
        await produto.receber_alteracoes()
        await produto.integrar_olist()
        await produto.integrar_snk()

if __name__=="__main__":

    asyncio.run(integrar_produtos())