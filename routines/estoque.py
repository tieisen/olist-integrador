import asyncio
from database.crud import empresa
from src.integrador.estoque import Estoque

# ROTINA A SER EXECUTADA DIARIAMENTE, A CADA 15 MINUTOS
# APÓS A ROTINA DE PRODUTOS E ANTES DA ROTINA DE PEDIDOS

if __name__=="__main__":

    empresas:list[dict]=[]

    empresas = asyncio.run(empresa.buscar())

    print("===================: INTEGRAÇÃO DE ESTOQUE :===================")    

    for i, emp in enumerate(empresas):
        print(f"\nEmpresa {emp.get('nome')} ({i+1}/{len(emp)})".upper())
        estoque = Estoque(codemp=emp.get('snk_codemp'))        
        asyncio.run(estoque.atualizar_olist())