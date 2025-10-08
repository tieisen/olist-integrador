import asyncio
from database.crud import empresa
from src.integrador.estoque import Estoque

# ROTINA A SER EXECUTADA DIARIAMENTE, A CADA 15 MINUTOS
# APÓS A ROTINA DE PRODUTOS E ANTES DA ROTINA DE PEDIDOS

async def integrar_estoque(codemp:int=None):
    
    empresas:list[dict]=[]
    empresas = await empresa.buscar(codemp=codemp)
    ack:list[bool]=[]

    print("===================: INTEGRAÇÃO DE ESTOQUE :===================")    

    for i, emp in enumerate(empresas):
        print(f"\nEmpresa {emp.get('nome')} ({i+1}/{len(empresas)})".upper())
        estoque = Estoque(codemp=emp.get('snk_codemp'))        
        ack.append(await estoque.atualizar_olist())
    
    return all(ack)

if __name__=="__main__":

    asyncio.run(integrar_estoque())
