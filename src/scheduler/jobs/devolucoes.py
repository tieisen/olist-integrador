import asyncio
from database.crud import empresa, ecommerce
from src.integrador.devolucao import Devolucao

# ROTINA A SER EXECUTADA DIARIAMENTE, ÀS 19H

async def integrar_devolucoes(codemp:int=None):
    empresas:list[dict]=[]
    ecommerces:list[dict]=[]
    emp:dict={}
    ecom:dict={}
    ack:list[bool]=[]    

    empresas = await empresa.buscar(codemp=codemp)

    print("::::::::::::::::::: INTEGRAÇÃO DE DEVOLUÇÕES :::::::::::::::::::")    

    for i, emp in enumerate(empresas):
        print(f"\nEmpresa {emp.get('nome')} ({i+1}/{len(empresas)})".upper())
        ecommerces = asyncio.run(ecommerce.buscar(empresa_id=emp.get('id')))
        for j, ecom in ecommerces:
            print(f"E-commerce {ecom.get('nome')} ({j+1}/{len(ecom)})".upper())
            devolucao = Devolucao(id_loja=ecom.get('id_loja'))
            ack.append(await devolucao.integrar_receber())
            ack.append(await devolucao.integrar_devolucoes())
    
    return all(ack)

if __name__=="__main__":

    asyncio.run(integrar_devolucoes())
