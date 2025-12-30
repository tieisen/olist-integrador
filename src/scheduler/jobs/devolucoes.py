import asyncio
from database.crud import empresa, ecommerce
from src.integrador.devolucao import Devolucao

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
        ecommerces = await ecommerce.buscar(empresa_id=emp.get('id'))
        for j, ecom in enumerate(ecommerces):
            print(f"E-commerce {ecom.get('nome')} ({j+1}/{len(ecommerces)})".upper())
            devolucao = Devolucao(id_loja=ecom.get('id_loja'),codemp=emp.get('snk_codemp'))
            ack.append(await devolucao.integrar_receber())
            ack.append(await devolucao.integrar_devolucoes())
    
    return all(ack)

if __name__=="__main__":

    asyncio.run(integrar_devolucoes())
