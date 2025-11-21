import asyncio
from database.crud import empresa
from src.integrador.produto import Produto

async def integrar_produtos(codemp:int=None) -> bool:

    empresas:list[dict]=[]
    ack:list[bool]=[]

    empresas = await empresa.buscar(codemp=codemp)

    print("::::::::::::::::::: INTEGRAÇÃO DE PRODUTOS :::::::::::::::::::")

    for i, emp in enumerate(empresas):
        print(f"\nEmpresa {emp.get('nome')} ({i+1}/{len(empresas)})".upper())
        produto = Produto(codemp=emp.get('snk_codemp'))        
        ack.append(await produto.receber_alteracoes())
        ack.append(await produto.integrar_olist())
        ack.append(await produto.integrar_snk())
    
    return all(ack)

if __name__=="__main__":

    asyncio.run(integrar_produtos())