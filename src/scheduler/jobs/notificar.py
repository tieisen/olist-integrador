import asyncio
from src.services.smtp import Email
from database.crud import empresa

async def enviar_notificacao(codemp:int=None):
    empresas:list[dict]=[]
    emp:dict={}
    
    empresas = await empresa.buscar(codemp=codemp)
    email = Email()

    print("::::::::::::::::::: ALERTAS DO INTEGRADOR :::::::::::::::::::")    

    for i, emp in enumerate(empresas):
        print(f"\nEmpresa {emp.get('nome')} ({i+1}/{len(empresas)})".upper())
        await email.notificar(empresa_id=emp.get('id'))

if __name__=="__main__":

    asyncio.run(enviar_notificacao())
