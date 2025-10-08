import asyncio
from src.services.smtp import Email
from database.crud import empresa

# ROTINA A SER EXECUTADA DIARIAMENTE, A CADA 1H

async def enviar_notificacao(codemp:int=None):
    empresas:list[dict]=[]
    emp:dict={}
    
    empresas = asyncio.run(empresa.buscar())
    email = Email()

    print("===================: ALERTAS DO INTEGRADOR :===================")    

    for i, emp in enumerate(empresas):
        print(f"\nEmpresa {emp.get('nome')} ({i+1}/{len(empresas)})".upper())
        asyncio.run(email.notificar(empresa_id=emp.get('id')))

if __name__=="__main__":

    asyncio.run(enviar_notificacao())
