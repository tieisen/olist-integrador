import asyncio
from database.crud import log, sankhya, olist
from src.utils.decorador import contexto, log_execucao

def retorno(ack):
    match ack:
        case True:
            print("Ok!")
        case None:
            print("Sem cache para excluir")
        case _:
            print("Erro ao excluir cache")

@contexto
@log_execucao
async def excluir_cache():
    print("-> Excluindo cache Sankhya...")
    ack_snk = await sankhya.excluir_cache()
    retorno(ack_snk)

    print("-> Excluindo cache Olist...")
    ack_olist = await olist.excluir_cache()
    retorno(ack_olist)
    
    print("-> Excluindo cache Logs...")
    ack_logs = await log.excluir_cache()
    retorno(ack_logs)   

if __name__=="__main__":
    asyncio.run(excluir_cache())