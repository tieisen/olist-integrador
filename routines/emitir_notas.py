from src.integrador.nota import Nota
import asyncio

# ROTINA A SER EXECUTADA EM DIAS ÚTEIS, ÀS 13H30

if __name__=="__main__":    
    nota = Nota()    
    asyncio.run(nota.emitir_notas())
    asyncio.run(nota.confirmar_notas())
    asyncio.run(nota.baixar_financeiro())
    