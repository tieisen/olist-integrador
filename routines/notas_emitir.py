from src.integrador.nota import Nota
from src.integrador.separacao import Separacao
import asyncio

# ROTINA A SER EXECUTADA EM DIAS ÚTEIS, ÀS 13H30

if __name__=="__main__":    
    nota = Nota()    
    separacao = Separacao()
    asyncio.run(nota.emitir())
    asyncio.run(separacao.checkout())
    asyncio.run(nota.confirmar())
    asyncio.run(nota.baixar_financeiro())
    