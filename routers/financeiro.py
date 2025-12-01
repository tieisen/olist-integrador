from fastapi import APIRouter, HTTPException, status
from src.integrador.financeiro import Financeiro
from datetime import datetime
import asyncio
from pydantic import BaseModel

router = APIRouter()

class FinanceiroModel(BaseModel):
    id_loja:int
    data:str

@router.post("/baixar")
async def baixar_contas(financeiro:FinanceiroModel) -> bool:
    """
    Baixa contas a receber com base no relatório de custos do e-commerce.
    """
    try:
        financeiro.data = datetime.strptime(financeiro.data, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Formato de data inválido. Use 'YYYY-MM-DD'.")
    fin = Financeiro(id_loja=financeiro.id_loja)
    if not asyncio.run(fin.executar_baixa(data=financeiro.data)):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao baixar contas a receber")
    return True
