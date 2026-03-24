from fastapi import APIRouter, HTTPException, status
from src.scheduler.jobs.financeiro import consultarRecebimentosShopee, integrar
from pydantic import BaseModel

router = APIRouter()

class FinanceiroModel(BaseModel):
    codemp:int
    idLoja:int | None = None
    data:str | None = None
    dias:int = 0

class FinanceiroShopeeModel(BaseModel):
    codemp:int

class FinanceiroResponse(BaseModel):
    status: bool
    exception: str | None = None

@router.post("/processar-shopee")
async def processar_shopee(financeiro:FinanceiroShopeeModel) -> bool:    
    """
    Busca os recebimentos na API da Shopee.
    """
    if not await consultarRecebimentosShopee(codemp=financeiro.codemp):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao processar recebimentos da Shopee")
    return True

@router.post("/integrar", status_code=status.HTTP_200_OK, response_model=FinanceiroResponse)
async def integrar_financeiro(financeiro:FinanceiroModel) -> FinanceiroResponse:
    """
    Processa e lança títulos a receber e taxas no Olist.
    """
    res:dict = await integrar(dtFim=financeiro.data,codemp=financeiro.codemp,idLoja=financeiro.idLoja,dias=financeiro.dias)
    if not res.get('status'):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Erro ao processar títulos: {res.get('exception')}")
    return res