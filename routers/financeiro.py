from fastapi import APIRouter, HTTPException, status
from src.scheduler.jobs.financeiro import consultarRecebimentosShopee, integrar, processar_titulos_planilha
from pydantic import BaseModel

router = APIRouter()

class FinanceiroModel(BaseModel):
    codemp:int | None = None
    idLoja:int | None = None
    dataFim:str | None = None
    dias:int = 0
    processaShopee:bool = True

class FinanceiroShopeeModel(BaseModel):
    codemp:int

class FinanceiroResponse(BaseModel):
    status: bool
    exception: str | None = None

class FinanceiroPlanilhaRegistroModel(BaseModel):
    valor: float
    descricao: str
    data: str
    pendente: bool

class FinanceiroPlanilhaRegistrosModel(BaseModel):
    id_pedido: str
    receita: FinanceiroPlanilhaRegistroModel | None = None
    despesa: FinanceiroPlanilhaRegistroModel | None = None

class FinanceiroPlanilhaModel(BaseModel):
    codemp: int
    idLoja: int
    dtVcto: str
    registros: list[FinanceiroPlanilhaRegistrosModel]

@router.post("/processar")
async def processar_titulos_ecommerce(titulos:FinanceiroPlanilhaModel) -> bool:
    """
    Processa e lança títulos a receber e taxas no Olist a partir do relatório do e-commerce
    """
    
    if not await processar_titulos_planilha(titulos.model_dump()):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao processar títulos")
    return True

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
    res:dict = await integrar(**financeiro.model_dump())
    if not res.get('status'):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Erro ao processar títulos: {res.get('exception')}")
    return res