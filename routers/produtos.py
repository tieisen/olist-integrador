from fastapi import APIRouter, HTTPException, status
from src.scheduler.jobs.produtos import integrar_produtos
from pydantic import BaseModel

router = APIRouter()

class ProdutoModel(BaseModel):
    codemp:int

@router.post("/integrar")
async def integrar(produto:ProdutoModel):
    """
    Cria o cadastro de produtos novos no Olist.
    Atualiza no Olist os dados dos produtos que tiveram alteração no cadastro do Sankhya.
    Atualiza no Sankhya os dados dos produtos que tiveram alteração no cadastro do Olist.
    """
    res:bool=None
    res = await integrar_produtos(codemp=produto.codemp)
    if not res:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao integrar produtos")
    return res