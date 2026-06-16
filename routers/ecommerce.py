from fastapi import APIRouter, HTTPException, status
from database.schemas import EcommerceCreate
from database.crud import ecommerce as crud

router = APIRouter()

@router.get("/buscar")
async def buscar_todos() -> list[dict]:
    """
    Busca todos os ecommerces
    """
    dados = await crud.buscar()
    if not dados:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail="Erro ao buscar lista de ecommerces")
    return dados

@router.get("/buscar/{codemp}")
async def buscar_codemp(codemp:int) -> dict:
    """
    Busca todos os ecommerces da empresa informada
    """
    dados = await crud.buscar(codemp=codemp)
    if not dados:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Erro ao buscar lista de ecommerces da empresa {codemp}")
    return dados

@router.post("")
async def criar(ecommerce: EcommerceCreate) -> dict:
    """
    Registra e-commerce na base
    """
    try:
        dados_default:dict = await crud.buscar_dados_cadastro(empresa_id=ecommerce.empresa_id)
        if not dados_default:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                                detail="Erro ao validar empresa")
        
        dados_enviados:dict = ecommerce.model_dump()
        dados_enviados.update(dados_default)
        dados_enviados['nome'] += f" - {dados_enviados['nome_empresa'][:-3]}"
        dados_enviados.pop('nome_empresa')
        
        ecommerce_id = await crud.criar(**dados_enviados)
        if ecommerce_id is None:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT,
                                detail="E-commerce já cadastrado")
        if ecommerce_id is False:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                                detail="Erro ao cadastrar e-commerce")
        
        return dados_enviados
    
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=str(e))
