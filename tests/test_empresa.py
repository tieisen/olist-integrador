import pytest
from database.models import Empresa
from database.crud import empresa as crud
from sqlalchemy.future import select
from tests.factories import fake_empresa

@pytest.mark.asyncio
async def test_criar(session):
    empresa_data = fake_empresa()
    ok = await crud.criar(**empresa_data, session=session)
    assert ok is True

    # Verifica se foi criada no banco de teste
    result = await session.execute(
        select(Empresa).where(Empresa.codigo_snk == empresa_data.get('codigo_snk'))
    )
    empresa = result.scalar_one_or_none()
    assert empresa is not None
    assert empresa.nome == empresa_data.get("nome")

@pytest.mark.asyncio
async def test_atualizar_id(session):
    empresa_data = fake_empresa()
    ok = await crud.atualizar_id(empresa_id=1,
                                 nome=empresa_data.get("nome"),
                                 session=session)
    assert ok is True

    # Verifica se foi criada no banco de teste
    result = await session.execute(
        select(Empresa).where(Empresa.id == 1)
    )
    empresa = result.scalar_one_or_none()
    assert empresa.nome == empresa_data.get("nome")
