import asyncio
from tests.conftest import engine_test, Base

async def setup_database():
    # Cria as tabelas no início da sessão de testes
    async with engine_test.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

if __name__ == "__main__":
    asyncio.run(setup_database())