from database.database import engine, Base
from database.models import *
import asyncio

async def criar_tabelas():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

if __name__ == "__main__":
    asyncio.run(criar_tabelas())