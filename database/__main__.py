from database.database import verificar_criar_banco, criar_tabelas
from database.models import *
import asyncio

async def main():
    await verificar_criar_banco()
    await criar_tabelas()

if __name__ == "__main__":
    asyncio.run(main())