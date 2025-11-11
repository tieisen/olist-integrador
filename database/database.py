from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
import os
from sqlalchemy.sql import text
from src.utils.load_env import load_env
load_env()

DATABASE_URL = os.getenv("POSTGRES_URL")
DB_NAME = os.getenv("DB_NAME")

if not all([DATABASE_URL,DB_NAME]):
    raise FileNotFoundError("DATABASE_URL e/ou DB_NAME não encontados no arquivo de ambiente")

engine = create_async_engine(DATABASE_URL+DB_NAME, echo=False)
engine_admin = create_async_engine(DATABASE_URL+"postgres", isolation_level="AUTOCOMMIT")
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()

async def verificar_criar_banco():
    """
    Verifica se o banco existe; se não, cria.
    Usa conexão direta com o PostgreSQL 'postgres' padrão.
    """
    async with engine_admin.begin() as conn:
        result = await conn.execute(
            text(f"SELECT 1 FROM pg_database WHERE datname = '{DB_NAME}'")
        )
        existe = result.scalar()
        if not existe:
            await conn.execute(text(f'CREATE DATABASE "{DB_NAME}"'))
            print(f"✅ Banco '{DB_NAME}' criado com sucesso!")
        else:
            print(f"ℹ️ Banco '{DB_NAME}' já existe.")

    await engine_admin.dispose()

async def criar_tabelas():
    """
    Cria (ou recria) as tabelas no banco especificado.
    """
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    print("✅ Tabelas criadas com sucesso!")
