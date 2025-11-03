import os
from dotenv import load_dotenv
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import text
from database.models import *  # seus models

load_dotenv('keys/.env')

# URL do banco de testes (NUNCA o da produção!)
TEST_DATABASE_URL = os.getenv("POSTGRES_TEST")

engine_test = create_async_engine(TEST_DATABASE_URL, echo=False, future=True)
AsyncSessionTest = sessionmaker(engine_test, class_=AsyncSession, expire_on_commit=False)

@pytest_asyncio.fixture
async def session():
    async with AsyncSessionTest() as session:
        try:
            yield session
        finally:
            await session.rollback()  # limpa após cada teste
            
