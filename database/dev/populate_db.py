import sys
import csv
import asyncio


from datetime import datetime
from pathlib import Path
from sqlalchemy import Boolean, Integer, DateTime
from sqlalchemy.future import select

root_dir = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(root_dir))

from database.database import AsyncSessionLocal
from database.models import Empresa, Ecommerce

DIR_PATH = f"{Path(__file__).parent}/data"


def cast_row(model, row: dict) -> dict:
    casted = {}
    for column in model.__table__.columns:
        if column.name not in row:
            continue
        value = row[column.name]
        if value in ("", "NULL"):
            casted[column.name] = None
        elif isinstance(column.type, Boolean):
            casted[column.name] = value.strip().lower() == "true"
        elif isinstance(column.type, Integer):
            casted[column.name] = int(value)
        elif isinstance(column.type, DateTime):
            casted[column.name] = datetime.fromisoformat(value)
        else:
            casted[column.name] = value
    return casted


async def populate_empresa():
    file_path = f"{DIR_PATH}/tabela-empresa.csv"
    if Path(file_path).exists():
        with open(file_path, "r", encoding="utf-8") as data:
            reader = csv.DictReader(data)

            async with AsyncSessionLocal() as session:
                for row in reader:
                    try:
                        result = await session.execute(
                                    select(Empresa)
                                    .where(Empresa.snk_codemp == int(row['snk_codemp']))
                                )
                        empresa = result.scalar_one_or_none()

                        if empresa:
                            print("Empresa já existe")
                            continue

                        empresa_a_cadastrar = Empresa(**cast_row(Empresa, row))
                        session.add(empresa_a_cadastrar)
                        await session.commit()
                    except Exception as e:
                        await session.rollback()
                        print(f"Erro ao inserir linha da empresa de ID: {row['id']}: {e}")


async def populate_ecommerce():
    file_path = f"{DIR_PATH}/tabela-ecommerce.csv"
    if Path(file_path).exists():
        with open(file_path, "r", encoding="utf-8") as data:
            reader = csv.DictReader(data)

            async with AsyncSessionLocal() as session:
                for row in reader:
                    try:
                        result = await session.execute(
                                    select(Ecommerce)
                                    .where(Ecommerce.id_loja == int(row['id_loja']),
                                        Ecommerce.empresa_id == int(row['empresa_id']))
                                )
                        ecommerce = result.scalar_one_or_none()
                        
                        if ecommerce:
                            print("ecommerce já existe")
                            continue

                        ecommerce_a_cadastrar = Ecommerce(**cast_row(Ecommerce, row))
                        session.add(ecommerce_a_cadastrar)
                        await session.commit()
                    except Exception as e:
                        await session.rollback()
                        print(f"Erro ao inserir linha do e-commerce de ID: {row['id']}: {e}")
            

async def populate_db():
    print("iniciando a população do banco")
    print("iniciando a população de empresas")
    await populate_empresa()
    print("iniciando a população de ecommerce")
    await populate_ecommerce()
    print('finalizou o processo de população do banco.')


if __name__ == "__main__":
    asyncio.run(populate_db())