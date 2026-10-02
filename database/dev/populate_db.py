import asyncio
import csv
import os
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import Boolean, DateTime, Integer
from sqlalchemy.future import select

root_dir = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(root_dir))

from database.crud.sankhya import buscar as snkBuscar  # noqa: E402
from database.crud.sankhya import criar as snkCriar  # noqa: E402
from database.database import AsyncSessionLocal  # noqa: E402
from database.models import Ecommerce, Empresa, Produto  # noqa: E402

load_dotenv()

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
    print("iniciando a população de empresas")
    file_path = f"{DIR_PATH}/tabela-empresa.csv"
    if Path(file_path).exists():
        with open(file_path, encoding="utf-8") as data:
            reader = csv.DictReader(data)

            async with AsyncSessionLocal() as session:
                for row in reader:
                    try:
                        result = await session.execute(
                            select(Empresa).where(Empresa.snk_codemp == int(row["snk_codemp"]))
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

    print("Finalizado a população de empresas")


async def populate_ecommerce():
    print("iniciando a população de ecommerce")
    file_path = f"{DIR_PATH}/tabela-ecommerce.csv"
    if Path(file_path).exists():
        with open(file_path, encoding="utf-8") as data:
            reader = csv.DictReader(data)

            async with AsyncSessionLocal() as session:
                for row in reader:
                    try:
                        result = await session.execute(
                            select(Ecommerce).where(
                                Ecommerce.id_loja == int(row["id_loja"]),
                                Ecommerce.empresa_id == int(row["empresa_id"]),
                            )
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

    print("Finalizado a população de Ecommerces")


async def populate_sankhya():
    app_id = int(os.getenv("SANKHYA_APP_ID")) or None
    if not app_id:
        raise Exception("app_id não encontrado nas variaveis de ambiente")

    x_token = os.getenv("SANKHYA_APP_X_TOKEN") or None
    if not x_token:
        raise Exception("xToken não encontrado nas variaveis de ambiente")

    auth_url = os.getenv("SANKHYA_URL_AUTH") or None
    if not auth_url:
        raise Exception("URL do autenticador não encontrada")

    existe = await snkBuscar(app_id)

    if existe:
        print("Integração sankhya já cadastrada")
    else:
        print("Criando integração do sankhya no banco")
        ack_sankhya = await snkCriar(app_id=app_id, x_token=x_token)
        print("Integração criado com sucesso") if ack_sankhya else print(
            "Erro ao criar integração no banco!"
        )


async def populate_produtos():
    print("Iniciando a população dos produtos")
    file_path = f"{DIR_PATH}/tabela-produtos.csv"
    if Path(file_path).exists():
        with open(file_path, encoding="utf-8") as data:
            reader = csv.DictReader(data)

            async with AsyncSessionLocal() as session:
                for row in reader:
                    try:
                        result = await session.execute(
                            select(Produto).where(
                                Produto.codprod == int(row["codprod"]),
                                Produto.idprod == int(row["idprod"]),
                                Produto.empresa_id == int(row["empresa_id"]),
                            )
                        )

                        produto = result.scalar_one_or_none()

                        if produto:
                            continue

                        produto_a_cadastrar = Produto(**cast_row(Produto, row))
                        session.add(produto_a_cadastrar)
                    except Exception as e:
                        print(
                            f"Erro ao inserir o produto: {row['codprod']} - {row['idprod']}, - {row['empresa_id']} - erro: {e}"  # noqa: E501
                        )
                await session.commit()
    print("Finalizada a população dos produtos")


async def populate_db():
    print("iniciando a população do banco")

    await populate_empresa()
    await populate_ecommerce()
    await populate_sankhya()
    await populate_produtos()

    print("finalizou o processo de população do banco.")


if __name__ == "__main__":
    asyncio.run(populate_db())
