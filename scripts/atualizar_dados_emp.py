import asyncio
import sys

from pathlib import Path

root_dir = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root_dir))

from database.crud.empresa import atualizar


async def atualizar_dados_emp():
    try:
        print("Atualizando dados da empresa...")
        await atualizar(
            id=0, 
            olist_admin_email="", 
            olist_admin_senha=""
        )
        print("Dados da empresa atualizado com sucesso!")
    except Exception as e:
        print(f"Erro ao atualizar dados da empresa: {e}")



if __name__ == "__main__":
    asyncio.run(atualizar_dados_emp())