async def testar_auth():
    from pathlib import Path
    import sys

    root_dir = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(root_dir))

    from src.utils.load_env import load_env
    from src.sankhya.autenticacao import Autenticacao

    load_env()

    auth = Autenticacao()

    token = await auth.solicitar_token()

    if token:
        print("Token obtido com sucesso: %s", token)
    else:
        print("falha ao obter token")


if __name__ == "__main__":
    import asyncio
    asyncio.run(testar_auth())