import sys
import json
import os
import requests

from pathlib import Path

root_dir = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root_dir))

from src.olist.nota import Nota
from src.olist.autenticacao import Autenticacao
from src.utils.load_env import load_env

load_env()

async def main():
    try:
        print("Buscando nota fiscal do pedido pela lógica do integrador")
        notaOlist = Nota(codemp=1, id_loja=13451, empresa_id=1)
        dados_nota = await notaOlist.buscar(cod_pedido="586615481732269343")
        print(json.dumps(dados_nota, indent=4, ensure_ascii=False))


        print("Buscando nota fiscal do pedido diretamente pelo Olist")
        url = f"{os.getenv('OLIST_API_URL')}{os.getenv('OLIST_ENDPOINT_NOTAS')}/401147449"
        print("url", url)

        
        autentication = Autenticacao(
                codemp=1,
                empresa_id=1
        )

        auth_code = await autentication.solicitar_auth_code()
        token = await autentication.solicitar_token(auth_code)


        headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token['access_token']}"
        }


        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            print(json.dumps(data, indent=4, ensure_ascii=False))
        else:
            raise Exception(f"Erro ao buscar contas a pagar. Status code: {response.status_code}, Response: {response.text}")
    except Exception as e:
        print(f"Erro ao buscar nota fiscal: {str(e)}")

if __name__ == "__main__":
    import asyncio

    asyncio.run(main())