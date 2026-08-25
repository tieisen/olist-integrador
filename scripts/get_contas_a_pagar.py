async def get_contas_a_pagar() -> dict:
    """
    Script para buscar contas a pagar no olist por id
    """

    try:
        import os, sys, json, requests
        from pathlib import Path

        root_dir = Path(__file__).resolve().parent.parent
        sys.path.insert(0, str(root_dir))

        from src.utils.load_env import load_env
        from src.olist.autenticacao import Autenticacao

        load_env()

        # url = f"{os.getenv('OLIST_API_URL')}{os.getenv('OLIST_ENDPOINT_FINANCEIRO_PAGAR')}?numeroDocumento=179450"
        url = f"{os.getenv('OLIST_API_URL')}{os.getenv('OLIST_ENDPOINT_FINANCEIRO_PAGAR')}/774253868" #753053887
        print("url", url)

        autentication = Autenticacao(
            codemp=4,
            empresa_id=5
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
            return {
                "status": True,
                "data": data
            }
        else:
            raise Exception(f"Erro ao buscar contas a pagar. Status code: {response.status_code}, Response: {response.text}")
        
    except Exception as e:
        return {    
            "status": False,
            "exception": str(e)
        }


if __name__ == "__main__":
    import asyncio
    result = asyncio.run(get_contas_a_pagar())
    print(result)