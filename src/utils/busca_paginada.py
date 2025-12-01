import os
import time
import requests
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

async def busca_paginada(token:str,url:str) -> list[dict]:

    def ordena_por_id(lista_itens:list[dict]) -> list[dict]:
        """
        Ordenação crescente dos pedidos por ID.
            :param lista_itens: lista de dicionários com os dados da busca
            :return list[dict]: lista de dicionários ordenada pelo ID
        """
        lista_itens.sort(key=lambda i: i['id'])
        return True

    status:int = 200
    itens:list[dict]=[]
    paginacao:dict = {}
    req_time_sleep:float = float(os.getenv('REQ_TIME_SLEEP',1.5))

    try:
        while status == 200:
            # Verifica se há paginação
            if paginacao:        
                if paginacao["limit"] + paginacao["offset"] < paginacao ["total"]:
                    offset = paginacao["limit"] + paginacao["offset"]
                    url+=f"&offset={offset}"
                else:
                    url = None

            if url:
                res = requests.get( url=url,
                                    headers={
                                        "Authorization":f"Bearer {token}",
                                        "Content-Type":"application/json",
                                        "Accept":"application/json"
                                    })
                status=res.status_code
                itens += res.json().get("itens",[])
                paginacao = res.json().get("paginacao",{})
                time.sleep(req_time_sleep)
            else:
                status = 0
        if status not in [200,0]:
            logger.error(f"Erro na busca paginada. Status: {status} - Resposta: {res.text}")
        ordena_por_id(itens)
    except Exception as e:
        logger.error(f"Erro ao realizar busca paginada: {e}")
    finally:
        pass

    return itens