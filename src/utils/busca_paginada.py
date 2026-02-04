import os, time, json, requests
from src.utils.log import set_logger
from src.utils.load_env import load_env
from src.utils.formatter import Formatter
load_env()
logger = set_logger(__name__)

async def paginar_olist(token:str,url:str) -> list[dict]:

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

async def paginar_snk(token:str,url:str,payload:dict) -> list[dict]:

    offset = 0
    limite_alcancado = False
    todos_resultados = []
    req_time_sleep:float = float(os.getenv('REQ_TIME_SLEEP',1.5))
    formatter = Formatter()

    try:
        while not limite_alcancado:
            time.sleep(req_time_sleep)
            payload['requestBody']['dataSet']['offsetPage'] = offset
            res = requests.get(
                url=url,
                headers={ 'Authorization':f"Bearer {token}" },
                json=payload
            )
            if res.status_code != 200:
                logger.error(f"Erro {res.status_code} na busca paginada. Resposta: {res.text}")
                raise Exception(f"Erro {res.status_code} na busca paginada.")
            elif res.json().get('status') == '1':
                todos_resultados.extend(formatter.return_format(res.json()))
                if res.json()['responseBody']['entities'].get('hasMoreResult') == 'true':
                    offset += 1
                else:   
                    limite_alcancado = True
    except Exception as e:
        logger.error(f"Falha ao realizar busca paginada: {e}")
        todos_resultados = []
    finally:
        pass

    return todos_resultados

async def paginar_shopee(url:str,headers:dict) -> list[dict]:

    itens:list[dict]=[]
    req_time_sleep:float = float(os.getenv('REQ_TIME_SLEEP',1.5))
    cursor_data:dict={'cursor': '', 'page_size': None}

    try:
        while cursor_data:
            time.sleep(req_time_sleep)
            resp = requests.post(
                url=url+f"&cursor={cursor_data.get('cursor')}",
                headers=headers
            )
            if not resp.ok:
                return False
            ret = json.loads(resp.content)        
            new_cursor, new_data = ret['response'].get('next_page'), ret['response'].get('list')
            itens.extend(new_data)
            cursor_data = new_cursor
    except Exception as e:
        logger.error(f"Erro ao realizar busca paginada: {e}")
    finally:
        pass

    return itens