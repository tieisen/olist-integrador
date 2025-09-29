import os
import logging
from dotenv import load_dotenv
from database.crud import empresa

load_dotenv('keys/.env')
logger = logging.getLogger(__name__)
logging.basicConfig(filename=os.getenv('PATH_LOGS'),
                    encoding='utf-8',
                    format=os.getenv('LOGGER_FORMAT'),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

class Empresa:

    def __init__(self):
        pass

    async def buscar(
            self,
            id:int=None,
            codemp:int=None
        ) -> list[dict]:

        dados_empresas:list[dict]=[]
        dados_empresas = await empresa.buscar(id=id,
                                              codemp=codemp)
        return dados_empresas