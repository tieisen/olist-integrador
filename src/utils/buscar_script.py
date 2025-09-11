import os
import logging
from dotenv import load_dotenv
from src.utils.log import Log

load_dotenv('keys/.env')
logger = logging.getLogger(__name__)
logging.basicConfig(filename=Log().buscar_path(),
                    encoding='utf-8',
                    format=os.getenv('LOGGER_FORMAT'),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

def buscar_script(parametro:str):

    path = os.getenv(parametro)
    if not path:
        erro = f"Parâmetro do diretório do script não informado. param: {path}"
        logger.error(erro)
        print(erro)
        return False
    
    try:
        with open(path, "r") as file:
            script = " ".join(line.strip() for line in file)
    except Exception as e:
        erro = f"Falha ao abrir arquivo do script em {path}. {e}"
        logger.error(erro)
        print(erro)
        return False
    
    if not script.strip():
        erro = f"Arquivo carregado de {path} sem conteúdo"
        logger.error(erro)
        print(erro)
        return False

    return script
