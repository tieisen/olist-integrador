import os
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

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
