import os
import logging
from datetime import datetime
from src.utils.load_env import load_env

load_env()

def buscar_path() -> str:
    mes_atual = datetime.now().strftime('%Y%m')
    if not os.path.exists(f"./logs/{mes_atual}.log"):
        os.makedirs("./logs/", exist_ok=True)
        with open(f"./logs/{mes_atual}.log", "w") as f:
            pass
    return f"./logs/{mes_atual}.log"

def set_logger(name:str) -> logging:   
    logger = logging.getLogger(name)
    logging.basicConfig(filename=buscar_path(),
                        encoding='utf-8',
                        format=os.getenv('LOGGER_FORMAT'),
                        datefmt='%Y-%m-%d %H:%M:%S',
                        level=logging.INFO)
    return logger