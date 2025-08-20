from database.database import SessionLocal
from database.models import TokenSankhya
from datetime import datetime
from src.utils.log import Log
import os
import logging
from dotenv import load_dotenv

load_dotenv('keys/.env')
logger = logging.getLogger(__name__)
logging.basicConfig(filename=Log().buscar_path(),
                    encoding='utf-8',
                    format=os.getenv('LOGGER_FORMAT'),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

def criar(token_criptografado: str, dh_expiracao_token: str):
    session = SessionLocal()
    try:
        novo_token = TokenSankhya(token_criptografado=token_criptografado,
                                  dh_solicitacao=datetime.now(),
                                  dh_expiracao_token=dh_expiracao_token)
        session.add(novo_token)
        session.commit()
        session.refresh(novo_token)
        session.close()
        return novo_token.token_criptografado
    except Exception as e:
        logger.error("Erro ao salvar token no banco de dados: %s",e)
        session.close()
        return False

def buscar():
    session = SessionLocal()
    token = session.query(TokenSankhya).order_by(TokenSankhya.id.desc()).first()
    session.close()
    return token