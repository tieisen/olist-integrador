from database.database import SessionLocal
from database.models import TokenSankhya

def criar(token_criptografado: str, dh_expiracao_token: str):
    session = SessionLocal()
    try:
        novo_token = TokenSankhya(token_criptografado=token_criptografado,
                                  dh_expiracao_token=dh_expiracao_token)
        session.add(novo_token)
        session.commit()
        session.refresh(novo_token)
        session.close()
        return novo_token.token_criptografado
    except:
        session.close()
        return False    

def buscar():
    session = SessionLocal()
    token = session.query(TokenSankhya).order_by(TokenSankhya.id.desc()).first()
    session.close()
    return token