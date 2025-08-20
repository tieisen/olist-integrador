from database.database import SessionLocal
from database.models import TokenOlist

def criar(token_criptografado:str,dh_expiracao_token:str,refresh_token_criptografado:str,dh_expiracao_refresh_token:str,id_token_criptografado:str):
    session = SessionLocal()
    try:
        novo_token = TokenOlist(token_criptografado=token_criptografado,
                                dh_expiracao_token=dh_expiracao_token,
                                refresh_token_criptografado=refresh_token_criptografado,
                                dh_expiracao_refresh_token=dh_expiracao_refresh_token,
                                id_token_criptografado=id_token_criptografado)
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
    token = session.query(TokenOlist).order_by(TokenOlist.id.desc()).first()
    session.close()
    return token        
