from sqlalchemy.orm import Session
from database.models.token_olist import TokenOlist
from database.schemas.token_olist import TokenOlistBase
from database.dependencies import get_db

def read_last():
    db: Session = next(get_db())
    token = db.query(TokenOlist).order_by(TokenOlist.id.desc()).first() 
    db.close()
    return token

def create(token: TokenOlistBase):
    db: Session = next(get_db())
    try:
        db_token = TokenOlist(**token.model_dump())
        db.add(db_token)
        db.commit()
        db.refresh(db_token)
        tkn = db_token.token_criptografado
        db.close()
        return tkn
    except:
        db.close()
        return False
