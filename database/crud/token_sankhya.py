from sqlalchemy.orm import Session
from database.models.token_sankhya import TokenSankhya
from database.schemas.token_sankhya import TokenSankhyaBase
from database.dependencies import get_db

def read_last():
    db: Session = next(get_db())
    return db.query(TokenSankhya).order_by(TokenSankhya.id.desc()).first()

def create(token: TokenSankhyaBase):
    db: Session = next(get_db())
    try:
        db_token = TokenSankhya(**token.model_dump())
        db.add(db_token)
        db.commit()
        db.refresh(db_token)
        return db_token.token_criptografado
    finally:
        db.close()
