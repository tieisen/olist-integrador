from database.database import SessionLocal
from database.models import TokenSankhya, TokenOlist, Log
from datetime import datetime, timedelta

def limpa_cache():

    tokens_snk:list=[]
    tokens_olist:list=[]
    logs:list=[]
    cache:list=[]

    session = SessionLocal()

    try:
        print("Buscando histórico dos tokens do Sankhya...")
        tokens_snk = session.query(TokenSankhya).filter(TokenSankhya.dh_solicitacao <= (datetime.today() - timedelta(days=5))).all()
        tokens_snk+=session.query(TokenSankhya).filter(TokenSankhya.dh_solicitacao.is_(None)).all()
    except:
        print("Erro ao buscar histórico dos tokens do Sankhya")
    finally:
        pass

    try:
        print("Buscando histórico dos tokens do Olist...")
        tokens_olist = session.query(TokenOlist).filter(TokenOlist.dh_solicitacao <= (datetime.today() - timedelta(days=5))).all()
        tokens_olist+=session.query(TokenOlist).filter(TokenOlist.dh_solicitacao.is_(None)).all()
    except:
        print("Erro ao buscar histórico dos tokens do Olist")
    finally:
        pass

    try:
        print("Buscando histórico dos logs...")
        logs = session.query(Log).filter(Log.dh_execucao <= (datetime.today() - timedelta(weeks=4))).all()
        logs+=session.query(Log).filter(Log.dh_execucao.is_(None)).all()
    except:
        print("Erro ao buscar histórico dos logs")        
    finally:
        pass

    cache = tokens_snk+tokens_olist+logs
    if cache:
        print("Excluindo cache...")
        try:
            for i in cache:
                session.delete(i)
            session.commit()
            print("Cache excluído com sucesso!")
        except:
            session.rollback()
            print("Erro ao limpar cache")
        finally:
            pass
    else:
        print("Sem cache para excluir")
    session.close()
    return True

if __name__=='__main__':
    limpa_cache()