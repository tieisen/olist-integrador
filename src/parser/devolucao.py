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

class Devolucao:

    def __init__(self):
        pass
           
    def to_sankhya(
            self,
            itens_olist:list[dict],
            itens_snk:list[dict]
        ) -> list[dict]:

        resultado:list[dict]=[]
        for item_olist in itens_olist:
            try:
                for item_snk in itens_snk:
                    # Procura o item devolvido na lista de itens da nota do Sankhya
                    if (int(item_olist.get('codigo')) == int(item_snk.get('codprod'))):
                        break
                    qtd_disponivel_devolucao = item_snk.get('qtdneg')-item_snk.get('qtdentregue')
                
                # Verifica se existe quantidade disponível para devolver
                if int(item_olist.get('quantidade')) > qtd_disponivel_devolucao:
                    print(f"Item {item_snk.get('codprod')} não pode ser devolvido pois a quantidade disponível para devolução é menor do que a quantiade a ser devolvida.\n\tQuantidade disponível para devolução: {qtd_disponivel_devolucao}.\n\tQuantidade a ser devolvida: {item_olist.get('quantidade')}")
                    continue

                # Verifica se o item já está na lista de retorno e soma a quantidade
                for item in resultado:
                    if item.get('$') == item_snk.get('sequencia'):
                        item['QTDFAT'] = item['QTDFAT']+item_olist.get('quantidade')
                        continue            

                # Adiciona o novo item na lista de retorno
                resultado.append({
                    "$": item_snk.get('sequencia'),
                    "QTDFAT": item_olist.get('quantidade')
                })
            except Exception as e:
                logger.error("Erro ao converter dados de devolução do item %s. %s",item_olist,e)
                print(f"Erro ao converter dados de devolução do item {item_olist}. {e}")
                continue

        return resultado
