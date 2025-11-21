from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

class Devolucao:

    def __init__(self):
        pass
           
    def to_sankhya(
            self,
            itens_olist:list[dict],
            itens_snk:list[dict]
        ) -> list[dict]:
        """
        Converte os dados dos pedidos no formato da API do Sankhya para realizar a devolução por NFD.
            :param itens_olist: dados da nota de devolução no Olist
            :param itens_snk: dados da nota de venda no Sankhya
            :return list[dict]: lista de dicionários com as dados dos itens da nota
        """        

        resultado:list[dict]=[]
        for item_olist in itens_olist:
            try:
                for item_snk in itens_snk:
                    # Procura o item devolvido na lista de itens da nota do Sankhya
                    if (int(item_olist.get('codigo')) == int(item_snk.get('codprod'))):                        
                        break

                qtd_disponivel_devolucao = int(item_snk.get('qtdneg'))-int(item_snk.get('qtdentregue'))
                
                # Verifica se existe quantidade disponível para devolver
                if int(item_olist.get('quantidade')) > qtd_disponivel_devolucao:
                    msg = f"Item {item_snk.get('codprod')} não pode ser devolvido pois a quantidade disponível para devolução é menor do que a quantiade a ser devolvida.\n\tQuantidade disponível para devolução: {qtd_disponivel_devolucao}.\n\tQuantidade a ser devolvida: {item_olist.get('quantidade')}"
                    logger.warning(msg)
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
                continue

        return resultado
