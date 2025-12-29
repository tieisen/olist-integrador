from src.utils.log import set_logger
from src.utils.load_env import load_env
from datetime import datetime
load_env()
logger = set_logger(__name__)

class Devolucao:

    def __init__(self):
        pass
           
    def to_sankhya(self,itens_olist:list[dict],itens_snk:list[dict]) -> list[dict]:
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
           
    def to_sankhya_(self,dados_empresa:dict,dados_nfd:dict) -> list[dict]:
        """
        Converte os dados da NFD no formato da API do Sankhya.
            :param dados_empresa: dicionário com os dados do cabeçalho da nota
            :param dados_nfd:lista de itens dos pedidos da API do Olist
            :return dict: dicionário com os dados do cabeçalho da nota
            :return list[dict]: lista de dicionários com as dados dos itens da nota
        """

        dados_cabecalho = {}
        dados_itens = []

        data_negociacao = datetime.strptime(dados_nfd.get('dataEmissao'),'%Y-%m-%d').strftime('%d/%m/%Y')
        observacao = f"NFD {dados_nfd.get('numero')} de {data_negociacao}\n"+dados_nfd.get('observacoes')
        
        dados_cabecalho['CIF_FOB'] = {"$":'C'}
        dados_cabecalho['CODCENCUS'] = {"$":'0'}
        dados_cabecalho['CODEMP'] = {"$":dados_empresa.get('snk_codemp_fornecedor')}
        dados_cabecalho['CODNAT'] = {"$":dados_empresa.get('snk_codnat')}
        dados_cabecalho['CODPARC'] = {"$":dados_empresa.get('snk_codparc')}
        dados_cabecalho['CODTIPOPER'] = {"$":dados_empresa.get('snk_top_devolucao')}
        dados_cabecalho['CODTIPVENDA'] = {"$":dados_empresa.get('snk_codtipvenda')}
        dados_cabecalho['CODVEND'] = {"$":dados_empresa.get('snk_codvend')}
        dados_cabecalho['DTNEG'] = {"$":data_negociacao}
        dados_cabecalho['NUNOTA'] = {},
        dados_cabecalho['NUMNOTA'] = {"$":int(dados_nfd.get('numero'))},
        dados_cabecalho['SERIENOTA'] = {"$":"2"},
        dados_cabecalho['TIPMOV'] = {"$":"D"}
        dados_cabecalho['OBSERVACAO'] = {"$":observacao}

        for item in dados_nfd.get('itens'):
            try:
                dados_item = {}
                dados_item['NUNOTA'] = {}
                dados_item['CODPROD'] = {"$":item.get('codigo')}
                dados_item['QTDNEG'] = {"$":item.get('quantidade')}
                dados_item['VLRUNIT'] = {"$":item.get('valorUnitario')}
                dados_item['PERCDESC'] = {"$":'0'}
                dados_item['IGNOREDESCPROMOQTD'] = {"$": "True"}
                dados_item['CODVOL'] = {"$":item.get('unidade')}
                dados_item['CODLOCALORIG'] = {"$":dados_empresa.get('snk_codlocal_ecommerce')}
                dados_itens.append(dados_item)
            except Exception as e:
                logger.error("Item %s. Erro: %s",item.get('codigo'),e)
                continue

        return dados_cabecalho, dados_itens
