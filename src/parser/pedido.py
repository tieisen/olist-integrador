import os
import re
import logging
from datetime import datetime
from dotenv import load_dotenv

from src.utils.log import Log
from src.utils.decorador.empresa import ensure_dados_empresa
from src.utils.decorador.ecommerce import ensure_dados_ecommerce

load_dotenv('keys/.env')
logger = logging.getLogger(__name__)
logging.basicConfig(filename=Log().buscar_path(),
                    encoding='utf-8',
                    format=os.getenv('LOGGER_FORMAT'),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

class Pedido:

    def __init__(self, id_loja:int):
        self.id_loja = id_loja
        self.dados_ecommerce:dict={}
        self.dados_empresa:dict={}
        
    @property
    def empresa_id(self):
        return self.dados_ecommerce.get('empresa_id')
    
    @property
    def codemp(self):
        return self.dados_empresa.get('snk_codemp')
    
    @ensure_dados_ecommerce
    @ensure_dados_empresa
    async def to_sankhya(
            self,
            dados_olist:dict,
            dados_cidade:list
        ) -> tuple[dict,list]:
        dados_sankhya = {}
        lista_itens = []

        dados_sankhya['AD_MKP_CODPED'] = {"$":str(dados_olist['ecommerce'].get('numeroPedidoEcommerce'))}
        dados_sankhya['AD_MKP_DESTINO'] = {"$": dados_cidade.get('codcid')} # ID DA CIDADE NO SAKNHYA
        dados_sankhya['AD_MKP_ID'] = {"$":dados_olist.get('id')}
        dados_sankhya['AD_MKP_NUMPED'] = {"$":dados_olist.get('numeroPedido')}
        dados_sankhya['AD_MKP_ORIGEM'] = {"$":dados_olist['ecommerce'].get('id')}
        dados_sankhya['CIF_FOB'] = {"$":'C'}
        dados_sankhya['CODCENCUS'] = {"$":'0'}
        dados_sankhya['CODCIDENTREGA'] = {"$":dados_cidade.get('codcid')}
        dados_sankhya['CODUFENTREGA'] = {"$":dados_cidade.get('uf')}
        dados_sankhya['CODCIDDESTINO'] = {"$":dados_cidade.get('codcid')}
        dados_sankhya['CODUFDESTINO'] = {"$":dados_cidade.get('uf')}
        dados_sankhya['CODEMP'] = {"$":self.codemp}
        dados_sankhya['CODNAT'] = {"$":self.dados_empresa.get('snk_codnat')}
        dados_sankhya['CODPARC'] = {"$":self.dados_empresa.get('snk_codparc')}
        dados_sankhya['CODTIPOPER'] = {"$":self.dados_empresa.get('snk_top_pedido')}
        dados_sankhya['CODTIPVENDA'] = {"$":self.dados_empresa.get('snk_codtipvenda')}
        dados_sankhya['CODVEND'] = {"$":self.dados_empresa.get('snk_codvend')}
        data_negociacao = datetime.strptime(dados_olist.get('data'),'%Y-%m-%d').strftime('%d/%m/%Y')
        dados_sankhya['DTNEG'] = {"$":data_negociacao}
        dados_sankhya['NUNOTA'] = {},
        dados_sankhya['TIPMOV'] = {"$":"P"}
        vlr_frete = dados_olist.get('valorFrete') - dados_olist.get('valorDesconto',0) if dados_olist.get('valorFrete') > 0 else ""
        dados_sankhya['VLRFRETE'] = {"$":vlr_frete}
        dados_sankhya['OBSERVACAO'] = {"$":f"Pedido #{dados_olist.get('numeroPedido')} importado do Olist."}

        for item in dados_olist.get('itens'):            
            dados_item = {}
            codprod = re.search(r'^\d{8}', item['produto'].get('sku'))
            dados_item['NUNOTA'] = {},
            dados_item['CODPROD'] = {"$":codprod.group()}
            dados_item['QTDNEG'] = {"$":item.get('quantidade')}
            dados_item['VLRUNIT'] = {"$":item.get('valorUnitario') if item.get('valorUnitario') > 0 else 0.01}
            dados_item['PERCDESC'] = {"$":'0'}
            dados_item['IGNOREDESCPROMOQTD'] = {"$": "True"}
            dados_item['CODVOL'] = {"$":"UN"}
            dados_item['CODLOCALORIG'] = {"$":self.dados_empresa.get('snk_codlocal_venda')}
            lista_itens.append(dados_item)
            
        return dados_sankhya, lista_itens
    
    @ensure_dados_ecommerce
    @ensure_dados_empresa
    async def to_sankhya_lote(
            self,
            lista_pedidos:list,
            lista_itens:list
        ) -> tuple[dict,list]:

        def formatar_pedidos(lista_pedidos):
            linhas = [f"- {pedido['numero']}/{pedido['codigo']}" for pedido in lista_pedidos]
            return f"Referente à {len(lista_pedidos)} pedidos:\n" + "\n".join(linhas)        

        dados_cabecalho = {}
        dados_itens = []

        data_negociacao = datetime.now().strftime('%d/%m/%Y')
        observacao = formatar_pedidos(lista_pedidos)
        
        dados_cabecalho['AD_MKP_ORIGEM'] = {"$":self.dados_ecommerce.get('id_loja')}
        dados_cabecalho['CIF_FOB'] = {"$":'C'}
        dados_cabecalho['CODCENCUS'] = {"$":'0'}
        dados_cabecalho['CODEMP'] = {"$":self.dados_empresa.get('snk_codemp')}
        dados_cabecalho['CODNAT'] = {"$":self.dados_empresa.get('snk_codnat')}
        dados_cabecalho['CODPARC'] = {"$":self.dados_empresa.get('snk_codparc')}
        dados_cabecalho['CODTIPOPER'] = {"$":self.dados_empresa.get('snk_top_pedido')}
        dados_cabecalho['CODTIPVENDA'] = {"$":self.dados_empresa.get('snk_codtipvenda')}
        dados_cabecalho['CODVEND'] = {"$":self.dados_empresa.get('snk_codvend')}
        dados_cabecalho['DTNEG'] = {"$":data_negociacao}
        dados_cabecalho['NUNOTA'] = {},
        dados_cabecalho['TIPMOV'] = {"$":"P"}
        dados_cabecalho['OBSERVACAO'] = {"$":observacao}

        for item in lista_itens:
            try:
                dados_item = {}
                dados_item['NUNOTA'] = {}
                dados_item['CODPROD'] = {"$":item.get('codprod')}
                dados_item['QTDNEG'] = {"$":item.get('qtdneg')}
                dados_item['VLRUNIT'] = {"$":item.get('vlrunit') if item.get('vlrunit') > 0 else 0.01}
                dados_item['PERCDESC'] = {"$":'0'}
                dados_item['IGNOREDESCPROMOQTD'] = {"$": "True"}
                dados_item['CODVOL'] = {"$":"UN"}
                dados_item['CODLOCALORIG'] = {"$":self.dados_empresa.get('snk_codlocal_venda')}
                dados_itens.append(dados_item)
            except Exception as e:
                logger.error("Item %s. Erro: %s",item.get('codprod'),e)
                print(f"Item {item.get('codprod')}. Erro: {e}")
                continue

        return dados_cabecalho, dados_itens

    def to_sankhya_devolucao(
            self,
            dados_olist:list,
            dados_sankhya:list
        ) -> list:

        dados_itens = []

        try:
            for item_olist in dados_olist:
                # Procura o item devolvido na lista de itens da nota do Sankhya
                for item_snk in dados_sankhya:                
                    if int(item_olist.get('sku'))==int(item_snk.get('codprod')):
                        break

                # Verifica se o item já está na lista de retorno e soma a quantidade                    
                for item in dados_itens:
                    if item.get('$') == item_snk.get('sequencia'):
                        item['QTDFAT'] = item['QTDFAT']+item_olist.get('quantidade')
                        continue            

                # Adiciona o novo item na lista de retorno
                dados_itens.append({
                    "$": item_snk.get('sequencia'),
                    "QTDFAT": item_olist.get('quantidade'),
                })
        except Exception as e:
            logger.error("Erro ao converter dados de devolução do item %s. %s",item_olist,e)
        finally:
            return dados_itens
