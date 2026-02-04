import re
from datetime import datetime
from src.utils.decorador import carrega_dados_ecommerce, carrega_dados_empresa
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

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
    
    @carrega_dados_ecommerce
    @carrega_dados_empresa
    async def to_sankhya(self,dados_olist:dict,dados_cidade:list) -> tuple[dict,list[dict]]:
        """
        Converte os dados dos pedidos no formato da API do Sankhya.
            :param data_olist: dados do pedido da API do Olist
            :param data_cidade: dados da cidade
            :return dict: dicionário com os dados do cabeçalho da nota
            :return list[dict]: lista de dicionários com as dados dos itens da nota
        """

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
            dados_item['CODVOL'] = {"$":item.get('unidade')}
            dados_item['CODLOCALORIG'] = {"$":self.dados_empresa.get('snk_codlocal_venda')}
            lista_itens.append(dados_item)
            
        return dados_sankhya, lista_itens
    
    @carrega_dados_ecommerce
    @carrega_dados_empresa
    async def to_sankhya_lote(self,lista_pedidos:list[dict],lista_itens:list[dict]) -> tuple[dict,list[dict]]:
        """
        Converte os dados dos pedidos no formato da API do Sankhya.
            :param lista_pedidos: lista de pedidos da API do Olist
            :param lista_itens: lista de itens dos pedidos da API do Olist            
            :return dict: dicionário com os dados do cabeçalho da nota
            :return list[dict]: lista de dicionários com as dados dos itens da nota
        """

        def formatar_pedidos(lista_pedidos):
            linhas = [f"{pedido['numero']}" for pedido in lista_pedidos]
            return f"Referente à {len(lista_pedidos)} pedidos:\n" + ", ".join(linhas)        

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
                dados_item['CODVOL'] = {"$":item.get('unidade')}
                dados_item['CODLOCALORIG'] = {"$":self.dados_empresa.get('snk_codlocal_venda')}
                dados_itens.append(dados_item)
            except Exception as e:
                logger.error("Item %s. Erro: %s",item.get('codprod'),e)
                continue
            
        return dados_cabecalho, dados_itens
    
    @carrega_dados_ecommerce
    @carrega_dados_empresa
    async def to_sankhya_pedido_venda(self,lista_itens:list[dict]) -> tuple[dict,list[dict]]:
        """
        Converte os dados dos pedidos no formato da API do Sankhya. Versão para pedido de transferência.
            :param lista_itens: lista de itens dos pedidos da API do Olist            
            :return dict: dicionário com os dados do cabeçalho da nota
            :return list[dict]: lista de dicionários com as dados dos itens da nota
        """

        dados_cabecalho = {}
        dados_itens = []

        data_negociacao = datetime.now().strftime('%d/%m/%Y')
        observacao = f"Itens de ressuprimento para vendas da loja {self.dados_ecommerce.get('nome')}"
        
        dados_cabecalho['AD_MKP_ORIGEM'] = {"$":self.dados_ecommerce.get('id_loja')}
        dados_cabecalho['CIF_FOB'] = {"$":'C'}
        dados_cabecalho['CODCENCUS'] = {"$":'0'}
        dados_cabecalho['CODEMP'] = {"$":self.dados_empresa.get('snk_codemp_fornecedor')}
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
                dados_item['QTDNEG'] = {"$":item.get('quantidade')}
                dados_item['VLRUNIT'] = {"$":item.get('valor',0) if item.get('valor',0) > 0 else 1}
                dados_item['PERCDESC'] = {"$":'0'}
                dados_item['IGNOREDESCPROMOQTD'] = {"$": "True"}
                dados_item['CODVOL'] = {"$":item.get('unidade')}
                dados_item['CODLOCALORIG'] = {"$":item.get('codlocal')}
                dados_itens.append(dados_item)
            except Exception as e:
                logger.error("Item %s. Erro: %s",item.get('codprod'),e)
                continue

        return dados_cabecalho, dados_itens
    
    @carrega_dados_empresa
    async def to_sankhya_atualiza_local(self,nunota:int,lista_sequencias:list) -> list[dict]:
        """
        Converte os dados dos pedidos no formato da API do Sankhya. Versão para atualizar o local de destino da nota de transferência.
            :param nunota: número da nota de transferência
            :param lista_sequencias: lista de itens do pedido
            :return list[dict]: lista de dicionários com as dados dos itens
        """        

        records:list[dict]=[]        
        for sequencia in lista_sequencias:
            try:            
                records.append({
                    "pk": {
                        "NUNOTA": nunota,
                        "SEQUENCIA": sequencia*-1
                    },
                    "values": {
                        "0": f"{self.dados_empresa.get('snk_codlocal_ecommerce')}"
                    }
                })
            except Exception as e:
                msg = f"Erro sequencia {sequencia} da nota {nunota}. {e}"
                logger.error(msg)
            continue
        return records
    
    @carrega_dados_ecommerce
    @carrega_dados_empresa
    async def to_sankhya_baixa_estoque_ecommerce(self,lista_itens:list) -> tuple[dict,list[dict]]:
        """
        Converte os dados dos pedidos no formato da API do Sankhya. Versão para realizar a baixa de estoque do ecommerce.
            :param lista_itens: lista de itens para baixa de estoque
            :return dict: dicionário com os dados do cabeçalho da nota
            :return list[dict]: lista de dicionários com as dados dos itens da nota
        """

        cabecalho:dict={}
        try:
            cabecalho = {
                "NUNOTA":  {"$": ""},
                "SERIENOTA":  {"$": "1"},
                "CODEMP":  {"$": self.dados_empresa.get('snk_codemp_fornecedor')},
                "CODPARC":  {"$": self.dados_empresa.get('snk_codparc')},
                "CODNAT":  {"$": "0"},
                "CODTIPVENDA":  {"$": "11"},
                "CODTIPOPER":  {"$": self.dados_empresa.get('snk_top_baixa_estoque')},
                "CODVEND":  {"$": self.dados_empresa.get('snk_codvend')},
                "CODOBSPADRAO":  {"$": "0"},
                "DTNEG": {"$": datetime.now().strftime('%d/%m/%Y')},
                "TIPMOV":  {"$": "V"},
                "CIF_FOB":  {"$": "C"},
                "TIPFRETE":  {"$": "N"},
                "OBSERVACAO":  {"$": "Baixa de estoque das vendas de ecommerce"}
            }

            itens:list[dict] = []
            for dados_item in lista_itens:
                vlrtot = round(dados_item.get('vlrunit') * dados_item.get('qtdneg'),3)
                item:dict={
                    "NUNOTA":  {"$": ""},
                    "SEQUENCIA":  {"$": ""},
                    "CODPROD":  {"$": dados_item.get('codprod')},
                    "QTDNEG":  {"$": dados_item.get('qtdneg')},
                    "VLRUNIT":  {"$":dados_item.get('vlrunit')},
                    "VLRTOT":  {"$": vlrtot},
                    "PERCDESC":  {"$": "0"},
                    "VLRDESC":  {"$": "0"},
                    "CODVOL":  {"$": dados_item.get('unidade')},
                    "CODLOCALORIG":  {"$": self.dados_empresa.get('snk_codlocal_ecommerce')},
                    "CONTROLE":  {"$": dados_item.get('controle') or ''}
                }
                itens.append(item)
                item = {}

        except Exception as e:
            print(str(e))
        finally:
            pass

        return cabecalho, itens
