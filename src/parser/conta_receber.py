import os
import pandas as pd
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

class ContaReceber:

    def __init__(self):
        self.taxa_blz_envios:float = float(os.getenv('BLZWEB_TAXA_ENVIO'))
        self.comissao_blz:float = float(os.getenv('BLZWEB_TAXA_COMISSAO'))

    def carregar_relatorio(self,path:str) -> dict:
        """
        Carrega o relatório de custos do e-commerce
            :param path(str): caminho do arquivo
            :return dict: dados do relatório de custos
        """

        relatorio = None

        def ler_relatorio(path:str):
            """
            Lê o arquivo Excel do relatório de custos
                :param path(str): caminho do arquivo
            """
            nonlocal relatorio
            df:pd.DataFrame = pd.DataFrame()
            try:
                df = pd.read_excel(path,engine="calamine",usecols="A:L")
            except Exception as e:
                logger.error(f"Erro ao ler arquivo do relatório de custos: {e}")
            finally:                
                relatorio = df.copy()
            return True

        def padroniza_colunas():
            """ Padroniza colunas do relatório de custos """
            nonlocal relatorio
            cols:list[str] = []
            try:
                cols = relatorio.columns
                cols = [c.replace(' ','_').replace('-','').replace('º','').replace('ã','a').replace('í','i').lower() for c in cols]
                relatorio.columns = cols
            except Exception as e:
                logger.error(f"Erro ao padronizar colunas do relatório de custos: {e}")
            finally:
                pass
            return True

        def trata_codigo_pedido():
            """ Adiciona uma coluna com o código do pedido tratado """

            nonlocal relatorio
            import re
            regex = r"\D+"
            subst = ""

            try:
                relatorio['pedido_ecommerce'] = relatorio['n_pedido_ecommerce'].apply(lambda x: re.sub(regex, subst, x) if len(x) != 14 else x)
            except Exception as e:
                logger.error(f"Erro ao tratar código do pedido: {e}")
            finally:
                pass
            return True
        
        try:
            ler_relatorio(path=path)
            padroniza_colunas()
            trata_codigo_pedido()
        except Exception as e:
            logger.error(f"Erro ao carregar relatório: {e}")
        finally:
            pass
        return relatorio.to_dict(orient='records')

    def recebimento(self,dados_ecommerce:dict,dados_conta:dict,dados_custo:dict,data:str=None) -> dict:
        """
        Converte os dados dos pedidos no formato da API do Sankhya.
            :param nuconf: número da conferência
            :param dados_olist: lista de pedidos da API do Olist
            :return list[dict]: lista de dicionários com as dados dos itens da conferência
        """        
        def formata_vlr(vlr:float) -> str:
            return vlr.__format__('.2f').replace('.',',')
        
        def calcula_comissao_blz(vlr_pedido:float) -> float:
            return vlr_pedido*self.comissao_blz+self.taxa_blz_envios
        
        # def formata_historico(texto:str) -> str:
        #     import re
        #     regex = r"\(.+\)"
        #     return re.sub(regex, '', texto, 1)
        
        def formata_historico(dados_custo:dict) -> str:
            return f"Referente ao pedido {dados_custo.get('n_pedido')} - OC nº {dados_custo.get('pedido_ecommerce')}"
        
        payload:dict = {}

        try:
            historico:str=''
            vlr_comissao:float=0            
            vlr_pago:float=0            
            if len(dados_custo.get('n_pedido_ecommerce')) == 14:
                # Shopee
                vlr_comissao = dados_custo.get('total_comissao',0) + dados_custo.get('frete_do_pedido',0)
                vlr_pago = dados_custo.get('total_liquido')
                historico = dados_conta.get('historico')
            else:
                # BLZ                
                vlr_comissao = round(calcula_comissao_blz(dados_custo.get('total')) + dados_custo.get('frete_do_pedido'),2)
                vlr_pago = round(dados_custo.get('total') + dados_custo.get('frete_do_pedido') - vlr_comissao,2)
                historico = dados_conta.get('historico')
            
            if len(historico) >= 150:
                historico = formata_historico(dados_custo=dados_custo)
            
            historico+=f"<br>\nTotal produtos R${formata_vlr(dados_custo.get('total'))} | Comissão R${formata_vlr(vlr_comissao-dados_custo.get('frete_do_pedido'))} | Frete R${formata_vlr(dados_custo.get('frete_do_pedido'))} | Incentivo R${formata_vlr(dados_custo.get('total_incentivo'))}"
            
            payload = {
                "contaDestino": {
                    "id": dados_ecommerce.get('id_conta_destino')
                },
                "data": data,
                "categoria": {
                    "id": dados_ecommerce.get('id_categoria_financeiro')
                },
                "historico": historico,
                "taxa": vlr_comissao,
                "juros": None,
                "desconto": None,
                "valorPago": vlr_pago,
                "acrescimo": dados_custo.get('total_incentivo')
            }
        except Exception as e:
            logger.error("Erro ao converter dados para baixa de contas a receber. %s", e)
            print(f"Erro ao converter dados para baixa de contas a receber. {e}")
        finally:
            pass
        return payload