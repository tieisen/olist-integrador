import os
import re
import unicodedata
import pandas as pd
from datetime import datetime
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

class Financeiro:

    def __init__(self):
        self.taxa_blz_envios:float = float(os.getenv('BLZWEB_TAXA_ENVIO'))
        self.comissao_blz:float = float(os.getenv('BLZWEB_TAXA_COMISSAO'))
        self.campos_snk = [
            "CODEMP", "NUMNOTA", "SERIENOTA", "DTNEG", "DESDOBRAMENTO", "DHMOV",
            "DTVENC", "CODPARC", "CODTIPOPER", "DHTIPOPER", "CODBCO", "CODCTABCOINT",
            "CODNAT", "CODTIPTIT", "HISTORICO", "VLRDESDOB", "TIPMULTA", "TIPJURO",
            "AUTORIZADO", "RECDESP", "PROVISAO","ORIGEM", "NUNOTA"
        ]

    def normalizar_texto(self, texto: str) -> str:
        if not texto:
            return texto

        # Normaliza para decompor acentos
        texto = unicodedata.normalize('NFKD', texto)

        # Remove acentos (caracteres combinantes)
        texto = ''.join(
            c for c in texto
            if not unicodedata.combining(c)
        )

        # Remove caracteres especiais (mantém letras, números e espaço)
        texto = re.sub(r'[^a-zA-Z0-9\s]', '', texto)

        # Remove espaços extras
        texto = re.sub(r'\s+', ' ', texto).strip()

        # Adiciona _ nos espaços
        texto = texto.replace(' ', '_')

        # Remove maiúsculas
        texto = texto.lower()

        return texto
    
    def olist_pagar(self,dtNeg:str,dtVcto:str,valor:float,numDocumento:int,historico:str,dados_empresa:dict) -> dict:
        """
        Converte os dados no formato da API do Olist.
            :param dtNeg: data da negociação no formato YYYY-MM-DD            
            :param dtVcto: data do vencimento no formato YYYY-MM-DD
            :param valor: valor do título
            :param numDocumento: número da NF
            :param historico: texto da observação do título
            :return dict: dicionário com os dados no padrão Olist
        """

        payload:dict={}

        if dados_empresa.get('id') == 5:
            categoria = 746666005
        elif dados_empresa.get('id') == 1:
            categoria = 367785383
        else:
            categoria = None

        payload = {
            "data": dtNeg,
            "dataVencimento": dtVcto,
            "valor": valor,
            "numeroDocumento": str(numDocumento),
            "contato": {
                "id": dados_empresa.get('olist_id_fornecedor_padrao')
            },
            "historico": historico,
            "categoria": {
                "id": categoria
            },
            "dataCompetencia": None,
            "ocorrencia": "U",
            "formaPagamento": 15,
            "diaVencimento": None,
            "quantidadeParcelas": 1,
            "diaSemanaVencimento": None
        }

        return payload
    
    def carregar_relatorio_olist(self,path:str=None,arquivo=None,lista:list[dict]=None) -> dict:
        """
        Carrega o relatório de custos do e-commerce
            :param path(str): caminho do arquivo
            :return dict: dados do relatório de custos
        """

        relatorio = None

        def ler_relatorio(path:str):
            """
            Busca e lê o arquivo Excel do relatório de custos
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

        def carregar_arquivo(arquivo):
            """
            Carrega o arquivo Excel do relatório de custos
                :param arquivo: arquivo carregado em memória
            """
            nonlocal relatorio
            conteudo = arquivo.read()
            df:pd.DataFrame = pd.DataFrame()
            try:

                df = pd.read_excel(conteudo,engine="calamine",usecols="A:L")
            except Exception as e:
                logger.error(f"Erro ao ler arquivo do relatório de custos: {e}")
            finally:                
                relatorio = df.copy()
            return True

        def converter_relatorio(lista:list[dict]):
            """
            Converte lista de dicionários em dataframe
                :param lista: lista de dicionários
            """
            nonlocal relatorio
            relatorio = pd.DataFrame(lista)
            return True

        def padroniza_colunas():
            """ Padroniza colunas do relatório de custos """
            nonlocal relatorio
            cols:list[str] = []
            try:
                cols = relatorio.columns
                cols = [self.normalizar_texto(c).lower() for c in cols]
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
            if arquivo:
                carregar_arquivo(arquivo=arquivo)
            elif lista:
                converter_relatorio(lista=lista)
            elif path:
                ler_relatorio(path=path)
            else:
                raise ValueError("Nenhum parâmetro válido para carregar o relatório de custos")
            padroniza_colunas()
            trata_codigo_pedido()
        except Exception as e:
            logger.error(f"Erro ao carregar relatório: {e}")
        finally:
            pass
        return relatorio.to_dict(orient='records')
    
    def carregar_relatorio_shopee(self,path:str=None,arquivo=None,lista:list[dict]=None) -> dict:
        """
        Carrega o relatório de recebimentos da shopee
            :param path(str): caminho do arquivo
            :return dict: dados do relatório de custos
        """

        relatorio = None

        def ler_relatorio(path:str):
            """
            Busca e lê o arquivo Excel do relatório de custos
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

        def carregar_arquivo(arquivo):
            """
            Carrega o arquivo Excel do relatório de custos
                :param arquivo: arquivo carregado em memória
            """
            nonlocal relatorio
            conteudo = arquivo.read()
            df:pd.DataFrame = pd.DataFrame()
            try:

                df = pd.read_excel(conteudo,engine="calamine",usecols="A:L")
            except Exception as e:
                logger.error(f"Erro ao ler arquivo do relatório de custos: {e}")
            finally:                
                relatorio = df.copy()
            return True

        def converter_relatorio(lista:list[dict]):
            """
            Converte lista de dicionários em dataframe
                :param lista: lista de dicionários
            """
            nonlocal relatorio
            relatorio = pd.DataFrame(lista)
            return True

        def padroniza_colunas():
            """ Padroniza colunas do relatório de custos """
            nonlocal relatorio
            cols:list[str] = []
            try:
                cols = relatorio.columns
                cols = [self.normalizar_texto(c) for c in cols]                
                relatorio.columns = cols
            except Exception as e:
                logger.error(f"Erro ao padronizar colunas do relatório de custos: {e}")
            finally:
                pass
            return True
        
        try:
            if arquivo:
                carregar_arquivo(arquivo=arquivo)
            elif lista:
                converter_relatorio(lista=lista)
            elif path:
                ler_relatorio(path=path)
            else:
                raise ValueError("Nenhum parâmetro válido para carregar o relatório de custos")
            padroniza_colunas()
        except Exception as e:
            logger.error(f"Erro ao carregar relatório: {e}")
        finally:
            pass
        return relatorio.to_dict(orient='records')

    def olist_receber_shopee(self,dados_ecommerce:dict,dados_conta:dict,dados_recebimento:dict) -> dict:
        """
        Converte os dados dos lançamentos no formato da API do Olist.
            :param dados_ecommerce: dicionário com os dados do e-commerce
            :param dados_conta: dicionário com os dados da conta a receber
            :param dados_recebimento: dicionário com os dados do recebimento do pedido
            :param data: data da baixa no formato dd/mm/aaaa
            :return dict: dicionário com os dados formatados para a API
        """        
        def formata_vlr(vlr:float) -> str:
            return vlr.__format__('.2f').replace('.',',')
        
        payload:dict = {}
        historico:str=''
        vlr_total_pedido:float=0
        vlr_pago:float=0
        vlr_desconto_total:float=None
        vlr_cupom:float=None
        dt_recebimento:str=None

        try:
            vlr_total_pedido = dados_conta.get('valor')
            vlr_pago = dados_recebimento.get('released_amount')
            historico = dados_conta.get('historico')
            dt_recebimento = dados_recebimento.get('actual_payout_time')

            if not any([vlr_total_pedido,vlr_pago]):
                raise ValueError("Total do pedido ou valor do recebimento não encontrado. vlr_total_pedido: %s. vlr_pago: %s", vlr_total_pedido, vlr_pago)

            if vlr_pago > vlr_total_pedido:
                vlr_cupom = round(vlr_pago - vlr_total_pedido,2)
                historico+=f"<br>\nTotal pedido R${formata_vlr(vlr_total_pedido)} | Incentivo(+) R${formata_vlr(vlr_cupom)} | Recebido R${formata_vlr(vlr_pago)}"            
            else:
                vlr_desconto_total = round(vlr_total_pedido - vlr_pago,2)
                historico+=f"<br>\nTotal pedido R${formata_vlr(vlr_total_pedido)} | Taxa/Comissão(-) R${formata_vlr(vlr_desconto_total)} | Recebido R${formata_vlr(vlr_pago)}"
            
            if not any([vlr_cupom,vlr_desconto_total]):
                raise ValueError("Valor do incentivo ou desconto não encontrado. vlr_pago: %s. vlr_total_pedido: %s", vlr_pago, vlr_total_pedido)

            payload = {
                "contaDestino": {
                    "id": dados_ecommerce.get('id_conta_destino')
                },
                "data": dt_recebimento,
                "categoria": {
                    "id": dados_ecommerce.get('id_categoria_financeiro')
                },
                "historico": historico,
                "taxa": vlr_desconto_total,
                "juros": None,
                "desconto": None,
                "valorPago": vlr_pago,
                "acrescimo": vlr_cupom
            }
        except Exception as e:
            logger.error("Erro ao converter dados para baixa de contas a receber do pedido %s. %s", dados_recebimento.get('id_do_pedido'), e)
        finally:
            pass
        return payload
    
    def olist_receber(self,dados_ecommerce:dict,dados_conta:dict,dados_custo:dict,data:str=None) -> dict:
        """
        Converte os dados dos lançamentos no formato da API do Olist.
            :param dados_ecommerce: dicionário com os dados do e-commerce
            :param dados_conta: dicionário com os dados da conta a receber
            :param dados_custo: dicionário com os dados dos custos do pedido
            :param data: data da baixa no formato dd/mm/aaaa
            :return dict: dicionário com os dados formatados para a API
        """        
        def formata_vlr(vlr:float) -> str:
            return vlr.__format__('.2f').replace('.',',')
        
        def calcula_comissao_blz(vlr_pedido:float) -> float:
            """ Calcula o valor da comissão do BLZWeb (taxa fixa de envio + percentual sobre o valor do pedido) """
            return vlr_pedido*self.comissao_blz+self.taxa_blz_envios
        
        def formata_historico(dados_custo:dict) -> str:
            return f"Referente ao pedido OC nº {dados_custo.get('pedido_ecommerce')}"
        
        def soma_valor_incentivo(vlr_total:float,vlr_incentivo:float) -> float:
            return round(vlr_total + vlr_incentivo,2)
        
        payload:dict = {}
        historico:str=''
        vlr_total_pedido:float=0
        vlr_desconto_total:float=0
        vlr_comissao:float=0            
        vlr_pago:float=0

        try:       
            if len(dados_custo.get('n_pedido_ecommerce')) == 14:
                # Shopee - valor da comissão é variável por pedido e já está informado no relatório
                vlr_comissao = dados_custo.get('total_comissao',0)
                vlr_desconto_total = vlr_comissao + dados_custo.get('frete_do_pedido',0)
                if dados_custo.get('total',0) < vlr_comissao:                    
                    vlr_total_pedido = soma_valor_incentivo(dados_custo.get('total',0),vlr_comissao) + dados_custo.get('frete_do_pedido',0)
                else:
                    vlr_total_pedido = dados_custo.get('total',0) + dados_custo.get('frete_do_pedido',0)
                vlr_pago = round(vlr_total_pedido - vlr_desconto_total,2)
            else:
                # BLZ - valor da comissão precisa ser calculado
                vlr_comissao = round(calcula_comissao_blz(dados_custo.get('total')),2)
                vlr_desconto_total = round(vlr_comissao + dados_custo.get('frete_do_pedido'),2)
                vlr_total_pedido = dados_custo.get('total',0) + dados_custo.get('frete_do_pedido',0)
                vlr_pago = round(vlr_total_pedido - vlr_desconto_total,2)
            
            # Se histórico muito grande (contas agrupadas), formatar
            historico = dados_conta.get('historico')            
            if len(historico) >= 150:
                historico = formata_historico(dados_custo=dados_custo)            
            historico+=f"<br>\nTotal pedido (produtos + frete) R${formata_vlr(vlr_total_pedido)} | Comissão R${formata_vlr(vlr_comissao)} | Valor pago R${formata_vlr(vlr_pago)}"
            
            payload = {
                "contaDestino": {
                    "id": dados_ecommerce.get('id_conta_destino')
                },
                "data": data,
                "categoria": {
                    "id": dados_ecommerce.get('id_categoria_financeiro')
                },
                "historico": historico,
                "taxa": vlr_desconto_total,
                "juros": None,
                "desconto": None,
                "valorPago": vlr_pago,
                "acrescimo": None
            }
        except Exception as e:
            logger.error("Erro ao converter dados para baixa de contas a receber do pedido %s. %s", dados_custo.get('n_pedido_ecommerce'), e)
        finally:
            pass
        return payload    