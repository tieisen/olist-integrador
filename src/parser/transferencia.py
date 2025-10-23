from datetime import datetime
from src.utils.decorador import carrega_dados_empresa, interno
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

class Transferencia:

    def __init__(self, codemp:int=None, empresa_id:int=None):
        self.codemp = codemp
        self.empresa_id = empresa_id
        self.dados_empresa = None

    @interno
    def cabecalho(self):
        cabecalho:dict={}
        try:
            cabecalho = {
                "NUNOTA":  {"$": ""},
                "SERIENOTA":  {"$": "1"},
                "CODEMP":  {"$": self.dados_empresa.get('snk_codemp_fornecedor')},
                "CODEMPNEGOC":  {"$": self.dados_empresa.get('snk_codemp')},
                "CODPARC":  {"$": self.dados_empresa.get('snk_codparc')},
                "CODNAT":  {"$": self.dados_empresa.get('snk_codnat_transferencia')},
                "CODTIPVENDA":  {"$": "0"},
                "CODTIPOPER":  {"$": self.dados_empresa.get('snk_top_transferencia')},
                "CODVEND":  {"$": "1"},
                "CODOBSPADRAO":  {"$": "0"},
                "DTNEG": {"$":datetime.now().strftime('%d/%m/%Y')},
                "TIPMOV":  {"$": "T"},
                "CIF_FOB":  {"$": "C"},
                "TIPFRETE":  {"$": "N"},
                "OBSERVACAO":  {"$": self.dados_empresa.get('snk_texto_transferencia')}
            }
        except Exception as e:
            print(str(e))
        finally:
            pass
        return cabecalho
    
    @interno
    def itens(
            self,
            itens_transferencia:list,
            nunota:int=None,        
            itens_transferidos:list=None
        ) -> list[dict]:

        dados_item = {}
        res = []
        
        # Se a nota de transferência está vazia, lança todos os itens recebidos no parâmetro
        if not itens_transferidos:
            for item in itens_transferencia:
                vlrtot = round(item.get('valor') * item.get('quantidade'),3)
                dados_item['NUNOTA'] = {"$":nunota or ""}
                dados_item['SEQUENCIA'] = {"$":""}
                dados_item['CODPROD'] = {"$":item.get('codprod')}
                dados_item['QTDNEG'] = {"$":item.get('quantidade')}
                dados_item['VLRUNIT'] = {"$":item.get('valor')}
                dados_item['VLRTOT'] = {"$":vlrtot}
                dados_item['PERCDESC'] = {"$": "0"}
                dados_item['VLRDESC'] = {"$": "0"}
                dados_item['CODVOL'] = {"$": 'UN'}
                dados_item['CODLOCALORIG'] = {"$": self.dados_empresa.get('snk_codlocal_venda')}
                dados_item['CODLOCALDEST'] = {"$": self.dados_empresa.get('snk_codlocal_venda')}
                dados_item['CONTROLE'] = {"$": item.get('controle')}
                res.append(dados_item)
                dados_item = {}            
            return res
        
        # Se a nota não está vazia...
        lista_itens_transferidos = [item.get('codprod') for item in itens_transferidos]
        # Valida se o item já está lançado na nota e adiciona a quantidade necessária
        if item.get('codigo') in itens_transferidos:
            if not item.get('lotes'):
                print(f"Item {item.get('codigo')} sem controle informado na nota")
                return []
            dado = {}

            for i in item.get('lotes'):
                for j in lista_itens_transferidos:
                    if int(j.get('codprod')) == int(item.get('codigo')) and j.get('controle') == i.get('lote'):
                        dado = j
                if not dado:
                    print(f"Erro ao buscar dado {j}")
                    return []                                
                dados_item['NUNOTA'] = {"$":nunota}
                dados_item['QTDNEG'] = {"$":int(i.get('quantidade'))+int(dado.get('qtdneg'))}
                dados_item['SEQUENCIA'] = {"$":dado.get('sequencia')}
                res.append(dados_item)
                dados_item = {}
        else:
        # Se o item não está lançado na nota, adiciona a quantidade total
            for i in item.get('lotes'):
                vlrtot = round(item.get('valor') * item.get('quantidade'),3)
                dados_item['NUNOTA'] = {"$":nunota}
                dados_item['SEQUENCIA'] = {"$":""}
                dados_item['CODPROD'] = {"$":item.get('codigo')}
                dados_item['QTDNEG'] = {"$":i.get('quantidade')}
                dados_item['VLRUNIT'] = {"$":item.get('valor')}
                dados_item['VLRTOT'] = {"$":vlrtot}
                dados_item['PERCDESC'] = {"$": "0"}
                dados_item['VLRDESC'] = {"$": "0"}
                dados_item['CODVOL'] = {"$": item.get('unidade')}
                dados_item['CODLOCALORIG'] = {"$": self.dados_empresa.get('snk_codlocal_venda')}
                dados_item['CODLOCALDEST'] = {"$": self.dados_empresa.get('snk_codlocal_venda')}
                dados_item['CONTROLE'] = {"$": i.get('lote')}
                res.append(dados_item)
                dados_item = {}
        return res

    @carrega_dados_empresa
    async def to_sankhya(
            self,
            objeto:str,
            nunota:int=None,
            itens_transferencia:list=None,
            itens_transferidos:list=None
        ) -> tuple[dict,list[dict]]:

        dados_cabecalho = {}
        dados_itens = []        

        if objeto == 'item' and not nunota:
            # Se for lançar somente os itens, API exige que o cabeçalho já exista
            return False            

        if objeto in ['cabecalho','nota']:
            dados_cabecalho = self.cabecalho()

        if objeto in ['item','nota']:
            dados_itens = self.itens(nunota=nunota,
                                     itens_transferencia=itens_transferencia,
                                     itens_transferidos=itens_transferidos)
        
        return dados_cabecalho,dados_itens