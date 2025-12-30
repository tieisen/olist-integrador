import os, requests
from src.utils.autenticador import token_snk
from src.utils.buscar_arquivo import buscar_script
from src.utils.formatter import Formatter
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

class Faturamento:

    def __init__(self, codemp:int=None, empresa_id:int=None):
        self.token = None
        self.codemp = codemp
        self.empresa_id = empresa_id
        self.formatter = Formatter()

    @token_snk
    async def buscar_itens(self,nunota:int=None) -> list[dict]:
        """
        Busca lista de itens conferidos no dia ou de um pedido.
            :param nunota: número único do pedido de venda
            :return list[dict]: lista com os dados dos itens conferidos
        """

        url = os.getenv('SANKHYA_URL_DBEXPLORER')
        if not url:
            logger.error("Erro relacionado à url. %s",url)
            return False

        parametro = 'SANKHYA_PATH_SCRIPT_CONFERIDOS_PEDIDO' if nunota else 'SANKHYA_PATH_SCRIPT_CONFERIDOS_DIA'
        script = buscar_script(parametro=parametro)

        try:
            if nunota:
                query = script.format_map({"nunota":nunota})
            else:
                query = script.format_map({"codemp":self.codemp})
        except Exception as e:
            erro = f"Falha ao formatar query do saldo de estoque por lote. {e}"
            print(erro)
            logger.error(erro)
            return False

        res = requests.get(
            url=url,
            headers={ 'Authorization':f"Bearer {self.token}" },
            json={
                "serviceName": "DbExplorerSP.executeQuery",
                "requestBody": {
                    "sql":query
                }
            })
        
        if res.status_code in (200,201) and res.json().get('status')=='1':
            return self.formatter.return_format(res.json())
        else:
            if nunota:
                logger.error("Erro ao buscar itens conferidos do pedido %s. %s",nunota,res.text)
            else:
                logger.error("Erro ao buscar itens conferidos no dia. %s",res.text)
            return False

    async def compara_saldos(self,saldo_estoque:list[dict],saldo_pedidos:list[dict]) -> list[dict]:
        """
        Compara os saldos de estoque com os itens conferidos.
            :param saldo_estoque: lista de dicionários com os dados dos itens do saldo de estoque
            :param saldo_pedidos: lista de dicionários com os dados dos itens conferidos
            :return list[dict]: lista com os dados dos itens a serem transferidos
        """        
        
        lista_transferir = []        

        for pedido in saldo_pedidos:
            qtd_transferir = None
            for i, estoque in enumerate(saldo_estoque):
                if estoque.get('codprod') == pedido.get('codprod') and estoque.get('controle') == pedido.get('controle'):
                    break
                # Verifica se passou por todo o saldo de estoque e não encontrou nada, assume a quantidade conferida
                if i == len(saldo_estoque)-1:
                    i = -1
            
            # Assume a quantidade conferida se passou por todo o saldo de estoque e não encontrou nada
            if i == -1:
                lista_transferir.append({
                    "codprod": pedido.get('codprod'),
                    "controle": pedido.get('controle'),
                    "quantidade": pedido.get('qtdtotalunit')
                })  
                continue

            # Verifica se precisa transferência    
            if estoque.get('qtd') < pedido.get('qtdtotalunit'):
                qtd_transferir = pedido.get('qtdtotalunit') - estoque.get('qtd')

            if qtd_transferir:
                # Se o item tem agrupamento mínimo configurado, utiliza esse valor pra transferência
                if int(estoque.get('agrupmin')) > 1:
                    if qtd_transferir <= int(estoque.get('agrupmin')):
                        qtd_transferir = int(estoque.get('agrupmin'))
                    else:
                        # Valida múltiplos do agrupamento mínimo
                        multiplo = int(estoque.get('agrupmin'))
                        while multiplo < qtd_transferir:
                            multiplo += int(estoque.get('agrupmin'))                        
                        qtd_transferir = multiplo
                        # Transfere a quantidade disponível, mesmo fora do agrupamento min.
                        if qtd_transferir > int(estoque.get('qtdmatriz')):
                            qtd_transferir = int(estoque.get('qtdmatriz'))

                        
                lista_transferir.append({
                    "codprod": pedido.get('codprod'),
                    "controle": pedido.get('controle'),
                    "quantidade": qtd_transferir
                })

        return lista_transferir