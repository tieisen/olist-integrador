import os
import logging
import requests
from dotenv import load_dotenv

from src.utils.decorador import token_snk
from src.utils.buscar_script import buscar_script
from src.utils.formatter import Formatter
from src.utils.log import Log

load_dotenv('keys/.env')
logger = logging.getLogger(__name__)
logging.basicConfig(filename=Log().buscar_path(),
                    encoding='utf-8',
                    format=os.getenv('LOGGER_FORMAT'),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

class Faturamento:

    def __init__(self, codemp:int=None, empresa_id:int=None):
        self.token = None
        self.codemp = codemp
        self.empresa_id = empresa_id
        self.formatter = Formatter()

    @token_snk
    async def buscar_itens(
            self,            
            nunota:int=None
        ):

        url = os.getenv('SANKHYA_URL_DBEXPLORER')
        if not url:
            print(f"Erro relacionado à url. {url}")
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
                print(f"Erro ao buscar itens conferidos do pedido {nunota}. {res.text}")
            else:
                logger.error("Erro ao buscar itens conferidos no dia. %s",res.text)
                print(f"Erro ao buscar itens conferidos no dia. {res.text}")
            return False

    async def compara_saldos(
            self,
            saldo_estoque:list,
            saldo_pedidos:list
        ):
        
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
                        
                lista_transferir.append({
                    "codprod": pedido.get('codprod'),
                    "controle": pedido.get('controle'),
                    "quantidade": qtd_transferir
                })

        return lista_transferir