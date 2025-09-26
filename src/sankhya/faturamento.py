import os
import logging
import requests
from src.sankhya.connect import Connect
from src.utils.formatter import Formatter
from dotenv import load_dotenv
from src.utils.log import Log

load_dotenv('keys/.env')
logger = logging.getLogger(__name__)
logging.basicConfig(filename=Log().buscar_path(),
                    encoding='utf-8',
                    format=os.getenv('LOGGER_FORMAT'),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

CONTEXTO = 'faturamento'

class Faturamento:

    def __init__(self):
        self.con = Connect()
        self.formatter = Formatter()

    async def buscar_itens(self, nunota:int=None):

        url = os.getenv('SANKHYA_URL_DBEXPLORER')
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False
        
        try:
            token = self.con.get_token()
        except Exception as e:
            print(f"Erro relacionado ao token de acesso. {e}")
            logger.error("Erro relacionado ao token de acesso. %s",e)
            return False  

        if nunota:
            query = f'''
                SELECT
                    COI.CODPROD,
                    COI.CONTROLE,
                    SUM(COI.QTDCONFVOLPAD) QTDTOTALUNIT
                FROM TGFCON2 CON
                    INNER JOIN TGFCOI2 COI ON CON.NUCONF = COI.NUCONF
                    INNER JOIN TGFCAB CAB ON CON.NUCONF = CAB.NUCONFATUAL
                WHERE CON.NUNOTAORIG = {nunota}
                GROUP BY COI.CODPROD, COI.CONTROLE
            '''
        else:
            query = '''
                SELECT
                    COI.CODPROD,
                    COI.CONTROLE,
                    SUM(COI.QTDCONFVOLPAD) QTDTOTALUNIT
                FROM TGFCON2 CON
                    INNER JOIN TGFCOI2 COI ON CON.NUCONF = COI.NUCONF
                    INNER JOIN TGFCAB CAB ON CON.NUCONF = CAB.NUCONFATUAL
                WHERE TRUNC(CON.DHFINCONF) = TRUNC(SYSDATE)
                    AND CAB.AD_MKP_ORIGEM IS NOT NULL
                    AND CAB.PENDENTE = 'S'
                GROUP BY COI.CODPROD, COI.CONTROLE
            '''

        res = requests.get(
            url=url,
            headers={ 'Authorization': token },
            json={
                "serviceName": "DbExplorerSP.executeQuery",
                "requestBody": {
                    "sql":query
                }
            })
        
        if res.status_code in (200,201) and res.json().get('status')=='1':
            # print(res.json())
            return self.formatter.return_format(res.json())
        else:
            if nunota:
                logger.error("Erro ao buscar itens conferidos do pedido %s. %s",nunota,res.text)
                print(f"Erro ao buscar itens conferidos do pedido {nunota}. {res.text}")
            else:
                logger.error("Erro ao buscar itens conferidos no dia. %s",res.text)
                print(f"Erro ao buscar itens conferidos no dia. {res.text}")
            return False

    async def compara_saldos(self, saldo_estoque:list=None, saldo_pedidos:list=None):

        if not all([saldo_estoque, saldo_pedidos]):
            print("Dados não informados")
            logger.error("Dados não informados")
            return False
        
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
                        if qtd_transferir > int(estoque.get('qtd')):
                            qtd_transferir = int(estoque.get('qtd'))

                        
                lista_transferir.append({
                    "codprod": pedido.get('codprod'),
                    "controle": pedido.get('controle'),
                    "quantidade": qtd_transferir
                })

        return lista_transferir