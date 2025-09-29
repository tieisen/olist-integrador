import os
import requests
from datetime import datetime, timedelta
from src.utils.decorador import carrega_dados_ecommerce
from src.utils.autenticador import token_olist
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

class Nota:

    def __init__(self, id_loja:int=None, codemp:int=None, empresa_id:int=None):  
        self.id_loja = id_loja
        self.codemp = codemp
        self.empresa_id = empresa_id        
        self.dados_ecommerce:dict = None
        self.token = None
        self.endpoint = os.getenv('OLIST_API_URL')+os.getenv('OLIST_ENDPOINT_NOTAS')
        self.endpoint_fin = os.getenv('OLIST_API_URL')+os.getenv('OLIST_ENDPOINT_FINANCEIRO')

    @carrega_dados_ecommerce
    @token_olist
    async def buscar(
            self,
            id:int=None,
            numero:int=None,
            cod_pedido:str=None
        ) -> bool:

        if not any([id, cod_pedido, numero]):
            logger.error("Nota não informada.")
            print("Nota não informada.")
            return False

        if id:
            url_ = self.endpoint+f"/{id}"
            cod_pedido = None
        
        if cod_pedido:
            url_ = self.endpoint+f"/?numeroPedidoEcommerce={cod_pedido}"
            id = None

        if numero:
            url_ = self.endpoint+f"/?numero={numero}"
            id = None             

        res = requests.get(
            url = url_,
            headers = {
                "Authorization":f"Bearer {self.token}",
                "Content-Type":"application/json",
                "Accept":"application/json"
            }
        )

        nota = None
        if res.status_code != 200:
            logger.error("Erro %s: %s", res.status_code, res.text)
            print(f"Erro {res.status_code}: {res.text}")
            return False
        
        if res.status_code == 200 and id and res.json().get('itens'):
            nota = res.json()
        
        if res.status_code == 200 and not id and res.json().get('itens'):
            url_id = self.endpoint+f"/{res.json().get('itens')[0].get('id')}"
            res = requests.get(
                url = url_id,
                headers = {
                    "Authorization":f"Bearer {self.token}",
                    "Content-Type":"application/json",
                    "Accept":"application/json"
                }
            )
            if res.status_code == 200 and url_id:
                nota = res.json()
                
        if nota:
            return nota
        else:
            print("Nota cancelada")
            logger.error("Nota cancelada")
            return False
    
    @carrega_dados_ecommerce
    @token_olist
    async def buscar_canceladas(self,data:str=None,tipo:str='S') -> list[dict]:

        if not data:
            data = (datetime.today()-timedelta(days=1)).strftime('%Y-%m-%d')
        else:
            data = datetime.strptime(data, '%Y-%m-%d').strftime('%Y-%m-%d')
            
        url = self.endpoint+f"/?tipo={tipo}&situacao=3&dataInicial={data}&dataFinal={data}"

        res = requests.get(
            url = url,
            headers = {
                "Authorization":f"Bearer {self.token}",
                "Content-Type":"application/json",
                "Accept":"application/json"
            }
        )

        if res.status_code != 200:
            logger.error("Erro %s: %s", res.status_code, res.text)
            print(f"Erro {res.status_code}: {res.text}")
            return False
        
        if not res.json().get('itens'):
            return []

        return res.json().get('itens')
    
    @carrega_dados_ecommerce
    @token_olist
    async def buscar_devolucoes(self,data:str=None) -> list[dict]:

        if not data:
            data = (datetime.today()-timedelta(days=1)).strftime('%Y-%m-%d')
        else:
            data = datetime.strptime(data, '%Y-%m-%d').strftime('%Y-%m-%d')
            
        url = self.endpoint+f"/?tipo=E&dataInicial={data}&dataFinal={data}"

        res = requests.get(
            url = url,
            headers = {
                "Authorization":f"Bearer {self.token}",
                "Content-Type":"application/json",
                "Accept":"application/json"
            }
        )

        if res.status_code != 200:
            logger.error("Erro %s: %s", res.status_code, res.text)
            print(f"Erro {res.status_code}: {res.text}")
            return False
        
        if not res.json().get('itens'):
            return []

        return res.json().get('itens')
    
    @carrega_dados_ecommerce
    @token_olist
    async def buscar_legado(
            self,
            id:int=None,
            cod_pedido:str=None
        ) -> bool:

        def desmembra_xml(
                dados_nota:dict,
                xml:str
            ):
                        
            from lxml import etree
            ns = {'nfe': 'http://www.portalfiscal.inf.br/nfe'}

            tree = etree.fromstring(xml.encode('utf-8'))
            ide = tree.xpath('//nfe:ide', namespaces=ns)
            itens = tree.xpath('//nfe:det', namespaces=ns)
            rastros_itens = [{"cProd":i[0].findtext('nfe:cProd', namespaces=ns),"rastro":r} for i in itens for r in i.xpath('.//nfe:rastro', namespaces=ns)]

            for i, item in enumerate(dados_nota.get("itens")):
                try:
                    for j in rastros_itens:
                        if j.get('cProd') == item.get('codigo'):
                            rastro = j.get('rastro')
                            break  
                    controles = []
                    if type(rastro) == list:
                        for r in rastro:
                            controles.append({
                                "quantidade": int(float(r.findtext('nfe:qLote', namespaces=ns))),
                                "lote": r.findtext('nfe:nLote', namespaces=ns),
                                "dtFab": r.findtext('nfe:dFab', namespaces=ns),
                                "dtVal": r.findtext('nfe:dVal', namespaces=ns)
                            })
                    else:
                        controles.append({
                            "quantidade": int(float(rastro.findtext('nfe:qLote', namespaces=ns))),
                            "lote": rastro.findtext('nfe:nLote', namespaces=ns),
                            "dtFab": rastro.findtext('nfe:dFab', namespaces=ns),
                            "dtVal": rastro.findtext('nfe:dVal', namespaces=ns)
                        })                    

                except Exception as e:
                    logger.error("Erro ao extrair dados de controle do produto %s. %s",item.get("codigo"),e)
                item['lotes'] = controles
            dados_nota['codChaveAcesso'] = ide[0].findtext('nfe:cNF', namespaces=ns)

            return dados_nota


        if not any([id, cod_pedido]):
            logger.error("Nota não informada.")
            print("Nota não informada.")
            return False
        
        if id:
            url_ = self.endpoint+f"/{id}"
            cod_pedido = None
        
        if cod_pedido:
            url_ = self.endpoint+f"/?numeroPedidoEcommerce={cod_pedido}"
            id = None             

        res = requests.get(
            url = url_,
            headers = {
                "Authorization":f"Bearer {self.token}",
                "Content-Type":"application/json",
                "Accept":"application/json"
            }
        )

        nota = None
        if res.status_code != 200:
            logger.error("Erro %s: %s", res.status_code, res.text)
            print(f"Erro {res.status_code}: {res.text}")
            return False
        
        if res.status_code == 200 and id and res.json().get('itens'):
            nota = res.json()
        
        if res.status_code == 200 and cod_pedido and res.json().get('itens'):
            url_id = self.endpoint+f"/{res.json().get('itens')[0].get('id')}"
            res = requests.get(
                url = url_id,
                headers = {
                    "Authorization":f"Bearer {self.token}",
                    "Content-Type":"application/json",
                    "Accept":"application/json"
                }
            )
            if res.status_code == 200 and url_id:
                nota = res.json()
                
        if nota:
            res = requests.get(
                url = self.endpoint+f"/{nota.get('id')}/xml",
                headers = {
                    "Authorization":f"Bearer {self.token}",
                    "Content-Type":"application/json",
                    "Accept":"application/json"
                }
            )
            if res.status_code != 200:
                print(f"Erro {res.status_code} ao buscar XML da nota: {res.text} cod {nota.get('numero')}")            
                logger.error("Erro %s ao buscar XML da nota: %s cod %s", res.status_code, res.text, nota.get('numero'))            
                return False

            if res.status_code == 200:
                return desmembra_xml(dados_nota=nota, xml=res.json().get('xmlNfe'))          
            
        else:
            print("Nota cancelada")
            logger.error("Nota cancelada")
            return False

    @carrega_dados_ecommerce
    @token_olist
    async def emitir(
            self,
            id:int
        ) -> dict:
        
        url = self.endpoint+f"/{id}/emitir"
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False  
        
        res = requests.post(
            url = url,
            headers = {
                "Authorization":f"Bearer {self.token}",
                "Content-Type":"application/json",
                "Accept":"application/json"
            },
            json={"enviarEmail":False}
        )

        if res.status_code != 200:
            logger.error("Erro %s: %s", res.status_code, res.text)
            print(f"Erro {res.status_code}: {res.text}")
            return False
        
        if res.status_code == 200:
            return res.json()        

    @carrega_dados_ecommerce
    @token_olist
    async def buscar_financeiro(
            self,
            serie:str=None,
            numero:str=None,
            id:int=None
        ) -> bool:
        
        if id:
            url = self.endpoint_fin+f"/{id}"
        elif all([serie, numero]):
            url = self.endpoint_fin+f"?numeroDocumento={serie}{numero}/01"
        else:            
            return False         

        res = requests.get(
            url = url,            
            headers = {
                "Authorization":f"Bearer {self.token}",
                "Content-Type":"application/json",
                "Accept":"application/json"
            }
        )

        if res.status_code != 200:
            logger.error("Erro %s: %s fin %s", res.status_code, res.text, f"{serie}{numero}/01")
            print(f"Erro {res.status_code}: {res.text} fin {serie}{numero}/01")
            return False
        
        if id:
            return res.json()
        else:
            return res.json().get('itens')[0]

    @carrega_dados_ecommerce
    @token_olist
    async def baixar_financeiro(
            self,
            id:int,
            valor:float
        ) -> bool:

        url = self.endpoint_fin+f"/{id}/baixar"
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False 
        
        payload = {
            "contaDestino": {
                "id": self.dados_ecommerce.get('id_conta_destino')
            },
            "data": None,
            "categoria": {
                "id": self.dados_ecommerce.get('id_categoria_destino')
            },
            "historico": None,
            "taxa": None,
            "juros": None,
            "desconto": None,
            "valorPago": valor,
            "acrescimo": None
        }

        res = requests.post(
            url = url,            
            headers = {
                "Authorization":f"Bearer {self.token}",
                "Content-Type":"application/json",
                "Accept":"application/json"
            },
            json=payload
        )
        
        # Financeiro baixado com sucesso (204) ou
        # Financeiro da nota já foi baixado (409)
        if res.status_code not in (409,204):
            logger.error("Erro %s: %s fin %s", res.status_code, res.text, id)            
            print(f"Erro {res.status_code}: {res.text}")
            return False       

        return True        