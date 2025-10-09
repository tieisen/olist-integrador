import os
import logging
import requests
from datetime import datetime
from dotenv import load_dotenv
from src.olist.connect import Connect
from src.utils.log import Log

load_dotenv('keys/.env')
logger = logging.getLogger(__name__)
logging.basicConfig(filename=Log().buscar_path(),
                    encoding='utf-8',
                    format=os.getenv('LOGGER_FORMAT'),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

class Nota:

    def __init__(self):  
        self.con = Connect()
        self.endpoint = os.getenv('OLIST_API_URL')+os.getenv('OLIST_ENDPOINT_NOTAS')

    async def buscar(self, id:int=None, id_ecommerce:str=None, numero:int=None) -> bool:

        if not any([id, id_ecommerce, numero]):
            logger.error("Nota não informada.")
            print("Nota não informada.")
            return False
        
        try:
            token = self.con.get_token()
        except Exception as e:
            logger.error("Erro relacionado ao token de acesso. %s",e)
            return False

        if id:
            url_ = self.endpoint+f"/{id}"
        
        if id_ecommerce:
            url_ = self.endpoint+f"/?numeroPedidoEcommerce={id_ecommerce}"
        
        if numero:
            url_ = self.endpoint+f"/?numero={numero}"

        res = requests.get(
            url = url_,
            headers = {
                "Authorization":f"Bearer {token}",
                "Content-Type":"application/json",
                "Accept":"application/json"
            }
        )

        if res.status_code != 200:
            logger.error("Erro %s: %s", res.status_code, res.text)
            print(f"Erro {res.status_code}: {res.text}")
            return False
        
        if res.status_code == 200 and id:
            return res.json()
        
        nota = None
        if res.status_code == 200 and any([id_ecommerce,numero]) and res.json().get('itens'):
            url_id = self.endpoint+f"/{res.json().get('itens')[0].get('id')}"
            res = requests.get(
                url = url_id,
                headers = {
                    "Authorization":f"Bearer {token}",
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
        
    async def buscar_canceladas(self) -> bool:

        try:
            token = self.con.get_token()
        except Exception as e:
            logger.error("Erro relacionado ao token de acesso. %s",e)
            return False

        url = self.endpoint+f"/?situacao=3&tipo=S&dataInicial={datetime.now().strftime('%Y-%m-%d')}&dataFinal={datetime.now().strftime('%Y-%m-%d')}"

        res = requests.get(
            url = url,
            headers = {
                "Authorization":f"Bearer {token}",
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

        lista_canceladas = [r.get('id') for r in res.json().get('itens')]
        return lista_canceladas

    async def buscar_legado(self, id:int=None, id_ecommerce:str=None) -> bool:

        def desmembra_xml(dados_nota:dict=None, xml=None):
            
            if not dados_nota or not xml:
                return {}
            
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


        if not any([id, id_ecommerce]):
            logger.error("Nota não informada.")
            print("Nota não informada.")
            return False
        
        try:
            token = self.con.get_token()
        except Exception as e:
            logger.error("Erro relacionado ao token de acesso. %s",e)
            return False

        if id:
            url_ = self.endpoint+f"/{id}"
            id_ecommerce = None
        
        if id_ecommerce:
            url_ = self.endpoint+f"/?numeroPedidoEcommerce={id_ecommerce}"
            id = None             

        res = requests.get(
            url = url_,
            headers = {
                "Authorization":f"Bearer {token}",
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
        
        if res.status_code == 200 and id_ecommerce and res.json().get('itens'):
            url_id = self.endpoint+f"/{res.json().get('itens')[0].get('id')}"
            res = requests.get(
                url = url_id,
                headers = {
                    "Authorization":f"Bearer {token}",
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
                    "Authorization":f"Bearer {token}",
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

    async def emitir(self, id:int=None):

        if not id:
            logger.error("ID não informado.")
            print("ID não informado.")
            return False
        
        url = self.endpoint+f"/{id}/emitir"
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False        
        
        try:
            token = self.con.get_token()
        except Exception as e:
            logger.error("Erro relacionado ao token de acesso. %s",e)
            return False
        
        res = requests.post(
            url = url,
            headers = {
                "Authorization":f"Bearer {token}",
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

    async def buscar_financeiro(self, serie:str=None, numero:str=None) -> bool:

        if not all([serie, numero]):
            logger.error("Nota não informada.")
            print("Nota não informada.")
            return False
        
        url = os.getenv('OLIST_API_URL')+os.getenv('OLIST_ENDPOINT_FINANCEIRO')+f"?numeroDocumento={serie}{numero}/01"
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False                
        
        try:
            token = self.con.get_token()
        except Exception as e:
            logger.error("Erro relacionado ao token de acesso. %s",e)
            return False             

        res = requests.get(
            url = url,            
            headers = {
                "Authorization":f"Bearer {token}",
                "Content-Type":"application/json",
                "Accept":"application/json"
            }
        )

        if res.status_code != 200:
            logger.error("Erro %s: %s fin %s", res.status_code, res.text, f"{serie}{numero}/01")
            print(f"Erro {res.status_code}: {res.text} fin {serie}{numero}/01")
            return False
        
        if res.status_code == 200:
            try:
                return res.json().get('itens')[0]
            except:
                return False
            # print(res.json())
            # url = os.getenv('OLIST_API_URL')+os.getenv('OLIST_ENDPOINT_FINANCEIRO')+f"/{res.json().get('itens')[0].get('id')}"
            # res_ = requests.get(
            #     url = url,            
            #     headers = {
            #         "Authorization":f"Bearer {token}",
            #         "Content-Type":"application/json",
            #         "Accept":"application/json"
            #     }
            # )
            # if res.status_code != 200:
            #     logger.error("Erro %s: %s fin %s", res.status_code, res.text, f"{serie}{numero}/01")
            #     return res_.json().get('itens')[0]
            # else:
            #     return res_.json()

    async def baixar_financeiro(self, id:int=None, valor:float=None) -> bool:

        if not all([id, valor]):
            logger.error("Dados do financeiro e da nota não informados.")
            print("Dados do financeiro e da nota não informados.")
            return False

        url = os.getenv('OLIST_API_URL')+os.getenv('OLIST_ENDPOINT_FINANCEIRO')+f"/{id}/baixar"
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False        
        
        try:
            token = self.con.get_token()
        except Exception as e:
            logger.error("Erro relacionado ao token de acesso. %s",e)
            return False             

        payload = {
            "contaDestino": {
                "id": 334742438
            },
            "data": None,
            "categoria": {
                "id": 347787528
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
                "Authorization":f"Bearer {token}",
                "Content-Type":"application/json",
                "Accept":"application/json"
            },
            json=payload
        )
        
        if res.status_code not in (409,204):
            logger.error("Erro %s: %s fin %s", res.status_code, res.text, id)            
            print(f"Erro {res.status_code}: {res.text}")
            return False
        
        # Financeiro baixado com sucesso (204) ou
        # Financeiro da nota já foi baixado (409)
        return True        