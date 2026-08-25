import os, sys, asyncio, requests, time
from pathlib import Path
from datetime import datetime

root_dir = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root_dir))

from src.utils.load_env import load_env
from src.sankhya.autenticacao import Autenticacao
from src.olist.autenticacao import Autenticacao as AuthOlist
from src.utils.formatter import Formatter
from src.olist.financeiro import Despesa
from pydantic import BaseModel

load_env()

class ContaAPagar(BaseModel):
    """
    Modelo de dados para representar uma conta a pagar no Olist.
    """
    data: str
    dataVencimento: str
    numeroDocumento: str
    contato: dict[str, int]
    historico: str
    categoria: dict[str, int]
    dataCompetencia: str | None = None
    ocorrencia: str
    formaPagamento: int
    diaVencimento: int | None = None
    quantidadeParcelas: int
    diaSemanaVencimento: int | None = None
    valor: float
    saldo: float

async def autentica_olist(codemp, empresa_id):
    """
    Faz a autenticação no olist para autorizar requests
    """

    auth = AuthOlist(codemp=codemp, empresa_id=empresa_id)
    auth_code = await auth.solicitar_auth_code()
    token = await auth.solicitar_token(auth_code)

    return token["access_token"]



async def get_despesas_sankhya() -> list:
    """
    Busca as despesas do Sankhya de um determinado período e retorna uma lista de dicionários com os dados das despesas.
    """

    url = os.getenv('SANKHYA_URL_LOAD_RECORDS')

    criteria = {
        "expression": {
                        "$": "CODTIPOPER = 1419 AND DTNEG BETWEEN TO_DATE('11/08/2026', 'DD/MM/YYYY') AND TO_DATE('17/08/2026', 'DD/MM/YYYY')"
                    }
    }

    auth = Autenticacao()
    token = await auth.solicitar_token()
    # print("token: %s", token)

    res = requests.get(
                url=url,
                headers={ "Authorization":f"Bearer {token['token']}" },
                json={
                    "serviceName": "CRUDServiceProvider.loadRecords",
                    "requestBody": {
                        "dataSet": {
                            "rootEntity": "Financeiro",
                            "includePresentationFields": "N",
                            "offsetPage": "0",
                            "criteria": criteria,
                            "entity": {
                                "fieldset": {
                                    "list": "NUMNOTA,NUFIN,DTNEG,DTVENC,VLRDESDOB,HISTORICO,CODEMP,CODPARC"
                                }
                            }
                        }
                    }
                })

    if res.status_code != 200:
        print(f"Erro ao buscar despesas do Sankhya. Status Code: {res.status_code}, Response: {res.text}")
    else:
        data = res.json()
        formatter = Formatter()
        despesas = formatter.return_format(data)
        return despesas



async def post_contas_a_pagar(token: str, payload: dict):
    """
    Envia para o Olist os dados do contas a pagar
    
    """

    print('Contas a pagar: ', payload)

    try: 
        url = "https://api.tiny.com.br/public-api/v3/contas-pagar"

        res = requests.post(
                    url = url,            
                    headers = {
                        "Authorization":f"Bearer {token}",
                        "Content-Type":"application/json",
                        "Accept":"application/json"
                    },
                    json=payload
        )

        # print("res: %s", res)
        if res.status_code != 201:
            raise Exception(f"Erro ao criar o contas a pagar")
        else:
            print("Contas a pagar criado com sucesso!")
        
    except Exception as e:
        print("Erro ao criar a despesa no Olist: %s", e)



async def get_contas_a_pagar_olist(token:str, nro_doc: int):
    """
    Busca o contas a pagar pelo número do documento
    """
    try:
        url = f"https://api.tiny.com.br/public-api/v3/contas-pagar?numeroDocumento={nro_doc}"

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}"
        }

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
                    data = response.json()
                    if len(data['itens']) > 0:
                        return {
                            "status": True,
                            "data": data
                        }
                    else:
                        return {
                            "status": False,
                            "data": {}
                        }
        else:
            return {    
                "status": False,
                "data": {}
            }
        
                
    except Exception as e:
        raise Exception(f"Erro ao buscar contas a pagar. Status code: {response.status_code}, Response: {response.text}")



async def main():
    """
    Script para resolver contas a pagar que não subiram para o Olist 
    """
    try:
        ecommerces = [
            {
                "codemp": 1,
                "empresa_id": 1,
                "contato_id": 753053887,
                "categoria_id": 367785383
            },
            {
                "codemp": 3,
                "empresa_id": 9,
                "contato_id": 891575459,
                "categoria_id": 336862207
            },
            {
                "codemp": 4,
                "empresa_id": 5,
                "contato_id": 730902274,
                "categoria_id": 746666005
            }
        ]


        despesas = await get_despesas_sankhya()
        print("despesas geral: ", len(despesas))
        for ecommerce in ecommerces:
            print(f"Subindo os dados da empresa {ecommerce['codemp']}...")

            token = await autentica_olist(codemp=ecommerce["codemp"],empresa_id=ecommerce["empresa_id"])

            despesas_por_empresa = [d for d in despesas if int(d['codemp']) == ecommerce['codemp']]
            print(f"Despesas encontradas: {len(despesas_por_empresa)}")

            for despesa in despesas_por_empresa:
                print("despesa:  %s", despesa)
                # despesa_obj = Despesa(empresa_id=1)

                # Verifica se já existe o registro na API do Olist
                ja_existe = await get_contas_a_pagar_olist(token=token, nro_doc=despesa["numnota"])

                if ja_existe['status'] == True:
                    print(f"Já existe o título para o financeiro de número único: {despesa['nufin']}")
                    print(f"ID no Olist: {ja_existe['data']}")
                    continue
                else: 
                    print(f"Prosseguindo com a inserção do financeiro: {despesa['nufin']}")

                    # Formata datas
                    data_obj = datetime.strptime(despesa['dtneg'], "%d/%m/%Y")
                    data_formatada = data_obj.strftime("%Y-%m-%d")

                    data_venc_obj = datetime.strptime(despesa['dtvenc'], "%d/%m/%Y")
                    data_venc_formatada = data_venc_obj.strftime("%Y-%m-%d")

                    payload = ContaAPagar(
                        data=data_formatada,
                        dataVencimento=data_venc_formatada,
                        ocorrencia="U",
                        formaPagamento=15,
                        historico=despesa["historico"],
                        numeroDocumento=despesa["numnota"],
                        quantidadeParcelas=0,
                        contato={"id": ecommerce['contato_id']},
                        categoria={"id": ecommerce['categoria_id']},
                        valor=despesa["vlrdesdob"],
                        saldo=despesa["vlrdesdob"]
                    )
                    # print("payload: %s", payload)
                    await post_contas_a_pagar(token=token, payload=payload.model_dump())
                    contas_a_pagar = await get_contas_a_pagar_olist(token=token, nro_doc=despesa["numnota"])
                    print("Contas a pagar: %s", contas_a_pagar)

            time.sleep(60)    
                
    except Exception as e:
        print(f"Erro ao tentar registrar uma despesas: {e}")

if __name__ == "__main__":
    asyncio.run(main())