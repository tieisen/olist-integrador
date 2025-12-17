from datetime import datetime
from database.crud import empresa, ecommerce
from src.integrador.financeiro import Financeiro
from src.services.bot import Bot

async def baixar_shopee(codemp:int,data:dict) -> dict:

    retorno:dict={}
    empresas:list[dict]=[]
    ecommerces:list[dict]=[]
    emp:dict={}
    ecom:dict={}

    print(":::::::::::::::::::: BAIXA DE TÍTULOS A RECEBER ::::::::::::::::::::")    

    empresas = await empresa.buscar(codemp=codemp)
    try:
        for i, emp in enumerate(empresas):
            print(f"\nEmpresa {emp.get('nome')} ({i+1}/{len(empresas)})".upper())
            ecommerces = await ecommerce.buscar(empresa_id=emp.get('id'))
            for j, ecom in enumerate(ecommerces):
                if 'SHOPEE' in ecom.get('nome').upper():
                    financeiro = Financeiro(id_loja=ecom.get('id_loja'),empresa_id=ecom.get('empresa_id'))
                    ack = await financeiro.baixar_contas_receber_shopee(relatorio_recebimentos=data)

        retorno = {
            "status": ack,
            "exception": "Ocorreu um erro ao baixar contas a receber. Verifique as contas pendentes no Olist" if not ack else None                    
        }                
    except Exception as e:
        print(f"{e}")
        retorno = {
            "status": False,
            "exception": f"{e}"
        }
    finally:
        return retorno

async def integrar_financeiro(data:str,codemp:int=None) -> dict:

    retorno:dict={}
    empresas:list[dict]=[]
    ecommerces:list[dict]=[]
    emp:dict={}
    ecom:dict={}
    lista_status:list[bool]=[]

    if not isinstance(data,datetime):
        try:
            data = datetime.strptime(data, "%Y-%m-%d")
        except:
            raise Exception(f"Data inválida. Use o formato YYYY-MM-DD. Data informada: {data}")


    print(":::::::::::::::::::: CONTAS A RECEBER ::::::::::::::::::::")    

    empresas = await empresa.buscar(codemp=codemp)
    try:
        for i, emp in enumerate(empresas):
            print(f"\nEmpresa {emp.get('nome')} ({i+1}/{len(empresas)})".upper())
            bot = Bot(empresa_id=emp.get('id'))
            if await bot.rotina_relatorio_custos(data=data):            
                ecommerces = await ecommerce.buscar(empresa_id=emp.get('id'))
                for j, ecom in enumerate(ecommerces):
                    print(f"E-commerce {ecom.get('nome')} ({j+1}/{len(ecommerces)})".upper())
                    financeiro = Financeiro(id_loja=ecom.get('id_loja'),empresa_id=ecom.get('empresa_id'))
                    # Agrupa títulos dos pedidos parcelados
                    await financeiro.agrupar_titulos_parcelados()                    
                    lista_status.append(await financeiro.executar_baixa(data=data))
                retorno = {
                    "status": all(lista_status),
                    "exception": "Erro ao baixar contas a receber" if not all(lista_status) else None
                }
            else:
                raise Exception("Erro ao baixar relatório de custos")
    except Exception as e:
        print(f"{e}")
        retorno = {
            "status": False,
            "exception": f"{e}"
        }
    finally:
        return retorno
