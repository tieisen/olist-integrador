from datetime import datetime
from database.crud import empresa, ecommerce
from src.integrador.financeiro import Financeiro
from src.services.bot import Bot

async def integrar_financeiro(data:str,codemp:int=None) -> dict:

    retorno:dict={}
    empresas:list[dict]=[]
    ecommerces:list[dict]=[]
    emp:dict={}
    ecom:dict={}

    if not isinstance(data,datetime):
        try:
            data = datetime.strptime(data, "%Y-%m-%d")
        except:
            raise Exception("Data inválida. Use o formato YYYY-MM-DD")


    print(":::::::::::::::::::: FATURAMENTO DE PEDIDOS ::::::::::::::::::::")    

    empresas = await empresa.buscar(codemp=codemp)
    try:
        for i, emp in enumerate(empresas):
            print(f"\nEmpresa {emp.get('nome')} ({i+1}/{len(empresas)})".upper())
            bot = Bot(empresa_id=emp.get('id'))
            # if await bot.rotina_relatorio_custos(data=data):
            if True:
                ecommerces = await ecommerce.buscar(empresa_id=emp.get('id'))
                for j, ecom in enumerate(ecommerces):
                    print(f"E-commerce {ecom.get('nome')} ({j+1}/{len(ecommerces)})".upper())
                    financeiro = Financeiro(id_loja=ecom.get('id_loja'))
                    if await financeiro.executar_baixa(data=data):
                        retorno = {
                            "status": True,
                            "exception": None
                        }
                    else:
                        raise Exception("Erro ao baixar contas a receber")
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
