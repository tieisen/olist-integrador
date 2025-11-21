from database.crud import empresa, ecommerce
from src.integrador.faturamento import Faturamento

async def integrar_faturamento(codemp:int=None, id_loja:int=None) -> dict:

    retorno:dict={}
    empresas:list[dict]=[]
    ecommerces:list[dict]=[]
    emp:dict={}
    ecom:dict={}

    print(":::::::::::::::::::: FATURAMENTO DE PEDIDOS ::::::::::::::::::::")    

    if not id_loja:
        empresas = await empresa.buscar(codemp=codemp)
        try:
            for i, emp in enumerate(empresas):
                print(f"\nEmpresa {emp.get('nome')} ({i+1}/{len(empresas)})".upper())
                ecommerces = await ecommerce.buscar(empresa_id=emp.get('id'))
                for j, ecom in enumerate(ecommerces):
                    print(f"E-commerce {ecom.get('nome')} ({j+1}/{len(ecommerces)})".upper())
                    faturamento = Faturamento(id_loja=ecom.get('id_loja'))
                    await faturamento.integrar_olist()
                    await faturamento.integrar_snk()
            retorno = {
                "status": True,
                "exception": None
            }
        except Exception as e:
            print(f"{e}")
            retorno = {
                "status": False,
                "exception": f"{e}"
            }
        finally:
            return retorno
    else:
        try:
            ecommerces = await ecommerce.buscar(id_loja=id_loja)
            ecom = ecommerces[0]
            print(f"E-commerce {ecom.get('nome')}".upper())
            faturamento = Faturamento(id_loja=id_loja)
            await faturamento.integrar_olist()
            await faturamento.integrar_snk(loja_unica=True)
            
            retorno = {
                "status": True,
                "exception": None
            }
        except Exception as e:
            retorno = {
                "status": False,
                "exception": f"{e}"
            }
        finally:
            return retorno

async def integrar_faturamento_olist(codemp:int=None, id_loja:int=None) -> dict:

    retorno:dict={}
    empresas:list[dict]=[]
    ecommerces:list[dict]=[]
    emp:dict={}
    ecom:dict={}

    print(":::::::::::::::::::: FATURAMENTO DE PEDIDOS NO OLIST ::::::::::::::::::::")

    if not id_loja:
        empresas = await empresa.buscar(codemp=codemp)
        try:
            for i, emp in enumerate(empresas):
                print(f"\nEmpresa {emp.get('nome')} ({i+1}/{len(empresas)})".upper())
                ecommerces = await ecommerce.buscar(empresa_id=emp.get('id'))
                for j, ecom in enumerate(ecommerces):
                    print(f"E-commerce {ecom.get('nome')} ({j+1}/{len(ecommerces)})".upper())
                    faturamento = Faturamento(id_loja=ecom.get('id_loja'))
                    await faturamento.integrar_olist()
            
            retorno = {
                "status": True,
                "exception": None
            }
        except Exception as e:
            retorno = {
                "status": False,
                "exception": f"{e}"
            }
        finally:
            return retorno

    else:
        try:
            ecommerces = await ecommerce.buscar(id_loja=id_loja)
            ecom = ecommerces[0]
            print(f"E-commerce {ecom.get('nome')}".upper())
            faturamento = Faturamento(id_loja=id_loja)
            await faturamento.integrar_olist()
            
            retorno = {
                "status": True,
                "exception": None
            }
        except Exception as e:
            retorno = {
                "status": False,
                "exception": f"{e}"
            }
        finally:
            return retorno


async def integrar_faturamento_snk(codemp:int=None) -> dict:

    retorno:dict={}
    empresas:list[dict]=[]
    ecommerces:list[dict]=[]
    emp:dict={}
    ecom:dict={}

    empresas = await empresa.buscar(codemp=codemp)

    print(":::::::::::::::::::: FATURAMENTO DE PEDIDOS NO SANKHYA ::::::::::::::::::::")    

    try:
        for i, emp in enumerate(empresas):
            print(f"\nEmpresa {emp.get('nome')} ({i+1}/{len(emp)})".upper())
            ecommerces = await ecommerce.buscar(empresa_id=emp.get('id'))
            for j, ecom in enumerate(ecommerces):
                print(f"E-commerce {ecom.get('nome')} ({j+1}/{len(ecommerces)})".upper())
                faturamento = Faturamento(id_loja=ecom.get('id_loja'))                
                await faturamento.integrar_snk()        
        retorno = {
            "status": True,
            "exception": None
        }
    except Exception as e:
        retorno = {
            "status": False,
            "exception": f"{e}"
        }
    finally:
        return retorno

async def integrar_venda_interna(codemp:int=None) -> dict:

    retorno:dict={}
    empresas:list[dict]=[]
    emp:dict={}

    empresas = await empresa.buscar(codemp=codemp)

    print(":::::::::::::::::::: VENDA INTERNA ::::::::::::::::::::")    

    try:
        for i, emp in enumerate(empresas):
            print(f"\nEmpresa {emp.get('nome')} ({i+1}/{len(empresas)})".upper())
            faturamento = Faturamento(codemp=emp.get('snk_codemp'))                
            await faturamento.realizar_venda_interna()        
        retorno = {
            "status": True,
            "exception": None
        }
    except Exception as e:
        retorno = {
            "status": False,
            "exception": f"{e}"
        }
    finally:
        return retorno

if __name__=="__main__":
    
    print("Olá!")