import asyncio
from database.crud import empresa, ecommerce
from src.integrador.pedido import Pedido
from src.integrador.separacao import Separacao

# ROTINA A SER EXECUTADA DIARIAMENTE, A CADA 15 MINUTOS
# APÓS A ROTINA DE ESTOQUE

async def receber_pedido_lote(codemp:int=None,id_loja:int=None) -> dict:

    retorno:dict={}
    empresas:list[dict]=[]
    ecommerces:list[dict]=[]
    emp:dict={}
    ecom:dict={}

    print("===================: RECEBIMENTO DE PEDIDOS :===================")    

    if not id_loja:
        empresas = await empresa.buscar(codemp=codemp)        

        try:
            for i, emp in enumerate(empresas):
                print(f"\nEmpresa {emp.get('nome')} ({i+1}/{len(emp)})".upper())
                ecommerces = await ecommerce.buscar(empresa_id=emp.get('id'))
                for j, ecom in ecommerces:
                    print(f"E-commerce {ecom.get('nome')} ({j+1}/{len(ecom)})".upper())
                    pedido = Pedido(id_loja=ecom.get('id_loja'))
                    separacao = Separacao(id_loja=ecom.get('id_loja'))
                    await pedido.receber_novos()
                    await separacao.receber()
            retorno = {
                "status": True,
                "exception": None
            }
        except Exception as e:
            retorno = {
                "status": False,
                "exception": e
            }
        finally:
            return retorno
    
    else:
        try:
            ecommerces = await ecommerce.buscar(id_loja=id_loja)
            ecom = ecommerces[0]
            print(f"E-commerce {ecom.get('nome')}".upper())
            pedido = Pedido(id_loja=id_loja)
            separacao = Separacao(id_loja=id_loja)
            await pedido.receber_novos()
            await separacao.receber()
            retorno = {
                "status": True,
                "exception": None
            }
        except Exception as e:
            retorno = {
                "status": False,
                "exception": e
            }
        finally:
            return retorno
    
async def receber_pedido_unico(id_loja:int,numero:int) -> dict:
    retorno:dict={}
    print("===================: RECEBIMENTO DE PEDIDO ÚNICO :===================")    
    pedido = Pedido(id_loja=id_loja)
    try:
        ack = await pedido.receber(num_pedido=numero)
        retorno = {
            "status": ack.get('success'),
            "exception": ack.get('__exception__')
        }
    except Exception as e:
        retorno = {
            "status": False,
            "exception": e
        }
    finally:
        return retorno

async def integrar_pedidos(codemp:int=None,id_loja:int=None) -> dict:

    retorno:dict={}
    empresas:list[dict]=[]
    ecommerces:list[dict]=[]
    emp:dict={}
    ecom:dict={}

    print("===================: INTEGRAÇÃO DE PEDIDOS :===================")

    if not id_loja:
        empresas = await empresa.buscar(codemp=codemp)

        try:
            for i, emp in enumerate(empresas):
                print(f"\nEmpresa {emp.get('nome')} ({i+1}/{len(emp)})".upper())
                ecommerces = await ecommerce.buscar(empresa_id=emp.get('id'))
                for j, ecom in ecommerces:
                    print(f"E-commerce {ecom.get('nome')} ({j+1}/{len(ecom)})".upper())
                    pedido = Pedido(id_loja=ecom.get('id_loja'))
                    await pedido.consultar_cancelamentos()
                    await pedido.integrar_novos()
                    await pedido.integrar_confirmacao()
            retorno = {
                "status": True,
                "exception": None
            }
        except Exception as e:
            retorno = {
                "status": False,
                "exception": e
            }
        finally:
            return retorno
    else:
        try:
            ecommerces = await ecommerce.buscar(id_loja=id_loja)
            ecom = ecommerces[0]
            print(f"E-commerce {ecom.get('nome')}".upper())
            pedido = Pedido(id_loja=id_loja)
            await pedido.consultar_cancelamentos()
            await pedido.integrar_novos()
            await pedido.integrar_confirmacao()
            retorno = {
                "status": True,
                "exception": None
            }
        except Exception as e:
            retorno = {
                "status": False,
                "exception": e
            }
        finally:
            return retorno                              

async def integrar_separacoes(codemp:int=None) -> dict:

    retorno:dict={}
    empresas:list[dict]=[]
    ecommerces:list[dict]=[]
    emp:dict={}
    ecom:dict={}

    empresas = await empresa.buscar(codemp=codemp)

    print("===================: INTEGRAÇÃO DE SEPARAÇÕES :===================")    

    try:
        for i, emp in enumerate(empresas):
            print(f"\nEmpresa {emp.get('nome')} ({i+1}/{len(emp)})".upper())
            ecommerces = await ecommerce.buscar(empresa_id=emp.get('id'))
            for j, ecom in ecommerces:
                print(f"E-commerce {ecom.get('nome')} ({j+1}/{len(ecom)})".upper())
                separacao = Separacao(id_loja=ecom.get('id_loja'))
                await separacao.receber()
        retorno = {
            "status": True,
            "exception": None
        }
    except Exception as e:
        retorno = {
            "status": False,
            "exception": e
        }
    finally:
        return retorno           

if __name__=="__main__":

    asyncio.run(receber_pedido_lote())