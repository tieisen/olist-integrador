import asyncio
from database.crud import empresa, ecommerce
from src.integrador.pedido import Pedido
from src.integrador.separacao import Separacao

# ROTINA A SER EXECUTADA DIARIAMENTE, A CADA 15 MINUTOS
# APÓS A ROTINA DE ESTOQUE

if __name__=="__main__":

    empresas:list[dict]=[]
    ecommerces:list[dict]=[]
    emp:dict={}
    ecom:dict={}

    empresas = asyncio.run(empresa.buscar())

    print("===================: INTEGRAÇÃO DE PEDIDOS :===================")    

    for i, emp in enumerate(empresas):
        print(f"\nEmpresa {emp.get('nome')} ({i+1}/{len(emp)})".upper())
        ecommerces = asyncio.run(ecommerce.buscar(empresa_id=emp.get('id')))
        for j, ecom in ecommerces:
            print(f"E-commerce {ecom.get('nome')} ({i+1}/{len(ecom)})".upper())
            pedido = Pedido(id_loja=ecom.get('id_loja'))
            separacao = Separacao(id_loja=ecom.get('id_loja'))

            asyncio.run(pedido.receber_novos())
            asyncio.run(separacao.receber())