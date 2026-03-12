from database.crud import empresa, ecommerce, shopee, nota
from src.olist.nota import Nota as NotaOlist
from src.integrador.financeiro import Receita, Despesa
import time

async def integrar(codemp:int|None=None,dtFim:str|None=None) -> dict:

    retorno:dict={
            "status": False,
            "exception": ""
        }
    empresas:list[dict]=[]
    ecommerces:list[dict]=[]
    emp:dict={}
    ecom:dict={}
    lista_notas_emitidas:list[dict]=[]
    lista_nota_lcto:list[dict]=[]

    empresas = await empresa.buscar(codemp=codemp)
    try:
        for i, emp in enumerate(empresas):
            print(f"\nEmpresa {emp.get('nome')} ({i+1}/{len(empresas)})".upper())
            receita = Receita(empresaId=emp.get('id'))
            despesa = Despesa(empresaId=emp.get('id'))            
            notas = NotaOlist(empresa_id=emp.get('id'))
            
            print("Buscando notas emitidas...")
            lista_notas_emitidas = await notas.buscarData(data=dtFim)
            print(f"Notas emitidas: {len(lista_notas_emitidas)}")
            
            print("Processando notas emitidas...")
            await receita.processarNotas(listaNotas=lista_notas_emitidas)

            ecommerces = await ecommerce.buscar(empresa_id=emp.get('id'))
            for j, ecom in enumerate(ecommerces):
                print(f"\nE-commerce {ecom.get('nome')} ({j+1}/{len(ecommerces)})".upper())
                receita.dados_ecommerce = None
                receita.id_loja = ecom.get('id_loja')
                despesa.dados_ecommerce = None                
                despesa.id_loja = ecom.get('id_loja')             
                    
                print(f"Buscando contas pendentes...")
                lista_nota_lcto = await nota.buscarPendenteLcto(ecommerce_id=ecom.get('id'))
                if not lista_nota_lcto:
                    continue
                
                for n, nota_lcto in enumerate(lista_nota_lcto):                    
                    if ('SHOPEE' in ecom.get('nome').upper()) and (nota_lcto['income_data'].get('fee_shopee',0)==0):
                        continue
                    time.sleep(receita.req_time_sleep)
                    print(f"->> Nota {nota_lcto.get('numero')}/{nota_lcto.get('id_nota')}: ({n+1}/{len(lista_nota_lcto)})".upper())
                    if not nota_lcto.get('id_financeiro'):
                        print(f"Formatando payload receita...")
                        await receita.formatarPayloadLcto(dadosConta=nota_lcto)
                        print(f"Lançando receita...")
                        await receita.lancarConta()
                        print(f"RECEITA OK!!")
                    
                    if len(str(ecom.get('id_loja'))) >= 9:
                        # Funcionários ou Parfum
                        await despesa.ignorarTaxa(id_nota=nota_lcto.get('id'))
                        continue
                    
                    if not nota_lcto.get('id_financeiro_taxa'):
                        print(f"Formatando payload despesa...")
                        await despesa.formatarPayloadLcto(dadosConta=nota_lcto)
                        print(f"Lançando despesa...")                    
                        await despesa.lancarConta()
                        print(f"DESPESA OK!!")
        
        retorno['status'] = True        
    except Exception as e:        
        retorno['exception'] = str(e)
    finally:
        pass
    
    return retorno

async def consultarRecebimentosShopee(codemp:int=None) -> dict:

    retorno:dict={}
    empresas:list[dict]=[]
    emp:dict={}

    print(":::::::::::::::::::: RECEBIMENTOS SHOPEE ::::::::::::::::::::")    

    empresas = await empresa.buscar(codemp=codemp)
    try:
        for i, emp in enumerate(empresas):
            print(f"\nEmpresa {emp.get('nome')} ({i+1}/{len(empresas)})".upper())
            receita = Receita(empresaId=emp.get('id'))            
            
            ecommerces = await ecommerce.buscar(empresa_id=emp.get('id'))
            for j, ecom in enumerate(ecommerces):
                if 'SHOPEE' in ecom.get('nome').upper():
                    loja_shopee = await shopee.buscar(ecommerce_id=ecom.get('id'))
                    if loja_shopee:
                        print("Buscando contas do Shopee...")
                        await receita.buscarContasShopee(ecommerceId=ecom.get('id'))
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