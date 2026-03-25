import time
from database.crud import empresa, ecommerce, shopee, nota
from src.olist.nota import Nota as NotaOlist
from src.integrador.financeiro import Receita, Despesa
from src.utils.log import set_logger
logger = set_logger(__name__)

async def integrar(codemp:int|None=None,idLoja:int|None=None,dataFim:str|None=None,dias:int=0,processaShopee:bool=True) -> dict:

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
    lista_estornos:list[dict]=[]
    receita:Receita=None
    despesa:Despesa=None

    empresas = await empresa.buscar(codemp=codemp)
    try:
        for i, emp in enumerate(empresas):
            # print(f"Empresa {emp.get('nome')} ({i+1}/{len(empresas)})".upper())
            receita = Receita(empresaId=emp.get('id'))
            despesa = Despesa(empresaId=emp.get('id'))            
            notas = NotaOlist(empresa_id=emp.get('id'))
            
            lista_notas_emitidas = await notas.buscarData(data=dataFim,dias=dias)
            # print("Processando notas emitidas...")
            await receita.processarNotas(listaNotas=lista_notas_emitidas)

            if idLoja:
                ecommerces = await ecommerce.buscar(id_loja=idLoja)
            else:
                ecommerces = await ecommerce.buscar(empresa_id=emp.get('id'))
                
            for j, ecom in enumerate(ecommerces):
                # print(f"E-commerce {ecom.get('nome')} ({j+1}/{len(ecommerces)})".upper())
                receita.dados_ecommerce = None
                receita.id_loja = ecom.get('id_loja')
                despesa.dados_ecommerce = None                
                despesa.id_loja = ecom.get('id_loja')  

                if processaShopee and ('SHOPEE' in ecom.get('nome').upper()):
                    # print("Processando recebimentos Shopee...")
                    await consultarRecebimentosShopee(codemp=emp.get('snk_codemp'),dtFim=dataFim,dias=dias)
                    
                    # print("Validando estornos...")
                    lista_estornos = await nota.buscarEstornoPendenteLcto(ecommerce_id=ecom.get('id'),data=dataFim)
                    logger.info(f"Estornos pendentes: {len(lista_estornos)}")
                    # print(f"Estornos pendentes: {len(lista_estornos)}")
                    if lista_estornos:
                        for n, estorno in enumerate(lista_estornos):
                            time.sleep(receita.req_time_sleep)
                            await despesa.buscarEstornoShopee(orderSn=estorno.get('income_data').get('order_sn'))                            

                # print(f"Buscando contas pendentes...")
                lista_nota_lcto = await nota.buscarPendenteLcto(ecommerce_id=ecom.get('id'),data=dataFim)
                logger.info(f"Contas pendentes: {len(lista_nota_lcto)}")
                # print(f"Contas pendentes: {len(lista_nota_lcto)}")
                if not lista_nota_lcto:
                    continue
                
                for n, nota_lcto in enumerate(lista_nota_lcto):                    
                    if ('SHOPEE' in ecom.get('nome').upper()):
                        if nota_lcto['income_data'].get('fee_shopee',0)==0:
                            continue
                        
                    time.sleep(receita.req_time_sleep)
                    # print(f"->> Nota {nota_lcto.get('numero')}/{nota_lcto.get('id_nota')}: ({n+1}/{len(lista_nota_lcto)})".upper())
                    if not nota_lcto.get('id_financeiro'):
                        await receita.formatarPayloadLcto(dadosConta=nota_lcto)
                        await receita.lancarConta()
                        # print(f"Conta lançada: {nota_lcto.get('numero')}/{nota_lcto.get('id_nota')}")
                    
                    if len(str(ecom.get('id_loja'))) >= 8:
                        # Funcionários ou Parfum
                        await despesa.ignorarTaxa(id_nota=nota_lcto.get('id'))
                        # print(f"Taxa ignorada: {nota_lcto.get('numero')}/{nota_lcto.get('id_nota')}")
                        continue
                    
                    time.sleep(receita.req_time_sleep)
                    if not nota_lcto.get('id_financeiro_taxa'):
                        await despesa.formatarPayloadLcto(dadosConta=nota_lcto)
                        await despesa.lancarConta()
                        # print(f"Conta de despesa lançada: {nota_lcto.get('numero')}/{nota_lcto.get('id_nota')}")

                    time.sleep(receita.req_time_sleep)
                    if 'fee_frete' in nota_lcto.get('income_data',{}) and not nota_lcto.get('id_financeiro_frete'):
                        await despesa.formatarPayloadLcto(dadosConta=nota_lcto)
                        await despesa.lancarConta()
                        # print(f"Conta de frete lançada: {nota_lcto.get('numero')}/{nota_lcto.get('id_nota')}")
        
        retorno['status'] = True        
    except Exception as e:        
        retorno['exception'] = str(e)
    finally:
        logger.info(f"Processamento finalizado. Status: {retorno.get('status')}. Exception: {retorno.get('exception')}")
        # print(f"Processamento finalizado. Status: {retorno.get('status')}. Exception: {retorno.get('exception')}")
        pass
    
    return retorno

async def consultarRecebimentosShopee(codemp:int=None,dataFim:str=None,dias:int=0) -> dict:

    retorno:dict={}
    empresas:list[dict]=[]
    emp:dict={}
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
                        await receita.buscarContasShopee(ecommerceId=ecom.get('id'),dtFim=dataFim,dias=dias)
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