import asyncio

from database.crud import ecommerce, empresa
from src.integrador.pedido import Pedido
from src.integrador.separacao import Separacao
from src.utils.log import set_logger

logger = set_logger(__name__)


async def receber_pedido_lote(codemp: int = None, id_loja: int = None) -> dict:

    retorno: dict = {}
    empresas: list[dict] = []
    ecommerces: list[dict] = []
    emp: dict = {}
    ecom: dict = {}

    print("::::::::::::::::::: RECEBIMENTO DE PEDIDOS :::::::::::::::::::")
    logger.info(f"[lote] Início do recebimento de pedidos (codemp={codemp}, id_loja={id_loja})")
    if not id_loja:
        empresas = await empresa.buscar(codemp=codemp)
        logger.info(f"[lote] {len(empresas or [])} empresa(s) encontrada(s)")
        try:
            if not empresas:
                retorno = {"status": True, "exception": None}
            else:
                for i, emp in enumerate(empresas):
                    print(f"\nEmpresa {emp.get('nome')} ({i + 1}/{len(empresas)})".upper())
                    logger.info(f"[lote] Empresa {emp.get('nome')} ({i + 1}/{len(empresas)})")
                    ecommerces = await ecommerce.buscar(empresa_id=emp.get("id"))
                    if not ecommerces:
                        print("Nenhum e-commerce vinculado à empresa")
                        logger.info(f"[lote] Empresa {emp.get('nome')} sem e-commerce vinculado")
                        continue
                    for j, ecom in enumerate(ecommerces):
                        print(f"E-commerce {ecom.get('nome')} ({j + 1}/{len(ecommerces)})".upper())
                        logger.info(
                            f"[lote] E-commerce {ecom.get('nome')} "
                            f"id_loja={ecom.get('id_loja')} ({j + 1}/{len(ecommerces)})"
                        )
                        pedido = Pedido(id_loja=ecom.get("id_loja"), codemp=emp.get("snk_codemp"))
                        separacao = Separacao(id_loja=ecom.get("id_loja"))
                        logger.info(
                            f"[lote] id_loja={ecom.get('id_loja')}: iniciando receber_novos"
                        )
                        await pedido.receber_novos()
                        logger.info(
                            f"[lote] id_loja={ecom.get('id_loja')}: receber_novos concluído"
                        )
                        logger.info(
                            f"[lote] id_loja={ecom.get('id_loja')}: iniciando separacao.receber"
                        )
                        await separacao.receber()
                        logger.info(
                            f"[lote] id_loja={ecom.get('id_loja')}: separacao.receber concluído"
                        )
                retorno = {"status": True, "exception": None}
        except Exception as e:
            logger.exception(f"[lote] Exceção no recebimento de pedidos: {e}")
            retorno = {"status": False, "exception": e}

        logger.info(f"[lote] Fim do recebimento de pedidos. Status: {retorno.get('status')}")
        return retorno

    else:
        try:
            ecommerces = await ecommerce.buscar(id_loja=id_loja)
            ecom = ecommerces[0]
            print(f"E-commerce {ecom.get('nome')}".upper())  # Print original
            logger.info(f"[lote] E-commerce {ecom.get('nome')} id_loja={id_loja}")
            pedido = Pedido(id_loja=id_loja)
            separacao = Separacao(id_loja=id_loja)
            logger.info(f"[lote] id_loja={id_loja}: iniciando receber_novos")
            await pedido.receber_novos()
            logger.info(f"[lote] id_loja={id_loja}: receber_novos concluído")
            logger.info(f"[lote] id_loja={id_loja}: iniciando separacao.receber")
            await separacao.receber()
            logger.info(f"[lote] id_loja={id_loja}: separacao.receber concluído")
            retorno = {"status": True, "exception": None}
        except Exception as e:
            logger.exception(f"[lote] Exceção no recebimento de pedidos (id_loja={id_loja}): {e}")
            retorno = {"status": False, "exception": e}

        logger.info(f"[lote] Fim do recebimento de pedidos. Status: {retorno.get('status')}")

        return retorno


async def receber_pedido_unico(id_loja: int, numero: int) -> dict:
    retorno: dict = {}
    print("::::::::::::::::::: RECEBIMENTO DE PEDIDO ÚNICO :::::::::::::::::::")
    pedido = Pedido(id_loja=id_loja)
    try:
        ack = await pedido.receber(num_pedido=numero)
        retorno = {"status": ack.get("success"), "exception": ack.get("__exception__")}
    except Exception as e:
        retorno = {"status": False, "exception": e}

    return retorno


async def integrar_pedidos(codemp: int = None, id_loja: int = None) -> dict:

    retorno: dict = {}
    empresas: list[dict] = []
    ecommerces: list[dict] = []
    lista_itens_retorno: list[dict] = []
    sucesso: list[bool] = []
    emp: dict = {}
    ecom: dict = {}

    print("::::::::::::::::::: INTEGRAÇÃO DE PEDIDOS :::::::::::::::::::")

    if not id_loja:
        empresas = await empresa.buscar(codemp=codemp)

        try:
            for i, emp in enumerate(empresas):
                print(f"\nEmpresa {emp.get('nome')} ({i + 1}/{len(empresas)})".upper())
                ecommerces = await ecommerce.buscar(empresa_id=emp.get("id"))
                for j, ecom in enumerate(ecommerces):
                    print(f"E-commerce {ecom.get('nome')} ({j + 1}/{len(ecommerces)})".upper())
                    pedido = Pedido(id_loja=ecom.get("id_loja"))
                    ack_integrar, lista_retorno = await pedido.integrar_novos()
                    ack_confirmar = await pedido.integrar_confirmacao()
                    sucesso.append(ack_integrar)
                    sucesso.append(ack_confirmar)
                    lista_itens_retorno.append(
                        {"ecommerce": ecom.get("nome"), "dados": lista_retorno}
                    )
            retorno = {
                "status": all(sucesso),
                "data": lista_itens_retorno,
                "exception": None
                if all(sucesso)
                else "Algum problema pode ter ocorrido. Verifique com o TI.",
            }
        except Exception as e:
            retorno = {"status": all(sucesso), "data": None, "exception": f"{e}"}
        finally:
            pass
    else:
        try:
            ecommerces = await ecommerce.buscar(id_loja=id_loja)
            ecom = ecommerces[0]
            print(f"E-commerce {ecom.get('nome')}".upper())
            pedido = Pedido(id_loja=id_loja)
            ack_integrar, lista_retorno = await pedido.integrar_novos()
            ack_confirmar = await pedido.integrar_confirmacao()
            sucesso.append(ack_integrar)
            sucesso.append(ack_confirmar)
            lista_itens_retorno.append({"ecommerce": ecom.get("nome"), "dados": lista_retorno})
            retorno = {
                "status": all(sucesso),
                "data": lista_itens_retorno,
                "exception": None
                if all(sucesso)
                else "Algum problema pode ter ocorrido. Verifique com o TI.",
            }
        except Exception as e:
            retorno = {"status": all(sucesso), "data": None, "exception": f"{e}"}
        finally:
            pass

    return retorno


async def integrar_separacoes(codemp: int = None) -> dict:

    retorno: dict = {}
    empresas: list[dict] = []
    ecommerces: list[dict] = []
    emp: dict = {}
    ecom: dict = {}

    empresas = await empresa.buscar(codemp=codemp)

    print("::::::::::::::::::: INTEGRAÇÃO DE SEPARAÇÕES :::::::::::::::::::")

    try:
        for i, emp in enumerate(empresas):
            print(f"\nEmpresa {emp.get('nome')} ({i + 1}/{len(empresas)})".upper())
            ecommerces = await ecommerce.buscar(empresa_id=emp.get("id"))
            for j, ecom in enumerate(ecommerces):
                print(f"E-commerce {ecom.get('nome')} ({j + 1}/{len(ecommerces)})".upper())
                separacao = Separacao(id_loja=ecom.get("id_loja"))
                await separacao.receber()
        retorno = {"status": True, "exception": None}
    except Exception as e:
        retorno = {"status": False, "exception": e}

    return retorno


async def anular_importacao(codemp: int, nunota: int) -> dict:
    print(f"::::::::::::::::::: ANULANDO IMPORTAÇÃO DO PEDIDO {nunota} :::::::::::::::::::")
    pedido = Pedido(codemp=codemp)
    ack = await pedido.anular_pedido_importado(nunota=nunota)
    return ack


async def reprocessar_separacao(codemp: int) -> dict:
    print("::::::::::::::::::: REPROCESSANDO RELATÓRIO DE SEPARAÇÃO :::::::::::::::::::")
    pedido = Pedido(codemp=codemp)
    ack = await pedido.reprocessar_relatorio_separacao(codemp=codemp)
    return ack


if __name__ == "__main__":
    asyncio.run(receber_pedido_lote())
