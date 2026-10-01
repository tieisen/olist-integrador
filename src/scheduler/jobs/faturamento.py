from collections.abc import Awaitable, Callable

from database.crud import ecommerce, empresa
from src.integrador.faturamento import Faturamento
from src.integrador.pedido import Pedido
from src.utils.log import set_logger

logger = set_logger(__name__)

# Rotina executada para um e-commerce. Retorna a mensagem de erro, ou None se deu certo.
Rotina = Callable[[dict, int | None], Awaitable[str | None]]


def _retorno(erros: list[str]) -> dict:
    if not erros:
        return {"status": True, "exception": None}
    return {"status": False, "exception": "; ".join(erros)}


async def _executar_rotina(ecom: dict, codemp: int | None, rotina: Rotina) -> str | None:
    """Isola a falha de um e-commerce para que não interrompa os demais."""
    try:
        erro = await rotina(ecom, codemp)
    except Exception as e:
        logger.exception("Erro inesperado no e-commerce %s", ecom.get("nome"))
        erro = str(e)
    return f"E-commerce {ecom.get('nome')}: {erro}" if erro else None


async def _por_empresa(codemp: int | None, titulo: str, rotina: Rotina) -> dict:
    """Executa a rotina em todos os e-commerces de todas as empresas."""
    print(titulo)
    erros: list[str] = []
    try:
        empresas = await empresa.buscar(codemp=codemp)
        for i, emp in enumerate(empresas):
            print(f"\nEmpresa {emp.get('nome')} ({i + 1}/{len(empresas)})".upper())
            ecommerces = await ecommerce.buscar(empresa_id=emp.get("id"))
            for j, ecom in enumerate(ecommerces):
                print(f"E-commerce {ecom.get('nome')} ({j + 1}/{len(ecommerces)})".upper())
                if erro := await _executar_rotina(ecom, emp.get("snk_codemp"), rotina):
                    erros.append(erro)
    except Exception as e:
        logger.exception("Erro ao percorrer empresas e e-commerces")
        erros.append(str(e))
    return _retorno(erros)


async def _por_loja(id_loja: int, titulo: str, rotina: Rotina) -> dict:
    """Executa a rotina em um único e-commerce, identificado pelo id_loja."""
    print(titulo)
    try:
        ecommerces = await ecommerce.buscar(id_loja=id_loja)
        if not ecommerces:
            return _retorno([f"E-commerce com id_loja={id_loja} não encontrado"])
        ecom = ecommerces[0]
        print(f"E-commerce {ecom.get('nome')}".upper())
        erro = await _executar_rotina(ecom, None, rotina)
    except Exception as e:
        logger.exception("Erro ao buscar o e-commerce id_loja=%s", id_loja)
        return _retorno([str(e)])
    return _retorno([erro] if erro else [])


# ROTINAS POR E-COMMERCE


async def _faturar_completo(ecom: dict, codemp: int | None) -> str | None:
    """Olist e depois Sankhya. Uma falha em uma etapa não impede a outra."""
    faturamento = Faturamento(id_loja=ecom.get("id_loja"), codemp=codemp)
    await Pedido(id_loja=ecom.get("id_loja"), codemp=codemp).consultar_cancelamentos()

    erros: list[str] = []
    if not await faturamento.integrar_olist():
        erros.append("falha ao faturar no Olist")
    status_snk = await faturamento.integrar_snk()
    if not status_snk.get("success"):
        erros.append(f"falha ao faturar no Sankhya: {status_snk.get('__exception__')}")
    return "; ".join(erros) or None


async def _faturar_loja_unica(ecom: dict, codemp: int | None) -> str | None:
    """Olist e depois Sankhya, como no lote. Falha em uma etapa não impede a outra."""
    faturamento = Faturamento(id_loja=ecom.get("id_loja"), codemp=codemp)

    erros: list[str] = []
    if not await faturamento.integrar_olist():
        erros.append("falha ao faturar no Olist")
    status_snk = await faturamento.integrar_snk(loja_unica=True)
    if not status_snk.get("success"):
        erros.append(f"falha ao faturar no Sankhya: {status_snk.get('__exception__')}")
    return "; ".join(erros) or None


async def _faturar_olist(ecom: dict, codemp: int | None) -> str | None:
    faturamento = Faturamento(id_loja=ecom.get("id_loja"), codemp=codemp)
    return None if await faturamento.integrar_olist() else "falha ao faturar no Olist"


async def _faturar_snk(ecom: dict, codemp: int | None) -> str | None:
    faturamento = Faturamento(id_loja=ecom.get("id_loja"), codemp=codemp)
    status_snk = await faturamento.integrar_snk()
    return None if status_snk.get("success") else status_snk.get("__exception__")


# JOBS


async def integrar_faturamento(codemp: int = None, id_loja: int = None) -> dict:
    titulo = ":::::::::::::::::::: FATURAMENTO DE PEDIDOS ::::::::::::::::::::"
    if id_loja:
        return await _por_loja(id_loja, titulo, _faturar_loja_unica)
    return await _por_empresa(codemp, titulo, _faturar_completo)


async def integrar_faturamento_olist(codemp: int = None, id_loja: int = None) -> dict:
    titulo = ":::::::::::::::::::: FATURAMENTO DE PEDIDOS NO OLIST ::::::::::::::::::::"
    if id_loja:
        return await _por_loja(id_loja, titulo, _faturar_olist)
    return await _por_empresa(codemp, titulo, _faturar_olist)


async def integrar_faturamento_snk(codemp: int = None) -> dict:
    titulo = ":::::::::::::::::::: FATURAMENTO DE PEDIDOS NO SANKHYA ::::::::::::::::::::"
    return await _por_empresa(codemp, titulo, _faturar_snk)


async def integrar_venda_interna(codemp: int = None) -> dict:
    print(":::::::::::::::::::: VENDA INTERNA ::::::::::::::::::::")
    erros: list[str] = []
    try:
        empresas = await empresa.buscar(codemp=codemp)
        for i, emp in enumerate(empresas):
            print(f"\nEmpresa {emp.get('nome')} ({i + 1}/{len(empresas)})".upper())
            try:
                faturamento = Faturamento(codemp=emp.get("snk_codemp"))
                if not await faturamento.realizar_venda_interna():
                    erros.append(f"Empresa {emp.get('nome')}: falha na venda interna")
            except Exception as e:
                logger.exception("Erro na venda interna da empresa %s", emp.get("nome"))
                erros.append(f"Empresa {emp.get('nome')}: {e}")
    except Exception as e:
        logger.exception("Erro ao buscar empresas")
        erros.append(str(e))
    return _retorno(erros)
