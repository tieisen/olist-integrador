import logging
import os
import time
from src.sankhya.nota import Nota as NotaSnk
from src.olist.nota import Nota as NotaOlist
from database.schemas import log_pedido as SchemaLogPedido
from database.schemas import log as SchemaLog
from database.crud import venda, log, log_pedido
from dotenv import load_dotenv
from src.utils.log import Log

load_dotenv('keys/.env')
logger = logging.getLogger(__name__)
logging.basicConfig(filename=Log().buscar_path(),
                    encoding='utf-8',
                    format=os.getenv('LOGGER_FORMAT'),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)
CONTEXTO = 'nota'

class Nota:

    def __init__(self):
        self.req_time_sleep = float(os.getenv('REQ_TIME_SLEEP', 1.5))

    async def emitir(self):
        log_id = log.create(log=SchemaLog.LogBase(de='olist', para='sankhya', contexto=CONTEXTO))
        obs = None
        # Busca notas pendentes
        print("Busca notas pendentes")

        notas_pendentes = venda.read_pendente_nota_olist()
        if not notas_pendentes:
            obs = "Nenhuma nota pendente"
            print(obs)
            return True
        
        print(f"{len(notas_pendentes)} notas pendentes encontradas")
        evento = 'F'
        obs = None
        first = True 
        nota_olist = NotaOlist()
        nota_snk = NotaSnk()

        for i, nota in enumerate(notas_pendentes):
            if not first:
                time.sleep(self.req_time_sleep)  # Evita rate limit
            first = False

            if obs:
                # Cria um log de erro se houver observação
                print(obs)
                log_pedido.create(log=SchemaLogPedido.LogPedidoBase(log_id=log_id,
                                                                    id_loja=notas_pendentes[i-1].id_loja,
                                                                    id_pedido=notas_pendentes[i-1].id_pedido,
                                                                    pedido_ecommerce=notas_pendentes[i-1].cod_pedido,
                                                                    nunota_pedido=notas_pendentes[i-1].nunota_pedido,
                                                                    nunota_nota=notas_pendentes[i-1].nunota_nota,
                                                                    evento=evento,
                                                                    status=False,
                                                                    obs=obs))
                obs = None
                            
            print("")
            print(f"Emitindo nota {i+1}/{len(notas_pendentes)}: {nota.num_pedido}")
            
            dados_nota = await nota_olist.buscar(id_ecommerce=nota.cod_pedido)
            if not dados_nota:
                obs = f"Nota do pedido {nota.cod_pedido} não encontrada"
                continue

            dados_emissao = await nota_olist.emitir(id=dados_nota.get('id'))
            if not dados_emissao:
                obs = f"Erro ao emitir nota {dados_nota.get('numero')} ref. pedido {nota.cod_pedido}"
                continue

            venda.update_faturado_olist(cod_pedido=nota.cod_pedido,
                                        num_nota=int(dados_nota.get('numero')),
                                        id_nota=dados_nota.get('id'))
            
            if not await nota_snk.informar_numero_e_chavenfe(nunota=nota.nunota_nota,
                                                             chavenfe=dados_emissao.get('chaveAcesso'),
                                                             numero=int(dados_emissao.get('numero')),
                                                             id_nota=dados_nota.get('id')):
                obs = f"Erro ao informar dados da nota {dados_nota.get('numero')} na venda {nota.nunota_nota} do Sankhya"
                continue

            log_pedido.create(log=SchemaLogPedido.LogPedidoBase(log_id=log_id,
                                                                id_loja=nota.id_loja,
                                                                id_pedido=nota.id_pedido,
                                                                pedido_ecommerce=nota.cod_pedido,
                                                                nunota_pedido=nota.nunota_pedido,
                                                                nunota_nota=nota.nunota_nota,
                                                                id_nota=dados_nota.get('id'),
                                                                evento=evento,
                                                                status=True))
            
            print(f"Nota {int(dados_nota.get('numero'))} emitida com sucesso!")
                        
        status_log = False if log_pedido.read_by_logid_status_false(log_id=log_id) else True
        log.update(id=log_id, log=SchemaLog.LogBase(sucesso=status_log))
        print(f"-> Processo de emissão de notas concluído! Status do log: {status_log}")
        return True

    async def _receber_notas(self):
        log_id = log.create(log=SchemaLog.LogBase(de='olist', para='sankhya', contexto=CONTEXTO))
        obs = None
        # Busca notas pendentes
        print("Busca notas pendentes")

        notas_pendentes = venda.read_pendente_nota_olist()
        if not notas_pendentes:
            obs = "Nenhuma nota pendente"
            print(obs)
            return True
        
        print(f"{len(notas_pendentes)} notas pendentes encontradas")
        evento = 'F'
        obs = None
        first = True 
        nota_olist = NotaOlist()
        
        #dados_para_atualizar = []

        for i, nota in enumerate(notas_pendentes):
            #dados = {}
            if not first:
                time.sleep(2)  # Evita rate limit
            first = False

            if obs:
                # Cria um log de erro se houver observação
                print(obs)
                log_pedido.create(log=SchemaLogPedido.LogPedidoBase(log_id=log_id,
                                                                    id_loja=notas_pendentes[i-1].id_loja,
                                                                    id_pedido=notas_pendentes[i-1].id_pedido,
                                                                    pedido_ecommerce=notas_pendentes[i-1].cod_pedido,
                                                                    nunota_pedido=notas_pendentes[i-1].nunota_pedido,
                                                                    nunota_nota=notas_pendentes[i-1].nunota_nota,
                                                                    evento=evento,
                                                                    status=False,
                                                                    obs=obs))
                obs = None
                            
            print("")
            print(f"Recebendo nota {i+1}/{len(notas_pendentes)}: {nota.num_pedido}")
            
            dados_nota = await nota_olist.buscar(id_ecommerce=nota.cod_pedido)
            if not dados_nota:
                obs = f"Nota do pedido {nota.cod_pedido} não encontrada"
                continue

            venda.update_faturado_olist(cod_pedido=nota.cod_pedido,
                                        num_nota=int(dados_nota.get('numero')),
                                        id_nota=dados_nota.get('id'))

            print(f"Recebendo financeiro da nota {i+1}/{len(notas_pendentes)}: {int(dados_nota.get('numero'))}")
            
            dados_financeiro = await nota_olist.buscar_financeiro(serie='2', numero=dados_nota.get('numero'))
            if not dados_financeiro:
                print(f"Financeiro da nota {int(dados_nota.get('numero'))} não encontrado no Olist")
                continue
            venda.update_baixa_financeiro(num_nota=int(dados_nota.get('numero')),
                                            id_financeiro=dados_financeiro.get('id'))
            #print(f"Financeiro da nota {int(dados_nota.get('numero'))} baixado com sucesso no Olist")

            venda.update_nota_confirma_snk(nunota_nota=nota.nunota_nota)

            print(f"Nota {int(dados_nota.get('numero'))} recebida com sucesso!")
            log_pedido.create(log=SchemaLogPedido.LogPedidoBase(log_id=log_id,
                                                                id_loja=nota.id_loja,
                                                                id_pedido=nota.id_pedido,
                                                                pedido_ecommerce=nota.cod_pedido,
                                                                nunota_pedido=nota.nunota_pedido,
                                                                nunota_nota=nota.nunota_nota,
                                                                id_nota=dados_nota.get('id'),
                                                                evento=evento,
                                                                status=True))
            
        status_log = False if log_pedido.read_by_logid_status_false(log_id=log_id) else True
        log.update(id=log_id, log=SchemaLog.LogBase(sucesso=status_log))
        print(f"-> Processo de recebimento de notas concluído! Status do log: {status_log}")
        return True

    async def _receber_financeiro(self):
        #log_id = log.create(log=SchemaLog.LogBase(de='olist', para='sankhya', contexto=CONTEXTO))
        obs = None
        # Busca notas pendentes
        print("Busca notas pendentes")

        fin_pendentes = venda.read_pendente_fin_olist()
        if not fin_pendentes:
            obs = "Nenhum pendente"
            print(obs)
            return True
        
        print(f"{len(fin_pendentes)} fin pendentes")
        evento = 'F'
        obs = None
        first = True 
        nota_olist = NotaOlist()
        
        #dados_para_atualizar = []

        for i, fin in enumerate(fin_pendentes):
            #dados = {}
            if not first:
                time.sleep(2)  # Evita rate limit
            first = False

            # if obs:
            #     # Cria um log de erro se houver observação
            #     print(obs)
            #     log_pedido.create(log=SchemaLogPedido.LogPedidoBase(log_id=log_id,
            #                                                         id_loja=notas_pendentes[i-1].id_loja,
            #                                                         id_pedido=notas_pendentes[i-1].id_pedido,
            #                                                         pedido_ecommerce=notas_pendentes[i-1].cod_pedido,
            #                                                         nunota_pedido=notas_pendentes[i-1].nunota_pedido,
            #                                                         nunota_nota=notas_pendentes[i-1].nunota_nota,
            #                                                         evento=evento,
            #                                                         status=False,
            #                                                         obs=obs))
            #     obs = None
                            
            # print("")
            # print(f"Recebendo nota {i+1}/{len(notas_pendentes)}: {nota.num_pedido}")
            
            # dados_nota = await nota_olist.buscar(id_ecommerce=nota.cod_pedido)
            # if not dados_nota:
            #     obs = f"Nota do pedido {nota.cod_pedido} não encontrada"
            #     continue

            # venda.update_faturado_olist(cod_pedido=nota.cod_pedido,
            #                             num_nota=int(dados_nota.get('numero')),
            #                             id_nota=dados_nota.get('id'))

            print("")
            print(f"Recebendo financeiro da nota {i+1}/{len(fin_pendentes)}: {fin.num_nota}")
            
            dados_financeiro = await nota_olist.buscar_financeiro(serie='2', numero=str(fin.num_nota).zfill(6))
            if not dados_financeiro:
                print(f"Financeiro da nota {fin.num_nota} não encontrado no Olist")
                continue
            venda.update_baixa_financeiro(num_nota=fin.num_nota,
                                          id_financeiro=dados_financeiro.get('id'))
            print(f"Financeiro da nota {fin.num_nota} recebido com sucesso")

            # print(f"Nota {int(dados_nota.get('numero'))} recebida com sucesso!")
            # log_pedido.create(log=SchemaLogPedido.LogPedidoBase(log_id=log_id,
            #                                                     id_loja=nota.id_loja,
            #                                                     id_pedido=nota.id_pedido,
            #                                                     pedido_ecommerce=nota.cod_pedido,
            #                                                     nunota_pedido=nota.nunota_pedido,
            #                                                     nunota_nota=nota.nunota_nota,
            #                                                     id_nota=dados_nota.get('id'),
            #                                                     evento=evento,
            #                                                     status=True))
            
        # status_log = False if log_pedido.read_by_logid_status_false(log_id=log_id) else True
        # log.update(id=log_id, log=SchemaLog.LogBase(sucesso=status_log))
        print(f"-> Processo de recebimento de financeiro concluído!")
        return True

    async def confirmar(self):

        log_id = log.create(log=SchemaLog.LogBase(de='olist', para='sankhya', contexto=CONTEXTO))
        obs = None
        # Busca notas pendentes
        print("Busca notas pendentes")
        notas_pendentes = venda.read_venda_faturada_confirmar_snk()
        if not notas_pendentes:
            obs = "Nenhuma nota pendente"
            print(obs)
            return True
        
        print(f"{len(notas_pendentes)} notas pendentes encontradas")
        evento = 'C'
        obs = None
        first = True 
        nota_snk = NotaSnk()

        for i, nota in enumerate(notas_pendentes):
            if not first:
                time.sleep(self.req_time_sleep)  # Evita rate limit
            first = False

            if obs:
                # Cria um log de erro se houver observação
                print(obs)
                log_pedido.create(log=SchemaLogPedido.LogPedidoBase(log_id=log_id,
                                                                    id_loja=notas_pendentes[i-1].id_loja,
                                                                    id_pedido=notas_pendentes[i-1].id_pedido,
                                                                    pedido_ecommerce=notas_pendentes[i-1].cod_pedido,
                                                                    nunota_pedido=notas_pendentes[i-1].nunota_pedido,
                                                                    nunota_nota=notas_pendentes[i-1].nunota_nota,
                                                                    id_nota=notas_pendentes[i-1].id_nota,
                                                                    evento=evento,
                                                                    status=False,
                                                                    obs=obs))
                obs = None
                            
            print("")
            print(f"Confirmando nota {i+1}/{len(notas_pendentes)}: {nota.num_nota}")

            ack = await nota_snk.confirmar(nunota=nota.nunota_nota)
            if not ack:
                dados_nota = await nota_snk.buscar(nunota=nota.nunota_nota)
                if not dados_nota:
                    obs = f"Erro ao confirmar nota {nota.num_nota}/{nota.nunota_nota} no Sankhya"                    
                    continue
                else:
                    print(f"Nota {nota.num_nota}/{nota.nunota_nota} já foi confirmada no Sankhya")
                    venda.update_nota_confirma_snk(nunota_nota=nota.nunota_nota)

            venda.update_nota_confirma_snk(nunota_nota=nota.nunota_nota)

            print(f"Nota {nota.num_nota}/{nota.nunota_nota} confirmada com sucesso no Sankhya")

        log_pedido.create(log=SchemaLogPedido.LogPedidoBase(log_id=log_id,
                                                            id_loja=nota.id_loja,
                                                            id_pedido=nota.id_pedido,
                                                            pedido_ecommerce=nota.cod_pedido,
                                                            nunota_pedido=nota.nunota_pedido,
                                                            nunota_nota=nota.nunota_nota,
                                                            id_nota=nota.id_nota,
                                                            evento=evento,
                                                            status=True))

        status_log = False if log_pedido.read_by_logid_status_false(log_id=log_id) else True
        log.update(id=log_id, log=SchemaLog.LogBase(sucesso=status_log))
        print(f"-> Processo de confirmação de notas concluído! Status do log: {status_log}")
        return True

    async def baixar_financeiro(self):

        log_id = log.create(log=SchemaLog.LogBase(de='olist', para='sankhya', contexto=CONTEXTO))
        obs = None
        # Busca notas pendentes
        print("Busca notas pendentes")
        notas_pendentes = venda.read_pendente_fin_olist()
        if not notas_pendentes:
            obs = "Nenhuma nota pendente"
            print(obs)
            return True
        
        print(f"{len(notas_pendentes)} notas pendentes encontradas")
        evento = 'F'
        obs = None
        first = True 
        nota_olist = NotaOlist()

        for i, nota in enumerate(notas_pendentes):
            if not first:
                time.sleep(self.req_time_sleep)  # Evita rate limit
            first = False

            if obs:
                # Cria um log de erro se houver observação
                print(obs)
                log_pedido.create(log=SchemaLogPedido.LogPedidoBase(log_id=log_id,
                                                                    id_loja=notas_pendentes[i-1].id_loja,
                                                                    id_pedido=notas_pendentes[i-1].id_pedido,
                                                                    pedido_ecommerce=notas_pendentes[i-1].cod_pedido,
                                                                    nunota_pedido=notas_pendentes[i-1].nunota_pedido,
                                                                    nunota_nota=notas_pendentes[i-1].nunota_nota,
                                                                    id_nota=notas_pendentes[i-1].id_nota,
                                                                    evento=evento,
                                                                    status=False,
                                                                    obs=obs))
                obs = None
                            
            print("")
            print(f"Baixando financeiro da nota {i+1}/{len(notas_pendentes)}: {nota.num_nota}")
            
            dados_financeiro = await nota_olist.buscar_financeiro(serie='2', numero=str(nota.num_nota).zfill(6))
            if not dados_financeiro:
                obs = f"Erro ao buscar financeiro da nota {nota.num_nota} no Olist"
                print(obs)
                continue
            
            ack_financeiro = await nota_olist.baixar_financeiro(id=dados_financeiro.get('id'),
                                                                valor=dados_financeiro.get('valor'))
            if ack_financeiro is None:
                print(f"Financeiro da nota {nota.num_nota} já está baixado no Olist")
                continue
            if ack_financeiro is False:
                obs = f"Erro ao baixar financeiro da nota {nota.num_nota} no Olist"
                continue
            
            venda.update_baixa_financeiro(num_nota=nota.num_nota,
                                          id_financeiro=dados_financeiro.get('id'))
            print(f"Financeiro da nota {nota.num_nota} baixado com sucesso no Olist")

            log_pedido.create(log=SchemaLogPedido.LogPedidoBase(log_id=log_id,
                                                                id_loja=notas_pendentes[i-1].id_loja,
                                                                id_pedido=notas_pendentes[i-1].id_pedido,
                                                                pedido_ecommerce=notas_pendentes[i-1].cod_pedido,
                                                                nunota_pedido=notas_pendentes[i-1].nunota_pedido,
                                                                nunota_nota=notas_pendentes[i-1].nunota_nota,
                                                                id_nota=notas_pendentes[i-1].id_nota,
                                                                evento=evento,
                                                                status=True))          
        
        status_log = False if obs else True
        log.update(id=log_id, log=SchemaLog.LogBase(sucesso=status_log))
        print(f"-> Processo de baixa do financeiro das notas concluído! Status do log: {status_log}")
        return True