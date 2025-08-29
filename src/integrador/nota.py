import logging
import os
import time
from src.sankhya.nota import Nota as NotaSnk
from src.olist.nota import Nota as NotaOlist
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
        self.serie_nfe = os.getenv('SANKHYA_SERIE_NF',1)

    async def emitir(self):
        log_id = log.criar(de='olist', para='sankhya', contexto=CONTEXTO)
        obs = None
        # Busca notas pendentes
        print("Busca notas pendentes")

        notas_pendentes = venda.buscar_autorizar()
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

        try:
            for i, nota in enumerate(notas_pendentes):
                if not first:
                    time.sleep(self.req_time_sleep)  # Evita rate limit
                first = False

                if obs:
                    # Cria um log de erro se houver observação
                    print(obs)
                    log_pedido.criar(log_id=log_id,
                                     id_loja=notas_pendentes[i-1].id_loja,
                                     id_pedido=notas_pendentes[i-1].id_pedido,
                                     pedido_ecommerce=notas_pendentes[i-1].cod_pedido,
                                     nunota_pedido=notas_pendentes[i-1].nunota_pedido,
                                     nunota_nota=notas_pendentes[i-1].nunota_nota,
                                     evento=evento,
                                     status=False,
                                     obs=obs)
                    obs = None
                                
                print("")
                print(f"Emitindo nota {i+1}/{len(notas_pendentes)}: {nota.num_pedido}")
                
                dados_nota = await nota_olist.buscar(id=nota.id_nota)
                if not dados_nota:
                    obs = f"Nota do pedido {nota.cod_pedido} não encontrada"
                    continue

                dados_emissao = await nota_olist.emitir(id=nota.id_nota)
                if not dados_emissao:
                    obs = f"Erro ao emitir nota {nota.num_nota} ref. pedido {nota.cod_pedido}"
                    continue

                venda.atualizar_nf_autorizada(id_nota=nota.id_nota)
                
                if not await nota_snk.informar_numero_e_chavenfe(nunota=nota.nunota_nota,
                                                                chavenfe=dados_emissao.get('chaveAcesso'),
                                                                numero=nota.num_nota,
                                                                id_nota=nota.id_nota):
                    obs = f"Erro ao informar dados da nota {nota.num_nota} na venda {nota.nunota_nota} do Sankhya"
                    continue

                log_pedido.criar(log_id=log_id,
                                 id_loja=nota.id_loja,
                                 id_pedido=nota.id_pedido,
                                 pedido_ecommerce=nota.cod_pedido,
                                 nunota_pedido=nota.nunota_pedido,
                                 nunota_nota=nota.nunota_nota,
                                 id_nota=dados_nota.get('id'),
                                 evento=evento,
                                 status=True)
                
                print(f"Nota {int(dados_nota.get('numero'))} emitida com sucesso!")
                            
            status_log = False if log_pedido.buscar_status_false(log_id=log_id) else True
            log.atualizar(id=log_id, sucesso=status_log)
            print(f"-> Processo de emissão de notas concluído! Status do log: {status_log}")
            return True
        except:
            return False
        
    async def emitir_legado(self):
        log_id = log.criar(de='olist', para='sankhya', contexto=CONTEXTO)
        obs = None
        # Busca notas pendentes
        print("Busca notas pendentes")

        notas_pendentes = venda.buscar_sem_nota()
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

        try:
            for i, nota in enumerate(notas_pendentes):
                if not first:
                    time.sleep(self.req_time_sleep)  # Evita rate limit
                first = False

                if obs:
                    # Cria um log de erro se houver observação
                    print(obs)
                    log_pedido.criar(log_id=log_id,
                                     id_loja=notas_pendentes[i-1].id_loja,
                                     id_pedido=notas_pendentes[i-1].id_pedido,
                                     pedido_ecommerce=notas_pendentes[i-1].cod_pedido,
                                     nunota_pedido=notas_pendentes[i-1].nunota_pedido,
                                     nunota_nota=notas_pendentes[i-1].nunota_nota,
                                     evento=evento,
                                     status=False,
                                     obs=obs)
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

                venda.atualizar_nf_gerada(cod_pedido=nota.cod_pedido,
                                          num_nota=int(dados_nota.get('numero')),
                                          id_nota=dados_nota.get('id'))
                
                if not await nota_snk.informar_numero_e_chavenfe(nunota=nota.nunota_nota,
                                                                chavenfe=dados_emissao.get('chaveAcesso'),
                                                                numero=int(dados_emissao.get('numero')),
                                                                id_nota=dados_nota.get('id')):
                    obs = f"Erro ao informar dados da nota {dados_nota.get('numero')} na venda {nota.nunota_nota} do Sankhya"
                    continue

                log_pedido.criar(log_id=log_id,
                                 id_loja=nota.id_loja,
                                 id_pedido=nota.id_pedido,
                                 pedido_ecommerce=nota.cod_pedido,
                                 nunota_pedido=nota.nunota_pedido,
                                 nunota_nota=nota.nunota_nota,
                                 id_nota=dados_nota.get('id'),
                                 evento=evento,
                                 status=True)
                
                print(f"Nota {int(dados_nota.get('numero'))} emitida com sucesso!")
                            
            status_log = False if log_pedido.buscar_status_false(log_id=log_id) else True
            log.atualizar(id=log_id, sucesso=status_log)
            print(f"-> Processo de emissão de notas concluído! Status do log: {status_log}")
            return True
        except:
            return False

    async def receber_notas_legado(self):
        log_id = log.criar(de='olist', para='sankhya', contexto=CONTEXTO)
        obs = None
        # Busca notas pendentes
        print("Busca notas pendentes")

        notas_pendentes = venda.buscar_sem_nota()
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
                time.sleep(2)  # Evita rate limit
            first = False

            if obs:
                # Cria um log de erro se houver observação
                print(obs)
                log_pedido.criar(log_id=log_id,
                                 id_loja=notas_pendentes[i-1].id_loja,
                                 id_pedido=notas_pendentes[i-1].id_pedido,
                                 pedido_ecommerce=notas_pendentes[i-1].cod_pedido,
                                 nunota_pedido=notas_pendentes[i-1].nunota_pedido,
                                 nunota_nota=notas_pendentes[i-1].nunota_nota,
                                 evento=evento,
                                 status=False,
                                 obs=obs)
                obs = None
                            
            print("")
            print(f"Recebendo nota {i+1}/{len(notas_pendentes)}: {nota.num_pedido}")
            
            dados_nota = await nota_olist.buscar(id_ecommerce=nota.cod_pedido)
            if not dados_nota:
                obs = f"Nota do pedido {nota.cod_pedido} não encontrada"
                continue

            venda.atualizar_nf_gerada(cod_pedido=nota.cod_pedido,
                                      num_nota=int(dados_nota.get('numero')),
                                      id_nota=dados_nota.get('id'))

            print(f"Recebendo financeiro da nota {i+1}/{len(notas_pendentes)}: {int(dados_nota.get('numero'))}")
            
            dados_financeiro = await nota_olist.buscar_financeiro(serie='2', numero=dados_nota.get('numero'))
            if not dados_financeiro:
                print(f"Financeiro da nota {int(dados_nota.get('numero'))} não encontrado no Olist")
                continue
            venda.atualizar_financeiro(num_nota=int(dados_nota.get('numero')),
                                       id_financeiro=dados_financeiro.get('id'))
            venda.atualizar_confirmada_nota(nunota_nota=nota.nunota_nota)

            print(f"Nota {int(dados_nota.get('numero'))} recebida com sucesso!")
            log_pedido.criar(log_id=log_id,
                             id_loja=nota.id_loja,
                             id_pedido=nota.id_pedido,
                             pedido_ecommerce=nota.cod_pedido,
                             nunota_pedido=nota.nunota_pedido,
                             nunota_nota=nota.nunota_nota,
                             id_nota=dados_nota.get('id'),
                             evento=evento,
                             status=True)
            
        status_log = False if log_pedido.buscar_status_false(log_id=log_id) else True
        log.atualizar(id=log_id, sucesso=status_log)
        print(f"-> Processo de recebimento de notas concluído! Status do log: {status_log}")
        return True

    async def confirmar_legado(self):
        nota_olist = NotaOlist()
        nota_snk = NotaSnk()
        obs = None
        # Busca as notas pendentes de confirmação
        print("Busca as notas pendentes de confirmação")
        notas_pendentes = venda.buscar_confirmar_nota()
        if not notas_pendentes:
            obs = "Nenhuma nota pendente"
            print(obs)
            return True  
        print(f"{len(notas_pendentes)} notas pendentes de confirmação encontradas")

        first = True
        for i, nota in enumerate(notas_pendentes):
            print("")            
            print(f"Nota #{nota.nunota_nota} - {i+1}/{len(notas_pendentes)}")
            time.sleep(float(os.getenv('REQ_TIME_SLEEP',1.5)))
            # Busca os dados da nota no Sankhya
            print("Busca os dados da nota no Sankhya")
            dados_nota_snk = await nota_snk.buscar(nunota=nota.nunota_nota)
            if not dados_nota_snk:
                obs = f"Nota {nota.nunota_nota} não encontrada no Sankhya"
                print(obs)
                continue

            # Busca os dados da nota no Olist
            print("Busca os dados da nota no Olist")
            dados_nota_olist = await nota_olist.buscar(id_ecommerce=nota.cod_pedido)
            if not dados_nota_olist:
                obs = f"Nota do pedido {nota.cod_pedido} não encontrada no Olist"
                print(obs)
                continue

            if dados_nota_olist.get('situacao') not in ['2','6','7']:
                obs = f"Situação da Nota é inválida {dados_nota_olist.get('situacao')}"
                print(obs)
                continue                

            venda.atualizar_nf_gerada(cod_pedido=nota.cod_pedido,
                                      num_nota=int(dados_nota_olist.get('numero')),
                                      id_nota=dados_nota_olist.get('id'))

            # Envia os dados da nota para o Sankhya
            print("Envia os dados da nota para o Sankhya")
            ack_envio_dados_nota = await nota_snk.informar_numero_e_chavenfe(nunota=nota.nunota_nota,
                                                                             chavenfe=dados_nota_olist.get('chaveAcesso'),
                                                                             numero=dados_nota_olist.get('numero'),
                                                                             id_nota=dados_nota_olist.get('id'))
            
            if not ack_envio_dados_nota:
                obs = f"Erro ao enviar dados da nota do pedido {nota.cod_pedido} para o Sankhya"
                print(obs)
                continue

            print(f"Dados da nota do pedido {nota.cod_pedido} enviados com sucesso para o Sankhya")

            ack_confirmacao_nota = await nota_snk.confirmar(nunota=nota.nunota_nota)
            if not ack_confirmacao_nota:
                obs = f"Erro ao confirmar nota {nota.nunota_nota} no Sankhya"
                print(obs)
                continue
            
            venda.atualizar_confirmada_nota(nunota_nota=nota.nunota_nota)
            print(f"Nota {nota.nunota_nota} confirmada com sucesso no Sankhya")

            # Baixa o financeiro da nota no Olist
            print("Baixa o financeiro da nota no Olist")
            dados_financeiro = await nota_olist.buscar_financeiro(serie=dados_nota_olist.get('serie'), numero=dados_nota_olist.get('numero'))
            if not dados_financeiro:
                obs = f"Erro ao buscar financeiro da nota {dados_nota_olist.get('numero')} no Olist"
                print(obs)
                continue
            
            ack_financeiro = await nota_olist.baixar_financeiro(id=dados_financeiro.get('id'),
                                                                valor=dados_nota_olist.get('parcelas')[0].get('valor'))
            if not ack_financeiro:
                obs = f"Erro ao baixar financeiro da nota {dados_nota_olist.get('numero')} no Olist"
                print(obs)
                continue
            
            venda.atualizar_financeiro(num_nota=dados_nota_olist.get('numero'),
                                       id_financeiro=dados_financeiro.get('id'))
            print(f"Financeiro da nota {dados_nota_olist.get('numero')} baixado com sucesso no Olist")
        
        print("Processo de confirmação de notas concluído.")
        return True

    async def confirmar(self):

        log_id = log.criar(de='olist', para='sankhya', contexto=CONTEXTO)
        obs = None
        # Busca notas pendentes
        print("Busca notas pendentes")
        notas_pendentes = venda.buscar_confirmar_nota()
        if not notas_pendentes:
            obs = "Nenhuma nota pendente"
            print(obs)
            return True
        
        print(f"{len(notas_pendentes)} notas pendentes encontradas")
        evento = 'C'
        obs = None
        first = True 
        nota_snk = NotaSnk()

        try:
            for i, nota in enumerate(notas_pendentes):
                if not first:
                    time.sleep(self.req_time_sleep)  # Evita rate limit
                first = False

                if obs:
                    # Cria um log de erro se houver observação
                    print(obs)
                    log_pedido.criar(log_id=log_id,
                                     id_loja=notas_pendentes[i-1].id_loja,
                                     id_pedido=notas_pendentes[i-1].id_pedido,
                                     pedido_ecommerce=notas_pendentes[i-1].cod_pedido,
                                     nunota_pedido=notas_pendentes[i-1].nunota_pedido,
                                     nunota_nota=notas_pendentes[i-1].nunota_nota,
                                     id_nota=notas_pendentes[i-1].id_nota,
                                     evento=evento,
                                     status=False,
                                     obs=obs)
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
                        # venda.atualizar_confirmada_nota(nunota_nota=nota.nunota_nota)

                venda.atualizar_confirmada_nota(nunota_nota=nota.nunota_nota)

                print(f"Nota {nota.num_nota}/{nota.nunota_nota} confirmada com sucesso no Sankhya")

            log_pedido.criar(log_id=log_id,
                             id_loja=nota.id_loja,
                             id_pedido=nota.id_pedido,
                             pedido_ecommerce=nota.cod_pedido,
                             nunota_pedido=nota.nunota_pedido,
                             nunota_nota=nota.nunota_nota,
                             id_nota=nota.id_nota,
                             evento=evento,
                             status=True)

            status_log = False if log_pedido.buscar_status_false(log_id=log_id) else True
            log.atualizar(id=log_id, sucesso=status_log)
            print(f"-> Processo de confirmação de notas concluído! Status do log: {status_log}")
            return True
        except:
            return False

    async def baixar_financeiro(self):

        log_id = log.criar(de='olist', para='sankhya', contexto=CONTEXTO)
        obs = None
        # Busca notas pendentes
        print("Busca notas pendentes")
        contas_pendentes = venda.buscar_financeiro_pendente()
        if not contas_pendentes:
            obs = "Nenhum financeiro pendente"
            print(obs)
            return True
        
        print(f"{len(contas_pendentes)} contas a receber pendentes encontradas")
        evento = 'F'
        obs = None
        first = True 
        nota_olist = NotaOlist()

        try:
            for i, nota in enumerate(contas_pendentes):
                if not first:
                    time.sleep(self.req_time_sleep)  # Evita rate limit
                first = False

                if obs:
                    # Cria um log de erro se houver observação
                    print(obs)
                    logger.error(obs)
                    log_pedido.criar(log_id=log_id,
                                     id_loja=contas_pendentes[i-1].id_loja,
                                     id_pedido=contas_pendentes[i-1].id_pedido,
                                     pedido_ecommerce=contas_pendentes[i-1].cod_pedido,
                                     nunota_pedido=contas_pendentes[i-1].nunota_pedido,
                                     nunota_nota=contas_pendentes[i-1].nunota_nota,
                                     id_nota=contas_pendentes[i-1].id_nota,
                                     evento=evento,
                                     status=False,
                                     obs=obs)
                    obs = None
                                
                print("")
                print(f"Baixando financeiro da nota {i+1}/{len(contas_pendentes)}: {nota.num_nota}")
                
                dados_financeiro = await nota_olist.buscar_financeiro(serie=str(self.serie_nfe),
                                                                      numero=str(nota.num_nota).zfill(6))
                if not dados_financeiro:
                    obs = f"Erro ao buscar financeiro da nota {nota.num_nota} no Olist"
                    print(obs)
                    continue
                
                if dados_financeiro.get('dataLiquidacao'):
                    print(f"Financeiro da nota {nota.num_nota} já está baixado no Olist")
                    venda.atualizar_financeiro(num_nota=nota.num_nota,
                                               id_financeiro=dados_financeiro.get('id'),
                                               dh_baixa=dados_financeiro.get('dataLiquidacao'))
                    log_pedido.criar(log_id=log_id,
                                     id_loja=nota.id_loja,
                                     id_pedido=nota.id_pedido,
                                     pedido_ecommerce=nota.cod_pedido,
                                     nunota_pedido=nota.nunota_pedido,
                                     nunota_nota=nota.nunota_nota,
                                     id_nota=nota.id_nota,
                                     evento=evento,
                                     status=True)                    
                    continue

                ack_financeiro = await nota_olist.baixar_financeiro(id=dados_financeiro.get('id'),
                                                                    valor=dados_financeiro.get('valor'))
                if not ack_financeiro:
                    obs = f"Erro ao baixar financeiro da nota {nota.num_nota} no Olist"
                    continue
                
                venda.atualizar_financeiro(num_nota=nota.num_nota,
                                           id_financeiro=dados_financeiro.get('id'))
                print(f"Financeiro da nota {nota.num_nota} baixado com sucesso no Olist")

                log_pedido.criar(log_id=log_id,
                                 id_loja=nota.id_loja,
                                 id_pedido=nota.id_pedido,
                                 pedido_ecommerce=nota.cod_pedido,
                                 nunota_pedido=nota.nunota_pedido,
                                 nunota_nota=nota.nunota_nota,
                                 id_nota=nota.id_nota,
                                 evento=evento,
                                 status=True)
            
            status_log = False if obs else True
            log.atualizar(id=log_id, sucesso=status_log)
            print(f"-> Processo de baixa de contas a receber concluído! Status do log: {status_log}")
            return True
        except Exception as e:
            logger.error(e)
            return False