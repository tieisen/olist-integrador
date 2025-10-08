import os
import time
from datetime import datetime
from src.parser.produto import Produto as Parser
from src.olist.produto import Produto as ProdutoOlist
from src.sankhya.produto import Produto as ProdutoSnk
from database.crud import log_produto as crudLogProd
from database.crud import log as crudLog
from database.crud import produto as crudProduto
from src.utils.decorador import contexto, carrega_dados_empresa, log_execucao, interno
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

class Produto:

    def __init__(self, codemp:int=None, empresa_id:int=None):
        self.codemp = codemp
        self.empresa_id = empresa_id
        self.contexto = 'produto'
        self.dados_empresa: dict = None
        self.snk = ProdutoSnk(codemp)
        self.olist = ProdutoOlist(codemp)
        self.parse = Parser()
        self.req_time_sleep = float(os.getenv('REQ_TIME_SLEEP',1.5))

    @contexto
    @log_execucao
    @carrega_dados_empresa
    async def receber_alteracoes(self,**kwargs):
        log_id = await crudLog.criar(empresa_id=self.dados_empresa.get('id'),
                                     de='olist',
                                     para='base',
                                     contexto=kwargs.get('_contexto'))        
        # Busca lista de produtos com alterações pendentes
        print("-> Buscando lista de produtos com alterações pendentes...")
        alteracoes_pendentes = await self.olist.buscar_alteracoes()
        if not alteracoes_pendentes:
            print("Sem alterações pendentes")
            await crudLog.atualizar(id=log_id,
                                    sucesso=True)
            return True
        print(f"{len(alteracoes_pendentes)} produtos com alteracoes pendentes")
        for i, alteracao in enumerate(alteracoes_pendentes):
            print(f"-> Produto #{alteracao.get('sku')}: {i+1}/{len(alteracoes_pendentes)}")
            time.sleep(self.req_time_sleep)
            # Item de imposto
            if int(alteracao.get('sku')) == 1:
                continue
            # Produto tipo simples porém sem SKU no cadastro do Olist
            if not int(alteracao.get('sku')):                
                obs = f"Produto ID {int(alteracao.get('id'))} sem SKU"
                logger.warning(obs)
                print(obs)
                await crudLogProd.criar(log_id=log_id,
                                        idprod=int(alteracao.get('id')),
                                        sucesso=False,
                                        obs=obs)                      
                continue
            # Valida se o produto tem registro na base
            produto_cadastrado = await crudProduto.buscar(empresa_id=int(self.dados_empresa.get('id')),
                                                          codprod=int(alteracao.get('sku')))
            if not produto_cadastrado:                
                dados_produto_olist = await self.olist.buscar(id=int(alteracao.get('id')))
                if not dados_produto_olist:
                    obs = f"Produto com ID {int(alteracao.get('id'))} não encontrado no Olist"
                    logger.warning(obs)
                    print(obs)
                    await crudLogProd.criar(log_id=log_id,
                                            idprod=int(alteracao.get('id')),
                                            sucesso=False,
                                            obs=obs)
                    continue                
                # Se o produto estiver ativo mas não cadastrado, adiciona ele na base
                if dados_produto_olist.get('situacao') in ['A','I']:                    
                    ack = await crudProduto.criar(codprod=int(alteracao.get('sku')),
                                                  idprod=int(alteracao.get('id')),
                                                  empresa_id=self.dados_empresa.get('id'))
                    if not ack:
                        obs = f"Falha ao cadastrar produtos {int(alteracao.get('sku'))}/{int(alteracao.get('id'))} na base"
                        logger.error(obs)
                        print(obs)
                        await crudLogProd.criar(log_id=log_id,
                                                codprod=int(alteracao.get('sku')),
                                                idprod=int(alteracao.get('id')),
                                                sucesso=False,
                                                obs=obs)
                    else:    
                        await crudLogProd.criar(log_id=log_id,
                                                codprod=int(alteracao.get('sku')),
                                                idprod=int(alteracao.get('id')),
                                                sucesso=True)
                    continue
                else:
                    obs = f"Produto {alteracao.get('sku',0)}/{alteracao.get('id',0)} não foi cadastrado. Status {dados_produto_olist.get('situacao')}"
                    logger.warning(obs)
                    print(obs)
                    await crudLogProd.criar(log_id=log_id,
                                            codprod=int(alteracao.get('sku')),
                                            idprod=int(alteracao.get('id')),
                                            sucesso=False,
                                            obs=obs)
                    continue                    
            ultima_alteracao = datetime.strptime(alteracao.get('dh_alter'),'%Y-%m-%d %H:%M:%S')
            if produto_cadastrado.get('dh_criacao') >= ultima_alteracao:
                # Produto foi cadastrado já com todas as alterações
                print(f"Produto {alteracao.get('sku',0)}/{alteracao.get('id',0)} foi cadastrado já com todas as alterações")
                continue
            if produto_cadastrado.get('dh_atualizacao') and (produto_cadastrado.get('dh_atualizacao') >= ultima_alteracao):
                # Produto já tem todas as alterações
                print(f"Produto {alteracao.get('sku',0)}/{alteracao.get('id',0)} já tem todas as alterações")
                continue
            if produto_cadastrado.get('pendencia'):
                # Produto já foi marcado pra atualizar mas ainda não foi importado pro Sankhya
                print(f"Produto {alteracao.get('sku',0)}/{alteracao.get('id',0)} já está na fila de atualização")
                continue
            # Marca o produto como pendente de atualização
            ack = await crudProduto.atualizar(codprod=int(alteracao.get('sku')),
                                              pendencia=True,
                                              empresa_id=self.dados_empresa.get('id'))
            if not ack:
                obs = f"Falha ao adicionar produto {int(alteracao.get('sku'))}/{int(alteracao.get('id'))} na fila de atualização"
                logger.error(obs)
                print(obs)
                await crudLogProd.criar(log_id=log_id,
                                        codprod=int(alteracao.get('sku')),
                                        idprod=int(alteracao.get('id')),
                                        sucesso=False,
                                        obs=obs)
            await crudLogProd.criar(log_id=log_id,
                                    codprod=int(alteracao.get('sku')),
                                    idprod=int(alteracao.get('id')))  
            print(f"Produto {alteracao.get('sku',0)}/{alteracao.get('id',0)} adicionado à fila de atualização")
        status_log = False if await crudLogProd.buscar_falhas(log_id) else True
        await crudLog.atualizar(id=log_id,sucesso=status_log)
        print("--> RECEBIMENTO DE ALTERAÇÕES NOS PRODUTOS DO OLIST CONCLUÍDA!")         
        return True
    
    @carrega_dados_empresa
    #@interno
    async def incluir_olist(self, produto:dict):  
        print("Inclusão:")
        # Valida existencia do produto no Olist
        print(f"Validando existência do produto {produto.get('codprod')} no Olist...")
        produto_cadastrado = await self.olist.buscar(sku=int(produto.get('codprod')))
        if produto_cadastrado and produto_cadastrado.get('situacao') in ['A','I']:
            produto['obs'] = f'Produto {produto.get('codprod')} já existe no Olist com ID {produto_cadastrado.get("id")}'
            produto['sucesso'] = True
            produto['idprod'] = produto_cadastrado.get('id')
            logger.warning(produto['obs'])
            print(produto['obs'])            
            return False
        # Busca dados do produto no Sankhya
        print("Buscando dados do produto no Sankhya...")
        dados_produto_sankhya = await self.snk.buscar(codprod=int(produto.get('codprod'))) 
        if not dados_produto_sankhya:
            produto['obs'] = f'Produto {produto.get('codprod')} não encontrado no Sankhya'
            produto['sucesso'] = False
            logger.warning(produto['obs'])
            print(produto['obs'])            
            return False
        dados_produto_sankhya = dados_produto_sankhya[0]

        # Converte para o formato da API do Olist
        print("Convertendo para o formato da API do Olist...")
        log_atualizacoes_olist, dados_formato_olist = self.parse.to_olist(data_sankhya=dados_produto_sankhya,
                                                                          dados_empresa=self.dados_empresa)
        if not log_atualizacoes_olist:
            produto['obs'] = f'Erro ao converter dados do produto {produto.get('codprod')}'
            produto['sucesso'] = False
            logger.error(produto['obs'])
            print(produto['obs'])            
            return False        
        # Envia dados para o Olist
        print("Enviando dados para o Olist...")
        ack_olist, dados_produto_olist = await self.olist.incluir(data=dados_formato_olist)
        if not ack_olist:
            produto['obs'] = f'''Erro ao incluir produto {produto.get('codprod')} no Olist.
                                 Payload:{dados_formato_olist}'''
            produto['sucesso'] = False
            logger.error(produto['obs'])
            print(produto['obs'])            
            return False
        # Salva o ID do produto na base
        print("Atualizando produto na base...")
        produto['idprod'] = dados_produto_olist.get('id')
        await crudProduto.criar(empresa_id=self.dados_empresa.get('id'),
                                codprod=int(produto.get('codprod')),                                
                                idprod=int(produto.get('idprod')))        
        # Converte o retorno da API do Olist para o formato da API do Sankhya
        print("Convertendo o retorno do Olist para o formato da API do Sankhya...")
        log_atualizacoes_snk, dados_formato_snk = self.parse.to_sankhya(data_olist=dados_produto_olist,
                                                                        data_sankhya=dados_produto_sankhya,
                                                                        type='insert')
        if not log_atualizacoes_snk:
            produto['obs'] = f"Erro ao converter resposta da API Olist do produto {produto.get('codprod')}/{produto.get('idprod')}"
            produto['sucesso'] = False
            logger.error(produto['obs'])
            print(produto['obs'])
            return False
        dados_formato_snk = self.snk.preparar_dados(payload=dados_formato_snk)
        
        # Atualiza ID no Sankhya
        print("Atualizando ID no Sankhya...")
        ack_snk = await self.snk.atualizar(codprod=int(produto.get('codprod')),
                                           seq=dados_produto_sankhya.get('seqemp'),
                                           payload=dados_formato_snk)
        if not ack_snk:
            produto['obs'] = f"Erro ao inserir o ID {produto.get('idprod')} no produto {produto.get('codprod')} no sankhya"
            produto['sucesso'] = False
            logger.error(produto['obs'])
            print(produto['obs'])
            return False
        produto['sucesso'] = True
        print("Produto sincronizado com sucesso!")
        return True
    
    @interno
    async def atualizar_olist(self, produto:dict):
        print("Atualização:")
        if not produto.get('idprod'):
            produto['obs'] = f'Produto {produto.get("codprod")} não pode ser atualizado se não tiver vínculo com o Olist pelo ID'
            produto['sucesso'] = False
            logger.warning(produto['obs'])
            print(produto['obs'])            
            return False
        # Busca dados do produto no Sankhya
        print("Buscando dados do produto no Sankhya...")        
        dados_produto_sankhya = await self.snk.buscar(codprod=int(produto.get('codprod'))) 
        if not dados_produto_sankhya:
            produto['obs'] = f'Produto {produto.get('codprod')}/{produto.get('idprod')} não encontrado no Sankhya'
            produto['sucesso'] = False
            logger.warning(produto['obs'])
            print(produto['obs'])            
            return False
        dados_produto_sankhya = dados_produto_sankhya[0]
        # Busca dados do produto no Olist
        print("Buscando dados do produto no Olist...")        
        dados_produto_olist = await self.olist.buscar(id=int(produto.get('idprod'))) 
        if not dados_produto_olist:
            produto['obs'] = f'Produto {produto.get('codprod')}/{produto.get('idprod')} não encontrado no Olist'
            produto['sucesso'] = False
            logger.warning(produto['obs'])
            print(produto['obs'])            
            return False
        # Converte para o formato da API do Olist
        print("Convertendo para o formato da API do Olist...")        
        log_atualizacoes_olist, dados_formato_olist = self.parse.to_olist(data_olist=dados_produto_olist,
                                                                          data_sankhya=dados_produto_sankhya)
        if not log_atualizacoes_olist:
            produto['obs'] = f'Erro ao converter dados do produto {produto.get('codprod')}/{produto.get('idprod')}'
            produto['sucesso'] = False
            logger.error(produto['obs'])
            print(produto['obs'])            
            return False
        if log_atualizacoes_olist == 0:
            produto['obs'] = f'Sem dados divergentes para atualizar no produto {produto.get('codprod')}/{produto.get('idprod')}'
            produto['sucesso'] = True
            return True
        # Envia dados para o Olist
        print("Enviando dados para o Olist...")        
        ack_atualizacao = await self.olist.atualizar(id=int(produto.get('idprod')),
                                                     data=dados_formato_olist)
        if not ack_atualizacao:
            produto['obs'] = f'''Erro ao atualizar produto {produto.get('codprod')}/{produto.get('idprod')} no Olist.
                                 Payload:{dados_formato_olist}'''
            produto['sucesso'] = False
            logger.error(produto['obs'])
            print(produto['obs'])            
            return False
        produto['sucesso'] = True
        print("Produto atualizado com sucesso!")
        return log_atualizacoes_olist
    
    @carrega_dados_empresa
    @interno
    async def atualizar_sankhya(self, produto:dict):
        print("Atualização:")
        # Busca os dados do produto no Olist
        print("Buscando dados do produto no Olist...")
        dados_produto_olist = await self.olist.buscar(id=produto.get('idprod'))
        if not dados_produto_olist:
            produto['obs'] = f'Produto {produto.get('codprod')}/{produto.get('idprod')} não encontrado no Olist'
            produto['sucesso'] = False
            logger.warning(produto['obs'])
            print(produto['obs'])            
            return False
        # Busca os dados do produto no Sankhya
        print(f"Buscando dados do produto no Sankhya...")
        dados_produto_sankhya = await self.snk.buscar(codprod=int(produto.get('codprod')))
        if not dados_produto_sankhya:
            produto['obs'] = f'Produto {produto.get('codprod')}/{produto.get('idprod')} não encontrado no Sankhya'
            produto['sucesso'] = False
            logger.warning(produto['obs'])
            print(produto['obs'])            
            return False
        dados_produto_sankhya = dados_produto_sankhya[0]
        # Valida o status do produto no Olist
        print("Validando o status do produto no Olist...")
        match dados_produto_olist.get('situacao'):
            case 'A' | 'I':
                tipo_atualizacao = 'update'
            case 'E':
                tipo_atualizacao = 'delete'
            case _:
                produto['obs'] = f"Produto {produto.get('codprod')} com a situação {dados_produto_olist.get('situacao')} no Olist"
                produto['sucesso'] = False
                logger.warning(produto['obs'])
                await crudProduto.atualizar(empresa_id=self.dados_empresa.get('id'),
                                            codprod=int(produto.get('codprod')),
                                            pendencia=False)        
        # Comparando dados dos sistemas
        print("Comparando dados dos sistemas...")
        log_atualizacoes, dados_atualizados = self.parse.to_sankhya(data_olist=dados_produto_olist,
                                                                    data_sankhya=dados_produto_sankhya,
                                                                    type=tipo_atualizacao)
        if not log_atualizacoes:
            produto['obs'] = f'Erro ao comparar dados do produto {produto.get('codprod')}/{produto.get('idprod')}'
            produto['sucesso'] = False
            logger.warning(produto['obs'])
            print(produto['obs'])
            return False
        if log_atualizacoes == 0 or not dados_atualizados:
            produto['obs'] = f'Sem dados divergentes para atualizar no produto {produto.get('codprod')}/{produto.get('idprod')}'
            produto['sucesso'] = True
            await crudProduto.atualizar(empresa_id=self.dados_empresa.get('id'),
                                        codprod=int(produto.get('codprod')),
                                        pendencia=False)
            return True

        # Converte para o formato da API do Sankhya
        print("Convertendo para o formato da API do Sankhya...")      
        dados_formato_snk = self.snk.preparar_dados(payload=dados_atualizados)
        # Envia dados para o Sankhya
        print("Enviando dados para o Sankhya...") 
        ack_atualizacao = await self.snk.atualizar(codprod=int(produto.get('codprod')),
                                                   codemp=self.codemp,
                                                   seq=dados_produto_sankhya.get('seqemp'),
                                                   payload=dados_formato_snk)
        if not ack_atualizacao:
            produto['obs'] = f'''Erro ao atualizar produto {produto.get('codprod')}/{produto.get('idprod')} no Sankhya.
                                 Payload:{dados_formato_snk}'''
            produto['sucesso'] = False
            logger.error(produto['obs'])
            print(produto['obs'])            
            return False
        produto['sucesso'] = True
        await crudProduto.atualizar(empresa_id=self.dados_empresa.get('id'),
                                    codprod=int(produto.get('codprod')),
                                    pendencia=False)
        print("Produto atualizado com sucesso!")
        return log_atualizacoes

    @contexto
    @log_execucao
    @carrega_dados_empresa
    async def integrar_olist(self, **kwargs):
        log_id = await crudLog.criar(empresa_id=self.dados_empresa.get('id'),
                                     de='sankhya',
                                     para='olist',
                                     contexto=kwargs.get('_contexto'))
        # Busca lista de produtos com alterações no Sankhya
        print("-> Buscando lista de produtos com alterações no Sankhya...")
        alteracoes_pendentes = await self.snk.buscar_alteracoes()
        if not alteracoes_pendentes:
            print("Sem alterações pendentes")
            await crudLog.atualizar(id=log_id,
                                    sucesso=True)
            return True
        print(f"{len(alteracoes_pendentes)} produtos com alteracoes pendentes")
        for i, produto in enumerate(alteracoes_pendentes):
            print(f"\n-> Produto #{produto.get('codprod')}: {i+1}/{len(alteracoes_pendentes)}")
            time.sleep(self.req_time_sleep)
            if produto.get('evento') == 'I':
                ack = await self.incluir_olist(produto=produto)
                if ack:
                    # Registro no log
                    await crudLogProd.criar(log_id=log_id,
                                            codprod=int(produto.get('codprod',0)),
                                            idprod=int(produto.get('idprod',0)),
                                            sucesso=produto.get('sucesso'),
                                            campo='all')
                else:
                    # Se falhou porque o produto já estava cadastrado no Olist
                    if produto.get('sucesso'):
                        pass
                    else:
                        # Registro no log
                        await crudLogProd.criar(log_id=log_id,
                                                codprod=int(produto.get('codprod',0)),
                                                idprod=int(produto.get('idprod',0)),
                                                sucesso=produto.get('sucesso'),
                                                obs=produto.get('obs'))
            elif produto.get('evento') == 'A':
                log_atualizacoes = await self.atualizar_olist(produto=produto)
                if isinstance(log_atualizacoes,list):
                    # Registro no log
                    for atualizacao in log_atualizacoes:
                        await crudLogProd.criar(log_id=log_id,
                                                codprod=int(produto.get('codprod',0)),
                                                idprod=int(produto.get('idprod',0)),
                                                sucesso=produto.get('sucesso'),
                                                campo=atualizacao.get('campo'),
                                                valor_old=str(atualizacao.get('valorOld')),
                                                valor_new=str(atualizacao.get('valorNew')))
                else:
                    # Registro no log
                    await crudLogProd.criar(log_id=log_id,
                                            codprod=int(produto.get('codprod',0)),
                                            idprod=int(produto.get('idprod',0)),
                                            sucesso=produto.get('sucesso'),
                                            obs=produto.get('obs'))        
        print("-> Removendo da lista de alterações pendentes...")
        await self.snk.excluir_alteracoes(lista_produtos=alteracoes_pendentes)            
        status_log = False if await crudLogProd.buscar_falhas(log_id) else True
        await crudLog.atualizar(id=log_id,sucesso=status_log)
        return True

    @contexto
    @log_execucao
    @carrega_dados_empresa
    async def integrar_snk(self, **kwargs):
        log_id = await crudLog.criar(empresa_id=self.dados_empresa.get('id'),
                                     de='olist',
                                     para='sankhya',
                                     contexto=kwargs.get('_contexto'))        
        # Busca fila de alterações
        print("-> Buscando produtos na fila de alteração...")        
        alteracoes_pendentes = await crudProduto.buscar_pendencias(empresa_id=self.dados_empresa.get('id'))        
        if not alteracoes_pendentes:
            print("Sem alterações pendentes")
            await crudLog.atualizar(id=log_id,
                                    sucesso=True)
            return True        
        print(f"{len(alteracoes_pendentes)} produtos com alteracoes pendentes")
        for i, produto in enumerate(alteracoes_pendentes):
            print(f"-> Produto #{produto.get('codprod')}: {i+1}/{len(alteracoes_pendentes)}")
            time.sleep(self.req_time_sleep)
            log_atualizacoes = await self.atualizar_sankhya(produto=produto)
            if isinstance(log_atualizacoes,list):
                # Registro no log
                for atualizacao in log_atualizacoes:
                    await crudLogProd.criar(log_id=log_id,
                                            codprod=int(produto.get('codprod',0)),
                                            idprod=int(produto.get('idprod',0)),
                                            sucesso=produto.get('sucesso'),
                                            campo=atualizacao.get('campo'),
                                            valor_old=str(atualizacao.get('valorOld')),
                                            valor_new=str(atualizacao.get('valorNew')))
            else:
                # Registro no log
                await crudLogProd.criar(log_id=log_id,
                                        codprod=int(produto.get('codprod',0)),
                                        idprod=int(produto.get('idprod',0)),
                                        sucesso=produto.get('sucesso'),
                                        obs=produto.get('obs'))                
        print("-> Removendo da lista de alterações pendentes...")
        await self.snk.excluir_alteracoes(lista_produtos=alteracoes_pendentes)
        status_log = False if await crudLogProd.buscar_falhas(log_id) else True
        await crudLog.atualizar(id=log_id,sucesso=status_log)
        return True