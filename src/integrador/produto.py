import logging
import os
import time
from datetime import datetime
from src.parser.produto import Produto as Parser
from src.olist.produto import Produto as ProdutoOlist
from src.sankhya.produto import Produto as ProdutoSnk
from database.crud import log_produto as crudLogProd
from dotenv import load_dotenv
from database.crud import log as crudLog
from database.crud import produto as crudProduto
from src.utils.log import Log

load_dotenv('keys/.env')
logger = logging.getLogger(__name__)
logging.basicConfig(filename=Log().buscar_path(),
                    encoding='utf-8',
                    format=os.getenv('LOGGER_FORMAT'),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

CONTEXTO = 'produto'

class Produto:

    def __init__(self):
        self.produto_snk = ProdutoSnk()
        self.produto_olist = ProdutoOlist()
        self.parse = Parser()
        self.req_time_sleep = float(os.getenv('REQ_TIME_SLEEP',1.5))

    async def receber_alteracoes(self):

        log_id = crudLog.criar(de='olist', para='base', contexto=CONTEXTO)

        alteracoes_raw = await self.produto_olist.buscar_alteracoes()
        if not alteracoes_raw:
            print("Sem alterações pendentes")
            crudLog.atualizar(id=log_id)
            return True

        print(f"{len(alteracoes_raw)} produtos com alteracoes pendentes")

        try:
            obs = None
            for i, alteracao in enumerate(alteracoes_raw):
                if obs:
                    logger.error(obs)
                    print(obs)
                    crudLogProd.criar(log_id=log_id,
                                      codprod=alteracoes_raw[i-1].get('sku',0),
                                      idprod=alteracoes_raw[i-1].get('id',0),
                                      sucesso=False,
                                      obs=obs)                    
                
                if not alteracao.get('sku'):
                    # Produto tipo simples porém sem SKU no cadastro do Olist
                    obs = f"Produto sem SKU. {alteracao}"
                    continue

                hist_produto = crudProduto.buscar_snk(cod_snk=alteracao.get('sku'))
                if not hist_produto:
                    # Se o produto estiver ativo, adiciona ele na base
                    dados_produto_olist = await self.produto_olist.buscar(id=alteracao.get('id'))
                    if dados_produto_olist.get('situacao') == 'A':
                        crudProduto.criar(cod_snk=alteracao.get('sku'),
                                          cod_olist=alteracao.get('id'))
                        crudLogProd.criar(log_id=log_id,
                                          codprod=alteracao.get('sku'),
                                          idprod=alteracao.get('id'),
                                          sucesso=True)
                    else:
                        obs = f"Produto {alteracao.get('sku',0)}/{alteracao.get('id',0)} não pode ser alterado pois não foi encontrado"               
                    continue

                ultima_alteracao = datetime.strptime(alteracao.get('dh_alter'),'%Y-%m-%d %H:%M:%S')

                if hist_produto.dh_cadastro >= ultima_alteracao:
                    continue

                if hist_produto.dh_atualizado and hist_produto.dh_atualizado >= ultima_alteracao:
                    continue

                if hist_produto.pendencia:
                    continue

                crudProduto.atualizar(cod_snk=alteracao.get('sku'), pendencia=True)
                crudLogProd.criar(log_id=log_id,
                                  codprod=alteracao.get('sku'),
                                  idprod=alteracao.get('id'))                  
                print(f"Produto {alteracao.get('sku')} adicionado à lista de alterações pendentes")
            
            status_log = False if crudLogProd.buscar_status_false(log_id) else True
            crudLog.atualizar(id=log_id,sucesso=status_log)
            return True
        except Exception as e:
            erro = f"Erro: {e}"
            logger.error(erro)
            print(erro)
            crudLog.atualizar(id=log_id,sucesso=False)
            return False

    async def atualizar_olist(self):

        log_id = crudLog.criar(de='sankhya', para='olist', contexto=CONTEXTO)
        obs = None
        # Busca lista de produtos com alterações no Sankhya
        print("Busca lista de produtos com alterações no Sankhya")        
        alteracoes_pendentes = await self.produto_snk.buscar_alteracoes()
        if not alteracoes_pendentes:
            obs = "Sem alterações pendentes"
            print(obs)
            crudLog.atualizar(id=log_id, sucesso=True)
            return True

        print(f"{len(alteracoes_pendentes)} produtos com alteracoes pendentes")

        try:
            for i, produto in enumerate(alteracoes_pendentes):
                if obs:
                    # Registro no log
                    crudLogProd.criar(log_id=log_id,
                                      codprod=alteracoes_pendentes[i-1].get('codprod'),
                                      idprod=alteracoes_pendentes[i-1].get('sku'),
                                      sucesso=alteracoes_pendentes[i-1].get('sucesso'),
                                      obs=obs)
                    obs = None

                print("")            
                print(f"Produto #{produto.get('codprod')} - {i+1}/{len(alteracoes_pendentes)}")
                time.sleep(float(os.getenv('REQ_TIME_SLEEP',1.5)))

                if produto.get('evento') == 'I':
                    
                    # Busca os dados do produto no Olist e no Sankhya para comparação
                    valida_produto_olist = await self.produto_olist.buscar(sku=int(produto.get('codprod')))
                    if valida_produto_olist and valida_produto_olist.get('situacao',None) == 'A':
                        obs = f'Produto {produto.get('codprod')} já existe no Olist'
                        produto['sucesso'] = False
                        continue # TODO >>

                    print(f"Inclusão do produto {produto.get('codprod')}")
                    print("Buscando dados do produto no Sankhya...")
                    dados_produto_sankhya = await self.produto_snk.buscar(codprod=int(produto.get('codprod'))) 
                    if not dados_produto_sankhya:
                        obs = 'Produto não encontrado no Sankhya'
                        produto['sucesso'] = False
                        continue

                    # Converte para o formato da API do Olist
                    print("Convertendo para o formato da API do Olist...")
                    log_atualizacoes_olist, dados_formato_olist = self.parse.to_olist(data_sankhya=dados_produto_sankhya)
                    if not log_atualizacoes_olist:
                        obs = "Erro ao converter dados do produto"
                        produto['sucesso'] = False
                        continue
                    
                    # Envia dados para o Olist
                    print("Enviando dados para o Olist...")
                    ack_olist, dados_produto_olist = await self.produto_olist.incluir(data=dados_formato_olist)
                    if not ack_olist:
                        obs = 'Erro ao incluir produto no Olist'
                        produto['sucesso'] = False
                        continue

                    # Converte o retorno da API do Olist para o formato da API do Sankhya
                    print("Convertendo o retorno do Olist para o formato da API do Sankhya...")
                    log_atualizacoes_snk, dados_formato_snk = self.parse.to_sankhya(data_olist=dados_produto_olist,
                                                                                    data_sankhya=dados_produto_sankhya,
                                                                                    type='insert')
                    if not log_atualizacoes_snk:
                        obs = "Erro ao converter resposta da API Olist."
                        produto['sucesso'] = False
                        continue

                    dados_formato_snk = self.produto_snk.prepapar_dados(payload=dados_formato_snk)

                    # Atualiza ID no Sankhya
                    print("Atualizando ID no Sankhya...")
                    ack_snk = await self.produto_snk.atualizar(codprod=dados_produto_sankhya.get('codprod'),
                                                            payload=dados_formato_snk)
                    if not ack_snk:
                        obs = 'Erro ao inserir o ID do produto no sankhya'
                        produto['sucesso'] = False
                        continue

                    produto['sucesso'] = True

                    print("Produto sincronizado com sucesso!")

                    # Registro no log
                    if log_atualizacoes_olist[0] == -1:
                        crudLogProd.criar(log_id=log_id,
                                          codprod=int(dados_produto_sankhya.get('codprod')),
                                          idprod=int(dados_produto_olist.get('id')),
                                          campo='all')
                    else:                                        
                        for atualizacao in log_atualizacoes_olist:
                            crudLogProd.criar(log_id=log_id,
                                              codprod=int(dados_produto_olist.get('sku')),
                                              idprod=int(dados_produto_olist.get('id')),
                                              campo=atualizacao.get('campo'),
                                              valor_old=str(atualizacao.get('valorOld')),
                                              valor_new=str(atualizacao.get('valorNew')))

                if produto.get('evento') == 'A':

                    if not produto.get('idprod'):
                        obs = 'Produto não pode ser atualizado se não tiver vínculo com o Olist pelo ID'
                        produto['sucesso'] = False
                        continue

                    print(f"Atualização do produto {produto.get('codprod')}")
                    print("Buscando dados do produto no Sankhya...")
                    # Busca dados do produto no Sankhya
                    dados_produto_sankhya = await self.produto_snk.buscar(codprod=int(produto.get('codprod'))) 
                    if not dados_produto_sankhya:
                        obs = 'Produto não encontrado no Sankhya'
                        produto['sucesso'] = False
                        continue

                    print("Buscando dados do produto no Olist...")
                    # Busca dados do produto no Olist
                    dados_produto_olist = await self.produto_olist.buscar(id=int(produto.get('idprod'))) 
                    if not dados_produto_olist:
                        obs = 'Produto não encontrado no Olist'
                        produto['sucesso'] = False
                        continue

                    print("Convertendo para o formato da API do Olist...")
                    # Converte para o formato da API do Olist
                    log_atualizacoes_olist, dados_formato_olist = self.parse.to_olist(data_olist=dados_produto_olist,
                                                                                      data_sankhya=dados_produto_sankhya)
                    if not log_atualizacoes_olist:
                        obs = "Erro ao comparar dados do produto"
                        produto['sucesso'] = False
                        continue
                    if log_atualizacoes_olist[0] == 0:
                        obs = "Sem dados para atualizar no produto"
                        produto['sucesso'] = True
                        continue

                    print("Atualizando dados no Olist...")
                    # Atualiza dados no Olist
                    ack_atualizacao = await self.produto_olist.atualizar(id=int(produto.get('idprod')),
                                                                         data=dados_formato_olist)

                    if not ack_atualizacao:
                        obs = 'Erro ao atualizar produto no Olist'
                        produto['sucesso'] = False
                        continue

                    produto['sucesso'] = True
                    print("Produto atualizado com sucesso!")

                    crudProduto.atualizar(cod_snk=dados_produto_sankhya.get('codprod'),
                                          pendencia=False)
                
                    # Registro no log                                      
                    for atualizacao in log_atualizacoes_olist:
                        crudLogProd.criar(log_id=log_id,
                                          codprod=int(dados_produto_olist.get('sku')),
                                          idprod=int(dados_produto_olist.get('id')),
                                          campo=atualizacao.get('campo'),
                                          valor_old=str(atualizacao.get('valorOld')),
                                          valor_new=str(atualizacao.get('valorNew')))
            
            print("Removendo da lista de alterações pendentes...")
            await self.produto_snk.excluir_alteracoes(lista_produtos=alteracoes_pendentes)            
            status_log = False if crudLogProd.buscar_status_false(log_id) else True
            crudLog.atualizar(id=log_id,sucesso=status_log)

            print("=========================")
            print("PROCESSO DE ATUALIZAÇÃO DE PRODUTOS NO OLIST CONCLUÍDO!")

            return True
        except:
            return False

    async def atualizar_snk(self):
        log_id = crudLog.criar(de='olist', para='sankhya', contexto=CONTEXTO)
        obs = None
        # Busca lista de produtos com alterações no Olist
        print("Busca lista de produtos com alterações no Olist")
        alteracoes_pendentes = crudProduto.buscar_pendencias()
        if not alteracoes_pendentes:
            obs = "Sem alterações pendentes"
            print(obs)
            crudLog.atualizar(id=log_id)
            return True

        print(f"{len(alteracoes_pendentes)} produtos com alteracoes pendentes")    
        
        try:
            for i, produto in enumerate(alteracoes_pendentes):
                if obs:
                    print(obs)
                    # Registro no log
                    crudLogProd.criar(log_id=log_id,
                                      codprod=alteracoes_pendentes[i-1].cod_snk,
                                      idprod=alteracoes_pendentes[i-1].cod_olist,
                                      sucesso=alteracoes_pendentes[i-1].sucesso,
                                      obs=obs)
                    obs = None

                print("")            
                print(f"Produto #{produto.cod_snk} - {i+1}/{len(alteracoes_pendentes)}")
                time.sleep(float(os.getenv('REQ_TIME_SLEEP',1.5)))

                # Busca os dados do produto no Olist e no Sankhya para comparação
                print(f"Buscando dados do produto no Olist...")
                dados_produto_olist = await self.produto_olist.buscar(id=produto.cod_olist)
                if not dados_produto_olist:
                    obs = 'Produto não encontrado no Olist'
                    setattr(produto,"sucesso",False)
                    continue

                print(f"Buscando dados do produto no Sankhya...")
                dados_produto_sankhya = await self.produto_snk.buscar(codprod=produto.cod_snk)
                if not dados_produto_sankhya:
                    obs = 'Produto não encontrado no Sankhya'
                    setattr(produto,"sucesso",False)
                    continue
                
                match dados_produto_olist.get('situacao'):
                    case 'A':
                        tipo_atualizacao = 'update'
                    case 'E':
                        tipo_atualizacao = 'delete'
                    case _:
                        obs = f"Produto {produto.cod_snk} com a situação {dados_produto_olist.get('situacao')} no Olist"
                        setattr(produto,"sucesso",True)
                        # logger.warning(obs)
                        crudProduto.atualizar(cod_snk=produto.cod_snk,pendencia=False)
                        continue
                
                print("Convertendo para o formato API Sankhya...")
                log_atualizacoes, dados_atualizados = self.parse.to_sankhya(data_olist=dados_produto_olist,
                                                                            data_sankhya=dados_produto_sankhya,
                                                                            type=tipo_atualizacao)

                if not log_atualizacoes:
                    obs = "Erro ao comparar dados do produto"
                    setattr(produto,"sucesso",False)
                    continue

                if log_atualizacoes[0] == 0:
                    obs = "Sem dados para atualizar no produto"
                    setattr(produto,"sucesso",True)
                    crudProduto.atualizar(cod_snk=produto.cod_snk,pendencia=False)
                    continue
            
                dados_formato_snk = self.produto_snk.prepapar_dados(payload=dados_atualizados)

                # Atualiza dados no Sankhya
                print("Atualizando dados no Sankhya...")
                ack_snk = await self.produto_snk.atualizar(codprod=produto.cod_snk,
                                                           payload=dados_formato_snk)
                if not ack_snk:
                    obs = 'Erro ao atualizar produto no sankhya'
                    setattr(produto,"sucesso",False)
                    continue

                setattr(produto,"sucesso",True)

                crudProduto.atualizar(cod_snk=produto.cod_snk,pendencia=False)
                
                print(f"Produto atualizado com sucesso!")

                # Registro no log                                      
                for atualizacao in log_atualizacoes:
                    crudLogProd.criar(log_id=log_id,
                                      codprod=produto.cod_snk,
                                      idprod=produto.cod_olist,
                                      campo=atualizacao.get('campo'),
                                      valor_old=str(atualizacao.get('valorOld')),
                                      valor_new=str(atualizacao.get('valorNew')))            

            print("Removendo da lista de alterações pendentes...")
            await self.produto_snk.excluir_alteracoes(lista_produtos=alteracoes_pendentes)

            crudLog.atualizar(id=log_id)
            print("=========================")
            print("PROCESSO DE ATUALIZAÇÃO DE PRODUTOS NO SANKHYA CONCLUÍDO!")
            return True
        except Exception as e:
            logger.error("Erro ao atualizar produto no sankhya: %s",e)
            print(f"Erro: {e}")
            return False