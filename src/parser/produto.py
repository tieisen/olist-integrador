import os
import re
import json
from src.utils.formatter import Formatter
from src.utils.log import set_logger
from src.utils.load_env import load_env
from src.utils.validador import Validador
load_env()
logger = set_logger(__name__)

class Produto:

    def __init__(self):
        self.formatter = Formatter()
        self.validar = Validador()

    def to_sankhya(self,data_olist:dict=None,data_sankhya:dict=None,type:str='update') -> tuple[list,dict]:
        """
        Valida alterações nos dados do produto e cria dicionário no formato da API do Sankhya.
            :param data_olist: dados do produto da API do Olist
            :param data_sankhya: dados do produto da API do Sankhya
            :param type: tipo de operação (update, insert ou delete)
            :return list: lista de alterações a serem aplicadas no produto            
            :return dict: dicionário com os dados do produto
        """

        if not type or type not in ['update', 'insert', 'delete']:
            logger.error(f"Tipo de operação inválido: {type}")
            print(f"Tipo de operação inválido. Deve ser 'update' ou 'insert' ou 'delete'")
            return [], {}

        new_data = {}
        updates = []

        if type == 'update':

            if not all([data_olist,data_sankhya]):
                logger.error("Dados insuficientes para atualização.")
                return [], {}

            if str(data_sankhya['id']).strip() != str(data_olist.get('id')).strip():
                updates.append({'campo':'id',
                                'valorOld':data_sankhya.get('id'),
                                'valorNew':data_olist.get('id')})
                new_data['id'] = str(data_olist.get('id'))

            if data_olist.get('produtoPai'):
                if str(data_sankhya['idprodpai']).strip() != str(data_olist['produtoPai'].get('id')).strip():
                    updates.append({'campo':'idprodpai',
                                    'valorOld':data_sankhya.get('idprodpai'),
                                    'valorNew':data_olist['produtoPai'].get('id')})                    
                    new_data['idprodpai'] = str(data_olist['produtoPai'].get('id'))
            else:
                if data_sankhya['idprodpai']:
                    updates.append({'campo':'idprodpai',
                                    'valorOld':data_sankhya.get('idprodpai'),
                                    'valorNew':None})
                    new_data['idprodpai'] = ''

            if not updates:
                return [0], {}
        
        if type == 'insert':
            if not data_olist:
                logger.error("Dados insuficientes para inserção.")
                print("Dados insuficientes para inserção.")
                return [], {}
            
            updates.append({'campo':'id',
                            'valorOld':None,
                            'valorNew':data_olist.get('id')})
            new_data['id'] = str(data_olist.get('id'))

        if type == 'delete':
            updates.append({'campo':'ativo',
                            'valorOld':'S',
                            'valorNew':'N'})
            new_data['ativo'] = 'N'

        return updates, new_data

    def to_olist(self,data_sankhya:dict,data_olist:dict=None,dados_empresa:dict=None) -> tuple[list,dict]:
        """
        Valida alterações nos dados do produto e cria dicionário no formato da API do Olist.
            :param data_sankhya: dados do produto da API do Sankhya
            :param data_olist: dados do produto da API do Olist
            :param dados_empresa: dados da empresa da API do Olist
            :return list: lista de alterações a serem aplicadas no produto
            :return dict: dicionário com os dados do produto
        """

        updates = []
        new_data = data_olist.copy() if data_olist else None

        if new_data and data_sankhya:
            if new_data.get('unidade') != data_sankhya.get('codvol'):
                updates.append({'campo':'unidade',
                                'valorOld':new_data.get('unidade'),
                                'valorNew':data_sankhya.get('codvol')})                      
                new_data['unidade'] = data_sankhya.get('codvol')

            if str(new_data.get('ncm')) != self.validar.ncm(data_sankhya.get('ncm')):
                ncm = self.validar.ncm(data_sankhya.get('ncm'))
                if ncm:
                    updates.append({'campo':'ncm',
                                    'valorOld':new_data.get('ncm'),
                                    'valorNew':data_sankhya.get('ncm')})                     
                    new_data['ncm'] = ncm

            if str(new_data.get('codigoEspecificadorSubstituicaoTributaria')) != self.validar.cest(data_sankhya.get('codespecst')):
                cest = self.validar.cest(data_sankhya.get('codespecst'))
                if cest:
                    updates.append({'campo':'cest',
                                    'valorOld':new_data.get('codigoEspecificadorSubstituicaoTributaria'),
                                    'valorNew':data_sankhya.get('codespecst')})                     
                    new_data['codigoEspecificadorSubstituicaoTributaria'] = cest

            if str(new_data.get('origem')) != str(data_sankhya.get('origprod')):
                updates.append({'campo':'origem',
                                'valorOld':new_data.get('origem'),
                                'valorNew':data_sankhya.get('origprod')})                      
                new_data['origem'] = int(data_sankhya.get('origprod'))

            if str(new_data.get('gtin')) != str(data_sankhya.get('referencia')):
                if self.validar.gtin(str(data_sankhya.get('referencia'))):
                    new_data['gtin'] = str(data_sankhya.get('referencia'))
                    updates.append({'campo':'gtin',
                                    'valorOld':new_data.get('gtin'),
                                    'valorNew':data_sankhya.get('referencia')})

            try:
                if float(new_data['dimensoes'].get('largura')) != float(data_sankhya.get('largura')):
                    updates.append({'campo':'dimensoes_largura',
                                    'valorOld':new_data['dimensoes'].get('largura'),
                                    'valorNew':data_sankhya.get('largura')})
                    new_data['dimensoes']['largura'] = float(data_sankhya.get('largura'))
            except:
                pass

            try:
                if float(new_data['dimensoes'].get('altura')) != float(data_sankhya.get('altura')):
                    updates.append({'campo':'dimensoes_largura',
                                    'valorOld':new_data['dimensoes'].get('largura'),
                                    'valorNew':data_sankhya.get('largura')})                   
                    new_data['dimensoes']['altura'] = float(data_sankhya.get('altura'))
            except:
                pass

            try:
                if float(new_data['dimensoes'].get('comprimento')) != float(data_sankhya.get('espessura')):
                    updates.append({'campo':'dimensoes_comprimento',
                                    'valorOld':new_data['dimensoes'].get('comprimento'),
                                    'valorNew':data_sankhya.get('espessura')})                    
                    new_data['dimensoes']['comprimento'] = float(data_sankhya.get('espessura'))
            except:
                pass

            try:
                if float(new_data['dimensoes'].get('pesoLiquido')) != float(data_sankhya.get('pesoliq')):
                    updates.append({'campo':'dimensoes_pesoLiquido',
                                    'valorOld':new_data['dimensoes'].get('pesoLiquido'),
                                    'valorNew':data_sankhya.get('pesoliq')})                    
                    new_data['dimensoes']['pesoLiquido'] = float(data_sankhya.get('pesoliq'))
            except:
                pass

            try:
                if float(new_data['dimensoes'].get('pesoBruto')) != float(data_sankhya.get('pesobruto')):
                    updates.append({'campo':'dimensoes_pesoBruto',
                                    'valorOld':new_data['dimensoes'].get('pesoBruto'),
                                    'valorNew':data_sankhya.get('pesobruto')})                   
                    new_data['dimensoes']['pesoBruto'] = float(data_sankhya.get('pesobruto'))
            except:
                pass

            try:
                if float(new_data['estoque'].get('minimo')) != float(data_sankhya.get('estmin')):
                    updates.append({'campo':'estoque_minimo',
                                    'valorOld':new_data['estoque'].get('minimo'),
                                    'valorNew':data_sankhya.get('estmin')})                       
                    new_data['estoque']['minimo'] = float(data_sankhya.get('estmin'))
            except:
                pass

            try:
                if float(new_data['estoque'].get('maximo')) != float(data_sankhya.get('estmax')):
                    updates.append({'campo':'estoque_maximo',
                                    'valorOld':new_data['estoque'].get('maximo'),
                                    'valorNew':data_sankhya.get('estmax')})                         
                    new_data['estoque']['maximo'] = float(data_sankhya.get('estmax'))
            except:
                pass

            if str(new_data['tributacao'].get('gtinEmbalagem')) != str(data_sankhya.get('referencia')):
                updates.append({'campo':'tributacao_gtinEmbalagem',
                                'valorOld':new_data['tributacao'].get('gtinEmbalagem'),
                                'valorNew':data_sankhya.get('referencia')})                   
                new_data['tributacao']['gtinEmbalagem'] = str(data_sankhya.get('referencia'))

            with open(os.getenv('OBJECT_PRODUTO',"src/json/produto.json"), "r", encoding="utf-8") as f:
                modelo_api = json.load(f)
            new_data = self.formatter.limpar_json(new_data,modelo_api.get('put'))
            if new_data['fornecedores'][0].get('id') == 0:
                new_data['fornecedores'][0]['id'] = dados_empresa.get('olist_id_fornecedor_padrao')
            new_data['fornecedores'][0]['padrao'] = True
            new_data['seo']['keywords']=['produto']

            if not updates:
                return [0], {}

        if data_sankhya and not new_data:
            updates.append(-1)
            new_data = {}
            new_data['sku'] = str(data_sankhya.get('codprod'))
            new_data['descricaoComplementar'] = ''
            new_data['unidade'] = str(data_sankhya.get('codvol'))
            new_data['unidadePorCaixa'] = 1
            new_data['ncm'] = self.validar.ncm(str(data_sankhya.get('ncm')))
            new_data['gtin'] = str(data_sankhya.get('referencia')) if self.validar.gtin(str(data_sankhya.get('referencia'))) else None
            new_data['origem'] = int(data_sankhya.get('origprod'))
            new_data['codigoEspecificadorSubstituicaoTributaria'] = self.validar.cest(str(data_sankhya.get('codespecst')))
            new_data['garantia'] = ''
            new_data['observacoes'] = ''
            new_data['marca'] = {'id': dados_empresa.get('olist_id_marca_padrao')}
            new_data['categoria'] = {'id': dados_empresa.get('olist_id_categoria_padrao')}
            new_data['precos'] = {'preco': 0,
                                  'precoPromocional': 0,
                                  'precoCusto': 0}
            new_data['dimensoes'] = {'embalagem': {'id': None,
                                                   'tipo': 2 },
                                        'largura': float(data_sankhya.get('largura')) if data_sankhya.get('largura') else None,
                                        'altura': float(data_sankhya.get('altura')) if data_sankhya.get('altura') else None,
                                        'comprimento': float(data_sankhya.get('espessura')) if data_sankhya.get('espessura') else None,
                                        'diametro': None,
                                        'pesoLiquido': float(data_sankhya.get('pesoliq')) if data_sankhya.get('pesoliq') else None,
                                        'pesoBruto': float(data_sankhya.get('pesobruto')) if data_sankhya.get('pesobruto') else None}
            new_data['tributacao'] = {'gtinEmbalagem': str(data_sankhya.get('referencia')) if data_sankhya.get('referencia') else None,
                                       'valorIPIFixo': 0,
                                       'classeIPI': None}
            new_data['seo'] = {'titulo': None,
                                'descricao': None,
                                'keywords': ['produto'],
                                'linkVideo': None,
                                'slug': None}
            new_data['descricao'] = data_sankhya.get('nome')
            new_data['tipo'] = 'S'
            new_data['estoque'] = {'controlar': True,
                                   'sobEncomenda': False,
                                   'minimo': int(data_sankhya.get('estmin')) if data_sankhya.get('estmin') else None,
                                   'maximo': int(data_sankhya.get('estmax')) if data_sankhya.get('estmax') else None,
                                   'diasPreparacao': 0,
                                   'localizacao': None}
            new_data['fornecedores'] = [{'id' : dados_empresa.get('olist_id_fornecedor_padrao'),
                                         'codigoProdutoNoFornecedor' : str(data_sankhya.get('refforn')),
                                         'padrao' : True }]
            new_data['grade'] = ['.']
        
        return updates, new_data
