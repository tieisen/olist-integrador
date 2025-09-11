import os
import re
import json
import logging
from datetime import datetime
from src.utils.formatter import Formatter
from src.utils.decorador.empresa import ensure_dados_empresa
from dotenv import load_dotenv
from src.utils.log import Log

load_dotenv('keys/.env')
logger = logging.getLogger(__name__)
logging.basicConfig(filename=Log().buscar_path(),
                    encoding='utf-8',
                    format=os.getenv('LOGGER_FORMAT'),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

class Produto:

    def __init__(
            self,
            codemp:int
        ):
        self.formatter = Formatter()
        self.codemp = codemp
        self.dados_empresa = None

    def to_sankhya(self, data_olist:dict=None, data_sankhya:dict=None, type:str='update') -> tuple[list,dict]:

        if not type or type not in ['update', 'insert', 'delete']:
            logger.error(f"Tipo de operação inválido: {type}")
            print(f"Tipo de operação inválido. Deve ser 'update' ou 'insert' ou 'delete'")
            return [], {}

        new_data = {}
        updates = []

        if type == 'update':

            if not all([data_olist,data_sankhya]):
                logger.error("Dados insuficientes para atualização.")
                print("Dados insuficientes para atualização.")
                return [], {}

            if data_sankhya['ad_mkp_idprod'] != str(data_olist.get('id')):
                updates.append({'campo':'ad_mkp_idprod',
                                'valorOld':data_sankhya.get('ad_mkp_idprod'),
                                'valorNew':data_olist.get('id')})
                new_data['ad_mkp_idprod'] = str(data_olist.get('id'))

            if data_olist.get('produtoPai'):
                if data_sankhya['ad_mkp_idprodpai'] != str(data_olist['produtoPai'].get('id')):
                    updates.append({'campo':'ad_mkp_idprodpai',
                                    'valorOld':data_sankhya.get('ad_mkp_idprodpai'),
                                    'valorNew':data_olist['produtoPai'].get('id')})                    
                    new_data['ad_mkp_idprodpai'] = str(data_olist['produtoPai'].get('id'))
            else:
                if data_sankhya['ad_mkp_idprodpai']:
                    updates.append({'campo':'ad_mkp_idprodpai',
                                    'valorOld':data_sankhya.get('ad_mkp_idprodpai'),
                                    'valorNew':None})
                    new_data['ad_mkp_idprodpai'] = ''

            if data_sankhya['ad_mkp_marca'] != str(data_olist['marca'].get('id')):
                updates.append({'campo':'ad_mkp_marca',
                                'valorOld':data_sankhya.get('ad_mkp_marca'),
                                'valorNew':data_olist['marca'].get('id')})
                new_data['ad_mkp_marca'] = str(data_olist['marca'].get('id'))

            if data_sankhya['ad_mkp_nome'] != data_olist.get('descricao'):
                updates.append({'campo':'ad_mkp_nome',
                                'valorOld':data_sankhya.get('ad_mkp_nome'),
                                'valorNew':data_olist.get('descricao')})                
                new_data['ad_mkp_nome'] = data_olist.get('descricao')

            if data_olist.get('categoria'):
                if data_sankhya['ad_mkp_categoria'] != str(data_olist['categoria'].get('id')):
                    updates.append({'campo':'ad_mkp_categoria',
                                    'valorOld':data_sankhya.get('ad_mkp_categoria'),
                                    'valorNew':data_olist['categoria'].get('id')})                  
                    new_data['ad_mkp_categoria'] = str(data_olist['categoria'].get('id'))
            else:
                updates.append({'campo':'ad_mkp_categoria',
                                'valorOld':None,
                                'valorNew':341974963})
                new_data['ad_mkp_categoria'] = 341974963
                
            if data_sankhya['ad_mkp_descricao'] != data_olist.get('descricaoComplementar'):
                updates.append({'campo':'ad_mkp_descricao',
                                'valorOld':data_sankhya.get('ad_mkp_descricao'),
                                'valorNew':data_olist.get('descricaoComplementar')})                  
                new_data['ad_mkp_descricao'] = data_olist.get('descricaoComplementar')
                
            new_data['ad_mkp_dhatualizado'] = datetime.now().strftime('%d/%m/%Y %H:%M:%S')            

            if not updates:
                return [0], {}
        
        if type == 'insert':
            if not data_olist:
                logger.error("Dados insuficientes para inserção.")
                print("Dados insuficientes para inserção.")
                return [], {}
            
            updates.append({'campo':'ad_mkp_idprod',
                            'valorOld':None,
                            'valorNew':data_olist.get('id')})
            new_data['ad_mkp_idprod'] = str(data_olist.get('id'))

        if type == 'delete':
            updates.append({'campo':'ad_mkp_integrado',
                            'valorOld':'S',
                            'valorNew':'N'})
            new_data['ad_mkp_categoria'] = new_data['ad_mkp_descricao'] = new_data['ad_mkp_dhatualizado'] = new_data['ad_mkp_estpol'] = new_data['ad_mkp_idprod'] = new_data['ad_mkp_idprodpai'] = new_data['ad_mkp_marca'] = new_data['ad_mkp_nome'] = new_data['ad_mkp_integrado'] = ''
            new_data['ad_mkp_integrado'] = 'N'

        return updates, new_data

    @ensure_dados_empresa
    def to_olist(self, data_sankhya:dict, data_olist:dict=None, ) -> tuple[list,dict]:
        updates = []
        new_data = data_olist.copy() if data_olist else None

        if new_data and data_sankhya:                
            if new_data.get('descricao') != data_sankhya.get('ad_mkp_nome'):
                updates.append({'campo':'descricao',
                                'valorOld':new_data.get('descricao'),
                                'valorNew':data_sankhya.get('ad_mkp_nome')})
                new_data['descricao'] = data_sankhya.get('ad_mkp_nome')

            if new_data.get('descricaoComplementar') != data_sankhya.get('ad_mkp_descricao'):
                updates.append({'campo':'descricaoComplementar',
                                'valorOld':new_data.get('descricaoComplementar'),
                                'valorNew':data_sankhya.get('ad_mkp_descricao')})                    
                new_data['descricaoComplementar'] = data_sankhya.get('ad_mkp_descricao')

            if new_data.get('unidade') != data_sankhya.get('codvol'):
                updates.append({'campo':'unidade',
                                'valorOld':new_data.get('unidade'),
                                'valorNew':data_sankhya.get('codvol')})                      
                new_data['unidade'] = data_sankhya.get('codvol')

            if str(re.sub(r"[.]", '', new_data.get('ncm'))) != str(data_sankhya.get('ncm')):
                updates.append({'campo':'ncm',
                                'valorOld':new_data.get('ncm'),
                                'valorNew':data_sankhya.get('ncm')})                      
                new_data['ncm'] = re.sub(r"(\d{4})(\d{2})(\d{2})", r"\1.\2.\3", data_sankhya.get('ncm'))

            if str(new_data.get('origem')) != str(data_sankhya.get('origprod')):
                updates.append({'campo':'origem',
                                'valorOld':new_data.get('origem'),
                                'valorNew':data_sankhya.get('origprod')})                      
                new_data['origem'] = int(data_sankhya.get('origprod'))

            if str(new_data.get('gtin')) != str(data_sankhya.get('referencia')):
                updates.append({'campo':'gtin',
                                'valorOld':new_data.get('gtin'),
                                'valorNew':data_sankhya.get('referencia')})                      
                new_data['gtin'] = str(data_sankhya.get('referencia'))

            try:
                if int(new_data['categoria'].get('id',0)) != int(data_sankhya.get('ad_mkp_categoria')):
                    updates.append({'campo':'categoria_id',
                                    'valorOld':new_data['categoria'].get('id'),
                                    'valorNew':data_sankhya.get('ad_mkp_categoria')})  
                    new_data['categoria'] = { 'id': int(data_sankhya.get('ad_mkp_categoria')) }
            except:
                pass

            try:
                if int(new_data['marca'].get('id')) != int(data_sankhya.get('ad_mkp_marca')):
                    updates.append({'campo':'marca_id',
                                    'valorOld':new_data['marca'].get('id'),
                                    'valorNew':data_sankhya.get('ad_mkp_marca')})                      
                    new_data['marca'] = { 'id': int(data_sankhya.get('ad_mkp_marca'))}
            except:
                pass

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
                new_data['fornecedores'][0]['id'] = self.dados_empresa.get('olist_id_fornecedor_padrao')
            new_data['fornecedores'][0]['padrao'] = True
            new_data['seo']['keywords']=['produto']

            if not updates:
                return [0], {}

        if data_sankhya and not new_data:
            updates.append(-1)
            new_data = {}
            new_data['sku'] = str(data_sankhya.get('codprod'))
            new_data['descricaoComplementar'] = str(data_sankhya.get('ad_mkp_descricao'))
            new_data['unidade'] = str(data_sankhya.get('codvol'))
            new_data['unidadePorCaixa'] = 1
            new_data['ncm'] = re.sub(r"(\d{4})(\d{2})(\d{2})", r"\1.\2.\3", data_sankhya.get('ncm'))
            new_data['gtin'] = str(data_sankhya.get('referencia')) if data_sankhya.get('referencia') else None
            new_data['origem'] = int(data_sankhya.get('origprod'))
            new_data['codigoEspecificadorSubstituicaoTributaria'] = str(data_sankhya.get('codespecst'))
            new_data['garantia'] = ''
            new_data['observacoes'] = ''
            new_data['marca'] = {'id': int(data_sankhya.get('ad_mkp_marca'))}
            new_data['categoria'] = {'id': int(data_sankhya.get('ad_mkp_categoria'))}
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
            new_data['descricao'] = data_sankhya.get('ad_mkp_nome')
            new_data['tipo'] = 'S'
            new_data['estoque'] = {'controlar': True,
                                    'sobEncomenda': False,
                                    'minimo': int(data_sankhya.get('estmin')) if data_sankhya.get('estmin') else None,
                                    'maximo': int(data_sankhya.get('estmax')) if data_sankhya.get('estmax') else None,
                                    'diasPreparacao': 0,
                                    'localizacao': None}
            new_data['fornecedores'] = [{'id' : self.dados_empresa.get('olist_id_fornecedor_padrao'),
                                         'codigoProdutoNoFornecedor' : str(data_sankhya.get('refforn')),
                                         'padrao' : True }]
            new_data['grade'] = ['.']
        
        return updates, new_data
