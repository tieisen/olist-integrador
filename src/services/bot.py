import os
import time
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.select import Select
from database.crud import log as crudLog
from src.utils.decorador import contexto
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

class Bot:

    def __init__(self,empresa_id:int=None):
        self.link_erp = os.getenv('OLIST_URL_ERP')
        self.link_estoque = os.getenv('OLIST_URL_ESTOQUE')
        self.link_produto = os.getenv('OLIST_URL_CAD_PRODUTO')
        self.link_relatorio_custos = os.getenv('OLIST_URL_RELATORIO_CUSTOS')
        self.link_contas_receber = os.getenv('OLIST_URL_CONTAS_RECEBER')
        self.link_gnre = os.getenv('OLIST_URL_GNRE')
        self.link_logout = os.getenv('OLIST_URL_LOGOUT')
        self.time_sleep = float(os.getenv('REQ_TIME_SLEEP'))
        self.username = os.getenv('OLIST_BOT_USERNAME')
        self.password = os.getenv('OLIST_BOT_PASSWORD')
        self.timeout_lotes = int(os.getenv('OLIST_TIMEOUT_LANCA_LOTES'))
        self.driver = None
        self.empresa_id = empresa_id
        self.contexto = 'bot'
        self.wait = None

    async def login(self):
        try:
            self.driver = webdriver.Firefox()
            self.driver.maximize_window() 
            self.driver.get(self.link_erp)
            # self.wait = WebDriverWait(self.driver, 10)
            WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.ID, "username")))
            login_input = self.driver.find_element(By.ID, "username")
            next_button = self.driver.find_element(By.XPATH, "//button[@class='sc-dAlyuH biayZs sc-dAbbOL ddEnAE']")
            login_input.clear()
            login_input.send_keys(self.username)
            next_button.click()

            WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.ID, "password")))
            pass_input =  self.driver.find_element(By.ID, "password")
            submit_button = self.driver.find_element(By.XPATH, "//button[@class='sc-dAlyuH biayZs sc-dAbbOL ddEnAE']")
            pass_input.clear()
            pass_input.send_keys(self.password)
            submit_button.click()

            try:
                WebDriverWait(self.driver, 5).until(EC.presence_of_element_located((By.XPATH, "//h3[@class='modal-title']")))
                elemento = self.driver.find_element(By.XPATH, "//h3[@class='modal-title']")
                if elemento.text == 'Este usuário já está logado em outro dispositivo':
                    btn_confirma_login = self.driver.find_element(By.XPATH, "//button[@class='btn btn-primary']")
                    btn_confirma_login.click()
                    WebDriverWait(self.driver, 5).until(EC.presence_of_element_located((By.XPATH, "//div[@class='sidebar-menu-logo-usuario']")))
                    time.sleep(self.time_sleep)
            except:
                pass

            if WebDriverWait(self.driver, 15).until(EC.presence_of_element_located((By.XPATH, "//div[@class='sidebar-menu-iniciais-usuario']"))):
                return True
            else:
                return False
            
        except Exception as e:
            print(f"Falha no login. {e}")
            logger.error("Falha no login. %s",e)
            return False

    async def trocar_empresa(self,empresa_id:int):
        STORYA = 1032724583
        OUTBEAUTY = 1124267820
        ELEMENTO_USUARIO = "//div[@class='sidebar-menu-iniciais-usuario']"        
        elemento_multiempresa = "//a[@onclick='logarNaEmpresaVinculada(:);']"

        match empresa_id:
            case 1:
                elemento_multiempresa = f"//a[@onclick='logarNaEmpresaVinculada({STORYA});']"
            case 5:
                elemento_multiempresa = f"//a[@onclick='logarNaEmpresaVinculada({OUTBEAUTY});']"
            case _:
                logger.error("Empresa não cadastrada no bot")
                return False

        if WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.XPATH, ELEMENTO_USUARIO))):
            elementos = self.driver.find_elements(By.XPATH, ELEMENTO_USUARIO)
            btn_usuario = elementos[1]
            btn_usuario.click()            
            time.sleep(1)
            multiempresa = self.driver.find_element(By.XPATH, elemento_multiempresa)
            multiempresa.click()               
            time.sleep(5)
            if WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.XPATH, ELEMENTO_USUARIO))):
                return True
            else:
                return False
        else:
            logger.error("Erro no botao usuário")
            return False

    async def logout(self):
        self.driver.get(self.link_logout)
        if WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.ID, "username"))):
            self.driver.quit()

    async def acessa_relatorio_custos(self):
        try:
            self.driver.get(self.link_relatorio_custos)
        except Exception as e:
            logger.error("Erro ao acessar relatorio de custos. %s",e)
            return False
        WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.ID, "opc-periodo")))
        return True

    async def gerar_relatorio_custos(self,data_inicial,data_final):
        if WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable(self.driver.find_element(By.ID, "opc-periodo"))):
            btn_intervalo = self.driver.find_element(By.ID, "opc-periodo")
            btn_intervalo.click()
        else:
            logger.error("Erro no botao intervalo")
            return False
        
        if WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.ID, "dataIni"))):

            data_ini = self.driver.find_element(By.ID, "dataIni")
            data_fim = self.driver.find_element(By.ID, "dataFim")

            data_ini.clear()
            data_fim.clear()

            data_ini.send_keys(data_inicial)
            data_fim.send_keys(data_final)

            btn_gerar = self.driver.find_element(By.ID,"btn-visualizar")
            btn_gerar.click()
            time.sleep(self.time_sleep)
            return True
        else:
            logger.error("Erro no selecionar opção")
            return False

    async def baixar_relatorio_custos(self):
        if WebDriverWait(self.driver, 60).until(EC.element_to_be_clickable(self.driver.find_element(By.ID, "btn-download"))):
            btn_download = self.driver.find_element(By.ID, "btn-download")
            btn_download.click()
            time.sleep(30)
            return True
        else:
            logger.error("Erro no botao download")
            return False
        
    @contexto
    async def rotina_relatorio_custos(self,data:datetime,**kwargs) -> bool:
        """
        Docstring for rotina_relatorio_custos
        
        :param self: Description
        :param data: Description
        :type data: datetime
        :param kwargs: Description
        :return: Description
        :rtype: bool
        """
        
        self.log_id = await crudLog.criar(empresa_id=self.empresa_id,
                                          de='olist',
                                          para='sankhya',
                                          contexto=kwargs.get('_contexto'))
        if not isinstance(data,datetime):
            try:
                data = datetime.strptime(data, "%Y-%m-%d")
            except:
                logger.error("Data em formato inválido")
                await crudLog.atualizar(id=self.log_id,sucesso=False)
                return False
            
        data_ini:datetime = data - timedelta(days=4)
        try:
            await self.login()
            if self.empresa_id != 1:
                sucesso_troca = await self.trocar_empresa(empresa_id=self.empresa_id)
                if not sucesso_troca:
                    await self.logout()
                    logger.error("Erro ao trocar de empresa no self")
                    await crudLog.atualizar(id=self.log_id,sucesso=False)
                    return False
            await self.acessa_relatorio_custos()
            await self.gerar_relatorio_custos(data_inicial=data_ini.strftime('%d/%m/%Y'),
                                              data_final=data.strftime('%d/%m/%Y'))
            await self.baixar_relatorio_custos()
            await crudLog.atualizar(id=self.log_id,sucesso=True)
            await self.logout()
            return True            
        except Exception as e:
            await self.logout()
            logger.error("Erro ao baixar relatorio de custos: %s",str(e))
            await crudLog.atualizar(id=self.log_id,sucesso=False)
            return False

    async def acessa_contas_receber(self) -> bool:
        try:
            # Acessa tela de contas a receber
            self.driver.get(self.link_contas_receber)
        except Exception as e:
            logger.error("Erro ao acessar contas a receber. %s",e)
            return False
        
        # Aguarda carregamento da tela        
        if not WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.ID, "periodoDatas"))):
            return False
        return True

    async def seleciona_filtro_historico(self) -> bool:
        # Busca botao de tipo de filtro
        if not WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable(self.driver.find_element(By.XPATH, "//span[@class='input-group-btn dropdown dropdown-in dropdown-refinar-busca']"))):
            logger.error("Erro no botao de tipo de filtro")
            return False
        btn_tipo_filtro = self.driver.find_element(By.XPATH, "//span[@class='input-group-btn dropdown dropdown-in dropdown-refinar-busca']")
        btn_tipo_filtro.click()

        # Busca lista de opções de filtro
        if not WebDriverWait(self.driver, 10).until(EC.visibility_of_element_located((By.XPATH, "//ul[@class='dropdown-menu dropdown-menu-right']"))):
            logger.error("Erro ao exibir opções de filtro")
            return False
        
        # Seleciona opção histórico
        try:
            lista_opcoes = self.driver.find_element(By.XPATH, "//ul[@class='dropdown-menu dropdown-menu-right']")
            opc_historico = lista_opcoes.find_elements(By.XPATH, "//a[@class='no-icon']")
            for opc in opc_historico:
                if opc.text == 'Histórico':
                    opc.click()
                    break
        except Exception as e:
            logger.error("Erro ao selecionar opção histórico. %s",e)
            return False
        return True    

    async def informa_historico(self, numero_nota:int) -> bool:
        
        # Busca campo de filtro
        if not WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable(self.driver.find_element(By.ID, "pesquisa-mini"))):
            logger.error("Erro no campo de filtro")
            return False
        campo_filtro = self.driver.find_element(By.ID, "pesquisa-mini")
        
        try:
            # Informa número da nota campo
            campo_filtro.clear()
            campo_filtro.send_keys(f"NF nº {numero_nota}")
            campo_filtro.send_keys(Keys.ENTER)
        except Exception as e:
            logger.error("Erro ao informar número da nota no campo de filtro. %s",e)
            return False

        # Aguarda carregamento da tabela
        if not WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable((By.ID, "tabelaListagem"))):
            logger.error("Erro ao aplicar filtro")
            return False
        
        return True

    async def alterna_aba_contas_emitidas(self) -> bool:
        
        # Aguarda carregamento da tela 
        if not WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable(self.driver.find_element(By.ID, "sit-1"))):
            logger.error("Erro no botao contas emitidas")
            return False
        
        # Clica na aba contas emitidas
        btn_aba_emitidas = self.driver.find_element(By.ID, "sit-1")
        btn_aba_emitidas.click()

        # Aguarda carregamento/atualização da tabela
        if not WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.ID, "tabelaListagem"))):
            logger.error("Erro ao alternar aba contas emitidas")
            return False
        return True

    async def seleciona_todos_titulos(self) -> bool:

        # Aguarda carregamento da tabela
        if not WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable((By.ID, "tabelaListagem"))):
            logger.error("Erro ao aplicar filtro")
            return False
        
        # Clica no checkbox de selecionar todos
        if not WebDriverWait(self.driver, 10).until(EC.visibility_of_element_located((By.XPATH, "//th[@class='checkbox-datatable ignore-config-coluna footable-visible footable-first-column js-check-label']"))):
            print("Erro ao buscar tabela")
            logger.error("Erro ao buscar tabela")
            return False
        
        table_header = self.driver.find_element(By.ID, "tabelaListagem")
        checkbox_th = table_header.find_element(By.CSS_SELECTOR, "thead tr th span.comp-placeholder")        
        checkbox_th.click()

        time.sleep(1)
        
        return True
    
    async def clica_btn_agrupar(self) -> bool:

        # Aguarda carregamento do botão agrupar
        if not WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[@original-title=' Agrupar contas']"))):
            if not WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[@class='btn btn-menu-acoes dropdown-toggle']"))):
                logger.error("Erro ao buscar botão de agrupar")
                return False
            btn_mais_acoes = self.driver.find_element(By.XPATH, "//button[@class='btn btn-menu-acoes dropdown-toggle']")
            btn_mais_acoes.click()

            if not WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//a[@class='act-agrupar']"))):
                logger.error("Erro ao buscar botão de agrupar")
                return False
            btn_agrupar = self.driver.find_element(By.XPATH, "//a[@class='act-agrupar']")
            btn_agrupar.click()
        
        else:
            # Clica no botão agrupar títulos
            btn_agrupar = self.driver.find_element(By.XPATH, "//button[@original-title=' Agrupar contas']")
            btn_agrupar.click()
        return True
    
    async def confima_agrupamento(self, data_vcto:str=None) -> bool:

        try:
            # Aguarda carregamento do modal de confirmação
            modal = WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".modal-content")))
            campo_data_vcto = WebDriverWait(modal, 10).until(EC.element_to_be_clickable((By.XPATH, ".//input[@name='vencimentoAgrupamento']")))

            # Informa data de vencimento
            if not data_vcto:
                data_vcto = datetime.now().strftime('%d/%m/%Y')
            campo_data_vcto.clear()
            campo_data_vcto.send_keys(data_vcto)

            time.sleep(1)

            # Busca botão de confirmar agrupamento
            if not WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable(self.driver.find_element(By.ID, "agruparContas"))):
                logger.error("Erro no botão de confirmar agrupamento")
                return False
            
            btn_confirma_agrupar = self.driver.find_element(By.ID, "agruparContas")
            btn_confirma_agrupar.click()

            # Aguarda carregamento/atualização da tabela
            if not WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.ID, "tabelaListagem"))):
                logger.error("Erro ao alternar aba contas emitidas")
                return False

            return True
        except Exception as e:
            logger.error("Erro ao confirmar agrupamento. %s",e)
            return False
    
    async def agrupa_titulos(self,data_vcto:str=None) -> bool:
        
        # Clica no botão agrupar títulos
        await self.clica_btn_agrupar()
        await self.confima_agrupamento(data_vcto=data_vcto)
        return True

    async def voltar_lista_contas(self) -> bool:
        # Aguarda carregamento do botão voltar
        if not WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//a[@class='btn btn-breadcrumb-voltar']"))):
            logger.error("Erro ao buscar botão de voltar para lista de contas")
            return False
        
        # Clica no botão voltar para lista de contas
        btn_voltar = self.driver.find_element(By.XPATH, "//a[@class='btn btn-breadcrumb-voltar']")
        btn_voltar.click()

        # Aguarda carregamento/atualização da tabela
        if not WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.ID, "tabelaListagem"))):
            logger.error("Erro ao voltar para lista de contas")
            return False
        return True

    async def rotina_agrupar_conta(self,numero_nota:int,data_vcto:str=None) -> bool:
        try:            
            await self.informa_historico(numero_nota=numero_nota)
            await self.alterna_aba_contas_emitidas()
            await self.seleciona_todos_titulos()
            await self.agrupa_titulos(data_vcto=data_vcto)
            return True
        except Exception as e:
            logger.error("Erro ao agrupar contas da nota %s. %s",numero_nota,str(e))            
            return False

    @contexto
    async def rotina_contas_receber(self,numero_nota:int=None,lista_notas:list[int]=None,data_vcto:str=None,**kwargs) -> bool:
        self.log_id = await crudLog.criar(empresa_id=self.empresa_id,
                                          de='olist',
                                          para='olist',
                                          contexto=kwargs.get('_contexto'))
        
        if not any([numero_nota,lista_notas]):
            logger.error("Nenhum número de nota informado para agrupar contas a receber")
            await crudLog.atualizar(id=self.log_id,sucesso=False)
            return False

        try:
            await self.login()
            if self.empresa_id != 1:
                sucesso_troca = await self.trocar_empresa(empresa_id=self.empresa_id)
                if not sucesso_troca:
                    await self.logout()
                    logger.error("Erro ao trocar de empresa no self")
                    await crudLog.atualizar(id=self.log_id,sucesso=False)
                    return False

            await self.acessa_contas_receber()
            await self.seleciona_filtro_historico()
            if lista_notas:
                for nota in lista_notas:
                    time.sleep(self.time_sleep)
                    await self.rotina_agrupar_conta(numero_nota=nota,data_vcto=data_vcto)
                    await self.voltar_lista_contas()
            else:                
                await self.rotina_agrupar_conta(numero_nota=numero_nota,data_vcto=data_vcto)

            await crudLog.atualizar(id=self.log_id,sucesso=True)
            await self.logout()
            return True
               
        except Exception as e:
            await self.logout()
            logger.error("Erro ao agrupar contas a receber: %s",str(e))
            await crudLog.atualizar(id=self.log_id,sucesso=False)
            return False

    async def acessa_gnre(self) -> bool:
        try:
            # Acessa tela do GNRE
            self.driver.get(self.link_gnre)
        except Exception as e:
            logger.error("Erro ao acessar GNRE. %s",e)
            return False
        
        # Aguarda carregamento da tela        
        if not WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.NAME, "pesquisa"))):
            return False
        return True

    async def alterna_aba_aguardando(self) -> bool:
        
        # Aguarda carregamento da tela 
        if not WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable(self.driver.find_element(By.XPATH, "//div[@class='icon-led icon-led-yellow icon-menu']"))):
            logger.error("Erro na aba aguardando")
            return False
        
        # Clica na aba contas emitidas
        btn_aba_aguardando = self.driver.find_element(By.XPATH, "//div[@class='icon-led icon-led-yellow icon-menu']")
        btn_aba_aguardando.click()

        # Aguarda carregamento/atualização da tabela
        if not WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.XPATH, "//table[@class='footable table table-hover']"))):
            logger.error("Erro ao alternar aba aguardando")
            return False
        return True

    async def seleciona_todas_guias(self) -> bool:
        
        # Aguarda carregamento/atualização da tabela
        if not WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.XPATH, "//table[@class='footable table table-hover']"))):
            logger.error("Erro ao alternar aba aguardando")
            return False
        
        # Localiza checkbox
        if not WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable(self.driver.find_element(By.XPATH, "//th[@class='small-width text-center']"))):
            logger.error("Erro no checkbox selecionar todas")
            return False
        
        # Clica na aba contas emitidas
        checkbox = self.driver.find_element(By.XPATH, "//th[@class='small-width text-center']")
        checkbox.click()

        return True

    @contexto
    async def rotina_guias_gnre(self,**kwargs) -> bool:
        self.log_id = await crudLog.criar(empresa_id=self.empresa_id,
                                          de='olist',
                                          para='sankhya',
                                          contexto=kwargs.get('_contexto'))
        
        pass

    async def habilita_controle_lotes(self,id_produto):
        self.driver.get(self.link_produto.format(id_produto))
        
        if WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable(self.driver.find_element(By.XPATH, "//button[@class='btn btn-primary btn-edicao-item']"))):
            btn_editar = self.driver.find_element(By.XPATH, "//button[@class='btn btn-primary btn-edicao-item']")
            btn_editar.click()
        else:
            logger.error("Erro no botao editar")
            return False
        
        if WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.ID, "controlarLotes"))):
            controle_lote = self.driver.find_element(By.ID, "controlarLotes")        
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
            controle_lote = Select(controle_lote)
            controle_lote.select_by_value('1')
            btn_salvar = self.driver.find_element(By.ID, "botaoSalvar")
            btn_salvar.click()
            time.sleep(self.time_sleep)
            return True
        else:
            logger.error("Erro no selecionar opção")
            return False

    async def desabilita_controle_lotes(self,id_produto):
        self.driver.get(self.link_produto.format(id_produto))
        
        if WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable(self.driver.find_element(By.XPATH, "//button[@class='btn btn-primary btn-edicao-item']"))):
            btn_editar = self.driver.find_element(By.XPATH, "//button[@class='btn btn-primary btn-edicao-item']")
            time.sleep(self.time_sleep)
            btn_editar.click()
            time.sleep(self.time_sleep)
        else:
            logger.error("Erro no botao editar")
            return False
        
        if WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.ID, "controlarLotes"))):
            controle_lote = self.driver.find_element(By.ID, "controlarLotes")
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(self.time_sleep)
            controle_lote = Select(controle_lote)
            controle_lote.select_by_value('0')
            btn_salvar = self.driver.find_element(By.ID, "botaoSalvar")
            time.sleep(self.time_sleep)
            btn_salvar.click()
            if WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable(self.driver.find_element(By.XPATH, "//button[@class='btn btn-primary btn-edicao-item']"))):
                return True
            else:                
                return False
        else:
            logger.error("Erro no selecionar opção")
            return False

    async def acessa_estoque_produto(self,id_produto):
        try:
            self.driver.get(self.link_estoque.format(id_produto))
        except Exception as e:
            logger.error("Falha ao acessar estoque do produto. %s",e)
            return False, None
        WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.ID, "tabelalancamentos")))
        return True

    async def busca_controle_lotes(self):
        lotes = []
        try:
            self.driver.execute_script("trocarAba('lotes'); return false;")
            WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable((By.ID, "tabelaListagem")))
            rows_lotes = self.driver.find_elements(By.XPATH, "//tr[@class='cursor-pointer']")
            if rows_lotes == []:
                return lotes
            for row in rows_lotes:
                campos_qtd = row.find_elements(By.CSS_SELECTOR, 'td.tline.text-right')
                lotes.append({
                    "lote": row.get_attribute('numerolote'),
                    "dtFab": datetime.strptime(row.get_attribute('datafabricacao'), "%Y-%m-%d"),
                    "dtVal": datetime.strptime(row.get_attribute('datavalidade'), "%Y-%m-%d"),
                    "qtd": int(campos_qtd[1].text)
                })
        except Exception as e:
            lotes = [-1]
            logger.error("Falha coletar dado de controle de lotes do produto. %s",e)
        finally:
            return lotes

    async def acessa_controle_lotes(self,id_lancamento):
        try:
            self.driver.execute_script("trocarAba('lancamentos'); return false;")
            if WebDriverWait(self.driver, 30).until(EC.element_to_be_clickable((By.ID, "tabelalancamentos"))):
                lcto = self.driver.find_element(By.XPATH, f"//tr[@idlancamento='{id_lancamento}']")
                dt_lancamento = lcto.get_attribute('dataLancamento')
                dt_lancamento = datetime.strptime(dt_lancamento,'%Y-%m-%d %H:%M:%S').strftime('%d/%m/%Y - %H:%M')
                lcto.find_element(By.TAG_NAME, "button").click()
                WebDriverWait(self.driver, 10).until(EC.visibility_of_element_located((By.XPATH, f"//ul[@data-before='{dt_lancamento}']")))
                div_gerenciar_lotes = self.driver.find_element(By.XPATH, f"//ul[@data-before='{dt_lancamento}']")
                item_gerenciar_lotes = div_gerenciar_lotes.find_elements(By.XPATH, f"//li[@id='im_2']")
                item_gerenciar_lotes[-1].click()                
                return True, self.driver
        except Exception as e:
            logger.error("Falha ao acessar aba de controle de lotes do produto. %s",e)
            return False, None

    async def lanca_lotes_saida(self,dados_lote):
        WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.XPATH, "//form[@name='formLancarLotesSaida']")))
        # informa os lotes
        try:
            lotes_rows = self.driver.find_elements(By.XPATH,"//tr[@class='linha-lote-estoque']")
            idestoque = lotes_rows[0].get_attribute('uidestoque')
            idproduto = lotes_rows[0].get_attribute('idproduto')

            if len(lotes_rows) < len(dados_lote):
                # Adiciona linhas no formulario
                print("Adicionando linhas no formulario")
                for i in range(len(dados_lote)-1):
                    if WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable(self.driver.find_element(By.XPATH, "//button[@class='btn btn-default btn-sm dropdown-toggle']"))):
                        btn_add = self.driver.find_element(By.XPATH, "//button[@class='btn btn-default btn-sm dropdown-toggle']")
                        btn_add.click()
                    else:
                        logger.error("Erro no botao adicionar")
                        return False
                    if WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable(self.driver.find_element(By.XPATH, "//a[@class='act-adicionar-linha-lote']"))):
                        add_lote = self.driver.find_element(By.XPATH, "//a[@class='act-adicionar-linha-lote']")
                        add_lote.click()
                    else:
                        logger.error("Erro no botao adicionar")
                        return False

            if len(lotes_rows) > len(dados_lote):
                # Remove linhas do formulário
                print("Removendo linhas do formulário")
                for i in range(len(lotes_rows-dados_lote)-1):
                    if WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable(self.driver.find_element(By.XPATH, "//button[@class='btn btn-default btn-sm dropdown-toggle']"))):
                        btn_remove = self.driver.find_elements(By.XPATH, "//button[@class='btn btn-default btn-sm dropdown-toggle']")
                        btn_remove[i].click()
                    else:
                        logger.error("Erro no botao remover")
                        return False
                    if WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.XPATH, "//ul[@class='dropdown-menu dropdown-menu-right']"))):
                        lista_opcoes_lote = self.driver.find_elements(By.XPATH, "//ul[@class='dropdown-menu dropdown-menu-right']")
                    else:
                        logger.error("Erro no botao remover")
                        return False
                    if WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable(self.driver.find_element(By.XPATH, "//a[@class='act-remover-linha-lote']"))):
                        del_lote = lista_opcoes_lote[i].find_element(By.XPATH, "//a[@class='act-remover-linha-lote']")
                        del_lote.click()
                    else:
                        logger.error("Erro no botao remover")
                        return False
                    if WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[@popup-action-id='0']"))):
                        btn_confirma = self.driver.find_elements(By.XPATH, "//button[@popup-action-id='0']")
                        btn_confirma[0].click()
                    else:
                        logger.error("Erro no botao confirmar remover")
                        return False

            for j, controle in enumerate(dados_lote):
                # Lança os dado do lote
                print("Informando os dados do lote")
                WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.XPATH, f"//input[@name='lotes[{idproduto}][{idestoque}][{j}][quantidade]']")))
                lote_qtd = self.driver.find_element(By.XPATH, f"//input[@name='lotes[{idproduto}][{idestoque}][{j}][quantidade]']")
                lote_numero = self.driver.find_element(By.XPATH, f"//select[@name='lotes[{idproduto}][{idestoque}][{j}][numeroLote]']")    
                lote_numero = Select(lote_numero)
                try:
                    lote_numero.select_by_value(controle.get('numeroLote'))
                except:
                    lote_numero.select_by_value('Sem lote')
                lote_qtd.clear()
                lote_qtd.send_keys(abs(int(controle.get('quantidade'))))    

            btn_enviar = self.driver.find_element(By.XPATH,"//button[@class='btn btn-primary']")
            btn_enviar.click()

            try:
                WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.XPATH, "//div[@style='max-height: 252px;']")))
                if WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable(self.driver.find_element(By.XPATH, "//button[@popup-action='confirm']"))):
                    btn_confirma = self.driver.find_element(By.XPATH, "//button[@popup-action='confirm']")
                    btn_confirma.click()
            except:
                pass                
            
            return True
        except Exception as e:
            logger.error("Erro ao informar os lotes na movimentação de saída: %s",e)
            return False

    async def lanca_lotes_entrada(self,dados_lote):
        WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.XPATH, "//form[@name='formLancarLotesEntrada']")))
        # informa os lotes
        try:
            lotes_rows = self.driver.find_elements(By.XPATH,"//tr[@class='linha-lote-estoque']")
            idestoque = lotes_rows[0].get_attribute('idestoque')
            idproduto = lotes_rows[0].get_attribute('idproduto')
        except Exception as e:
            logger.error("Erro ao coletar os dados de estoque e produto. %s",e)
            return False

        if len(lotes_rows) < len(dados_lote) and len(dados_lote) > 1:
            try:
                # Adiciona linhas no formulario
                for i in range(len(dados_lote)-1):
                    if WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable(self.driver.find_element(By.XPATH, "//button[@class='btn btn-default btn-sm dropdown-toggle']"))):
                        time.sleep(self.time_sleep)
                        btn_add = self.driver.find_element(By.XPATH, "//button[@class='btn btn-default btn-sm dropdown-toggle']")
                        btn_add.click()                    
                    if WebDriverWait(self.driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//ul[@class='dropdown-menu dropdown-menu-right']"))):
                        time.sleep(self.time_sleep)
                        lista_opcoes_lote = self.driver.find_element(By.XPATH, "//ul[@class='dropdown-menu dropdown-menu-right']")
                        add_lote = lista_opcoes_lote.find_elements(By.TAG_NAME, "a")
                        add_lote[0].click()
            except Exception as e:
                logger.error("Erro ao adicionar linhas de lote. %s",e)
                return False

        try:
            for j, controle in enumerate(dados_lote):
                # Lança os dado do lote                
                lote_codigo = self.driver.find_element(By.XPATH, f"//input[@name='lotes[{idproduto}][{idestoque}][{j}][numeroLote]']")
                lote_fabric = self.driver.find_element(By.XPATH, f"//input[@name='lotes[{idproduto}][{idestoque}][{j}][dataFabricacao]']")
                lote_valid = self.driver.find_element(By.XPATH, f"//input[@name='lotes[{idproduto}][{idestoque}][{j}][dataValidade]']")
                lote_qtd = self.driver.find_element(By.XPATH, f"//input[@name='lotes[{idproduto}][{idestoque}][{j}][quantidade]']")

                lote_codigo.clear()
                lote_fabric.clear()
                lote_valid.clear()
                lote_qtd.clear()

                lote_codigo.send_keys(controle.get('numeroLote'))
                lote_fabric.send_keys(controle.get('dataFabricacao'))
                lote_valid.send_keys(controle.get('dataValidade'))
                lote_qtd.send_keys(int(controle.get('quantidade')))
        except Exception as e:
            logger.error("Erro ao lançar os lotes. %s",e)
            return False

        try:
            btn_enviar = self.driver.find_element(By.XPATH,"//button[@class='btn btn-primary']")
            btn_enviar.text
            btn_enviar.click()            
        except Exception as e:
            logger.error("Erro ao enviar os lotes. %s",e)
            return False
        
        return True
