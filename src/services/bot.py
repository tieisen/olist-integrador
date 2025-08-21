import os
import time
import logging
from datetime import datetime
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.select import Select
from src.utils.log import Log

load_dotenv('keys/.env')
logger = logging.getLogger(__name__)
logging.basicConfig(filename=Log().buscar_path(),
                    encoding='utf-8',
                    format=os.getenv('LOGGER_FORMAT'),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

class Bot:
    def __init__(self):
        self.link_erp = os.getenv('OLIST_URL_ERP')
        self.link_estoque = os.getenv('OLIST_URL_ESTOQUE')
        self.link_produto = os.getenv('OLIST_URL_CAD_PRODUTO')
        self.link_logout = os.getenv('OLIST_URL_LOGOUT')
        self.time_sleep = float(os.getenv('REQ_TIME_SLEEP'))
        self.username = os.getenv('OLIST_BOT_USERNAME')
        self.password = os.getenv('OLIST_BOT_PASSWORD')
        self.timeout_lotes = int(os.getenv('OLIST_TIMEOUT_LANCA_LOTES'))

    async def login(self):
        try:
            driver = webdriver.Firefox()
            driver.maximize_window() 
            driver.get(self.link_erp)
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "username")))
            login_input = driver.find_element(By.ID, "username")
            next_button = driver.find_element(By.XPATH, "//button[@class='sc-dAlyuH biayZs sc-dAbbOL ddEnAE']")
            login_input.clear()
            login_input.send_keys(self.username)
            next_button.click()

            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "password")))
            pass_input = driver.find_element(By.ID, "password")
            submit_button = driver.find_element(By.XPATH, "//button[@class='sc-dAlyuH biayZs sc-dAbbOL ddEnAE']")
            pass_input.clear()
            pass_input.send_keys(self.password)
            submit_button.click()

            try:
                WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.XPATH, "//h3[@class='modal-title']")))
                elemento = driver.find_element(By.XPATH, "//h3[@class='modal-title']")
                if elemento.text == 'Este usuário já está logado em outro dispositivo':
                    btn_confirma_login = driver.find_element(By.XPATH, "//button[@class='btn btn-primary']")
                    btn_confirma_login.click()
                    WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.XPATH, "//div[@class='sidebar-menu-logo-usuario']")))
                    time.sleep(self.time_sleep)
            except:
                pass
            return True, driver
        except Exception as e:
            logger.error("Falha no login. %s",e)
            return False, None 

    async def logout(self,driver):
        driver.get(self.link_logout)
        driver.quit()        

    async def habilita_controle_lotes(self,driver,id_produto):
        driver.get(self.link_produto.format(id_produto))
        
        if WebDriverWait(driver, 10).until(EC.element_to_be_clickable(driver.find_element(By.XPATH, "//button[@class='btn btn-primary btn-edicao-item']"))):
            btn_editar = driver.find_element(By.XPATH, "//button[@class='btn btn-primary btn-edicao-item']")
            btn_editar.click()
        else:
            logger.error("Erro no botao editar")
            return False, driver
        
        if WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "controlarLotes"))):
            controle_lote = driver.find_element(By.ID, "controlarLotes")        
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
            controle_lote = Select(controle_lote)
            controle_lote.select_by_value('1')
            btn_salvar = driver.find_element(By.ID, "botaoSalvar")
            btn_salvar.click()
            time.sleep(self.time_sleep)
            return True, driver
        else:
            logger.error("Erro no selecionar opção")
            return False, driver

    async def desabilita_controle_lotes(self,driver,id_produto):
        driver.get(self.link_produto.format(id_produto))
        
        if WebDriverWait(driver, 10).until(EC.element_to_be_clickable(driver.find_element(By.XPATH, "//button[@class='btn btn-primary btn-edicao-item']"))):
            btn_editar = driver.find_element(By.XPATH, "//button[@class='btn btn-primary btn-edicao-item']")
            btn_editar.click()
        else:
            logger.error("Erro no botao editar")
            return False, driver
        
        if WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "controlarLotes"))):
            controle_lote = driver.find_element(By.ID, "controlarLotes")        
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(self.time_sleep)
            controle_lote = Select(controle_lote)
            controle_lote.select_by_value('0')
            btn_salvar = driver.find_element(By.ID, "botaoSalvar")
            btn_salvar.click()
            return True, driver
        else:
            logger.error("Erro no selecionar opção")
            return False, driver

    async def acessa_estoque_produto(self,driver,id_produto):
        try:
            driver.get(self.link_estoque.format(id_produto))
        except Exception as e:
            logger.error("Falha ao acessar estoque do produto. %s",e)
            return False, None
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "tabelalancamentos")))
        return True, driver

    async def busca_controle_lotes(self,driver):
        lotes = []
        try:
            driver.execute_script("trocarAba('lotes'); return false;")
            WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, "tabelaListagem")))
            rows_lotes = driver.find_elements(By.XPATH, "//tr[@class='cursor-pointer']")
            if rows_lotes == []:
                return lotes, driver
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
            return lotes, driver

    async def acessa_controle_lotes(self,driver,id_lancamento):
        try:
            driver.execute_script("trocarAba('lancamentos'); return false;")
            if WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.ID, "tabelalancamentos"))):
                lcto = driver.find_element(By.XPATH, f"//tr[@idlancamento='{id_lancamento}']")
                dt_lancamento = lcto.get_attribute('dataLancamento')
                dt_lancamento = datetime.strptime(dt_lancamento,'%Y-%m-%d %H:%M:%S').strftime('%d/%m/%Y - %H:%M')
                lcto.find_element(By.TAG_NAME, "button").click()
                WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.XPATH, f"//ul[@data-before='{dt_lancamento}']")))
                div_gerenciar_lotes = driver.find_element(By.XPATH, f"//ul[@data-before='{dt_lancamento}']")
                item_gerenciar_lotes = div_gerenciar_lotes.find_elements(By.XPATH, f"//li[@id='im_2']")
                item_gerenciar_lotes[-1].click()                
                return True, driver
        except Exception as e:
            logger.error("Falha ao acessar aba de controle de lotes do produto. %s",e)
            return False, None

    async def lanca_lotes_saida(self,driver,dados_lote):
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//form[@name='formLancarLotesSaida']")))
        # informa os lotes
        try:
            lotes_rows = driver.find_elements(By.XPATH,"//tr[@class='linha-lote-estoque']")
            idestoque = lotes_rows[0].get_attribute('uidestoque')
            idproduto = lotes_rows[0].get_attribute('idproduto')

            if len(lotes_rows) < len(dados_lote):
                # Adiciona linhas no formulario
                print("Adicionando linhas no formulario")
                for i in range(len(dados_lote)-1):
                    if WebDriverWait(driver, 10).until(EC.element_to_be_clickable(driver.find_element(By.XPATH, "//button[@class='btn btn-default btn-sm dropdown-toggle']"))):
                        btn_add = driver.find_element(By.XPATH, "//button[@class='btn btn-default btn-sm dropdown-toggle']")
                        btn_add.click()
                    else:
                        logger.error("Erro no botao adicionar")
                        return False, driver                        
                    if WebDriverWait(driver, 10).until(EC.element_to_be_clickable(driver.find_element(By.XPATH, "//a[@class='act-adicionar-linha-lote']"))):
                        add_lote = driver.find_element(By.XPATH, "//a[@class='act-adicionar-linha-lote']")
                        add_lote.click()
                    else:
                        logger.error("Erro no botao adicionar")
                        return False, driver                          

            if len(lotes_rows) > len(dados_lote):
                # Remove linhas do formulário
                print("Removendo linhas do formulário")
                for i in range(len(lotes_rows-dados_lote)-1):
                    if WebDriverWait(driver, 10).until(EC.element_to_be_clickable(driver.find_element(By.XPATH, "//button[@class='btn btn-default btn-sm dropdown-toggle']"))):
                        btn_remove = driver.find_elements(By.XPATH, "//button[@class='btn btn-default btn-sm dropdown-toggle']")
                        btn_remove[i].click()
                    else:
                        logger.error("Erro no botao remover")
                        return False, driver                          
                    if WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//ul[@class='dropdown-menu dropdown-menu-right']"))):
                        lista_opcoes_lote = driver.find_elements(By.XPATH, "//ul[@class='dropdown-menu dropdown-menu-right']")
                    else:
                        logger.error("Erro no botao remover")
                        return False, driver                           
                    if WebDriverWait(driver, 10).until(EC.element_to_be_clickable(driver.find_element(By.XPATH, "//a[@class='act-remover-linha-lote']"))):
                        del_lote = lista_opcoes_lote[i].find_element(By.XPATH, "//a[@class='act-remover-linha-lote']")
                        del_lote.click()
                    else:
                        logger.error("Erro no botao remover")
                        return False, driver                           
                    if WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[@popup-action-id='0']"))):
                        btn_confirma = driver.find_elements(By.XPATH, "//button[@popup-action-id='0']")
                        btn_confirma[0].click()
                    else:
                        logger.error("Erro no botao confirmar remover")
                        return False, driver                           

            for j, controle in enumerate(dados_lote):
                # Lança os dado do lote
                print("Informando os dados do lote")
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, f"//input[@name='lotes[{idproduto}][{idestoque}][{j}][quantidade]']")))
                lote_qtd = driver.find_element(By.XPATH, f"//input[@name='lotes[{idproduto}][{idestoque}][{j}][quantidade]']")
                lote_numero = driver.find_element(By.XPATH, f"//select[@name='lotes[{idproduto}][{idestoque}][{j}][numeroLote]']")    
                lote_numero = Select(lote_numero)
                try:
                    lote_numero.select_by_value(controle.get('numeroLote'))
                except:
                    lote_numero.select_by_value('Sem lote')
                lote_qtd.clear()
                lote_qtd.send_keys(abs(int(controle.get('quantidade'))))    

            btn_enviar = driver.find_element(By.XPATH,"//button[@class='btn btn-primary']")
            btn_enviar.click()

            try:
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//div[@style='max-height: 252px;']")))
                if WebDriverWait(driver, 10).until(EC.element_to_be_clickable(driver.find_element(By.XPATH, "//button[@popup-action='confirm']"))):
                    btn_confirma = driver.find_element(By.XPATH, "//button[@popup-action='confirm']")
                    btn_confirma.click()
            except:
                pass                
            
            return True, driver
        except Exception as e:
            logger.error("Erro ao informar os lotes na movimentação de saída: %s",e)
            return False, driver

    async def lanca_lotes_entrada(self,driver,dados_lote):
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//form[@name='formLancarLotesEntrada']")))
        # informa os lotes
        try:
            lotes_rows = driver.find_elements(By.XPATH,"//tr[@class='linha-lote-estoque']")
            idestoque = lotes_rows[0].get_attribute('idestoque')
            idproduto = lotes_rows[0].get_attribute('idproduto')
        except Exception as e:
            logger.error("Erro ao coletar os dados de estoque e produto. %s",e)
            return False, driver

        if len(lotes_rows) < len(dados_lote) and len(dados_lote) > 1:
            try:
                # Adiciona linhas no formulario
                for i in range(len(dados_lote)-1):
                    if WebDriverWait(driver, 10).until(EC.element_to_be_clickable(driver.find_element(By.XPATH, "//button[@class='btn btn-default btn-sm dropdown-toggle']"))):
                        time.sleep(self.time_sleep)
                        btn_add = driver.find_element(By.XPATH, "//button[@class='btn btn-default btn-sm dropdown-toggle']")
                        btn_add.click()                    
                    if WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//ul[@class='dropdown-menu dropdown-menu-right']"))):
                        time.sleep(self.time_sleep)
                        lista_opcoes_lote = driver.find_element(By.XPATH, "//ul[@class='dropdown-menu dropdown-menu-right']")
                        add_lote = lista_opcoes_lote.find_elements(By.TAG_NAME, "a")
                        add_lote[0].click()
            except Exception as e:
                logger.error("Erro ao adicionar linhas de lote. %s",e)
                return False, driver

        try:
            for j, controle in enumerate(dados_lote):
                # Lança os dado do lote                
                lote_codigo = driver.find_element(By.XPATH, f"//input[@name='lotes[{idproduto}][{idestoque}][{j}][numeroLote]']")
                lote_fabric = driver.find_element(By.XPATH, f"//input[@name='lotes[{idproduto}][{idestoque}][{j}][dataFabricacao]']")
                lote_valid = driver.find_element(By.XPATH, f"//input[@name='lotes[{idproduto}][{idestoque}][{j}][dataValidade]']")
                lote_qtd = driver.find_element(By.XPATH, f"//input[@name='lotes[{idproduto}][{idestoque}][{j}][quantidade]']")

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
            return False, driver

        try:
            btn_enviar = driver.find_element(By.XPATH,"//button[@class='btn btn-primary']")
            btn_enviar.text
            btn_enviar.click()            
        except Exception as e:
            logger.error("Erro ao enviar os lotes. %s",e)
            return False, driver
        
        return True, driver
