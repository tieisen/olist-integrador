from faker import Faker
from datetime import datetime, timedelta
import random

fake = Faker("pt_BR")  # opcional: usar contexto do Brasil

# ================= EMPRESA =================
def fake_empresa():
    return dict(
        codigo_snk=fake.random_int(min=10, max=9999),
        nome=fake.company(),
        cnpj=fake.cnpj(),
        serie_nfe=str(fake.random_int(min=1, max=100)),
        client_id=fake.uuid4(),
        client_secret=fake.uuid4(),
        admin_email=fake.email(),
        admin_senha=fake.password()
    )

# ================= PRODUTO =================
def fake_produto():
    return dict(
        cod_snk=fake.random_int(min=10000000, max=99999999),
        cod_olist=fake.random_int(min=10000000, max=99999999),
        empresa_id=fake.random_int(min=1, max=5)
    )

# ================= OLIST =================
def fake_olist():
    return dict(
        token_criptografado=fake.sha256(),
        dh_expiracao_token=datetime.now() + timedelta(days=7),
        refresh_token_criptografado=fake.sha256(),
        dh_expiracao_refresh_token=datetime.now() + timedelta(days=30),
        id_token_criptografado=fake.sha256(),
        empresa_id=fake.random_int(min=1, max=5)
    )

# ================= SANKHYA =================
def fake_sankhya():
    return dict(
        token_criptografado=fake.sha256(),
        dh_expiracao_token=datetime.now() + timedelta(days=7),
        empresa_id=fake.random_int(min=1, max=5)
    )

# ================= ECOMMERCE =================
def fake_ecommerce():
    return dict(
        id_loja=fake.random_int(min=1000, max=9999),
        nome=fake.company(),
        empresa_id=fake.random_int(min=1, max=5)
    )

# ================= PEDIDO =================
def fake_pedido():
    return dict(
        id_loja=fake.random_int(min=1, max=5),
        id_pedido=fake.random_int(min=100000, max=999999),
        cod_pedido=fake.uuid4(),
        num_pedido=fake.random_int(min=1, max=9999)        
    )

# ================= NOTA =================
def fake_nota():
    return dict(
        id_nota=fake.random_int(min=1000, max=9999),
        dh_emissao=datetime.now(),
        numero=fake.random_int(min=1000, max=9999),
        serie=fake.random_int(min=1, max=100),
        id_financeiro=fake.random_int(min=1000, max=9999),
        nunota=fake.random_int(min=1000, max=9999),
        pedido_id=fake.random_int(min=1, max=5)
    )

# ================= DEVOLUCAO =================
def fake_devolucao():
    return dict(
        id_nota=fake.random_int(min=1000, max=9999),
        dh_emissao=datetime.now(),
        numero=fake.random_int(min=1000, max=9999),
        serie=fake.random_int(min=1, max=100),
        chave_acesso=fake.bothify(text='####################'),        
        nunota=fake.random_int(min=1000, max=9999),
        nota_id=fake.random_int(min=1, max=5)
    )

# ================= LOG =================
def fake_log():
    return dict(
        contexto=fake.word(),
        de=fake.word(),
        para=fake.word(),
        sucesso=fake.boolean(),
        empresa_id=fake.random_int(min=1, max=5)
    )

# ================= LOG ESTOQUE =================
def fake_log_estoque():
    return dict(
        codprod=fake.random_int(min=1000, max=9999),
        idprod=fake.random_int(min=1000, max=9999),
        qtdmov=fake.random_int(min=1, max=100),
        status_estoque=fake.boolean(),
        status_lotes=fake.boolean(),
        obs=fake.sentence(),
        log_id=fake.random_int(min=1, max=5)
    )

# ================= LOG PEDIDO =================
def fake_log_pedido():
    return dict(
        evento=random.choice(['R', 'I', 'C', 'F', 'N', 'D']),
        status=fake.boolean(),
        obs=fake.sentence(),
        log_id=fake.random_int(min=1, max=5),
        pedido_id=fake.random_int(min=1, max=5)
    )

# ================= LOG PRODUTO =================
def fake_log_produto():
    return dict(
        codprod=fake.random_int(min=1000, max=9999),
        idprod=fake.random_int(min=1000, max=9999),
        campo=fake.word(),
        valor_old=fake.word(),
        valor_new=fake.word(),
        sucesso=fake.boolean(),
        obs=fake.sentence(),
        log_id=fake.random_int(min=1, max=5),
        produto_id=fake.random_int(min=1, max=5)
    )
