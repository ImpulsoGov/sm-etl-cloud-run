# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Configurações gerais de conexão com o banco de dados.

Atributos:
    SQLALCHEMY_DATABASE_URL: Cadeia de conexão com o banco de dados PostgreSQL.
    engine: Objeto de conexão entre o SQLAlchemy e o banco de dados.
    Base: Base para a definição de modelos objeto-relacionais (ORM) segundo no
        [paradigma declarativo do SQLAlchemy][sqlalchemy-declarativo].

[sqlalchemy-declarativo]: https://docs.sqlalchemy.org/en/13/orm/extensions/declarative/index.html
"""

# NECESSARIO PARA RODAR LOCALMENTE: Adiciona o caminho do diretório `sm_cloud_run` ao sys.path
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'sm_cloud_run')))
###

import os
from typing import Any, Final
import logging
import numpy as np
import sqlalchemy as sa
from dotenv import load_dotenv
from psycopg2.extensions import AsIs, register_adapter
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from google.cloud import secretmanager
import json

from utilitarios.bd_utilitarios import TabelasRefletidasDicionario

logging.info("Configurando interface com o banco de dados...")

logging.info("Obtendo parâmetros de conexão com o banco de dados...")
load_dotenv()

chave = "projects/567502497958/secrets/acesso_banco_analitico_saude_mental/versions/latest"
client = secretmanager.SecretManagerServiceClient()
response = client.access_secret_version(name=chave)

BD_HOST: Final[str] = json.loads(response.payload.data.decode("UTF-8"))["IMPULSOETL_BD_HOST"]
BD_PORTA: Final[int] = json.loads(response.payload.data.decode("UTF-8"))["IMPULSOETL_BD_PORTA"]
BD_NOME: Final[str] = json.loads(response.payload.data.decode("UTF-8"))["IMPULSOETL_BD_NOME"]
BD_USUARIO: Final[str] = json.loads(response.payload.data.decode("UTF-8"))["IMPULSOETL_BD_USUARIO"]
BD_SENHA: Final[str] = json.loads(response.payload.data.decode("UTF-8"))["IMPULSOETL_BD_SENHA"]

BD_URL: Final[sa.engine.URL] = sa.engine.URL.create(
    drivername="postgresql+psycopg2",
    username=BD_USUARIO,
    password=BD_SENHA,
    host=BD_HOST,
    port=BD_PORTA,
    database=BD_NOME,
)
logging.debug("Banco de dados: {uri}", uri=BD_URL.render_as_string())
logging.info("OK")

logging.info("Criando motor de conexão com o banco de dados...")
engine = sa.create_engine(BD_URL, pool_pre_ping=True)
logging.info("OK")

logging.info("Criando sessão...")
Sessao = sessionmaker(autocommit=False, autoflush=False, bind=engine)
logging.info("OK")

logging.info("Definindo metadados...")
meta = sa.MetaData(bind=engine)
logging.info("OK")

# obter esquemas diretamente do banco de dados e refletí-los como um dicionário
# contendo as classes de objetos equivalentes
logging.info(
    "Espelhando a estrutura das tabelas pré-existentes no banco de dados...",
)
tabelas = TabelasRefletidasDicionario(meta, views=True)
logging.info("OK")

logging.info("Criando base declarativa para a definição de novos modelos...")
Base = declarative_base(metadata=meta)
logging.info("OK")

logging.info("Definindo parâmetros para versionamento de tabelas...")
versionamento_parametros: dict[str, Any] = {
    "table_name": "%s_versoes",
    "transaction_column_name": "transacao_id",
    "end_transaction_column_name": "transacao_final_id",
    "operation_type_column_name": "transacao_tipo",
}
logging.debug("Parâmetros: {parametros}", parametros=versionamento_parametros)

logging.info("Interface com o banco de dados configurada com sucesso.")

logging.info("Configurando adaptadores tipos numpy no psycopg2...")
register_adapter(np.int64, AsIs)
register_adapter(np.float64, AsIs)
logging.info("OK.")
