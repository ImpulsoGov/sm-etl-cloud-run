# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT


"""Funções e classes úteis para interagir com o banco de dados da Impulso."""


from __future__ import annotations

# NECESSARIO PARA RODAR LOCALMENTE: Adiciona o caminho do diretório `sm_cloud_run` ao sys.path
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'sm_cloud_run')))
###

import csv
from io import StringIO
from typing import Iterable, cast, Literal
from datetime import datetime

import pandas as pd
from pandas.io.sql import SQLTable
from psycopg2.errors import Error as Psycopg2Error
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import DBAPIError, InvalidRequestError
from sqlalchemy.orm.session import Session
from sqlalchemy.schema import MetaData, Table
from sqlalchemy.sql import update, and_, select, insert
import logging


class TabelasRefletidasDicionario(dict):
    """Representa um dicionário de tabelas refletidas de um banco de dados."""

    def __init__(self, metadata_obj: MetaData, **kwargs):
        """Instancia um dicionário de tabelas refletidas de um banco de dados.

        Funciona exatamente como o dicionário de tabelas refletidas de um banco
        de dados acessível por meio da propriedade `tables` de um objeto
        [`sqlalchemy.schema.MetaData`][], com a exceção de que as chaves
        referentes a tabelas ou consultas ainda não refletidas são espelhadas
        sob demanda quando requisitadas pelo método `__getitem__()`
        (equivalente a obter a representação de uma tabela do dicionário
        chamando `dicionario["nome_do_schema.nome_da_tabela"]`).

        Argumentos:
            metadata_obj: instância da classe [`sqlalchemy.schema.MetaData`][]
                da biblioteca SQLAlchemy,
            **kwargs: Parâmetros adicionais a serem passados para o método
                [`reflect()`][] do objeto de metadados ao se tentar obter uma
                tabela ainda não espelhada no banco de dados.

        [`sqlalchemy.schema.MetaData`]: https://docs.sqlalchemy.org/en/14/core/metadata.html#sqlalchemy.schema.MetaData
        [`reflect()`][]: https://docs.sqlalchemy.org/en/14/core/metadata.html#sqlalchemy.schema.MetaData.reflect
        """
        self.meta = metadata_obj
        self.kwargs = kwargs

    def __getitem__(self, chave: str) -> Table:
        try:
            return self.meta.tables[chave]
        except (InvalidRequestError, KeyError):
            schema = None
            try:
                schema, tabela_nome = chave.split(".", maxsplit=1)
            except ValueError:
                tabela_nome = chave
            logging.debug(f"Espelhando tabela `{chave}`...")
            self.meta.reflect(schema=schema, only=[tabela_nome], **self.kwargs)
            logging.debug("OK.")
            return self.meta.tables[chave]

    def __setitem__(self, chave: str, valor: Table) -> None:
        self.meta.tables[chave] = valor

    def __repr__(self) -> str:
        return self.meta.tables.__repr__()

    def update(self, *args, **kwargs) -> None:
        for chave, valor in dict(*args, **kwargs).items():
            self[chave] = valor


def postgresql_copiar_dados(
    tabela_dados: SQLTable,
    conexao: Connection | Engine,
    colunas: Iterable[str],
    dados_iterador: Iterable,
) -> None:
    """Inserir dados em uma tabela usando o comando COPY do PostgreSQL.

    Esta função deve ser passada como valor para o argumento `method` do
    método [`pandas.DataFrame.to_sql()`][].

    Argumentos:
        tabela_dados: objeto [`pandas.io.sql.SQLTable`][] com a representação
            da tabela SQL a ser inserida.
        conexao: objeto [`sqlalchemy.engine.Engine`][] ou
            [`sqlalchemy.engine.Connection`][] contendo a conexão de alto nível
            com o banco de dados gerenciada pelo SQLAlchemy.
        colunas: lista com os nomes de colunas a serem inseridas.
        dados_iterador: Iterável com os dados a serem inseridos.

    Exceções:
        Levanta uma subclasse da exceção [`psycopg2.Error`][] caso algum erro
        seja retornado pelo backend.

    Veja também:
        - [Documentação][io-sql-method] do Pandas sobre a implementação de
        funções personalizadas de inserção.
        - [Artigo][insert-a-pandas-dataframe-into-postgres] com comparação da
        performance de métodos de inserção de DataFrames bancos de dados
        PostgreSQL.

    [`pandas.DataFrame.to_sql()`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_sql.html
    [`sqlalchemy.engine.Engine`]: https://docs.sqlalchemy.org/en/14/core/connections.html#sqlalchemy.engine.Engine
    [`sqlalchemy.engine.Connection`]: https://docs.sqlalchemy.org/en/14/core/connections.html#sqlalchemy.engine.Connection
    [`pandas.io.sql.SQLTable`]: https://github.com/pandas-dev/pandas/blob/a8968bfa696d51f73769c54f2630a9530488236a/pandas/io/sql.py#L762
    [`psycopg2.Error`]: https://www.psycopg.org/docs/module.html#psycopg2.Error
    [io-sql-method]: https://pandas.pydata.org/pandas-docs/stable/user_guide/io.html#io-sql-method
    [insert-a-pandas-dataframe-into-postgres]: https://ellisvalentiner.com/post/a-fast-method-to-insert-a-pandas-dataframe-into-postgres
    """

    try:
        # obter conexão de DBAPI a partir de uma conexão existente
        conector_dbapi = conexao.connection  # type: ignore
    except AttributeError:
        # obter conexão de DBAPI diretamente a partir da engine;
        # ver https://docs.sqlalchemy.org/en/14/core/connections.html
        # #working-with-the-dbapi-cursor-directly
        conector_dbapi = conexao.raw_connection()  # type: ignore

    with conector_dbapi.cursor() as cursor:  # type: ignore
        buffer = StringIO()
        writer = csv.writer(buffer)
        writer.writerows(dados_iterador)
        buffer.seek(0)

        enumeracao_colunas = ", ".join(
            '"{}"'.format(coluna) for coluna in colunas
        )
        if tabela_dados.schema:
            tabela_nome = "{}.{}".format(
                tabela_dados.schema,
                tabela_dados.name,
            )
        else:
            tabela_nome = tabela_dados.name

        expressao_sql = "COPY {} ({}) FROM STDIN WITH CSV".format(
            tabela_nome,
            enumeracao_colunas,
        )
        cursor.copy_expert(sql=expressao_sql, file=buffer)  # type: ignore

    return None


def carregar_dataframe(
    sessao: Session,
    df: pd.DataFrame,
    tabela_destino: str,
    passo: int | None = 10000,
    teste: bool = False,
) -> int:
    """Carrega dados públicos para o banco de dados analítico da ImpulsoGov.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        df: [`DataFrame`][] contendo os dados a serem carregados na tabela de
            destino, já no formato utilizado pelo banco de dados da ImpulsoGov.
        tabela_destino: nome da tabela de destino, qualificado com o nome do
            schema (formato `nome_do_schema.nome_da_tabela`).
        passo: Indica quantos registros devem ser enviados para a base de dados
            de cada vez. Por padrão, são inseridos 10.000 registros em cada
            transação. Se o valor for `None`, todo o DataFrame é carregado de
            uma vez.
        teste: Indica se o carregamento deve ser executado em modo teste. Se
            verdadeiro, faz *rollback* de todas as operações; se falso, libera
            o ponto de recuperação criado.

    Retorna:
        Código de saída do processo de carregamento. Se o carregamento
        for bem sucedido, o código de saída será `0`.

    Note:
        Esta função não faz *commit* das alterações do banco. Após o retorno
        desta função, o commit deve ser feito manualmente (método
        `sessao.commit()`) ou implicitamente por meio do término sem erros de
        um gerenciador de contexto (`with Sessao() as sessao: # ...`) no qual a
        função de carregamento tenha sido chamada.

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    [`DataFrame`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    [`transformar_pa()`]: impulsoetl.siasus.procedimentos.transformar_pa
    """

    num_registros = len(df)
    schema_nome, tabela_nome = tabela_destino.split(".", maxsplit=1)

    logging.info(
        f"Carregando {num_registros} registros para a tabela `{tabela_destino}`..."
    )

    logging.debug("Formatando colunas de data...")
    colunas_data = df.select_dtypes(include="datetime").columns
    df[colunas_data] = df[colunas_data].applymap(
        lambda dt: dt.isoformat() if pd.notna(dt) else None
    )

    # logging.info("Copiando registros...")

    ponto_de_recuperacao = sessao.begin_nested()
    conexao = sessao.connection()
    try:
        df.to_sql(
            name=tabela_nome,
            con=conexao,
            schema=schema_nome,
            if_exists="append",
            index=False,
            chunksize=passo,
            method=postgresql_copiar_dados,
        )
    # trata exceções levantadas pelo backend
    except (DBAPIError, Psycopg2Error) as erro:
        ponto_de_recuperacao.rollback()
        sessao.rollback()
        if isinstance(erro, DBAPIError):
            erro.hide_parameters = True
            erro = cast(Psycopg2Error, erro.orig)
        logging.error(
            "Erro ao inserir registros na tabela `{}` (Código {})",
            tabela_destino,
            erro.pgcode,
        )
        logging.debug(
            "({}.{}) {}",
            erro.__class__.__module__,
            erro.__class__.__name__,
            erro.pgerror,
        )
        return erro.pgcode
    else:
        ponto_de_recuperacao.commit()

    logging.info(f"Carregamento concluído em {tabela_destino}.")

    return 0


def validar_dataframe(
    df_transformada: pd.DataFrame
) -> pd.DataFrame:
    assert isinstance(df_transformada, pd.DataFrame), "Não é um DataFrame"
    assert len(df_transformada) > 0, "Dataframe vazio."



def deletar_conflitos(sessao, tabela_ref, ftp_arquivo_nome_df):
    """
    Deleta registros conflitantes na tabela de destino com base no nome do arquivo FTP.

    Args:
        sessao: Sessão ativa do banco de dados.
        tabela_ref: Referência à tabela de destino.
        ftp_arquivo_nome_df: Nome do arquivo FTP que serve como referência para deletar os registros.

    Returns:
        None
    """
    delete_result = sessao.execute(
        tabela_ref.delete()
        .where(tabela_ref.c.ftp_arquivo_nome == ftp_arquivo_nome_df)
    )
    num_deleted = delete_result.rowcount

    if num_deleted > 0:
        logging.info(f"Número de linhas deletadas: {num_deleted}")
    else:
        logging.info("Nenhum registro foi deletado.")


def inserir_timestamp_ftp_metadados(
    sessao: Session,
    uf_sigla: str, 
    periodo_data_inicio: datetime.date,
    coluna_atualizar: Literal['timestamp_etl_gcs', 'timestamp_load_bd', 'timestamp_etl_ftp_metadados'],
    tipo: Literal['PA', 'BI', 'PS', 'RD', 'HB', 'PF']
):
    """
    Insere um timestamp na tabela _saude_mental_configuracoes.sm_metadados_ftp quando os 
    parâmetros uf_sigla e periodo_data_inicio são iguais aos das colunas sigla_uf
    e processamento_periodo_data_inicio.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        uf_sigla (str): Sigla da Unidade Federativa.
        periodo_data_inicio (datetime.date): Data de início do período.
        tipo (str): Tipo do ETL a ser utilizado na condição de filtragem. 
            Valores aceitos:  
        coluna_atualizar (str): Nome da coluna a ser atualizada.

    Retorna:
        None
    """
    
    from utilitarios.bd_config import tabelas
    sm_metadados_ftp = tabelas["_saude_mental_configuracoes.sm_metadados_ftp"]

    try:
        timestamp_atual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Cria a requisição de atualização
        requisicao_atualizar = (
            update(sm_metadados_ftp)
            .where(and_(
                sm_metadados_ftp.c.tipo == tipo, 
                sm_metadados_ftp.c.sigla_uf == uf_sigla,
                sm_metadados_ftp.c.processamento_periodo_data_inicio == periodo_data_inicio
                ) 
            )
            .values({coluna_atualizar: timestamp_atual})
        )

        # Executa a atualização
        sessao.execute(requisicao_atualizar)
        sessao.commit()
        logging.info("Metadados do FTP inseridos com sucesso!")
        
    except Exception as e:
        # Em caso de erro, desfaz a transação
        sessao.rollback()
        print(f"Erro ao atualizar timestamp: {e}")



def inserir_timestamp_sisab_metadados(
    sessao: Session,
    periodo_data_inicio: datetime.date,
    municipios_painel: str, 
    coluna_atualizar: Literal['timestamp_etl_gcs', 'timestamp_load_bd'],
    tipo: Literal['SISAB_tipo_equipe', 'SISAB_resolutividade_condicao']
):
    """
    Insere ou atualiza um timestamp na tabela _saude_mental_configuracoes.sm_metadados_sisab 
    com base no valor de periodo_data_inicio.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        periodo_data_inicio (datetime.date): Data de início do período.
        coluna_atualizar (str): Nome da coluna do timestamp a ser atualizado.
        
    Retorna:
        None
    """
    from utilitarios.bd_config import tabelas

    sm_metadados_sisab = tabelas["_saude_mental_configuracoes.sm_metadados_sisab"]

    try:
        timestamp_atual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        requisicao_atualizar = (
            update(sm_metadados_sisab)
            .where(and_(
                sm_metadados_sisab.c.periodo_data_inicio == periodo_data_inicio),
                sm_metadados_sisab.c.tipo == tipo
            )
            .values({
                coluna_atualizar: timestamp_atual, 
                "municipios_id_sus": municipios_painel,
                })
        )
        sessao.execute(requisicao_atualizar)
        logging.info(f"Timestamp atualizado para o período {periodo_data_inicio}.")

        # Commit da transação
        sessao.commit()
        
    except Exception as e:
        # Em caso de erro, desfaz a transação
        sessao.rollback()
        logging.error(f"Erro ao inserir ou atualizar timestamp: {e}")
        raise
