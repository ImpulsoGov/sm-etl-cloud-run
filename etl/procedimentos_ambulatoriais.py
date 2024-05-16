from __future__ import annotations

import re
import shutil
import logging
import datetime
import argparse


from contextlib import closing
from ftplib import FTP, error_perm  # noqa: B402  # nosec: B402
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Generator, cast
from urllib.request import urlopen

import os
import pandas as pd
import numpy as np

from dbfread import DBF, FieldParser
from more_itertools import ichunked
from pysus.utilities.readdbc import dbc2dbf

from google.cloud import storage

import re
from ftplib import FTP
from typing import Generator

# Utilizado no tratamento
import janitor
from frozendict import frozendict
from uuid6 import uuid7
from sqlalchemy.orm import Session

from utilitarios.datasus_ftp import extrair_dbc_lotes
from utilitarios.datas import agora_gmt_menos3, periodo_por_data
from utilitarios.geografias import id_sus_para_id_impulso
from utilitarios.bd_config import Sessao

from utilitarios.logger_config import logger_config


# Para inserir resultados no GCS
def upload_to_bucket(bucket_name, blob_path, plain_text):
    bucket = storage.Client().bucket(bucket_name)
    blob = bucket.blob(blob_path)
    url = blob.upload_from_string(plain_text.encode('utf8'))
    return url


# set up logging to file
logger_config()



DE_PARA_PA: Final[frozendict] = frozendict(
    {
        "PA_CODUNI": "estabelecimento_id_scnes",
        "PA_GESTAO": "gestao_unidade_geografica_id_sus",
        "PA_CONDIC": "gestao_condicao_id_siasus",
        "PA_UFMUN": "unidade_geografica_id_sus",
        "PA_REGCT": "regra_contratual_id_scnes",
        "PA_INCOUT": "incremento_outros_id_sigtap",
        "PA_INCURG": "incremento_urgencia_id_sigtap",
        "PA_TPUPS": "estabelecimento_tipo_id_sigtap",
        "PA_TIPPRE": "prestador_tipo_id_sigtap",
        "PA_MN_IND": "estabelecimento_mantido",
        "PA_CNPJCPF": "estabelecimento_id_cnpj",
        "PA_CNPJMNT": "mantenedora_id_cnpj",
        "PA_CNPJ_CC": "receptor_credito_id_cnpj",
        "PA_MVM": "processamento_periodo_data_inicio",
        "PA_CMP": "realizacao_periodo_data_inicio",
        "PA_PROC_ID": "procedimento_id_sigtap",
        "PA_TPFIN": "financiamento_tipo_id_sigtap",
        "PA_SUBFIN": "financiamento_subtipo_id_sigtap",
        "PA_NIVCPL": "complexidade_id_siasus",
        "PA_DOCORIG": "instrumento_registro_id_siasus",
        "PA_AUTORIZ": "autorizacao_id_siasus",
        "PA_CNSMED": "profissional_id_cns",
        "PA_CBOCOD": "profissional_vinculo_ocupacao_id_cbo2002",
        "PA_MOTSAI": "desfecho_motivo_id_siasus",
        "PA_OBITO": "obito",
        "PA_ENCERR": "encerramento",
        "PA_PERMAN": "permanencia",
        "PA_ALTA": "alta",
        "PA_TRANSF": "transferencia",
        "PA_CIDPRI": "condicao_principal_id_cid10",
        "PA_CIDSEC": "condicao_secundaria_id_cid10",
        "PA_CIDCAS": "condicao_associada_id_cid10",
        "PA_CATEND": "carater_atendimento_id_siasus",
        "PA_IDADE": "usuario_idade",
        "IDADEMIN": "procedimento_idade_minima",
        "IDADEMAX": "procedimento_idade_maxima",
        "PA_FLIDADE": "compatibilidade_idade_id_siasus",
        "PA_SEXO": "usuario_sexo_id_sigtap",
        "PA_RACACOR": "usuario_raca_cor_id_siasus",
        "PA_MUNPCN": "usuario_residencia_municipio_id_sus",
        "PA_QTDPRO": "quantidade_apresentada",
        "PA_QTDAPR": "quantidade_aprovada",
        "PA_VALPRO": "valor_apresentado",
        "PA_VALAPR": "valor_aprovado",
        "PA_UFDIF": "atendimento_residencia_ufs_distintas",
        "PA_MNDIF": "atendimento_residencia_municipios_distintos",
        "PA_DIF_VAL": "procedimento_valor_diferenca_sigtap",
        "NU_VPA_TOT": "procedimento_valor_vpa",
        "NU_PA_TOT": "procedimento_valor_sigtap",
        "PA_INDICA": "aprovacao_status_id_siasus",
        "PA_CODOCO": "ocorrencia_id_siasus",
        "PA_FLQT": "erro_quantidade_apresentada_id_siasus",
        "PA_FLER": "erro_apac",
        "PA_ETNIA": "usuario_etnia_id_sus",
        "PA_VL_CF": "complemento_valor_federal",
        "PA_VL_CL": "complemento_valor_local",
        "PA_VL_INC": "incremento_valor",
        "PA_SRV_C": "servico_especializado_id_scnes",
        "PA_INE": "equipe_id_ine",
        "PA_NAT_JUR": "estabelecimento_natureza_juridica_id_scnes",
    },
)

TIPOS_PA: Final[frozendict] = frozendict(
    {
        "estabelecimento_id_scnes": "object",
        "gestao_unidade_geografica_id_sus": "object",
        "gestao_condicao_id_siasus": "object",
        "unidade_geografica_id_sus": "object",
        "regra_contratual_id_scnes": "object",
        "incremento_outros_id_sigtap": "object",
        "incremento_urgencia_id_sigtap": "object",
        "estabelecimento_tipo_id_sigtap": "object",
        "prestador_tipo_id_sigtap": "object",
        "estabelecimento_mantido": "bool",
        "estabelecimento_id_cnpj": "object",
        "mantenedora_id_cnpj": "object",
        "receptor_credito_id_cnpj": "object",
        "processamento_periodo_data_inicio": "datetime64[ns]",
        "realizacao_periodo_data_inicio": "datetime64[ns]",
        "procedimento_id_sigtap": "object",
        "financiamento_tipo_id_sigtap": "object",
        "financiamento_subtipo_id_sigtap": "object",
        "complexidade_id_siasus": "object",
        "instrumento_registro_id_siasus": "object",
        "autorizacao_id_siasus": "object",
        "profissional_id_cns": "object",
        "profissional_vinculo_ocupacao_id_cbo2002": "object",
        "desfecho_motivo_id_siasus": "object",
        "obito": "bool",
        "encerramento": "bool",
        "permanencia": "bool",
        "alta": "bool",
        "transferencia": "bool",
        "condicao_principal_id_cid10": "object",
        "condicao_secundaria_id_cid10": "object",
        "condicao_associada_id_cid10": "object",
        "carater_atendimento_id_siasus": "object",
        "usuario_idade": "Int64",
        "procedimento_idade_minima": "Int64",
        "procedimento_idade_maxima": "Int64",
        "compatibilidade_idade_id_siasus": "object",
        "usuario_sexo_id_sigtap": "object",
        "usuario_raca_cor_id_siasus": "object",
        "usuario_residencia_municipio_id_sus": "object",
        "quantidade_apresentada": "Int64",
        "quantidade_aprovada": "Int64",
        "valor_apresentado": "Float64",
        "valor_aprovado": "Float64",
        "atendimento_residencia_ufs_distintas": "bool",
        "atendimento_residencia_municipios_distintos": "bool",
        "procedimento_valor_diferenca_sigtap": "Float64",
        "procedimento_valor_vpa": "Float64",
        "procedimento_valor_sigtap": "Float64",
        "aprovacao_status_id_siasus": "object",
        "ocorrencia_id_siasus": "object",
        "erro_quantidade_apresentada_id_siasus": "object",
        "erro_apac": "object",
        "usuario_etnia_id_sus": "object",
        "complemento_valor_federal": "Float64",
        "complemento_valor_local": "Float64",
        "incremento_valor": "Float64",
        "servico_id_sigtap": "object",
        "servico_classificacao_id_sigtap": "object",
        "equipe_id_ine": "object",
        "estabelecimento_natureza_juridica_id_scnes": "object",
        "id": "object",
        "periodo_id": "object",
        "unidade_geografica_id": "object",
        "criacao_data": "datetime64[ns]",
        "atualizacao_data": "datetime64[ns]",
    },
)

COLUNAS_DATA_AAAAMM: Final[list[str]] = [
    "realizacao_periodo_data_inicio",
    "processamento_periodo_data_inicio",
]

COLUNAS_NUMERICAS: Final[list[str]] = [
    nome_coluna
    for nome_coluna, tipo_coluna in TIPOS_PA.items()
    if tipo_coluna.lower() == "int64" or tipo_coluna.lower() == "float64"
]


def _para_booleano(valor: str) -> bool | float:
    """Transforma um valor binário '0' ou '1' em booleano. Suporta NaNs."""
    if valor == "0":
        return False
    elif valor == "1":
        return True
    else:
        return np.nan


def extrair_pa(
    uf_sigla: str,
    periodo_data_inicio: datetime.date,
    passo: int = 100000,
) -> Generator[pd.DataFrame, None, None]:
    """Extrai registros de procedimentos ambulatoriais do FTP do DataSUS.

    Argumentos:
        uf_sigla: Sigla da Unidade Federativa cujos procedimentos se pretende
            obter.
        periodo_data_inicio: Dia de início da competência desejada,
            representado como um objeto [`datetime.date`][].
        passo: Número de registros que devem ser convertidos em DataFrame a
            cada iteração.

    Gera:
        A cada iteração, devolve um objeto [`pandas.DataFrames`][] com um
        trecho do arquivo de procedimentos ambulatoriais lido e convertido.

    [`pandas.DataFrame`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    [`datetime.date`]: https://docs.python.org/3/library/datetime.html#date-objects
    """

    arquivo_padrao = "PA{uf_sigla}{periodo_data_inicio:%y%m}[a-z]?.dbc".format(
        uf_sigla=uf_sigla,
        periodo_data_inicio=periodo_data_inicio,
    )

    return extrair_dbc_lotes(
        ftp="ftp.datasus.gov.br",
        caminho_diretorio="/dissemin/publicos/SIASUS/200801_/Dados",
        arquivo_nome=re.compile(arquivo_padrao, re.IGNORECASE),
        passo=passo,
    )


def transformar_pa(
    sessao: Session,
    pa: pd.DataFrame,
) -> pd.DataFrame:
    """Transforma um `DataFrame` de procedimentos ambulatoriais do SIASUS.
    
    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        pa: objeto [`pandas.DataFrame`][] contendo os dados de um arquivo de
            disseminação de procedimentos ambulatoriais do SIASUS, conforme
            extraídos para uma unidade federativa e competência (mês) pela
            função [`extrair_pa()`][].
        condicoes: conjunto opcional de condições a serem aplicadas para
            filtrar os registros obtidos da fonte. O valor informado deve ser
            uma *string* com a sintaxe utilizada pelo método
            [`pandas.DataFrame.query()`][]. Por padrão, o valor do argumento é
            `None`, o que equivale a não aplicar filtro algum.

    Note:
        Para otimizar a performance, os filtros são aplicados antes de qualquer
        outra transformação nos dados, de forma que as condições fornecidas
        devem considerar que o nome, os tipos e os valores aparecem exatamente
        como registrados no arquivo de disseminação disponibilizado no FTP
        público do DataSUS. Verifique o [Informe Técnico][it-siasus] para mais
        informações.

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    [`pandas.DataFrame`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    [`extrair_pa()`]: impulsoetl.siasus.procedimentos.extrair_pa
    [`pandas.DataFrame.query()`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.query.html
    [it-siasus]: https://drive.google.com/file/d/1DC5093njSQIhMHydYptlj2rMbrMF36y6
    """
    logging.info(
        f"Transformando DataFrame com {len(pa)} procedimentos "
        + "ambulatoriais."
    )
    memoria_usada=pa.memory_usage(deep=True).sum() / 10 ** 6
    logging.debug(
        f"Memória ocupada pelo DataFrame original:  {memoria_usada:.2f} mB."
    )

    # aplica condições de filtragem dos registros
    condicoes = "(PA_TPUPS == '70') or PA_PROC_ID.str.startswith('030106') or PA_PROC_ID.str.startswith('030107') or PA_PROC_ID.str.startswith('030108') or PA_CIDPRI.str.startswith('F') or PA_CIDPRI.str.startswith('F') or PA_CIDPRI.str.startswith('X6') or PA_CIDPRI.str.startswith('X7') or PA_CIDPRI.str.contains('^X8[0-4][0-9]*') or PA_CIDPRI.str.startswith('R78') or PA_CIDPRI.str.startswith('T40') or (PA_CIDPRI == 'Y870') or PA_CIDPRI.str.startswith('Y90') or PA_CIDPRI.str.startswith('Y91') or (PA_CBOCOD in ['223905', '223915', '225133', '223550', '239440', '239445', '322220']) or PA_CBOCOD.str.startswith('2515') or (PA_CATEND == '02')"
    logging.info(
        f"Filtrando DataFrame com {len(pa)} procedimentos "
        + "ambulatoriais.",
    )
    pa = pa.query(condicoes, engine="python")
    logging.info(
        f"Registros após aplicar filtro: {len(pa)}."
    )

    pa_transformada = (
        pa  # noqa: WPS221  # ignorar linha complexa no pipeline
        # renomear colunas
        .rename_columns(function=lambda col: col.strip())
        .rename_columns(DE_PARA_PA)
        # processar colunas com datas
        .transform_columns(
            COLUNAS_DATA_AAAAMM,
            function=lambda dt: pd.to_datetime(
                dt,
                format="%Y%m",
                errors="coerce",
            ),
        )
        # tratar como NA colunas com valores nulos
        .replace("", np.nan)
        .transform_columns(
            [
                "regra_contratual_id_scnes",
                "incremento_outros_id_sigtap",
                "incremento_urgencia_id_sigtap",
                "mantenedora_id_cnpj",
                "receptor_credito_id_cnpj",
                "financiamento_subtipo_id_sigtap",
                "condicao_principal_id_cid10",
                "autorizacao_id_siasus",
                "profissional_id_cns",
                "condicao_principal_id_cid10",
                "condicao_secundaria_id_cid10",
                "condicao_associada_id_cid10",
                "desfecho_motivo_id_siasus",
                "usuario_sexo_id_sigtap",
                "usuario_raca_cor_id_siasus",
            ],
            function=lambda elemento: (
                np.nan
                if pd.notna(elemento)
                and all(digito == "0" for digito in elemento)
                else elemento
            ),
        )
        .transform_columns(
            [
                "carater_atendimento_id_siasus",
                "usuario_residencia_municipio_id_sus",
                "atendimento_residencia_ufs_distintas",
                "atendimento_residencia_municipios_distintos",
            ],
            function=lambda elemento: (
                np.nan
                if pd.isna(elemento)
                or all(digito == "9" for digito in elemento)
                else elemento
            ),
        )
        .update_where(
            "usuario_idade == '999'",
            target_column_name="usuario_idade",
            target_val=np.nan,
        )
        # processar colunas lógicas
        .transform_column(
            "estabelecimento_mantido",
            function=lambda elemento: True if elemento == "M" else False,
        )
        .transform_columns(
            [
                "obito",
                "encerramento",
                "permanencia",
                "alta",
                "transferencia",
                "atendimento_residencia_ufs_distintas",
                "atendimento_residencia_municipios_distintos",
            ],
            function=_para_booleano,
        )
        .update_where(
            "@pd.isna(desfecho_motivo_id_siasus)",
            target_column_name=[
                "obito",
                "encerramento",
                "permanencia",
                "alta",
                "transferencia",
            ],
            target_val=np.nan,
        )
        # separar código do serviço e código da classificação do serviço
        .transform_column(
            "servico_especializado_id_scnes",
            function=lambda cod: cod[:3] if pd.notna(cod) else np.nan,
            dest_column_name="servico_id_sigtap",
        )
        .transform_column(
            "servico_especializado_id_scnes",
            function=lambda cod: cod[3:] if pd.notna(cod) else np.nan,
            dest_column_name="servico_classificacao_id_sigtap",
        )
        .remove_columns("servico_especializado_id_scnes")
        # adicionar id
        .add_column("id", str())
        .transform_column("id", function=lambda _: uuid7().hex)


        # adicionar id do periodo
        .transform_column(
            "realizacao_periodo_data_inicio",
            function=lambda dt: periodo_por_data(sessao=sessao, data=dt).id,
            dest_column_name="periodo_id",
        )

        # adicionar id da unidade geografica
        .transform_column(
            "unidade_geografica_id_sus",
            function=lambda id_sus: id_sus_para_id_impulso(
                sessao=sessao,
                id_sus=id_sus,
            ),
            dest_column_name="unidade_geografica_id",
        )


        # adicionar datas de inserção e atualização
        .add_column("criacao_data", agora_gmt_menos3())
        .add_column("atualizacao_data", agora_gmt_menos3())


        # # garantir tipos
        # .change_type(
        #     # HACK: ver https://github.com/pandas-dev/pandas/issues/25472
        #     COLUNAS_NUMERICAS,
        #     "float",
        # )
        # .astype(TIPOS_PA)
    )
    memoria_usada = pa_transformada.memory_usage(deep=True).sum() / 10 ** 6
    logging.debug(        
        f"Memória ocupada pelo DataFrame transformado: {memoria_usada:.2f} mB."
    )
    return pa_transformada




# def baixar_e_processar_pa(uf_sigla: str, periodo_data_inicio: datetime.date):
#     """
#     Baixa e processa dados de procedimentos ambulatoriais do SIASUS para uma 
#     determinada Unidade Federativa e período de tempo.

#     Argumentos:
#         uf_sigla (str): A sigla da Unidade Federativa para a qual os dados de 
#         procedimentos ambulatoriais serão baixados e processados.
        
#         periodo_data_inicio (datetime.date): A data de início do período de 
#         tempo para o qual os dados serão obtidos. Deve ser fornecida como um 
#         objeto `datetime.date`.

#     Retorna:
#         dict: Um dicionário contendo informações sobre o status da operação, 
#         o estado, o período, o caminho do arquivo final no Google Cloud Storage 
#         (GCS) e a lista de arquivos originais DBC capturados.

#     A função se conecta ao FTP do DataSUS para obter os arquivos de procedimentos 
#     ambulatoriais correspondentes à Unidade Federativa e ao período de tempo 
#     fornecidos. Em seguida, os dados são processados conforme especificações do 
#     Informe Técnico do SIASUS, incluindo renomeação de colunas, tratamento de 
#     valores nulos, adição de IDs e datas, entre outros.

#     Após o processamento, os dados são salvos localmente em um arquivo CSV e 
#     carregados para o Google Cloud Storage (GCS). O caminho do arquivo final 
#     no GCS e a lista de arquivos originais DBC capturados são incluídos no 
#     dicionário de retorno, juntamente com informações sobre o estado e o 
#     período dos dados processados.
#     """

#     # Extrair dados
#     df_dados_todos = []
#     arquivos_capturados = []
#     session = Sessao()

#     for arquivo_dbc, _, df_dados in extrair_pa(
#         uf_sigla=uf_sigla,
#         periodo_data_inicio=periodo_data_inicio,
#     ):
#         df_dados = transformar_pa(session, df_dados)
#         df_dados_todos.append(df_dados)
#         arquivos_capturados.append(arquivo_dbc)

#     # Concatenar DataFrames
#     df_dados_final = pd.concat(df_dados_todos)

#     # Salvar localmente
#     nome_arquivo_csv = f"siasus_procedimentos_disseminacao_{uf_sigla}_{periodo_data_inicio:%y%m}.csv"
#     df_dados_final.to_csv(nome_arquivo_csv, index=False)

#     path_gcs = f"saude-mental/dados-publicos/siasus/{uf_sigla}/{nome_arquivo_csv}"
#     # Salvar no GCS
#     upload_to_bucket(
#         bucket_name="camada-bronze", 
#         blob_path=path_gcs,
#         plain_text=df_dados_final.to_csv()
#     )

#     response = {
#         "status": "OK",
#         "estado": uf_sigla,
#         "periodo": f"{periodo_data_inicio:%y%m}",
#         "arquivo_final_gcs": f"gcs://camada-bronze/{path_gcs}",
#         "arquivos_origem_dbc": list(set(arquivos_capturados)),
#     }

#     session.close()

#     return response



def baixar_e_processar_pa(uf_sigla: str, periodo_data_inicio: datetime.date):
    """
    Baixa e processa dados de procedimentos ambulatoriais do SIASUS para uma 
    determinada Unidade Federativa e período de tempo.

    Argumentos:
        uf_sigla (str): A sigla da Unidade Federativa para a qual os dados de 
        procedimentos ambulatoriais serão baixados e processados.
        
        periodo_data_inicio (datetime.date): A data de início do período de 
        tempo para o qual os dados serão obtidos. Deve ser fornecida como um 
        objeto `datetime.date`.

    Retorna:
        dict: Um dicionário contendo informações sobre o status da operação, 
        o estado, o período, o caminho do arquivo final no Google Cloud Storage 
        (GCS) e a lista de arquivos originais DBC capturados.

    A função se conecta ao FTP do DataSUS para obter os arquivos de procedimentos 
    ambulatoriais correspondentes à Unidade Federativa e ao período de tempo 
    fornecidos. Em seguida, os dados são processados conforme especificações do 
    Informe Técnico do SIASUS, incluindo renomeação de colunas, tratamento de 
    valores nulos, adição de IDs e datas, entre outros.

    Após o processamento, os dados são salvos localmente em um arquivo CSV e 
    carregados para o Google Cloud Storage (GCS). O caminho do arquivo final 
    no GCS e a lista de arquivos originais DBC capturados são incluídos no 
    dicionário de retorno, juntamente com informações sobre o estado e o 
    período dos dados processados.
    """

    # Extrair dados
    df_dados_todos = []
    # arquivos_capturados = []
    session = Sessao()

    for df_dados in extrair_pa(
        uf_sigla=uf_sigla,
        periodo_data_inicio=periodo_data_inicio,
    ):
        df_dados = transformar_pa(session, df_dados)
        df_dados_todos.append(df_dados)
        # arquivos_capturados.append(arquivo_dbc)

    # Concatenar DataFrames
    df_dados_final = pd.concat(df_dados_todos)

    # Salvar localmente
    nome_arquivo_csv = f"siasus_procedimentos_disseminacao_{uf_sigla}_{periodo_data_inicio:%y%m}.csv"
    df_dados_final.to_csv(nome_arquivo_csv, index=False)

    path_gcs = f"saude-mental/dados-publicos/siasus/{uf_sigla}/{nome_arquivo_csv}"
    # Salvar no GCS
    upload_to_bucket(
        bucket_name="camada-bronze", 
        blob_path=path_gcs,
        plain_text=df_dados_final.to_csv()
    )

    response = {
        "status": "OK",
        "estado": uf_sigla,
        "periodo": f"{periodo_data_inicio:%y%m}",
        "arquivo_final_gcs": f"gcs://camada-bronze/{path_gcs}",
    }

    session.close()

    return response