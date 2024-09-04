from __future__ import annotations

# NECESSARIO PARA RODAR LOCALMENTE: Adiciona o caminho do diretório `sm_cloud_run` ao sys.path
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'sm_cloud_run')))
###

import re
import logging
import datetime
import numpy as np
import pandas as pd
from typing import Generator, Final
from sqlalchemy.orm import Session

# Utilizado no tratamento
import janitor
from frozendict import frozendict
from uuid6 import uuid7
from utilitarios.config_painel_sm import municipios_painel, condicoes_bpa_i
from utilitarios.datas import agora_gmt_menos3, periodo_por_data, de_aaaammdd_para_timestamp
from utilitarios.geografias import id_sus_para_id_impulso
from utilitarios.bd_utilitarios import inserir_timestamp_ftp_metadados

# Utilitarios
from utilitarios.datasus_ftp import extrair_dbc_lotes
from utilitarios.bd_config import Sessao
from utilitarios.cloud_storage import upload_to_bucket
from utilitarios.logger_config import logger_config

# set up logging to file
logger_config()


DE_PARA_BPA_I: Final[frozendict] = frozendict(
    {
        "CODUNI": "estabelecimento_id_scnes",
        "GESTAO": "gestao_unidade_geografica_id_sus",
        "CONDIC": "gestao_condicao_id_siasus",
        "UFMUN": "unidade_geografica_id_sus",
        "TPUPS": "estabelecimento_tipo_id_sigtap",
        "TIPPRE": "prestador_tipo_id_sigtap",
        "MN_IND": "estabelecimento_mantido",
        "CNPJCPF": "estabelecimento_id_cnpj",
        "CNPJMNT": "mantenedora_id_cnpj",
        "CNPJ_CC": "receptor_credito_id_cnpj",
        "DT_PROCESS": "processamento_periodo_data_inicio",
        "DT_ATEND": "realizacao_periodo_data_inicio",
        "PROC_ID": "procedimento_id_sigtap",
        "TPFIN": "financiamento_tipo_id_sigtap",
        "SUBFIN": "financiamento_subtipo_id_sigtap",
        "COMPLEX": "complexidade_id_siasus",
        "AUTORIZ": "autorizacao_id_siasus",
        "CNSPROF": "profissional_id_cns",
        "CBOPROF": "profissional_vinculo_ocupacao_id_cbo2002",
        "CIDPRI": "condicao_principal_id_cid10",
        "CATEND": "carater_atendimento_id_siasus",
        "CNS_PAC": "usuario_id_cns_criptografado",
        "DTNASC": "usuario_nascimento_data",
        "TPIDADEPAC": "usuario_idade_tipo_id_sigtap",
        "IDADEPAC": "usuario_idade",
        "SEXOPAC": "usuario_sexo_id_sigtap",
        "RACACOR": "usuario_raca_cor_id_siasus",
        "MUNPAC": "usuario_residencia_municipio_id_sus",
        "QT_APRES": "quantidade_apresentada",
        "QT_APROV": "quantidade_aprovada",
        "VL_APRES": "valor_apresentado",
        "VL_APROV": "valor_aprovado",
        "UFDIF": "atendimento_residencia_ufs_distintas",
        "MNDIF": "atendimento_residencia_municipios_distintos",
        "ETNIA": "usuario_etnia_id_sus",
        "NAT_JUR": "estabelecimento_natureza_juridica_id_scnes",
    },
)

COLUNAS_DATA_AAAAMMDD: Final[list[str]] = [
    "usuario_nascimento_data",
]

COLUNAS_DATA_AAAAMM: Final[list[str]] = [
    "realizacao_periodo_data_inicio",
    "processamento_periodo_data_inicio",
]


def extrair_bpa_i(
    uf_sigla: str,
    periodo_data_inicio: datetime.date,
    passo: int = 100000,
) -> Generator[pd.DataFrame, None, None]:
    """Extrai registros de Boletins de Produção Ambulatorial do FTP do DataSUS.

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

    arquivo_padrao = "BI{uf_sigla}{periodo_data_inicio:%y%m}(?:_\d+)?.dbc".format(
        uf_sigla=uf_sigla,
        periodo_data_inicio=periodo_data_inicio,
    )

    yield from extrair_dbc_lotes(
        ftp="ftp.datasus.gov.br",
        caminho_diretorio="/dissemin/publicos/SIASUS/200801_/Dados",
        arquivo_nome=re.compile(arquivo_padrao, re.IGNORECASE),
        passo=passo,
    )


def transformar_bpa_i(
    sessao: Session,
    bpa_i: pd.DataFrame,
    uf_sigla: str,
    periodo_data_inicio: datetime.date,
) -> pd.DataFrame:
    """Transforma um `DataFrame` de BPA-i obtido do FTP público do DataSUS.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        bpa_i: objeto [`pandas.DataFrame`][] contendo os dados de um arquivo de
            disseminação de Boletins de Produção Ambulatorial -
            individualizados, conforme extraídos para uma unidade federativa e
            competência (mês) pela função [`extrair_bpa_i()`][].

    Note:
        Para otimizar a performance, os filtros são aplicados antes de qualquer
        outra transformação nos dados, de forma que as condições fornecidas
        devem considerar que o nome, os tipos e os valores aparecem exatamente
        como registrados no arquivo de disseminação disponibilizado no FTP
        público do DataSUS. Verifique o [Informe Técnico][it-siasus] para mais
        informações.

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    [`pandas.DataFrame`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    [`extrair_bpa_i()`]: impulsoetl.siasus.bpa_i.extrair_bpa_i
    [`pandas.DataFrame.query()`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.query.html
    [it-siasus]: https://drive.google.com/file/d/1DC5093njSQIhMHydYptlj2rMbrMF36y6
    """
    logging.info(
        f"Transformando DataFrame com {len(bpa_i)} registros "
        + "de BPA-i."
    )

    memoria_usada=bpa_i.memory_usage(deep=True).sum() / 10 ** 6
    logging.debug(
        f"Memória ocupada pelo DataFrame original:  {memoria_usada:.2f} mB."
    )


    # aplica filtragem para municípios participantes (procedimento registrado no muni ou paciente residente no muni)
    filtragem_municipios = f"(UFMUN in {municipios_painel}) or (MUNPAC in {municipios_painel})"
    bpa_i = bpa_i.query(filtragem_municipios, engine="python")
    logging.info(
        f"Registros após aplicar filtro de seleção de municípios: {len(bpa_i)}."
    )


    # aplica condições de filtragem de SM nos registros
    logging.info(
        f"Filtrando DataFrame com {len(bpa_i)} registros de BPA-i.",
    )
    bpa_i = bpa_i.query(condicoes_bpa_i, engine="python")
    logging.info(
        f"Registros após aplicar filtro: {len(bpa_i)}."
    )
    

    bpa_i_transformada = (
        bpa_i  # noqa: WPS221  # ignorar linha complexa no pipeline
        # renomear colunas
        .rename_columns(function=lambda col: col.strip())
        .rename_columns(DE_PARA_BPA_I)
        # processar colunas com datas
        .transform_columns(
            COLUNAS_DATA_AAAAMM,
            function=lambda dt: pd.to_datetime(
                dt,
                format="%Y%m",
                errors="coerce",
            ),
        )
        .transform_columns(
            COLUNAS_DATA_AAAAMMDD,
            function=lambda dt: de_aaaammdd_para_timestamp(dt, erros="coerce"),
        )
        # processar colunas lógicas
        .transform_column(
            "estabelecimento_mantido",
            function=lambda elemento: True if elemento == "M" else False,
        )
        .transform_columns(
            [
                "atendimento_residencia_ufs_distintas",
                "atendimento_residencia_municipios_distintos",
            ],
            function=lambda elemento: True if elemento == "1" else False,
        )
        # tratar como NA colunas com valores nulos
        .replace("", np.nan)
        .transform_columns(
            [
                "mantenedora_id_cnpj",
                "receptor_credito_id_cnpj",
                "financiamento_subtipo_id_sigtap",
                "condicao_principal_id_cid10",
                "autorizacao_id_siasus",
            ],
            function=lambda elemento: (
                np.nan
                if all(digito == "0" for digito in elemento)
                else elemento
            ),
        )
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

        # adicionar coluna ftp_arquivo_nome
        .add_column(
            "ftp_arquivo_nome",
            f"BI{uf_sigla}{periodo_data_inicio:%y%m}"
        )

    )
    memoria_usada=(bpa_i_transformada.memory_usage(deep=True).sum() / 10 ** 6)
    logging.debug(
        "Memória ocupada pelo DataFrame transformado: {memoria_usada:.2f} mB."
    )
    return bpa_i_transformada



def baixar_e_processar_bpa_i(uf_sigla: str, periodo_data_inicio: datetime.date):
    """
    Baixa e processa dados de registros de BPA Individualizado do SIASUS para uma 
    determinada Unidade Federativa e período de tempo.

    Argumentos:
        uf_sigla (str): A sigla da Unidade Federativa para a qual os dados de 
        BPA Individualizado serão baixados e processados.
        
        periodo_data_inicio (datetime.date): A data de início do período de 
        tempo para o qual os dados serão obtidos. Deve ser fornecida como um 
        objeto `datetime.date`.

    Retorna:
        dict: Um dicionário contendo informações sobre o status da operação, 
        o estado, o período, o caminho do arquivo final no Google Cloud Storage 
        (GCS) e a lista de arquivos originais DBC capturados.

    A função se conecta ao FTP do DataSUS para obter os arquivos de BPA-i 
    correspondentes à Unidade Federativa e ao período de tempo fornecidos. 
    Em seguida, os dados são processados conforme especificações do 
    Informe Técnico do SIASUS, incluindo renomeação de colunas, tratamento de 
    valores nulos, adição de IDs e datas, entre outros.

    Após o processamento, os dados são carregados em um arquivo CSV para o 
    Google Cloud Storage (GCS). O caminho do arquivo final no GCS e a lista 
    de arquivos originais DBC capturados são incluídos no dicionário de retorno, 
    juntamente com informações sobre o estado e o período dos dados processados.
    """

    # Extrair dados
    sessao = Sessao()
    
    bpa_i_lotes = extrair_bpa_i(
        uf_sigla=uf_sigla,
        periodo_data_inicio=periodo_data_inicio,
    )

    dfs_transformados = []
    contador = 0

    for bpa_i_lote in bpa_i_lotes:
        bpa_i_transformada = transformar_bpa_i(
            sessao=sessao,
            bpa_i=bpa_i_lote,
            uf_sigla=uf_sigla,
            periodo_data_inicio=periodo_data_inicio,
        )

        dfs_transformados.append(bpa_i_transformada)
        contador += len(bpa_i_transformada)

    logging.info("Concatenando lotes de dataframes transformados e validados.")
    df_final = pd.concat(dfs_transformados, ignore_index=True)
    logging.info(f"{contador} registros concatenados no dataframe.")    

    # Salvar no GCS
    logging.info("Realizando upload para bucket do GCS...")
    nome_arquivo_csv = f"siasus_bpa_i_{uf_sigla}_{periodo_data_inicio:%y%m}.csv"
    path_gcs = f"saude-mental/dados-publicos/siasus/bpa-i-disseminacao/{uf_sigla}/{nome_arquivo_csv}"
    
    upload_to_bucket(
        bucket_name="camada-bronze", 
        blob_path=path_gcs,
        dados=df_final.to_csv(index=False)
    )

    # Registrar na tabela de metadados do FTP
    logging.info("Inserindo timestamp na tabela de metadados do FTP...")
    inserir_timestamp_ftp_metadados(
        sessao, 
        uf_sigla, 
        periodo_data_inicio, 
        coluna_atualizar='timestamp_etl_gcs',
        tipo='BI'
    )

    # Obter sumário de resposta
    response = {
        "status": "OK",
        "estado": uf_sigla,
        "periodo": f"{periodo_data_inicio:%y%m}",
        "num_registros": contador,
        "arquivo_final_gcs": f"gcs://camada-bronze/{path_gcs}",
    }

    logging.info(
        f"Processamento de BPA-i finalizado para {uf_sigla} ({periodo_data_inicio:%y%m})."
        f"Status: {response['status']}, Número de registros: {response['num_registros']}, Arquivo GCS: {response['arquivo_final_gcs']}"
    )

    sessao.close()

    return response

