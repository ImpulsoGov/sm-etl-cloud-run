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
import roman
from utilitarios.config_painel_sm import municipios_ativos_painel
from utilitarios.datas import agora_gmt_menos3, periodo_por_data
from utilitarios.geografias import id_sus_para_id_impulso
from utilitarios.bd_utilitarios import inserir_timestamp_ftp_metadados

# Utilitarios
from utilitarios.datasus_ftp import extrair_dbc_lotes
from utilitarios.bd_config import Sessao
from utilitarios.cloud_storage import upload_to_bucket
from utilitarios.logger_config import logger_config

# set up logging to file
logger_config()



DE_PARA_VINCULOS: Final[frozendict] = frozendict(
    {
        "CNES": "estabelecimento_id_scnes",
        "CODUFMUN": "estabelecimento_municipio_id_sus",
        "REGSAUDE": "estabelecimento_regiao_saude_id_sus",
        "MICR_REG": "estabelecimento_microrregiao_saude_id_sus",
        "DISTRSAN": "estabelecimento_distrito_sanitario_id_sus",
        "DISTRADM": "estabelecimento_distrito_administrativo_id_sus",
        "TPGESTAO": "estabelecimento_gestao_condicao_id_scnes",
        "PF_PJ": "estabelecimento_personalidade_juridica_id_scnes",
        "CPF_CNPJ": "estabelecimento_id_cpf_cnpj",
        "NIV_DEP": "estabelecimento_mantido",
        "CNPJ_MAN": "estabelecimento_mantenedora_id_cnpj",
        "ESFERA_A": "estabelecimento_esfera_id_scnes",
        "ATIVIDAD": "estabelecimento_atividade_ensino_id_scnes",
        "RETENCAO": "estabelecimento_tributos_retencao_id_scnes",
        "NATUREZA": "estabelecimento_natureza_id_scnes",
        "CLIENTEL": "estabelecimento_fluxo_id_scnes",
        "TP_UNID": "estabelecimento_tipo_id_scnes",
        "TURNO_AT": "estabelecimento_turno_id_scnes",
        "NIV_HIER": "estabelecimento_hierarquia_id_scnes",
        "TERCEIRO": "estabelecimento_terceiro",
        "CPF_PROF": "profissional_id_cpf_criptografado",
        "CPFUNICO": "profissional_cpf_unico",
        "CBO": "ocupacao_id_cbo2002",
        "CBOUNICO": "ocupacao_cbo_unico",
        "NOMEPROF": "profissional_nome",
        "CNS_PROF": "profissional_id_cns",
        "CONSELHO": "profissional_conselho_tipo_id_scnes",
        "REGISTRO": "profissional_id_conselho",
        "VINCULAC": "tipo_id_scnes",
        "VINCUL_C": "contratado",
        "VINCUL_A": "autonomo",
        "VINCUL_N": "sem_vinculo_definido",
        "PROF_SUS": "atendimento_sus",
        "PROFNSUS": "atendimento_nao_sus",
        "HORAOUTR": "atendimento_carga_outras",
        "HORAHOSP": "atendimento_carga_hospitalar",
        "HORA_AMB": "atendimento_carga_ambulatorial",
        "COMPETEN": "periodo_data_inicio",
        "UFMUNRES": "profissional_residencia_municipio_id_sus",
        "NAT_JUR": "estabelecimento_natureza_juridica_id_scnes",
    },
)


COLUNAS_DATA_AAAAMM: Final[list[str]] = [
    "periodo_data_inicio",
]


def _para_booleano(valor: str) -> bool | float:
    """Transforma um valor binário '0' ou '1' em booleano. Suporta NaNs."""
    if valor == "0":
        return False
    elif valor == "1":
        return True
    else:
        return np.nan


def _romano_para_inteiro(texto: str) -> str | float:
    if pd.isna(texto):
        return np.nan
    try:
        return str(roman.fromRoman(texto))
    except roman.InvalidRomanNumeralError:
        return texto
    

def extrair_vinculos(
    uf_sigla: str,
    periodo_data_inicio: datetime.date,
    passo: int = 50000,
) -> Generator[pd.DataFrame, None, None]:
    """Extrai registros de vínculos profissionais do FTP do DataSUS.

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

    return extrair_dbc_lotes(
        ftp="ftp.datasus.gov.br",
        caminho_diretorio="/dissemin/publicos/CNES/200508_/Dados/PF",
        arquivo_nome="PF{uf_sigla}{periodo_data_inicio:%y%m}.dbc".format(
            uf_sigla=uf_sigla,
            periodo_data_inicio=periodo_data_inicio,
        ),
        passo=passo,
    )



def transformar_vinculos(
    sessao: Session,
    vinculos: pd.DataFrame,
    uf_sigla: str,
    periodo_data_inicio: datetime.date,
) -> pd.DataFrame:
    """Transforma um `DataFrame` de vínculos do SCNES.

    Parâmetros:
        sessao (sqlalchemy.orm.session.Session): 
            Sessão de banco de dados usada para acessar a base de dados da ImpulsoGov.
        pa (pandas.DataFrame): 
            DataFrame contendo os dados de vínculos profissionais, conforme 
            extraídos para uma unidade federativa e competência (mês) específica.
        uf_sigla (str): 
            Sigla da unidade federativa para a qual os dados foram extraídos.
        periodo_data_inicio (datetime.date): 
            Data de início do período (mês e ano) correspondente aos dados extraídos.

    Retorna:
        Um [`DataFrame`][] com dados de vínculos profissionais tratados para
        inserção no banco de dados da ImpulsoGov.

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    [`DataFrame`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    [`pysus.online_data.CNES.download()`]: http://localhost:9090/@https://github.com/AlertaDengue/PySUS/blob/600c61627b7998a1733b71ac163b3de71324cfbe/pysus/online_data/CNES.py#L28
    """
    
    logging.info(
        f"Transformando DataFrame com {len(vinculos)} vínculos "
        + "profissionais do SCNES."
    )
    memoria_usada=vinculos.memory_usage(deep=True).sum() / 10**6
    logging.debug(
        f"Memória ocupada pelo DataFrame original:  {memoria_usada:.2f} mB."
    )

    # aplica filtragem para municípios participantes 
    municipios_painel = municipios_ativos_painel(sessao)
    filtragem_municipios = f"(CODUFMUN in {municipios_painel})"
    vinculos = vinculos.query(filtragem_municipios, engine="python")
    logging.info(
        f"Registros após aplicar filtro de seleção de municípios: {len(vinculos)}."
    )


    vinculos_transformado = (
        vinculos  # noqa: WPS221  # ignorar linha complexa no pipeline
        # renomear colunas
        .rename_columns(function=lambda col: col.strip())
        .rename_columns(DE_PARA_VINCULOS)
        # processar colunas com datas
        .transform_columns(
            COLUNAS_DATA_AAAAMM,
            function=lambda dt: pd.to_datetime(
                dt,
                format="%Y%m",
                errors="coerce",
            ),
        )
        # limpar e completar códigos de região e distrito de saúde
        .transform_column(
            "estabelecimento_regiao_saude_id_sus",
            _romano_para_inteiro,
        )
        .transform_column(
            "estabelecimento_regiao_saude_id_sus",
            lambda id_sus: (
                re.sub("[^0-9]", "", id_sus) if pd.notna(id_sus) else np.nan
            ),
        )
        .transform_columns(
            [
                "estabelecimento_regiao_saude_id_sus",
                "estabelecimento_distrito_sanitario_id_sus",
                "estabelecimento_distrito_administrativo_id_sus",
            ],
            lambda id_sus: (id_sus.zfill(4) if pd.notna(id_sus) else np.nan),
        )
        .transform_column(
            "estabelecimento_microrregiao_saude_id_sus",
            lambda id_sus: (id_sus.zfill(6) if pd.notna(id_sus) else np.nan),
        )
        # limpar registros no conselho profissional
        .transform_column(
            "profissional_id_conselho",
            lambda id_conselho: (
                re.sub("[^0-9]", "", id_conselho)
                if pd.notna(id_conselho)
                else np.nan
            ),
        )
        # tratar como NA colunas com valores nulos
        .replace("", np.nan)
        .transform_columns(
            [
                "estabelecimento_regiao_saude_id_sus",
                "estabelecimento_microrregiao_saude_id_sus",
                "estabelecimento_distrito_sanitario_id_sus",
                "estabelecimento_distrito_administrativo_id_sus",
                "estabelecimento_id_cpf_cnpj",
                "estabelecimento_mantenedora_id_cnpj",
                "profissional_id_conselho",
                "profissional_residencia_municipio_id_sus",
            ],
            function=lambda elemento: (
                np.nan
                if pd.notna(elemento)
                and all(digito == "0" for digito in elemento)
                else elemento
            ),
        )
        # processar colunas lógicas
        .transform_column(
            "estabelecimento_mantido",
            function=lambda elemento: True if elemento == "1" else False,
        )
        .transform_columns(
            [
                "estabelecimento_terceiro",
                "contratado",
                "autonomo",
                "sem_vinculo_definido",
                "atendimento_sus",
                "atendimento_nao_sus",
            ],
            function=_para_booleano,
        )
        # adicionar id
        .add_column("id", str())
        .transform_column("id", function=lambda _: uuid7().hex)
        # adicionar id do periodo
        .transform_column(
            "periodo_data_inicio",
            function=lambda dt: periodo_por_data(sessao=sessao, data=dt).id,
            dest_column_name="periodo_id",
        )
        # adicionar id da unidade geografica
        .transform_column(
            "estabelecimento_municipio_id_sus",
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
            f"PF{uf_sigla}{periodo_data_inicio:%y%m}"
        )


    )
    logging.debug(
        "Memória ocupada pelo DataFrame transformado: {memoria_usada:.2f} mB.",
        memoria_usada=(
            vinculos_transformado.memory_usage(deep=True).sum() / 10**6
        ),
    )
    return vinculos_transformado


def baixar_e_processar_vinculos(
    uf_sigla: str,
    periodo_data_inicio: datetime.date,

) -> None:
    """Baixa, transforma e carrega dados de vinculos profissionais.

    Argumentos:
        uf_sigla (str): A sigla da Unidade Federativa para a qual os dados de 
        vínculos serão baixados e processados.
        
        periodo_data_inicio (datetime.date): A data de início do período de 
        tempo para o qual os dados serão obtidos. Deve ser fornecida como um 
        objeto `datetime.date`.

    Retorna:
        dict: Um dicionário contendo informações sobre o status da operação, 
        o estado, o período, o caminho do arquivo final no Google Cloud Storage 
        (GCS) e a lista de arquivos originais DBC capturados.

    A função se conecta ao FTP do DataSUS para obter os arquivos de vínculos 
    correspondentes à Unidade Federativa e ao período de tempo fornecidos. 
    Em seguida, os dados são processados conforme especificações do Informe 
    Técnico do SIASUS, incluindo renomeação de colunas, tratamento de valores 
    nulos, adição de IDs e datas, entre outros.

    Após o processamento, os dados são salvos localmente em um arquivo CSV e 
    carregados para o Google Cloud Storage (GCS). O caminho do arquivo final 
    no GCS e a lista de arquivos originais DBC capturados são incluídos no 
    dicionário de retorno, juntamente com informações sobre o estado e o 
    período dos dados processados.
    """
    # Extrair dados
    sessao = Sessao()

    # Extrair dados
    vinculos_lotes = extrair_vinculos(
        uf_sigla=uf_sigla,
        periodo_data_inicio=periodo_data_inicio,
    )

    dfs_transformados = []
    contador = 0

    for vinculos_lote in vinculos_lotes:
        vinculos_transformada = transformar_vinculos(
            sessao=sessao,
            vinculos=vinculos_lote,
            uf_sigla=uf_sigla,
            periodo_data_inicio=periodo_data_inicio,
        )

        dfs_transformados.append(vinculos_transformada)
        contador += len(vinculos_transformada)

    
    logging.info("Concatenando lotes de dataframes transformados e validados.")
    df_final = pd.concat(dfs_transformados, ignore_index=True)
    logging.info(f"{contador} registros concatenadosno dataframe.")    

    # Salvar no GCS
    logging.info("Realizando upload para bucket do GCS...")
    nome_arquivo_csv = f"scnes_vinculos_disseminacao_{uf_sigla}_{periodo_data_inicio:%y%m}.csv"
    path_gcs = f"saude-mental/dados-publicos/scnes/vinculos/{uf_sigla}/{nome_arquivo_csv}"
    
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
        tipo='PF'
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
        f"Processamento de Vínculos finalizado para {uf_sigla} ({periodo_data_inicio:%y%m})."
        f"Status: {response['status']}, Número de registros: {response['num_registros']}, Arquivo GCS: {response['arquivo_final_gcs']}"
    )

    sessao.close()

    return response