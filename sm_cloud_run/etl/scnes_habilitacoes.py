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


logger_config()


DE_PARA_HABILITACOES: Final[frozendict] = frozendict(
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
        "COD_CEP": "estabelecimento_cep",
        "VINC_SUS": "atendimento_sus",
        "TP_PREST": "prestador_tipo_id_fca",
        "SGRUPHAB": "habilitacao_id_scnes",
        "COMPETEN": "periodo_data_inicio",
        "CMPT_INI": "vigencia_data_inicio",
        "CMPT_FIM": "vigencia_data_fim",
        "DTPORTAR": "portaria_data",
        "PORTARIA": "portaria_nome",
        "MAPORTAR": "portaria_periodo_data_inicio",
        "NULEITOS": "leitos_quantidade",
        "NAT_JUR": "estabelecimento_natureza_juridica_id_scnes",
    },
)

COLUNAS_DATA_AAAAMM: Final[list[str]] = [
    "periodo_data_inicio",
    "vigencia_data_inicio",
    "vigencia_data_fim",
    "portaria_periodo_data_inicio",
]

COLUNAS_DATA_AAAAMMDD: Final[list[str]] = ["portaria_data"]


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
    

def extrair_habilitacoes(
    uf_sigla: str,
    periodo_data_inicio: datetime.date,
    passo: int = 50000,
) -> Generator[pd.DataFrame, None, None]:
    """Extrai registros de habilitações de estabelecimentos do FTP do DataSUS.

    Argumentos:
        uf_sigla: Sigla da Unidade Federativa cujos estabelecimentos se pretende
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
        caminho_diretorio="/dissemin/publicos/CNES/200508_/Dados/HB",
        arquivo_nome="HB{uf_sigla}{periodo_data_inicio:%y%m}.dbc".format(
            uf_sigla=uf_sigla,
            periodo_data_inicio=periodo_data_inicio,
        ),
        passo=passo,
    )



def transformar_habilitacoes(
    sessao: Session,
    habilitacoes: pd.DataFrame,
    uf_sigla: str,
    periodo_data_inicio: datetime.date,
) -> pd.DataFrame:
    """Transforma um `DataFrame` de habilitações do SCNES.

        Parâmetros:
            sessao (sqlalchemy.orm.session.Session): 
                Sessão de banco de dados usada para acessar a base de dados da ImpulsoGov.
            habilitações (pandas.DataFrame): 
                DataFrame contendo os dados de habilitações de estabelecimentos de saúde,
                conforme extraídos para uma unidade federativa e competência (mês) 
                específica.
            uf_sigla (str): 
                Sigla da unidade federativa para a qual os dados foram extraídos.
            periodo_data_inicio (datetime.date): 
                Data de início do período (mês e ano) correspondente aos dados extraídos.

        Retorna:
            Um [`DataFrame`][] com dados de habilitações de estabelecimentos tratados para
            inserção no banco de dados da ImpulsoGov.

        [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
        [`DataFrame`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
        [`pysus.online_data.CNES.download()`]: http://localhost:9090/@https://github.com/AlertaDengue/PySUS/blob/600c61627b7998a1733b71ac163b3de71324cfbe/pysus/online_data/CNES.py#L28
    """
   
    logging.info(
        f"Transformando DataFrame com {len(habilitacoes)} habilitações "
        + "de estabelecimentos do SCNES."
    )
    memoria_usada=habilitacoes.memory_usage(deep=True).sum() / 10**6
    logging.debug(
        f"Memória ocupada pelo DataFrame original:  {memoria_usada:.2f} mB."        
    )


    # aplica filtragem para municípios participantes 
    municipios_painel = municipios_ativos_painel(sessao)
    filtragem_municipios = f"(CODUFMUN in {municipios_painel})"
    habilitacoes = habilitacoes.query(filtragem_municipios, engine="python")
    logging.info(
        f"Registros após aplicar filtro de seleção de municípios: {len(habilitacoes)}."
    )
    

    habilitacoes_transformado = (
        habilitacoes  # noqa: WPS221  # ignorar linha complexa no pipeline
        # renomear colunas
        .rename_columns(function=lambda col: col.strip())
        .rename_columns(DE_PARA_HABILITACOES)
        # adicionar datas de inserção e atualização
        .add_column("criacao_data", agora_gmt_menos3())
        .add_column("atualizacao_data", agora_gmt_menos3())
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
            function=lambda dt: pd.to_datetime(
                dt,
                format="%d/%m/%Y",
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
                "atendimento_sus",
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
        
        # adicionar coluna ftp_arquivo_nome
        .add_column(
            "ftp_arquivo_nome",
            f"HB{uf_sigla}{periodo_data_inicio:%y%m}"
        )
    )

    memoria_usada=habilitacoes_transformado.memory_usage(deep=True).sum() / 10**6
    logging.debug(
        f"Memória ocupada pelo DataFrame transformado: {memoria_usada:.2f} mB."        
    )
    return habilitacoes_transformado



def baixar_e_processar_habilitacoes(uf_sigla: str, periodo_data_inicio: datetime.date):
    """
    Baixa e processa dados de registros de habilitações de estabelecimentos de 
    saúde do SCNES para uma determinada Unidade Federativa e período de tempo.

    Argumentos:
        uf_sigla (str): A sigla da Unidade Federativa para a qual os dados de 
        serão baixados e processados.
        
        periodo_data_inicio (datetime.date): A data de início do período de 
        tempo para o qual os dados serão obtidos. Deve ser fornecida como um 
        objeto `datetime.date`.

    Retorna:
        dict: Um dicionário contendo informações sobre o status da operação, 
        o estado, o período, o caminho do arquivo final no Google Cloud Storage 
        (GCS) e a lista de arquivos originais DBC capturados.

    A função se conecta ao FTP do DataSUS para obter os arquivos de habilitações
    correspondentes à Unidade Federativa e ao período de tempo fornecidos. 
    Em seguida, os dados são processados e tratados.

    Após o processamento, os dados são carregados em um arquivo CSV para o 
    Google Cloud Storage (GCS). O caminho do arquivo final no GCS e a lista 
    de arquivos originais DBC capturados são incluídos no dicionário de retorno, 
    juntamente com informações sobre o estado e o período dos dados processados.
    """

    # Extrair dados
    sessao = Sessao()
    
    habilitacoes_lotes = extrair_habilitacoes(
        uf_sigla=uf_sigla,
        periodo_data_inicio=periodo_data_inicio,
    )

    dfs_transformados = []
    contador = 0

    for habilitacoes_lote in habilitacoes_lotes:
        habilitacoes_transformada = transformar_habilitacoes(
            sessao=sessao,
            habilitacoes=habilitacoes_lote,
            uf_sigla=uf_sigla,
            periodo_data_inicio=periodo_data_inicio,
        )

        dfs_transformados.append(habilitacoes_transformada)
        contador += len(habilitacoes_transformada)

    logging.info("Concatenando lotes de dataframes transformados e validados.")
    df_final = pd.concat(dfs_transformados, ignore_index=True)
    logging.info(f"{contador} registros concatenados no dataframe.")    

    # Salvar no GCS
    logging.info("Realizando upload para bucket do GCS...")
    nome_arquivo_csv = f"scnes_habilitacoes_{uf_sigla}_{periodo_data_inicio:%y%m}.csv"
    path_gcs = f"saude-mental/dados-publicos/scnes/habilitacoes/{uf_sigla}/{nome_arquivo_csv}"
    
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
        tipo='HB'
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
        f"Processamento de Habilitações finalizado para {uf_sigla} ({periodo_data_inicio:%y%m})."
        f"Status: {response['status']}, Número de registros: {response['num_registros']}, Arquivo GCS: {response['arquivo_final_gcs']}"
    )

    sessao.close()

    return response


# RODAR LOCALMENTE
if __name__ == "__main__":
    from datetime import datetime

    # Define os parâmetros de teste
    periodo_data_inicio = datetime.strptime("2024-08-01", "%Y-%m-%d").date()
    

    baixar_e_processar_habilitacoes(
        uf_sigla='PB',
        periodo_data_inicio=periodo_data_inicio
    )