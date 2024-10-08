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
from utilitarios.config_painel_sm import municipios_ativos_painel, condicoes_pa
from utilitarios.datas import agora_gmt_menos3, periodo_por_data
from utilitarios.geografias import id_sus_para_id_impulso
from utilitarios.bd_utilitarios import inserir_timestamp_ftp_metadados

# Utilitarios
from utilitarios.datasus_ftp import extrair_dbc_lotes
from utilitarios.bd_config import Sessao
from utilitarios.cloud_storage import upload_to_bucket
from utilitarios.logger_config import logger_config


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

COLUNAS_DATA_AAAAMM: Final[list[str]] = [
    "realizacao_periodo_data_inicio",
    "processamento_periodo_data_inicio",
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
    passo: int = 200000,
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
    uf_sigla: str,
    periodo_data_inicio: datetime.date,
) -> pd.DataFrame:
    """Transforma um `DataFrame` de procedimentos ambulatoriais do SIASUS.

    Esta função realiza uma série de operações de transformação em um `DataFrame` 
    que contém dados de procedimentos ambulatoriais extraídos do SIASUS. 
    As transformações incluem filtragem de registros, renomeação de colunas, 
    tratamento de valores nulos, e a adição de colunas específicas, entre outras 
    operações.

    Parâmetros:
        sessao (sqlalchemy.orm.session.Session): 
            Sessão de banco de dados usada para acessar a base de dados da ImpulsoGov.
        pa (pandas.DataFrame): 
            DataFrame contendo os dados de procedimentos ambulatoriais, conforme 
            extraídos para uma unidade federativa e competência (mês) específica.
        uf_sigla (str): 
            Sigla da unidade federativa para a qual os dados foram extraídos.
        periodo_data_inicio (datetime.date): 
            Data de início do período (mês e ano) correspondente aos dados extraídos.

    Retorno:
        pandas.DataFrame: 
            DataFrame transformado com os procedimentos ambulatoriais, pronto 
            para ser inserido ou atualizado na base de dados.

    Notas:
        - Os filtros são aplicados antecipadamente para otimizar a performance. 
        Isso significa que os nomes, tipos e valores das colunas devem corresponder 
        exatamente aos registros originais do arquivo de disseminação disponível no 
        FTP público do DataSUS. Consulte o Informe Técnico do SIASUS para mais 
        detalhes.

    Referências:
        - [`sqlalchemy.orm.session.Session`](https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session): API de Sessão do SQLAlchemy.
        - [`pandas.DataFrame`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html): API do DataFrame do Pandas.
        - [`pandas.DataFrame.query()`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.query.html
        - [it-siasus]: https://drive.google.com/file/d/1DC5093njSQIhMHydYptlj2rMbrMF36y6
    """
    logging.info(
        f"Transformando DataFrame com {len(pa)} procedimentos "
        + "ambulatoriais."
    )
    memoria_usada=pa.memory_usage(deep=True).sum() / 10 ** 6
    logging.debug(
        f"Memória ocupada pelo DataFrame original:  {memoria_usada:.2f} mB."
    )


    # aplica filtragem para municípios participantes (procedimento registrado no muni ou paciente residente no muni)
    municipios_painel = municipios_ativos_painel(sessao)
    filtragem_municipios = f"(PA_UFMUN in {municipios_painel}) or (PA_MUNPCN in {municipios_painel})"
    pa = pa.query(filtragem_municipios, engine="python")
    logging.info(
        f"Registros após aplicar filtro de seleção de municípios: {len(pa)}."
    )


    # aplica condições de filtragem dos registros
    logging.info(
        f"Filtrando DataFrame com {len(pa)} procedimentos "
        + "ambulatoriais.",
    )
    pa = pa.query(condicoes_pa, engine="python")
    logging.info(
        f"Registros após aplicar filtro de condições de saúde mental: {len(pa)}."
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

        # adicionar coluna ftp_arquivo_nome
        .add_column(
            "ftp_arquivo_nome",
            f"PA{uf_sigla}{periodo_data_inicio:%y%m}"
        )
    )

    memoria_usada = pa_transformada.memory_usage(deep=True).sum() / 10 ** 6
    logging.debug(        
        f"Memória ocupada pelo DataFrame transformado: {memoria_usada:.2f} mB."
    )
    return pa_transformada


def validar_pa(pa_transformada: pd.DataFrame) -> pd.DataFrame:
    assert isinstance(pa_transformada, pd.DataFrame), "Não é um DataFrame"
    # assert len(pa_transformada) > 0, "DataFrame vazio."
    nulos_por_coluna = pa_transformada.applymap(pd.isna).sum()
    assert nulos_por_coluna["quantidade_apresentada"] == 0, (
        "A quantidade apresentada é um valor nulo."
    )
    assert nulos_por_coluna["quantidade_aprovada"] == 0, (
        "A quantidade aprovada é um valor nulo."
    )
    assert nulos_por_coluna["realizacao_periodo_data_inicio"] == 0, (
        "A competência de realização é um valor nulo."
    )


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
    sessao = Sessao()

    pa_lotes = extrair_pa(
        uf_sigla=uf_sigla,
        periodo_data_inicio=periodo_data_inicio,
    )

    dfs_transformados = []
    contador = 0

    for pa_lote in pa_lotes:
        pa_transformada = transformar_pa(
            sessao=sessao,
            pa=pa_lote,
            uf_sigla=uf_sigla,
            periodo_data_inicio=periodo_data_inicio,
        )
        try: 
            validar_pa(pa_transformada)
        except AssertionError as mensagem:
            raise RuntimeError(
                "Dados inválidos encontrados após a transformação:"
                + " {}".format(mensagem),
            )
        else: 
            dfs_transformados.append(pa_transformada)
            contador += len(pa_transformada)

    logging.info("Concatenando lotes de dataframes transformados e validados.")
    df_final = pd.concat(dfs_transformados, ignore_index=True)    
    logging.info(f"{contador} registros concatenados no dataframe.")
    
    # Salvar no GCS
    logging.info("Realizando upload para bucket do GCS...")
    nome_arquivo_csv = f"siasus_procedimentos_disseminacao_{uf_sigla}_{periodo_data_inicio:%y%m}.csv"
    path_gcs = f"saude-mental/dados-publicos/siasus/procedimentos-disseminacao/{uf_sigla}/{nome_arquivo_csv}"
    
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
        tipo='PA'
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
        f"Processamento de PA finalizado para {uf_sigla} ({periodo_data_inicio:%y%m})."
        f"Status: {response['status']}, Número de registros: {response['num_registros']}, Arquivo GCS: {response['arquivo_final_gcs']}"
    )

    sessao.close()    

    return response
