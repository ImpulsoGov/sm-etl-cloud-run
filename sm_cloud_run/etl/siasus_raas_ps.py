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

# Utilizado no tratamento
import janitor
from frozendict import frozendict
from uuid6 import uuid7
from sqlalchemy.orm import Session

# Utilitarios
from utilitarios.datasus_ftp import extrair_dbc_lotes
from utilitarios.datas import agora_gmt_menos3, periodo_por_data, de_aaaammdd_para_timestamp
from utilitarios.geografias import id_sus_para_id_impulso
from utilitarios.bd_config import Sessao
from utilitarios.cloud_storage import upload_to_bucket
from utilitarios.logger_config import logger_config



# set up logging to file
logger_config()


DE_PARA_RAAS_PS: Final[frozendict] = frozendict(
    {
        "CNES_EXEC": "estabelecimento_id_scnes",
        "GESTAO": "gestao_unidade_geografica_id_sus",
        "CONDIC": "gestao_condicao_id_siasus",
        "UFMUN": "unidade_geografica_id_sus",
        "TPUPS": "estabelecimento_tipo_id_sigtap",
        "TIPPRE": "prestador_tipo_id_sigtap",
        "MN_IND": "estabelecimento_mantido",
        "CNPJCPF": "estabelecimento_id_cnpj",
        "CNPJMNT": "mantenedora_id_cnpj",
        "DT_PROCESS": "processamento_periodo_data_inicio",
        "DT_ATEND": "realizacao_periodo_data_inicio",
        "CNS_PAC": "usuario_id_cns_criptografado",
        "DTNASC": "usuario_nascimento_data",
        "TPIDADEPAC": "usuario_idade_tipo_id_sigtap",
        "IDADEPAC": "usuario_idade",
        "NACION_PAC": "usuario_nacionalidade_id_sus",
        "SEXOPAC": "usuario_sexo_id_sigtap",
        "RACACOR": "usuario_raca_cor_id_siasus",
        "ETNIA": "usuario_etnia_id_sus",
        "MUNPAC": "usuario_residencia_municipio_id_sus",
        "MOT_COB": "desfecho_motivo_id_siasus",
        "DT_MOTCOB": "desfecho_data",
        "CATEND": "carater_atendimento_id_siasus",
        "CIDPRI": "condicao_principal_id_cid10",
        "CIDASSOC": "condicao_associada_id_cid10",
        "ORIGEM_PAC": "procedencia_id_siasus",
        "DT_INICIO": "raas_data_inicio",
        "DT_FIM": "raas_data_fim",
        "COB_ESF": "esf_cobertura",
        "CNES_ESF": "esf_estabelecimento_id_scnes",
        "DESTINOPAC": "desfecho_destino_id_siasus",
        "PA_PROC_ID": "procedimento_id_sigtap",
        "PA_QTDPRO": "quantidade_apresentada",
        "PA_QTDAPR": "quantidade_aprovada",
        "PA_SRV": "servico_id_sigtap",
        "PA_CLASS_S": "servico_classificacao_id_sigtap",
        "SIT_RUA": "usuario_situacao_rua",
        "TP_DROGA": "usuario_abuso_substancias",
        "LOC_REALIZ": "local_realizacao_id_siasus",
        "INICIO": "data_inicio",
        "FIM": "data_fim",
        "PERMANEN": "permanencia_duracao",
        "QTDATE": "quantidade_atendimentos",
        "QTDPCN": "quantidade_usuarios",
        "NAT_JUR": "estabelecimento_natureza_juridica_id_scnes",
    },
)

COLUNAS_DATA_AAAAMMDD: Final[list[str]] = [
    "usuario_nascimento_data",
    "raas_data_inicio",
    "raas_data_fim",
    "data_inicio",
    "data_fim",
    "desfecho_data",
]

COLUNAS_DATA_AAAAMM: Final[list[str]] = [
    "realizacao_periodo_data_inicio",
    "processamento_periodo_data_inicio",
]


def extrair_raas_ps(
    uf_sigla: str,
    periodo_data_inicio: datetime.date,
    passo: int = 100000,
) -> Generator[pd.DataFrame, None, None]:
    """Extrai registros de RAAS Psicossociais do FTP do DataSUS.

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
        caminho_diretorio="/dissemin/publicos/SIASUS/200801_/Dados",
        arquivo_nome="PS{uf_sigla}{periodo_data_inicio:%y%m}.dbc".format(
            uf_sigla=uf_sigla,
            periodo_data_inicio=periodo_data_inicio,
        ),
        passo=passo,
    )


def transformar_raas_ps(
    sessao: Session,
    raas_ps: pd.DataFrame,
) -> pd.DataFrame:
    """Transforma um `DataFrame` de RAAS obtido do FTP público do DataSUS.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        raas_ps: objeto [`pandas.DataFrame`][] contendo os dados de um arquivo
            de disseminação de Registros de Ações Ambulatoriais em Saúde -
            RAAS, conforme extraídos para uma unidade federativa e competência
            (mês) pela função [`extrair_raas()`][].

    Note:
        Para otimizar a performance, os filtros são aplicados antes de qualquer
        outra transformação nos dados, de forma que as condições fornecidas
        devem considerar que o nome, os tipos e os valores aparecem exatamente
        como registrados no arquivo de disseminação disponibilizado no FTP
        público do DataSUS. Verifique o [Informe Técnico][it-siasus] para mais
        informações.

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    [`pandas.DataFrame`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    [`extrair_raas_ps()`]: impulsoetl.siasus.raas.extrair_raas
    [`pandas.DataFrame.query()`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.query.html
    [it-siasus]: https://drive.google.com/file/d/1DC5093njSQIhMHydYptlj2rMbrMF36y6
    """

    logging.info(
        f"Transformando DataFrame com {len(raas_ps)} registros de RAAS."
    )

    raas_ps_transformada = (

        raas_ps  # noqa: WPS221  # ignorar linha complexa no pipeline
        # renomear colunas
        .rename_columns(function=lambda col: col.strip())
        .rename_columns(DE_PARA_RAAS_PS)
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
            ["usuario_situacao_rua", "esf_cobertura"],
            function=lambda elemento: True if elemento == "S" else False,
        )
        # processar coluna de uso de substâncias
        .transform_column(
            "usuario_abuso_substancias",
            function=lambda elemento: bool("A" in elemento),
            dest_column_name="usuario_abuso_substancias_alcool",
        )
        .transform_column(
            "usuario_abuso_substancias",
            function=lambda elemento: bool("C" in elemento),
            dest_column_name="usuario_abuso_substancias_crack",
        )
        .transform_column(
            "usuario_abuso_substancias",
            function=lambda elemento: bool("O" in elemento),
            dest_column_name="usuario_abuso_substancias_outras",
        )
        .transform_column(
            "usuario_abuso_substancias",
            function=lambda elemento: bool(len(elemento)),
        )
        # transformar coluna de duração
        .transform_column(
            "permanencia_duracao",
            function=lambda elemento: (
                "{} days".format(elemento) if elemento else np.nan
            ),
        )
        # tratar como NA colunas com valores nulos
        .replace("", np.nan)
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
        
    )
    return raas_ps_transformada



def baixar_e_processar_raas_ps(uf_sigla: str, periodo_data_inicio: datetime.date):
    """
    Baixa e processa dados de RAAS-PS do SIASUS para uma 
    determinada Unidade Federativa e período de tempo.

    Argumentos:
        uf_sigla (str): A sigla da Unidade Federativa para a qual os dados de 
        RAAS-PS serão baixados e processados.
        
        periodo_data_inicio (datetime.date): A data de início do período de 
        tempo para o qual os dados serão obtidos. Deve ser fornecida como um 
        objeto `datetime.date`.

    Retorna:
        dict: Um dicionário contendo informações sobre o status da operação, 
        o estado, o período, o caminho do arquivo final no Google Cloud Storage 
        (GCS) e a lista de arquivos originais DBC capturados.

    A função se conecta ao FTP do DataSUS para obter os arquivos de RAAS-PS 
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

    session = Sessao()

    # Extrair dados
    raas_ps_lotes = extrair_raas_ps(
        uf_sigla=uf_sigla,
        periodo_data_inicio=periodo_data_inicio,
    )

    dfs_transformados = []
    contador = 0

    for raas_ps_lote in raas_ps_lotes:
        raas_ps_transformada = transformar_raas_ps(
            sessao=session,
            raas_ps=raas_ps_lote,
        )

        dfs_transformados.append(raas_ps_transformada)
        contador += len(raas_ps_transformada)

    
    logging.info("Concatenando lotes de dataframes transformados e validados.")
    df_final = pd.concat(dfs_transformados, ignore_index=True)
    logging.info(f"{contador} registros concatenadosno dataframe.")    

    # Salvar no GCS
    logging.info("Realizando upload para bucket do GCS...")
    nome_arquivo_csv = f"siasus_raas_ps_disseminacao_{uf_sigla}_{periodo_data_inicio:%y%m}.csv"
    path_gcs = f"saude-mental/dados-publicos/siasus/raas-psicossocial/{uf_sigla}/{periodo_data_inicio:%y%m}/{nome_arquivo_csv}"
    
    # Salvar no GCS
    upload_to_bucket(
        bucket_name="camada-bronze", 
        blob_path=path_gcs,
        dados=df_final.to_csv()
    )

    response = {
        "status": "OK",
        "estado": uf_sigla,
        "periodo": f"{periodo_data_inicio:%y%m}",
        "num_registros": {contador},
        "arquivo_final_gcs": f"gcs://camada-bronze/{path_gcs}",
    }

    session.close()

    logging.info(
        f"Processamento de RAAS-ps finalizado para {uf_sigla} ({periodo_data_inicio:%y%m})."
        f"Status: {response['status']}, Número de registros: {response['num_registros']}, Arquivo GCS: {response['arquivo_final_gcs']}"
    )

    return response


# RODAR LOCALMENTE
if __name__ == "__main__":
    from datetime import datetime

    # Define os parâmetros de teste
    uf_sigla = "ES"
    periodo_data_inicio = datetime.strptime("2024-04-01", "%Y-%m-%d").date()

    # Chama a função principal com os parâmetros de teste
    baixar_e_processar_raas_ps(uf_sigla, periodo_data_inicio)