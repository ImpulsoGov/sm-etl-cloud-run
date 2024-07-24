from __future__ import annotations

# NECESSARIO PARA RODAR LOCALMENTE: Adiciona o caminho do diretório `sm_cloud_run` ao sys.path
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'sm_cloud_run')))
###

import re
import sys
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


DE_PARA_AIH_RD: Final[frozendict] = frozendict(
    {
        "UF_ZI": "gestao_unidade_geografica_id_sus",
        "ANO_CMPT": "processamento_periodo_ano_inicio",
        "MES_CMPT": "processamento_periodo_mes_inicio",
        "ESPEC": "leito_especialidade_id_sigtap",
        "CGC_HOSP": "estabelecimento_id_cnpj",
        "N_AIH": "aih_id_sihsus",
        "IDENT": "aih_tipo_id_sihsus",
        "CEP": "usuario_residencia_cep",
        "MUNIC_RES": "usuario_residencia_municipio_id_sus",
        "NASC": "usuario_nascimento_data",
        "SEXO": "usuario_sexo_id_sihsus",
        "UTI_MES_TO": "uti_diarias",
        "MARCA_UTI": "uti_tipo_id_sihsus",
        "UTI_INT_TO": "unidade_intermediaria_diarias",
        "DIAR_ACOM": "acompanhante_diarias",
        "QT_DIARIAS": "diarias",
        "PROC_SOLIC": "procedimento_solicitado_id_sigtap",
        "PROC_REA": "procedimento_realizado_id_sigtap",
        "VAL_SH": "valor_servicos_hospitalares",
        "VAL_SP": "valor_servicos_profissionais",
        "VAL_TOT": "valor_total",
        "VAL_UTI": "valor_uti",
        "US_TOT": "valor_total_dolar",
        "DT_INTER": "aih_data_inicio",
        "DT_SAIDA": "aih_data_fim",
        "DIAG_PRINC": "condicao_principal_id_cid10",
        "DIAG_SECUN": "condicao_secundaria_id_cid10",
        "COBRANCA": "desfecho_motivo_id_sihsus",
        "GESTAO": "gestao_condicao_id_sihsus",
        "IND_VDRL": "exame_vdrl",
        "MUNIC_MOV": "unidade_geografica_id_sus",
        "COD_IDADE": "usuario_idade_tipo_id_sigtap",
        "IDADE": "usuario_idade",
        "DIAS_PERM": "permanencia_duracao",
        "MORTE": "obito",
        "NACIONAL": "usuario_nacionalidade_id_sigtap",
        "CAR_INT": "carater_atendimento_id_sihsus",
        "HOMONIMO": "usuario_homonimo",
        "NUM_FILHOS": "usuario_filhos_quantidade",
        "INSTRU": "usuario_instrucao_id_sihsus",
        "CID_NOTIF": "condicao_notificacao_id_cid10",
        "CONTRACEP1": "usuario_contraceptivo_principal_id_sihsus",
        "CONTRACEP2": "usuario_contraceptivo_secundario_id_sihsus",
        "GESTRISCO": "gestacao_risco",
        "INSC_PN": "usuario_id_pre_natal",
        "SEQ_AIH5": "remessa_aih_id_sequencial_longa_permanencia",
        "CBOR": "usuario_ocupacao_id_cbo2002",
        "CNAER": "usuario_atividade_id_cnae",
        "VINCPREV": "usuario_vinculo_previdencia_id_sihsus",
        "GESTOR_COD": "autorizacao_gestor_motivo_id_sihsus",
        "GESTOR_TP": "autorizacao_gestor_tipo_id_sihsus",
        "GESTOR_CPF": "autorizacao_gestor_id_cpf",
        "GESTOR_DT": "autorizacao_gestor_data",
        "CNES": "estabelecimento_id_scnes",
        "CNPJ_MANT": "mantenedora_id_cnpj",
        "INFEHOSP": "infeccao_hospitalar",
        "CID_ASSO": "condicao_associada_id_cid10",
        "CID_MORTE": "condicao_obito_id_cid10",
        "COMPLEX": "complexidade_id_sihsus",
        "FINANC": "financiamento_tipo_id_sigtap",
        "FAEC_TP": "financiamento_subtipo_id_sigtap",
        "REGCT": "regra_contratual_id_scnes",
        "RACA_COR": "usuario_raca_cor_id_sihsus",
        "ETNIA": "usuario_etnia_id_sus",
        "SEQUENCIA": "remessa_aih_id_sequencial",
        "REMESSA": "remessa_id_sihsus",
    },
)

DE_PARA_AIH_RD_ADICIONAIS: Final[frozendict] = frozendict(
    {
        "NATUREZA": "estabelecimento_natureza_id_scnes",
        "NAT_JUR": "estabelecimento_natureza_juridica_id_scnes",
        "AUD_JUST": "cns_ausente_justificativa_auditor",
        "SIS_JUST": "cns_ausente_justificativa_estabelecimento",
        "VAL_SH_FED": "valor_servicos_hospitalares_complemento_federal",
        "VAL_SP_FED": "valor_servicos_profissionais_complemento_federal",
        "VAL_SH_GES": "valor_servicos_hospitalares_complemento_local",
        "VAL_SP_GES": "valor_servicos_profissionais_complemento_local",
        "VAL_UCI": "valor_unidade_neonatal",
        "MARCA_UCI": "unidade_neonatal_tipo_id_sihsus",
        "DIAGSEC1": "condicao_secundaria_1_id_cid10",
        "DIAGSEC2": "condicao_secundaria_2_id_cid10",
        "DIAGSEC3": "condicao_secundaria_3_id_cid10",
        "DIAGSEC4": "condicao_secundaria_4_id_cid10",
        "DIAGSEC5": "condicao_secundaria_5_id_cid10",
        "DIAGSEC6": "condicao_secundaria_6_id_cid10",
        "DIAGSEC7": "condicao_secundaria_7_id_cid10",
        "DIAGSEC8": "condicao_secundaria_8_id_cid10",
        "DIAGSEC9": "condicao_secundaria_9_id_cid10",
        "TPDISEC1": "condicao_secundaria_1_tipo_id_sihsus",
        "TPDISEC2": "condicao_secundaria_2_tipo_id_sihsus",
        "TPDISEC3": "condicao_secundaria_3_tipo_id_sihsus",
        "TPDISEC4": "condicao_secundaria_4_tipo_id_sihsus",
        "TPDISEC5": "condicao_secundaria_5_tipo_id_sihsus",
        "TPDISEC6": "condicao_secundaria_6_tipo_id_sihsus",
        "TPDISEC7": "condicao_secundaria_7_tipo_id_sihsus",
        "TPDISEC8": "condicao_secundaria_8_tipo_id_sihsus",
        "TPDISEC9": "condicao_secundaria_9_tipo_id_sihsus",
        "UTI_MES_IN": "_nao_documentado_uti_mes_in",
        "UTI_MES_AN": "_nao_documentado_uti_mes_an",
        "UTI_MES_AL": "_nao_documentado_uti_mes_al",
        "UTI_INT_IN": "_nao_documentado_uti_int_in",
        "UTI_INT_AN": "_nao_documentado_uti_int_an",
        "UTI_INT_AL": "_nao_documentado_uti_int_al",
        "VAL_SADT": "_nao_documentado_val_sadt",
        "VAL_RN": "_nao_documentado_val_rn",
        "VAL_ACOMP": "_nao_documentado_val_acomp",
        "VAL_ORTP": "_nao_documentado_val_ortp",
        "VAL_SANGUE": "_nao_documentado_val_sangue",
        "VAL_SADTSR": "_nao_documentado_val_sadtsr",
        "VAL_TRANSP": "_nao_documentado_val_transp",
        "VAL_OBSANG": "_nao_documentado_val_obsang",
        "VAL_PED1AC": "_nao_documentado_val_ped1ac",
        "RUBRICA": "_nao_documentado_rubrica",
        "NUM_PROC": "_nao_documentado_num_proc",
        "TOT_PT_SP": "_nao_documentado_tot_pt_sp",
        "CPF_AUT": "_nao_documentado_cpf_aut",
    }
)

COLUNAS_DATA_AAAAMMDD: Final[list[str]] = [
    "usuario_nascimento_data",
    "aih_data_inicio",
    "aih_data_fim",
    "autorizacao_gestor_data",
]


def _para_booleano(valor: str) -> bool | float:
    """Transforma um valor binário '0' ou '1' em booleano. Suporta NaNs."""
    if valor == "0":
        return False
    elif valor == "1":
        return True
    else:
        return np.nan


def extrair_aih_rd(
    uf_sigla: str,
    periodo_data_inicio: datetime.date,
    passo: int = 100000,
) -> Generator[pd.DataFrame, None, None]:
    """Extrai autorizações de internações hospitalares do FTP do DataSUS.

    Argumentos:
        uf_sigla: Sigla da Unidade Federativa cujas AIHs se pretende obter.
        periodo_data_inicio: Dia de início da competência desejada,
            representado como um objeto [`datetime.date`][].
        passo: Número de registros que devem ser convertidos em DataFrame a
            cada iteração.

    Gera:
        A cada iteração, devolve um objeto [`pandas.DataFrames`][] com um
        trecho do arquivo com resumos das autorizações de internações
        hospitalares lido e convertido.

    [`pandas.DataFrame`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    [`datetime.date`]: https://docs.python.org/3/library/datetime.html#date-objects
    """

    return extrair_dbc_lotes(
        ftp="ftp.datasus.gov.br",
        caminho_diretorio="/dissemin/publicos/SIHSUS/200801_/Dados",
        arquivo_nome="RD{uf_sigla}{periodo_data_inicio:%y%m}.dbc".format(
            uf_sigla=uf_sigla,
            periodo_data_inicio=periodo_data_inicio,
        ),
        passo=passo,
    )
























































DE_PARA_aih_rd: Final[frozendict] = frozendict(
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


def extrair_aih_rd(
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


def transformar_aih_rd(
    sessao: Session,
    aih_rd: pd.DataFrame,
) -> pd.DataFrame:
    """Transforma um `DataFrame` de BPA-i obtido do FTP público do DataSUS.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        aih_rd: objeto [`pandas.DataFrame`][] contendo os dados de um arquivo de
            disseminação de Boletins de Produção Ambulatorial -
            individualizados, conforme extraídos para uma unidade federativa e
            competência (mês) pela função [`extrair_aih_rd()`][].

    Note:
        Para otimizar a performance, os filtros são aplicados antes de qualquer
        outra transformação nos dados, de forma que as condições fornecidas
        devem considerar que o nome, os tipos e os valores aparecem exatamente
        como registrados no arquivo de disseminação disponibilizado no FTP
        público do DataSUS. Verifique o [Informe Técnico][it-siasus] para mais
        informações.

    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    [`pandas.DataFrame`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
    [`extrair_aih_rd()`]: impulsoetl.siasus.aih_rd.extrair_aih_rd
    [`pandas.DataFrame.query()`]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.query.html
    [it-siasus]: https://drive.google.com/file/d/1DC5093njSQIhMHydYptlj2rMbrMF36y6
    """
    logging.info(
        f"Transformando DataFrame com {len(aih_rd)} registros "
        + "de BPA-i."
    )

    memoria_usada=aih_rd.memory_usage(deep=True).sum() / 10 ** 6
    logging.debug(
        f"Memória ocupada pelo DataFrame original:  {memoria_usada:.2f} mB."
    )


    # aplica condições de filtragem dos registros
    condicoes = "(TPUPS == '70') or PROC_ID.str.startswith('030106') or PROC_ID.str.startswith('030107') or PROC_ID.str.startswith('030108') or CIDPRI.str.startswith('F') or CIDPRI.str.startswith('F') or CIDPRI.str.startswith('X6') or CIDPRI.str.startswith('X7') or CIDPRI.str.contains('^X8[0-4][0-9]*') or CIDPRI.str.startswith('R78') or CIDPRI.str.startswith('T40') or (CIDPRI == 'Y870') or CIDPRI.str.startswith('Y90') or CIDPRI.str.startswith('Y91') or (CBOPROF in ['223905', '223915', '225133', '223550', '239440', '239445', '322220']) or CBOPROF.str.startswith('2515') or (CATEND == '02')"
    logging.info(
        f"Filtrando DataFrame com {len(aih_rd)} registros de BPA-i.",
    )
    aih_rd = aih_rd.query(condicoes, engine="python")
    logging.info(
        f"Registros após aplicar filtro: {len(aih_rd)}."
    )


    aih_rd_transformada = (
        aih_rd  # noqa: WPS221  # ignorar linha complexa no pipeline
        # renomear colunas
        .rename_columns(function=lambda col: col.strip())
        .rename_columns(DE_PARA_aih_rd)
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

    )
    memoria_usada=(aih_rd_transformada.memory_usage(deep=True).sum() / 10 ** 6)
    logging.debug(
        "Memória ocupada pelo DataFrame transformado: {memoria_usada:.2f} mB."
    )
    return aih_rd_transformada


def baixar_e_processar_aih_rd(uf_sigla: str, periodo_data_inicio: datetime.date):
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
    session = Sessao()
    
    aih_rd_lotes = extrair_aih_rd(
        uf_sigla=uf_sigla,
        periodo_data_inicio=periodo_data_inicio,
    )

    dfs_transformados = []
    contador = 0

    for aih_rd_lote in aih_rd_lotes:
        aih_rd_transformada = transformar_aih_rd(
            sessao=session,
            aih_rd=aih_rd_lote,
        )

        dfs_transformados.append(aih_rd_transformada)
        contador += len(aih_rd_transformada)

    logging.info("Concatenando lotes de dataframes transformados e validados.")
    df_final = pd.concat(dfs_transformados, ignore_index=True)
    logging.info(f"{contador} registros concatenadosno dataframe.")    

    # Salvar no GCS
    logging.info("Realizando upload para bucket do GCS...")
    nome_arquivo_csv = f"siasus_aih_rd_{uf_sigla}_{periodo_data_inicio:%y%m}.csv"
    path_gcs = f"saude-mental/dados-publicos/siasus/bpa-i-disseminacao/{uf_sigla}/{nome_arquivo_csv}"
    
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

    logging.info(
        f"Processamento de BPA-i finalizado para {uf_sigla} ({periodo_data_inicio:%y%m})."
        f"Status: {response['status']}, Número de registros: {response['num_registros']}, Arquivo GCS: {response['arquivo_final_gcs']}"
    )

    session.close()

    return response


# RODAR LOCALMENTE
if __name__ == "__main__":
    from datetime import datetime

    # Define os parâmetros de teste
    uf_sigla = "RJ"
    periodo_data_inicio = datetime.strptime("2024-04-01", "%Y-%m-%d").date()

    # Chama a função principal com os parâmetros de teste
    baixar_e_processar_aih_rd(uf_sigla, periodo_data_inicio)
