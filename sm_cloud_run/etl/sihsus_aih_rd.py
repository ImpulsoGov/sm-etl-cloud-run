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

# Utilizados no tratamento
import janitor
from frozendict import frozendict
from uuid6 import uuid7
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
    passo: int = 500000,
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


def transformar_aih_rd(
    sessao: Session,
    aih_rd: pd.DataFrame,
    uf_sigla: str,
    periodo_data_inicio: datetime.date,
) -> pd.DataFrame:
    """Transforma um `DataFrame` de autorizações de internação do SIHSUS.
    
    """

    logging.info(
        f"Transformando DataFrame com {len(aih_rd)} procedimentos "
        + "ambulatoriais."
    )
    memoria_usada=aih_rd.memory_usage(deep=True).sum() / 10 ** 6
    logging.debug(
        f"Memória ocupada pelo DataFrame original:  {memoria_usada:.2f} mB."
    )

    # Junta nomes de colunas e tipos adicionais aos obrigatórios
    de_para = dict(DE_PARA_AIH_RD, **DE_PARA_AIH_RD_ADICIONAIS)

    # corrigir nomes de colunas mal formatados
    aih_rd = aih_rd.rename_columns(function=lambda col: col.strip().upper())

    aih_rd_transformada = (
        aih_rd  # noqa: WPS221  # ignorar linha complexa no pipeline
        # adicionar colunas faltantes, com valores vazios
        .add_columns(
            **{
                coluna: ""
                for coluna in DE_PARA_AIH_RD_ADICIONAIS.keys()
                if not (coluna in aih_rd.columns)
            }
        )
        .rename_columns(de_para)
        # processar colunas com datas
        .join_apply(
            lambda i: pd.Timestamp(
                int(i["processamento_periodo_ano_inicio"]),
                int(i["processamento_periodo_mes_inicio"]),
                1,
            ),
            new_column_name="periodo_data_inicio",
        )
        .remove_columns(
            [
                "processamento_periodo_ano_inicio",
                "processamento_periodo_mes_inicio",
            ]
        )
        .transform_columns(
            COLUNAS_DATA_AAAAMMDD,
            function=lambda dt: de_aaaammdd_para_timestamp(dt, erros="coerce"),
        )
        # tratar como NA colunas com valores nulos
        .replace("", np.nan)
        .change_type("usuario_filhos_quantidade", str)
        .transform_columns(
            [
                "uti_tipo_id_sihsus",
                "condicao_secundaria_id_cid10",
                "estabelecimento_natureza_id_scnes",
                "estabelecimento_natureza_juridica_id_scnes",
                "usuario_instrucao_id_sihsus",
                "condicao_notificacao_id_cid10",
                "usuario_contraceptivo_principal_id_sihsus",
                "usuario_contraceptivo_secundario_id_sihsus",
                "usuario_filhos_quantidade",
                "usuario_id_pre_natal",
                "usuario_ocupacao_id_cbo2002",
                "usuario_atividade_id_cnae",
                "usuario_vinculo_previdencia_id_sihsus",
                "autorizacao_gestor_motivo_id_sihsus",
                "autorizacao_gestor_tipo_id_sihsus",
                "autorizacao_gestor_id_cpf",
                "condicao_associada_id_cid10",
                "condicao_obito_id_cid10",
                "regra_contratual_id_scnes",
                "usuario_etnia_id_sus",
                "condicao_secundaria_1_tipo_id_sihsus",
                "condicao_secundaria_2_tipo_id_sihsus",
                "condicao_secundaria_3_tipo_id_sihsus",
                "condicao_secundaria_4_tipo_id_sihsus",
                "condicao_secundaria_5_tipo_id_sihsus",
                "condicao_secundaria_6_tipo_id_sihsus",
                "condicao_secundaria_7_tipo_id_sihsus",
                "condicao_secundaria_8_tipo_id_sihsus",
                "condicao_secundaria_9_tipo_id_sihsus",
            ],
            function=lambda elemento: (
                np.nan
                if pd.notna(elemento)
                and all(digito == "0" for digito in elemento)
                else elemento
            ),
        )
        # processar colunas lógicas
        .transform_columns(
            [
                "obito",
                "exame_vdrl",
                "usuario_homonimo",
                "gestacao_risco",
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
            f"RD{uf_sigla}{periodo_data_inicio:%y%m}"
        )
    )

    memoria_usada= aih_rd_transformada.memory_usage(deep=True).sum() / 10**6
    logging.debug(
        f"Memória ocupada pelo DataFrame transformado: {memoria_usada:.2f} mB."        
        )
    return aih_rd_transformada


def baixar_e_processar_aih_rd(uf_sigla: str, periodo_data_inicio: datetime.date):
    """
    Baixa e processa dados de registros de autorizações de internação do SIHSUS
    para uma determinada Unidade Federativa e período de tempo.

    Argumentos:
        uf_sigla (str): A sigla da Unidade Federativa para a qual os dados 
        serão baixados e processados.
        
        periodo_data_inicio (datetime.date): A data de início do período de 
        tempo para o qual os dados serão obtidos. Deve ser fornecida como um 
        objeto `datetime.date`.

    Retorna:
        dict: Um dicionário contendo informações sobre o status da operação, 
        o estado, o período, o caminho do arquivo final no Google Cloud Storage 
        (GCS) e a lista de arquivos originais DBC capturados.

    A função se conecta ao FTP do DataSUS para obter os arquivos de AIH 
    correspondentes à Unidade Federativa e ao período de tempo fornecidos. 
    Em seguida, os dados são processados e tratados.

    Após o processamento, os dados são carregados em um arquivo CSV para o 
    Google Cloud Storage (GCS). O caminho do arquivo final no GCS e a lista 
    de arquivos originais DBC capturados são incluídos no dicionário de retorno, 
    juntamente com informações sobre o estado e o período dos dados processados.
    """

    # Extrair dados
    sessao = Sessao()
    
    aih_rd_lotes = extrair_aih_rd(
        uf_sigla=uf_sigla,
        periodo_data_inicio=periodo_data_inicio,
    )

    dfs_transformados = []
    contador = 0

    for aih_rd_lote in aih_rd_lotes:
        aih_rd_transformada = transformar_aih_rd(
            sessao=sessao,
            aih_rd=aih_rd_lote,
            uf_sigla=uf_sigla,
            periodo_data_inicio=periodo_data_inicio,
        )

        dfs_transformados.append(aih_rd_transformada)
        contador += len(aih_rd_transformada)

    logging.info("Concatenando lotes de dataframes transformados e validados.")
    df_final = pd.concat(dfs_transformados, ignore_index=True)
    logging.info(f"{contador} registros concatenados no dataframe.")    

    # Salvar no GCS
    logging.info("Realizando upload para bucket do GCS...")
    nome_arquivo_csv = f"sihsus_aih_rd_{uf_sigla}_{periodo_data_inicio:%y%m}.csv"
    path_gcs = f"saude-mental/dados-publicos/sihsus/aih-reduzida/{uf_sigla}/{nome_arquivo_csv}"
    
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
        tipo='RD'
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
        f"Processamento de AIH-rd finalizado para {uf_sigla} ({periodo_data_inicio:%y%m})."
        f"Status: {response['status']}, Número de registros: {response['num_registros']}, Arquivo GCS: {response['arquivo_final_gcs']}"
    )

    sessao.close()

    return response

