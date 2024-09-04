from __future__ import annotations

# NECESSARIO PARA RODAR LOCALMENTE: Adiciona o caminho do diretório `sm_cloud_run` ao sys.path
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'sm_cloud_run')))
###

import os
import pandas as pd
import numpy as np
import logging
import datetime
from typing import Final

import janitor
from frozendict import frozendict
from utilitarios.bd_config import Sessao, tabelas
from utilitarios.bd_utilitarios import inserir_timestamp_ftp_metadados

from utilitarios.cloud_storage import download_from_bucket
from utilitarios.bd_utilitarios import carregar_dataframe, validar_dataframe, deletar_conflitos
from utilitarios.logger_config import logger_config

# set up logging to file
logger_config()


TIPOS_AIH_RD: Final[frozendict] = frozendict(
    {
        "gestao_unidade_geografica_id_sus": "object",
        "periodo_data_inicio": "datetime64[ns]",
        "leito_especialidade_id_sigtap": "object",
        "estabelecimento_id_cnpj": "object",
        "aih_id_sihsus": "object",
        "aih_tipo_id_sihsus": "object",
        "usuario_residencia_cep": "object",
        "usuario_residencia_municipio_id_sus": "object",
        "usuario_nascimento_data": "datetime64[ns]",
        "usuario_sexo_id_sihsus": "object",
        "uti_diarias": "int64",
        "uti_tipo_id_sihsus": "object",
        "unidade_intermediaria_diarias": "int64",
        "acompanhante_diarias": "int64",
        "diarias": "int64",
        "procedimento_solicitado_id_sigtap": "object",
        "procedimento_realizado_id_sigtap": "object",
        "valor_servicos_hospitalares": "object",
        "valor_servicos_profissionais": "object",
        "valor_total": "float64",
        "valor_uti": "float64",
        "valor_total_dolar": "float64",
        "aih_data_inicio": "datetime64[ns]",
        "aih_data_fim": "datetime64[ns]",
        "condicao_principal_id_cid10": "object",
        "condicao_secundaria_id_cid10": "object",
        "desfecho_motivo_id_sihsus": "object",
        "estabelecimento_natureza_id_scnes": "object",
        "estabelecimento_natureza_juridica_id_scnes": "object",
        "gestao_condicao_id_sihsus": "object",
        "exame_vdrl": "bool",
        "unidade_geografica_id_sus": "object",
        "usuario_idade_tipo_id_sigtap": "object",
        "usuario_idade": "int64",
        "permanencia_duracao": "int64",
        "obito": "bool",
        "usuario_nacionalidade_id_sigtap": "object",
        "carater_atendimento_id_sihsus": "object",
        "usuario_homonimo": "bool",
        "usuario_filhos_quantidade": "Int64",
        "usuario_instrucao_id_sihsus": "object",
        "condicao_notificacao_id_cid10": "object",
        "usuario_contraceptivo_principal_id_sihsus": "object",
        "usuario_contraceptivo_secundario_id_sihsus": "object",
        "gestacao_risco": "bool",
        "usuario_id_pre_natal": "object",
        "remessa_aih_id_sequencial_longa_permanencia": "object",
        "usuario_ocupacao_id_cbo2002": "object",
        "usuario_atividade_id_cnae": "object",
        "usuario_vinculo_previdencia_id_sihsus": "object",
        "autorizacao_gestor_motivo_id_sihsus": "object",
        "autorizacao_gestor_tipo_id_sihsus": "object",
        "autorizacao_gestor_id_cpf": "object",
        "autorizacao_gestor_data": "datetime64[ns]",
        "estabelecimento_id_scnes": "object",
        "mantenedora_id_cnpj": "object",
        "infeccao_hospitalar": "bool",
        "condicao_associada_id_cid10": "object",
        "condicao_obito_id_cid10": "object",
        "complexidade_id_sihsus": "object",
        "financiamento_tipo_id_sigtap": "object",
        "financiamento_subtipo_id_sigtap": "object",
        "regra_contratual_id_scnes": "object",
        "usuario_raca_cor_id_sihsus": "object",
        "usuario_etnia_id_sus": "object",
        "remessa_aih_id_sequencial": "object",
        "remessa_id_sihsus": "object",
        "cns_ausente_justificativa_auditor": "object",
        "cns_ausente_justificativa_estabelecimento": "object",
        "valor_servicos_hospitalares_complemento_federal": "float64",
        "valor_servicos_profissionais_complemento_federal": "float64",
        "valor_servicos_hospitalares_complemento_local": "float64",
        "valor_servicos_profissionais_complemento_local": "float64",
        "valor_unidade_neonatal": "float64",
        "unidade_neonatal_tipo_id_sihsus": "object",
        "condicao_secundaria_1_id_cid10": "object",
        "condicao_secundaria_2_id_cid10": "object",
        "condicao_secundaria_3_id_cid10": "object",
        "condicao_secundaria_4_id_cid10": "object",
        "condicao_secundaria_5_id_cid10": "object",
        "condicao_secundaria_6_id_cid10": "object",
        "condicao_secundaria_7_id_cid10": "object",
        "condicao_secundaria_8_id_cid10": "object",
        "condicao_secundaria_9_id_cid10": "object",
        "condicao_secundaria_1_tipo_id_sihsus": "object",
        "condicao_secundaria_2_tipo_id_sihsus": "object",
        "condicao_secundaria_3_tipo_id_sihsus": "object",
        "condicao_secundaria_4_tipo_id_sihsus": "object",
        "condicao_secundaria_5_tipo_id_sihsus": "object",
        "condicao_secundaria_6_tipo_id_sihsus": "object",
        "condicao_secundaria_7_tipo_id_sihsus": "object",
        "condicao_secundaria_8_tipo_id_sihsus": "object",
        "condicao_secundaria_9_tipo_id_sihsus": "object",
        "id": "object",
        "periodo_id": "object",
        "unidade_geografica_id": "object",
        "criacao_data": "datetime64[ns]",
        "atualizacao_data": "datetime64[ns]",
        "_nao_documentado_uti_mes_in": "object",
        "_nao_documentado_uti_mes_an": "object",
        "_nao_documentado_uti_mes_al": "object",
        "_nao_documentado_uti_int_in": "object",
        "_nao_documentado_uti_int_an": "object",
        "_nao_documentado_uti_int_al": "object",
        "_nao_documentado_val_sadt": "object",
        "_nao_documentado_val_rn": "object",
        "_nao_documentado_val_acomp": "object",
        "_nao_documentado_val_ortp": "object",
        "_nao_documentado_val_sangue": "object",
        "_nao_documentado_val_sadtsr": "object",
        "_nao_documentado_val_transp": "object",
        "_nao_documentado_val_obsang": "object",
        "_nao_documentado_val_ped1ac": "object",
        "_nao_documentado_rubrica": "object",
        "_nao_documentado_num_proc": "object",
        "_nao_documentado_tot_pt_sp": "object",
        "_nao_documentado_cpf_aut": "object",
        "ftp_arquivo_nome": "object",
    },
)


COLUNAS_NUMERICAS: Final[list[str]] = [
    nome_coluna
    for nome_coluna, tipo_coluna in TIPOS_AIH_RD.items()
    if tipo_coluna.lower() == "int64" or tipo_coluna.lower() == "float64"
]


def transformar_tipos(
    aih_rd: pd.DataFrame,
) -> pd.DataFrame:
    """
    """    
    logging.info(
        f"Forçando tipos para colunas "
    )

    aih_rd_transformado = (
        aih_rd
        # garantir tipos
        # HACK: ver https://github.com/pandas-dev/pandas/issues/25472
        .astype({col: "float" for col in COLUNAS_NUMERICAS})
        .astype(TIPOS_AIH_RD)
    )
    return aih_rd_transformado


def inserir_aih_rd_postgres(
    uf_sigla: str,
    periodo_data_inicio: datetime.date
):
    sessao = Sessao()
    tabela_destino = "dados_publicos.sm_sihsus_aih_reduzida_disseminacao"
    passo = 100000

    try:   
        # Baixar CSV do GCS e carregar em um DataFrame
        path_gcs = f"saude-mental/dados-publicos/sihsus/aih-reduzida/{uf_sigla}/sihsus_aih_rd_{uf_sigla}_{periodo_data_inicio:%y%m}.csv"    
        
        aih_rd = download_from_bucket(
            bucket_name="camada-bronze", 
            blob_path=path_gcs)        
        
        logging.info("Iniciando processo de exclusão de registros da tabela destino (se necessário)...")

        # Deleta conflitos para evitar duplicação de dados
        deletar_conflitos(
            sessao, 
            tabela_ref = tabelas[tabela_destino], 
            ftp_arquivo_nome_df = aih_rd['ftp_arquivo_nome'].iloc[0]
        ) 


        # Divide o DataFrame em lotes
        num_lotes = len(aih_rd) // passo + 1
        aih_rd_lotes = np.array_split(aih_rd, num_lotes)

        contador = 0
        for aih_rd_lote in aih_rd_lotes:
            aih_rd_transformado = transformar_tipos( 
                aih_rd=aih_rd_lote,
            )
            try:
                validar_dataframe(aih_rd_transformado)
            except AssertionError as mensagem:
                sessao.rollback()
                raise RuntimeError(
                    "Dados inválidos encontrados após a transformação:"
                    + " {}".format(mensagem),
                )

            carregamento_status = carregar_dataframe(
                sessao=sessao,
                df=aih_rd_transformado,
                tabela_destino=tabela_destino,
                passo=None,
            )
            if carregamento_status != 0:
                sessao.rollback()
                raise RuntimeError(
                    "Execução interrompida em razão de um erro no "
                    + "carregamento."
                )
            contador += len(aih_rd_lote)


        # Registrar na tabela de metadados do FTP
        logging.info("Inserindo timestamp na tabela de metadados do FTP...")
        inserir_timestamp_ftp_metadados(
            sessao, 
            uf_sigla, 
            periodo_data_inicio, 
            coluna_atualizar='timestamp_load_bd',
            tipo='RD'
        )

        # Se tudo ocorreu sem erros, commita a transação
        sessao.commit()

    except Exception as e:
        # Em caso de erro, faz rollback da transação
        sessao.rollback()
        raise RuntimeError(f"Erro durante a inserção no banco de dados: {format(str(e))}")

    finally:
        # Independentemente de sucesso ou falha, fecha a sessão
        sessao.close()

    response = {
        "status": "OK",
        "tipo": "RD",
        "estado": uf_sigla,
        "periodo": f"{periodo_data_inicio:%y%m}",
        "insercoes": contador,
    }

    return response



