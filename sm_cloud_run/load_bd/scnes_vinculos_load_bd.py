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

TIPOS_VINCULOS: Final[frozendict] = frozendict(
    {
        "id": "object",
        "unidade_geografica_id": "object",
        "periodo_id": "object",
        "estabelecimento_id_scnes": "object",
        "estabelecimento_municipio_id_sus": "object",
        "estabelecimento_regiao_saude_id_sus": "object",
        "estabelecimento_microrregiao_saude_id_sus": "object",
        "estabelecimento_distrito_sanitario_id_sus": "object",
        "estabelecimento_distrito_administrativo_id_sus": "object",
        "estabelecimento_gestao_condicao_id_scnes": "object",
        "estabelecimento_personalidade_juridica_id_scnes": "object",
        "estabelecimento_id_cpf_cnpj": "object",
        "estabelecimento_mantido": "bool",
        "estabelecimento_mantenedora_id_cnpj": "object",
        "estabelecimento_esfera_id_scnes": "object",
        "estabelecimento_atividade_ensino_id_scnes": "object",
        "estabelecimento_tributos_retencao_id_scnes": "object",
        "estabelecimento_natureza_id_scnes": "object",
        "estabelecimento_tipo_id_scnes": "object",
        "estabelecimento_fluxo_id_scnes": "object",
        "estabelecimento_turno_id_scnes": "object",
        "estabelecimento_hierarquia_id_scnes": "object",
        "estabelecimento_terceiro": "bool",
        "profissional_id_cpf_criptografado": "object",
        "profissional_cpf_unico": "object",
        "ocupacao_id_cbo2002": "object",
        "ocupacao_cbo_unico": "object",
        "profissional_nome": "object",
        "profissional_id_cns": "object",
        "profissional_conselho_tipo_id_scnes": "object",
        "profissional_id_conselho": "object",
        "tipo_id_scnes": "object",
        "contratado": "bool",
        "autonomo": "bool",
        "sem_vinculo_definido": "bool",
        "atendimento_sus": "bool",
        "atendimento_nao_sus": "bool",
        "atendimento_carga_outras": "int64",
        "atendimento_carga_hospitalar": "int64",
        "atendimento_carga_ambulatorial": "int64",
        "periodo_data_inicio": "datetime64[ns]",
        "profissional_residencia_municipio_id_sus": "object",
        "estabelecimento_natureza_juridica_id_scnes": "object",
        "criacao_data": "datetime64[ns]",
        "atualizacao_data": "datetime64[ns]",
        "ftp_arquivo_nome": "object",
    },
)


COLUNAS_NUMERICAS: Final[list[str]] = [
    nome_coluna
    for nome_coluna, tipo_coluna in TIPOS_VINCULOS.items()
    if tipo_coluna.lower() == "int64" or tipo_coluna.lower() == "float64"
]

COLUNAS_BOOLEANAS: Final[list[str]] = [
    nome_coluna
    for nome_coluna, tipo_coluna in TIPOS_VINCULOS.items()
    if tipo_coluna == "bool"
]


def transformar_tipos(
    vinculos: pd.DataFrame,
) -> pd.DataFrame:
    """
    """    
    logging.info(
        f"Forçando tipos para colunas "
    )

    vinculos_transformado = (
        vinculos  
        # garantir tipos
        .change_type(
            # HACK: ver https://github.com/pandas-dev/pandas/issues/25472
            COLUNAS_NUMERICAS,
            "float",
        )
        .transform_columns(
            COLUNAS_BOOLEANAS,
            function=lambda elemento: True if elemento == "True" else False,
        )
        .astype(TIPOS_VINCULOS)
    )
    return vinculos_transformado


def inserir_vinculos_postgres(
    uf_sigla: str,
    periodo_data_inicio: datetime.date
):
    sessao = Sessao()    
    tabela_destino = "dados_publicos.sm_scnes_vinculos_disseminacao"    
    passo = 100000

    try:   
        # Baixar CSV do GCS e carregar em um DataFrame
        path_gcs = f"saude-mental/dados-publicos/scnes/vinculos/{uf_sigla}/scnes_vinculos_disseminacao_{uf_sigla}_{periodo_data_inicio:%y%m}.csv"    
        
        vinculos = download_from_bucket(
            bucket_name="camada-bronze", 
            blob_path=path_gcs)        
        
        logging.info("Iniciando processo de exclusão de registros da tabela destino (se necessário)...")

        # Deleta conflitos para evitar duplicação de dados
        deletar_conflitos(
            sessao, 
            tabela_ref = tabelas[tabela_destino], 
            ftp_arquivo_nome_df = vinculos['ftp_arquivo_nome'].iloc[0]
        ) 

        # Divide o DataFrame em lotes
        num_lotes = len(vinculos) // passo + 1
        vinculos_lotes = np.array_split(vinculos, num_lotes)

        contador = 0
        for vinculos_lote in vinculos_lotes:
            vinculos_transformado = transformar_tipos(
                vinculos=vinculos_lote,
            )
            try:
                validar_dataframe(vinculos_transformado)
            except AssertionError as mensagem:
                sessao.rollback()
                raise RuntimeError(
                    "Dados inválidos encontrados após a transformação:"
                    + " {}".format(mensagem),
                )

            carregamento_status = carregar_dataframe(
                sessao=sessao,
                df=vinculos_transformado,
                tabela_destino=tabela_destino,
                passo=None,
            )
            if carregamento_status != 0:
                sessao.rollback()
                raise RuntimeError(
                    "Execução interrompida em razão de um erro no "
                    + "carregamento."
                )
            contador += len(vinculos_lote)

        # Registrar na tabela de metadados do FTP
        logging.info("Inserindo timestamp na tabela de metadados do FTP...")
        inserir_timestamp_ftp_metadados(
            sessao, 
            uf_sigla, 
            periodo_data_inicio, 
            coluna_atualizar='timestamp_load_bd',
            tipo='PF'
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
        "tipo": "PF",
        "estado": uf_sigla,
        "periodo": f"{periodo_data_inicio:%y%m}",
        "insercoes": contador,
    }

    return response


