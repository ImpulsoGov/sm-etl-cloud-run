from __future__ import annotations

# # NECESSARIO PARA RODAR LOCALMENTE: Adiciona o caminho do diretório `sm_cloud_run` ao sys.path
# import os
# import sys
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'sm_cloud_run')))
# ###

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

from utilitarios.logger_config import logger_config
from utilitarios.cloud_storage import download_from_bucket
from utilitarios.bd_utilitarios import carregar_dataframe, validar_dataframe, deletar_conflitos

# set up logging to file
logger_config()


TIPOS_HABILITACOES: Final[frozendict] = frozendict(
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
        "estabelecimento_tributos_retencao_id_scnes": "object",
        "estabelecimento_natureza_id_scnes": "object",
        "estabelecimento_fluxo_id_scnes": "object",
        "estabelecimento_atividade_ensino_id_scnes": "object",
        "estabelecimento_tipo_id_scnes": "object",
        "estabelecimento_turno_id_scnes": "object",
        "estabelecimento_hierarquia_id_scnes": "object",
        "estabelecimento_terceiro": "bool",
        "atendimento_sus": "bool",
        "prestador_tipo_id_fca": "object",
        "habilitacao_id_scnes": "object",
        "vigencia_data_inicio": "datetime64[ns]",
        "vigencia_data_fim": "datetime64[ns]",
        "portaria_data": "datetime64[ns]",
        "portaria_nome": "object",
        "portaria_periodo_data_inicio": "datetime64[ns]",
        "leitos_quantidade": "int64",
        "periodo_data_inicio": "datetime64[ns]",
        "estabelecimento_natureza_juridica_id_scnes": "object",
        "estabelecimento_cep": "object",
        "criacao_data": "datetime64[ns]",
        "atualizacao_data": "datetime64[ns]",
        "ftp_arquivo_nome": "object",
    },
)

COLUNAS_NUMERICAS: Final[list[str]] = [
    nome_coluna
    for nome_coluna, tipo_coluna in TIPOS_HABILITACOES.items()
    if tipo_coluna.lower() == "int64" or tipo_coluna.lower() == "float64"
]

COLUNAS_BOOLEANAS: Final[list[str]] = [
    nome_coluna
    for nome_coluna, tipo_coluna in TIPOS_HABILITACOES.items()
    if tipo_coluna == "bool"
]

def transformar_tipos(
    habilitacoes: pd.DataFrame,
) -> pd.DataFrame:
    """
    """    
    logging.info(
        f"Forçando tipos para colunas "
    )

    habilitacoes_transformado = (
        habilitacoes  
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
        .astype(TIPOS_HABILITACOES)
    )
    return habilitacoes_transformado


def inserir_habilitacoes_postgres(
    uf_sigla: str,
    periodo_data_inicio: datetime.date
):
    sessao = Sessao()    
    tabela_destino = "dados_publicos.sm_scnes_habilitacoes_disseminacao"    
    passo = 100000

    try:   
        # Baixar CSV do GCS e carregar em um DataFrame
        path_gcs = f"saude-mental/dados-publicos/scnes/habilitacoes/{uf_sigla}/scnes_habilitacoes_{uf_sigla}_{periodo_data_inicio:%y%m}.csv"    
        
        habilitacoes = download_from_bucket(
            bucket_name="camada-bronze", 
            blob_path=path_gcs)        
        
        logging.info("Iniciando processo de exclusão de registros da tabela destino (se necessário)...")

        # Deleta conflitos para evitar duplicação de dados
        deletar_conflitos(
            sessao, 
            tabela_ref = tabelas[tabela_destino], 
            ftp_arquivo_nome_df = habilitacoes['ftp_arquivo_nome'].iloc[0]
        ) 

        # Divide o DataFrame em lotes
        num_lotes = len(habilitacoes) // passo + 1
        habilitacoes_lotes = np.array_split(habilitacoes, num_lotes)

        contador = 0
        for habilitacoes_lote in habilitacoes_lotes:
            habilitacoes_transformado = transformar_tipos(
                habilitacoes=habilitacoes_lote,
            )
            try:
                validar_dataframe(habilitacoes_transformado)
            except AssertionError as mensagem:
                sessao.rollback()
                raise RuntimeError(
                    "Dados inválidos encontrados após a transformação:"
                    + " {}".format(mensagem),
                )

            carregamento_status = carregar_dataframe(
                sessao=sessao,
                df=habilitacoes_transformado,
                tabela_destino=tabela_destino,
                passo=None,
            )
            if carregamento_status != 0:
                sessao.rollback()
                raise RuntimeError(
                    "Execução interrompida em razão de um erro no "
                    + "carregamento."
                )
            contador += len(habilitacoes_lote)

        # Registrar na tabela de metadados do FTP
        logging.info("Inserindo timestamp na tabela de metadados do FTP...")
        inserir_timestamp_ftp_metadados(
            sessao, 
            uf_sigla, 
            periodo_data_inicio, 
            coluna_atualizar='timestamp_load_bd',
            tipo='HB'
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
        "tipo": "HB",
        "estado": uf_sigla,
        "periodo": f"{periodo_data_inicio:%y%m}",
        "insercoes": contador,
    }

    return response

