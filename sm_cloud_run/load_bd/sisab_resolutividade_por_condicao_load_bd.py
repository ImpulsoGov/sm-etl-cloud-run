# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT

"""Extrai dados de produção da APS a partir do GCS e armazena no BD."""

# NECESSARIO PARA RODAR LOCALMENTE: Adiciona o caminho do diretório `sm_cloud_run` ao sys.path
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'sm_cloud_run')))
###

import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import numpy as np

import datetime
from typing import Final
from frozendict import frozendict
from sqlalchemy import delete
from sqlalchemy.orm import Query, Session

import janitor
from utilitarios.bd_config import Sessao, tabelas
from utilitarios.bd_utilitarios import carregar_dataframe
from utilitarios.bd_utilitarios import inserir_timestamp_sisab_metadados
from utilitarios.config_painel_sm import municipios_ativos_painel
from utilitarios.cloud_storage import sisab_download_from_bucket

import logging
from utilitarios.logger_config import logger_config
logger_config()



TIPOS_SISAB_RESOLUTIVIDADE: Final[frozendict] = frozendict(
    {
        "id": "object",
        "unidade_geografica_id": "object",
        "unidade_geografica_id_sus": "object",
        "periodo_id": "object",
        "periodo_data_inicio": "datetime64[ns]",
        "tipo_producao": "object",
        "conduta": "object",
        "problema_condicao_avaliada": "object",
        "quantidade_registrada": "Int64",
        "atualizacao_data": "datetime64[ns]",
    }
)

COLUNAS_NUMERICAS: Final[list[str]] = [
    nome_coluna
    for nome_coluna, tipo_coluna in TIPOS_SISAB_RESOLUTIVIDADE.items()
    if tipo_coluna.lower() == "int64" or tipo_coluna.lower() == "float64"
]


def transformar_tipos(
    df_extraido: pd.DataFrame,
) -> pd.DataFrame:
    """
    """    
    logging.info(
        f"Forçando tipos para colunas "
    )

    df_extraido = (
        df_extraido
        .change_type(
            COLUNAS_NUMERICAS,
            "float",
        )
    )

    df_tratado = (
        df_extraido  
        # garantir tipos        
        .astype(TIPOS_SISAB_RESOLUTIVIDADE, errors="ignore").where(
            df_extraido.notna(), None
        )
    )
    return df_tratado


def obter_lista_registros_inseridos(
    sessao: Session,
    tabela_destino: str,
) -> Query:
    """Obtém lista de registro da períodos que já constam na tabela.
    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
        acessar a base de dados da ImpulsoGov.
        tabela_destino: Tabela que irá acondicionar os dados.
    Retorna:
        Lista de períodos que já constam na tabela destino 
    [`sqlalchemy.orm.session.Session`]: https://docs.sqlalchemy.org/en/14/orm/session_api.html#sqlalchemy.orm.Session
    """

    tabela = tabelas[tabela_destino]
    registros = sessao.query(tabela.c.periodo_data_inicio).distinct(
        tabela.c.periodo_data_inicio
    )

    logging.info("Leitura dos períodos inseridos no banco Impulso OK!")
    return registros


def carregar_dados(
    periodo_data_inicio: datetime.date,
) -> int:
    """
    """     
    sessao = Sessao()  
    tabela_destino = "dados_publicos.sm_sisab_resolutividade_por_condicao_teste"
    
    logging.info("Excluíndo registros se houver atualização retroativa...")
    
    tabela_relatorio_producao = tabelas[tabela_destino]
    registros_inseridos = obter_lista_registros_inseridos(sessao, tabela_destino)

    if any(
        [registro.periodo_data_inicio == periodo_data_inicio for registro in registros_inseridos]
    ):
        limpar = (
            delete(tabela_relatorio_producao)
            .where(tabela_relatorio_producao.c.periodo_data_inicio == periodo_data_inicio)
        )
        logging.debug(limpar)
        resultado = sessao.execute(limpar)
        logging.info(f"Número de linhas deletadas: {resultado.rowcount}")

    logging.info("Baixando dados do GCS...")
    # Baixar CSV do GCS e carregar em um DataFrame
    path_gcs = f"saude-mental/dados-publicos/sisab/conduta-por-condicao/sisab_resolutividade_por_condicao_{periodo_data_inicio:%y%m}.csv"       

    df_extraido = sisab_download_from_bucket(
        bucket_name="camada-bronze", 
        blob_path=path_gcs
    ) 

    try:
        logging.info("Compatibilizando tipos...")
        df_tratado = transformar_tipos(
            df_extraido=df_extraido
        )


        logging.info("Carregando dados em tabela...")
        carregamento_status = carregar_dataframe(
            sessao=sessao, 
            df=df_tratado, 
            tabela_destino=tabela_destino
        )
        if carregamento_status != 0:
            sessao.rollback()
            raise RuntimeError(
                "Execução interrompida em razão de um erro no carregamento."
            )
        
        # Registrar na tabela de metadados do banco
        logging.info("Inserindo timestamp na tabela de metadados do FTP...")
        municipios_painel = municipios_ativos_painel(sessao)
        inserir_timestamp_sisab_metadados(
            sessao, 
            periodo_data_inicio, 
            municipios_painel,
            coluna_atualizar='timestamp_load_bd',
            tipo='SISAB_resolutividade_condicao'
        )

        sessao.commit()
    
    except Exception as e:
        sessao.rollback()
        raise RuntimeError(f"Erro durante a inserção no banco de dados: {format(str(e))}")
    
    finally:
        sessao.close()

    logging.info(
        f"Carregamento concluído para a tabela `{tabela_destino}`: "
        + f"adicionadas {len(df_tratado)} novas linhas."
    )

    response = {
        "status": "OK",
        "tipo": "SISAB_resolutividade_condicao",
        "periodo": f"{periodo_data_inicio:%y%m}",
        "insercoes": len(df_tratado),
    }

    return response




# RODAR LOCALMENTE
if __name__ == "__main__":
    from datetime import datetime
    periodo_data_inicio = datetime.strptime("2024-06-01", "%Y-%m-%d").date()
    carregar_dados(periodo_data_inicio)

















   


