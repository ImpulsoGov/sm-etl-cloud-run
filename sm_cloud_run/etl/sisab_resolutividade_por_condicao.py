# SPDX-FileCopyrightText: 2021, 2022 ImpulsoGov <contato@impulsogov.org>
#
# SPDX-License-Identifier: MIT

"""Extrai dados de produção da APS a partir do SISAB."""

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
from datetime import date
from typing import Final
from frozendict import frozendict
from sqlalchemy.orm import Session

from utilitarios.sisab_relatorio_producao_utilitarios import extrair_producao_por_municipio, transformar_producao_por_municipio
from utilitarios.config_painel_sm import municipios_ativos_painel
from utilitarios.cloud_storage import sisab_upload_to_bucket
from utilitarios.datas import periodo_por_data, agora_gmt_menos3
from utilitarios.geografias import id_sus_para_id_impulso
from utilitarios.bd_config import Sessao
from utilitarios.bd_utilitarios import inserir_timestamp_sisab_metadados
from uuid6 import uuid7
import janitor

import logging
from utilitarios.logger_config import logger_config
logger_config()


## INÍCIO EXTRAÇÃO

def extrair_relatorio(periodo_competencia: datetime.date)-> pd.DataFrame():
    """
    Extrai relatório de produção por problema/condição avaliada e conduta a partir da página do SISAB

     Argumentos:
        periodo_data_inicio: Data da competência 
     Retorna:
        Objeto [`pandas.DataFrame`] com os dados extraídos.
    """
    df_consolidado = pd.DataFrame()
    
    try:
        df_parcial = extrair_producao_por_municipio(
            tipo_producao="Atendimento Individual",
            competencias=[periodo_competencia],
            selecoes_adicionais={
                "Problema/Condição Avaliada": "Selecionar Todos", 
                "Conduta":"Selecionar Todos",
            },
            ).pipe(transformar_producao_por_municipio)
        
        df_consolidado = df_consolidado.append(df_parcial)

    except Exception as e:
        RuntimeError(f"Erro: {format(str(e))}")
        pass

    # print(df_consolidado)
    return df_consolidado


# ## INÍCIO TRATAMENTO

COLUNAS_RENOMEAR: Final[dict[str, str]] = {
    "Conduta":"conduta",
    "Problema/Condição Avaliada":"problema_condicao_avaliada",
    "quantidade_aprovada":"quantidade_registrada",
    "municipio_id_sus":"unidade_geografica_id_sus"
}

COLUNAS_EXCLUIR = [
    'uf_sigla', 
    # 'municipio_id_sus', 
    'municipio_nome', 
    # 'periodo_data_inicio', 
]

COLUNAS_TIPOS: Final[frozendict] = frozendict(
    {        
    "conduta":"str",
    "problema_condicao_avaliada":"str",
    "quantidade_registrada":"int",
    # "periodo_id":"str",
    # "unidade_geografica_id_sus":"str",
    "tipo_producao":"str"
    }
)

COLUNAS_ORDENADAS = [
    'id',
    'unidade_geografica_id',
    'unidade_geografica_id_sus',
    'periodo_id',
    'periodo_data_inicio',
    'tipo_producao',
    'conduta',
    'problema_condicao_avaliada',
    'quantidade_registrada',
    'atualizacao_data'    
]

def renomear_colunas(df_extraido: pd.DataFrame) -> pd.DataFrame:
    df_extraido.rename(columns=COLUNAS_RENOMEAR, inplace=True)
    return df_extraido

def excluir_colunas(df_extraido: pd.DataFrame) -> pd.DataFrame:
    df_extraido.drop(columns=COLUNAS_EXCLUIR, inplace=True)
    return df_extraido

def tratar_tipos(df_extraido: pd.DataFrame) -> pd.DataFrame:
    df_extraido = df_extraido.astype(COLUNAS_TIPOS, errors="ignore").where(
        df_extraido.notna(), None
    )
    return df_extraido



def tratamento_dados(
    sessao: Session,
    df_extraido: pd.DataFrame, 
    unidade_geografica_id_sus: list[str]
) -> pd.DataFrame:
    
    logging.info("Iniciando o tratamento dos dados...")

    df_extraido = excluir_colunas(df_extraido)
    df_extraido = renomear_colunas(df_extraido)
    df_extraido = df_extraido.loc[df_extraido['unidade_geografica_id_sus'].isin(unidade_geografica_id_sus)].reset_index(drop=True)
    df_extraido['tipo_producao']='Atendimento Individual'
    tratar_tipos(df_extraido) 

    # Insere IDs de unidade geografica e período
    df_extraido_com_ids = (
        df_extraido
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
        .add_column("atualizacao_data", agora_gmt_menos3())
        .reindex(columns=COLUNAS_ORDENADAS)
    )   
    logging.info("Dados tratados com sucesso!")

    return df_extraido_com_ids


def baixar_e_processar_sisab_resolutividade(periodo_data_inicio: datetime.date):
    
    try:
        df_extraido = extrair_relatorio(
            periodo_competencia=periodo_data_inicio,
        )

        sessao = Sessao()
        municipios_painel = municipios_ativos_painel(sessao)
        df_tratado = tratamento_dados(
            sessao=sessao,
            df_extraido=df_extraido,
            unidade_geografica_id_sus = municipios_painel
        )

        # Registrar na tabela de metadados do banco
        logging.info("Inserindo timestamp na tabela de metadados do FTP...")
        inserir_timestamp_sisab_metadados(
            sessao, 
            periodo_data_inicio, 
            municipios_painel,
            coluna_atualizar='timestamp_etl_gcs',
            tipo='SISAB_resolutividade_condicao'
        )
        
        # Salvar no GCS
        logging.info("Realizando upload para bucket do GCS...")
        nome_arquivo_csv = f"sisab_resolutividade_por_condicao_{periodo_data_inicio:%y%m}.csv"
        path_gcs = f"saude-mental/dados-publicos/sisab/conduta-por-condicao/{nome_arquivo_csv}"
        
        sisab_upload_to_bucket(
            bucket_name="camada-bronze", 
            blob_path=path_gcs,
            dados=df_tratado.to_csv(index=False)
        )

    except Exception as e:
        # Em caso de erro, desfaz a transação
        sessao.rollback()
        logging.error(f"Erro ao inserir ou atualizar timestamp: {e}")
        raise

    finally:
        sessao.close()
    


    # Obter sumário de resposta
    response = {
        "status": "OK",
        "periodo": f"{periodo_data_inicio:%y%m}",
        "num_registros": len(df_tratado),
        "arquivo_final_gcs": f"gcs://camada-bronze/{path_gcs}",
    }
    

    logging.info(
        f"Processamento de SISAB - Resolutividade por condição finalizado para ({periodo_data_inicio:%y%m})."
    )

    return response




# RODAR LOCALMENTE
if __name__ == "__main__":
    from datetime import datetime

    # Define os parâmetros de teste
    periodo_data_inicio = datetime.strptime("2024-06-01", "%Y-%m-%d").date()
    
    # extrair_relatorio(
    #     periodo_competencia=periodo_data_inicio,
    # )

    baixar_e_processar_sisab_resolutividade(
        periodo_data_inicio=periodo_data_inicio
    )
