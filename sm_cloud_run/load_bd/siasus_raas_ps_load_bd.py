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
from io import StringIO

import janitor
from frozendict import frozendict
from utilitarios.bd_config import Sessao, tabelas
from utilitarios.bd_utilitarios import inserir_timestamp_ftp_metadados

from utilitarios.cloud_storage import download_from_bucket
from utilitarios.bd_utilitarios import carregar_dataframe, validar_dataframe, deletar_conflitos
from utilitarios.logger_config import logger_config

# set up logging to file
logger_config()

TIPOS_RAAS_PS: Final[frozendict] = frozendict(
    {
        "estabelecimento_id_scnes": "object",
        "gestao_unidade_geografica_id_sus": "object",
        "gestao_condicao_id_siasus": "object",
        "unidade_geografica_id_sus": "object",
        "estabelecimento_tipo_id_sigtap": "object",
        "prestador_tipo_id_sigtap": "object",
        "estabelecimento_mantido": "bool",
        "estabelecimento_id_cnpj": "object",
        "mantenedora_id_cnpj": "object",
        "processamento_periodo_data_inicio": "datetime64[ns]",
        "realizacao_periodo_data_inicio": "datetime64[ns]",
        "usuario_id_cns_criptografado": "object",
        "usuario_nascimento_data": "datetime64[ns]",
        "usuario_idade_tipo_id_sigtap": "object",
        "usuario_idade": "Int64",
        "usuario_nacionalidade_id_sus": "object",
        "usuario_sexo_id_sigtap": "object",
        "usuario_raca_cor_id_siasus": "object",
        "usuario_etnia_id_sus": "object",
        "usuario_residencia_municipio_id_sus": "object",
        "desfecho_motivo_id_siasus": "object",
        "desfecho_data": "datetime64[ns]",
        "carater_atendimento_id_siasus": "object",
        "condicao_principal_id_cid10": "object",
        "condicao_associada_id_cid10": "object",
        "procedencia_id_siasus": "object",
        "raas_data_inicio": "datetime64[ns]",
        "raas_data_fim": "datetime64[ns]",
        "esf_cobertura": "bool",
        "esf_estabelecimento_id_scnes": "object",
        "desfecho_destino_id_siasus": "object",
        "procedimento_id_sigtap": "object",
        "quantidade_apresentada": "Int64",
        "quantidade_aprovada": "Int64",
        "servico_id_sigtap": "object",
        "servico_classificacao_id_sigtap": "object",
        "usuario_situacao_rua": "bool",
        "usuario_abuso_substancias": "bool",
        "usuario_abuso_substancias_alcool": "bool",
        "usuario_abuso_substancias_crack": "bool",
        "usuario_abuso_substancias_outras": "bool",
        "local_realizacao_id_siasus": "object",
        "data_inicio": "datetime64[ns]",
        "data_fim": "datetime64[ns]",
        # coluna de duração idealmente seria do tipo 'timedelta[ns]', mas esse
        # tipo não converte facilmente para o tipo INTERVEL do PostgreSQL;
        # ver https://stackoverflow.com/q/55516374/7733563
        "permanencia_duracao": "object",
        "quantidade_atendimentos": "Int64",
        "quantidade_usuarios": "Int64",
        "estabelecimento_natureza_juridica_id_scnes": "object",
        "id": str,
        "periodo_id": str,
        "unidade_geografica_id": str,
        "criacao_data": "datetime64[ns]",
        "atualizacao_data": "datetime64[ns]",
        "ftp_arquivo_nome": "object",
    },
)

COLUNAS_NUMERICAS: Final[list[str]] = [
    nome_coluna
    for nome_coluna, tipo_coluna in TIPOS_RAAS_PS.items()
    if tipo_coluna == "Int64"
]


def transformar_tipos(
    raas_ps: pd.DataFrame,
) -> pd.DataFrame:
    """
    """    
    logging.info(
        f"Forçando tipos para colunas "
    )

    raas_ps_transformado = (
        raas_ps 
        # garantir tipos
        .change_type(
            # HACK: ver https://github.com/pandas-dev/pandas/issues/25472
            COLUNAS_NUMERICAS,
            "float",
        )
        .astype(TIPOS_RAAS_PS)
    )
    return raas_ps_transformado


def inserir_raas_ps_postgres(
    uf_sigla: str,
    periodo_data_inicio: datetime.date
):
    sessao = Sessao()
    tabela_destino = "dados_publicos.sm_siasus_raas_psicossocial_disseminacao"
    passo = 100000

    try:   
        # Baixar CSV do GCS e carregar em um DataFrame
        path_gcs = f"saude-mental/dados-publicos/siasus/raas-psicossocial/{uf_sigla}/siasus_raas_ps_disseminacao_{uf_sigla}_{periodo_data_inicio:%y%m}.csv"    
        
        raas_ps = download_from_bucket(
            bucket_name="camada-bronze", 
            blob_path=path_gcs)
        
        
        logging.info("Iniciando processo de exclusão de registros da tabela destino (se necessário)...")

        # Deleta conflitos para evitar duplicação de dados
        deletar_conflitos(
            sessao, 
            tabela_ref = tabelas[tabela_destino], 
            ftp_arquivo_nome_df = raas_ps['ftp_arquivo_nome'].iloc[0]
        ) 


        # Divide o DataFrame em lotes
        num_lotes = len(raas_ps) // passo + 1
        raas_ps_lotes = np.array_split(raas_ps, num_lotes)

        contador = 0
        for raas_ps_lote in raas_ps_lotes:
            raas_ps_transformado = transformar_tipos(
                raas_ps=raas_ps_lote,
            )
            try:
                validar_dataframe(raas_ps_transformado)
            except AssertionError as mensagem:
                sessao.rollback()
                raise RuntimeError(
                    "Dados inválidos encontrados após a transformação:"
                    + " {}".format(mensagem),
                )

            carregamento_status = carregar_dataframe(
                sessao=sessao,
                df=raas_ps_transformado,
                tabela_destino=tabela_destino,
                passo=None,
            )
            if carregamento_status != 0:
                sessao.rollback()
                raise RuntimeError(
                    "Execução interrompida em razão de um erro no "
                    + "carregamento."
                )
            contador += len(raas_ps_lote)


        # Registrar na tabela de metadados do FTP
        logging.info("Inserindo timestamp na tabela de metadados do FTP...")
        inserir_timestamp_ftp_metadados(
            sessao, 
            uf_sigla, 
            periodo_data_inicio, 
            coluna_atualizar='timestamp_load_bd',
            tipo='PS'
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
        "tipo": "PS",
        "estado": uf_sigla,
        "periodo": f"{periodo_data_inicio:%y%m}",
        "insercoes": contador,
    }

    return response