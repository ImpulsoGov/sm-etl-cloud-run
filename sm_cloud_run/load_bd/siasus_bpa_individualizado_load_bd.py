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

TIPOS_BPA_I: Final[frozendict] = frozendict(
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
        "receptor_credito_id_cnpj": "object",
        "processamento_periodo_data_inicio": "datetime64[ns]",
        "realizacao_periodo_data_inicio": "datetime64[ns]",
        "procedimento_id_sigtap": "object",
        "financiamento_tipo_id_sigtap": "object",
        "financiamento_subtipo_id_sigtap": "object",
        "complexidade_id_siasus": "object",
        "autorizacao_id_siasus": "object",
        "profissional_id_cns": "object",
        "profissional_vinculo_ocupacao_id_cbo2002": "object",
        "condicao_principal_id_cid10": "object",
        "carater_atendimento_id_siasus": "object",
        "usuario_id_cns_criptografado": "object",
        "usuario_nascimento_data": "datetime64[ns]",
        "usuario_idade_tipo_id_sigtap": "object",
        "usuario_idade": "Int64",
        "usuario_sexo_id_sigtap": "object",
        "usuario_raca_cor_id_siasus": "object",
        "usuario_residencia_municipio_id_sus": "object",
        "quantidade_apresentada": "Int64",
        "quantidade_aprovada": "Int64",
        "valor_apresentado": "Float64",
        "valor_aprovado": "Float64",
        "atendimento_residencia_ufs_distintas": "bool",
        "atendimento_residencia_municipios_distintos": "bool",
        "usuario_etnia_id_sus": "object",
        "estabelecimento_natureza_juridica_id_scnes": "object",
        "id": "str",
        "periodo_id": "str",
        "unidade_geografica_id": "str",
        "criacao_data": "datetime64[ns]",
        "atualizacao_data": "datetime64[ns]",
        "ftp_arquivo_nome": "object",
    },
)

COLUNAS_NUMERICAS: Final[list[str]] = [
    nome_coluna
    for nome_coluna, tipo_coluna in TIPOS_BPA_I.items()
    if tipo_coluna.lower() == "int64" or tipo_coluna.lower() == "float64"
]


def transformar_tipos(
    bpa_i: pd.DataFrame,
) -> pd.DataFrame:
    """
    """    
    logging.info(
        f"Forçando tipos para colunas "
    )

    bpa_i_transformado = (
        bpa_i  
        # garantir tipos
        .change_type(
            # HACK: ver https://github.com/pandas-dev/pandas/issues/25472
            COLUNAS_NUMERICAS,
            "float",
        )
        .astype(TIPOS_BPA_I)
    )
    return bpa_i_transformado


def inserir_bpa_i_postgres(
    uf_sigla: str,
    periodo_data_inicio: datetime.date
):
    sessao = Sessao()    
    tabela_destino = "dados_publicos.sm_siasus_bpa_i_disseminacao"    
    passo = 100000

    try:   
        # Baixar CSV do GCS e carregar em um DataFrame
        path_gcs = f"saude-mental/dados-publicos/siasus/bpa-i-disseminacao/{uf_sigla}/siasus_bpa_i_{uf_sigla}_{periodo_data_inicio:%y%m}.csv"    
        
        bpa_i = download_from_bucket(
            bucket_name="camada-bronze", 
            blob_path=path_gcs)        
        
        logging.info("Iniciando processo de exclusão de registros da tabela destino (se necessário)...")

        # Deleta conflitos para evitar duplicação de dados
        deletar_conflitos(
            sessao, 
            tabela_ref = tabelas[tabela_destino], 
            ftp_arquivo_nome_df = bpa_i['ftp_arquivo_nome'].iloc[0]
        ) 

        # Divide o DataFrame em lotes
        num_lotes = len(bpa_i) // passo + 1
        bpa_i_lotes = np.array_split(bpa_i, num_lotes)

        contador = 0
        for bpa_i_lote in bpa_i_lotes:
            bpa_i_transformado = transformar_tipos(
                bpa_i=bpa_i_lote,
            )
            try:
                validar_dataframe(bpa_i_transformado)
            except AssertionError as mensagem:
                sessao.rollback()
                raise RuntimeError(
                    "Dados inválidos encontrados após a transformação:"
                    + " {}".format(mensagem),
                )

            carregamento_status = carregar_dataframe(
                sessao=sessao,
                df=bpa_i_transformado,
                tabela_destino=tabela_destino,
                passo=None,
            )
            if carregamento_status != 0:
                sessao.rollback()
                raise RuntimeError(
                    "Execução interrompida em razão de um erro no "
                    + "carregamento."
                )
            contador += len(bpa_i_lote)

        # Registrar na tabela de metadados do FTP
        logging.info("Inserindo timestamp na tabela de metadados do FTP...")
        inserir_timestamp_ftp_metadados(
            sessao, 
            uf_sigla, 
            periodo_data_inicio, 
            coluna_atualizar='timestamp_load_bd',
            tipo='BI'
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
        "tipo": "BI",
        "estado": uf_sigla,
        "periodo": f"{periodo_data_inicio:%y%m}",
        "insercoes": contador,
    }

    return response



# # RODAR LOCALMENTE
# if __name__ == "__main__":
#     from datetime import datetime

#     # Defina os parâmetros de teste
#     uf_sigla = "AC"
#     periodo_data_inicio = datetime.strptime("2023-02-01", "%Y-%m-%d").date()

#     # Chame a função principal com os parâmetros de teste
#     inserir_bpa_i_postgres(uf_sigla, periodo_data_inicio)
