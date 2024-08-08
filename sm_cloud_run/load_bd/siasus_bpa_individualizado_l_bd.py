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
from io import StringIO

from google.cloud import storage

import janitor
from frozendict import frozendict
from sqlalchemy.orm import Session
from utilitarios.bd_config import Sessao, tabelas

from utilitarios.logger_config import logger_config
from utilitarios.cloud_storage import download_from_bucket
from utilitarios.bd_utilitarios import carregar_dataframe

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
    },
)

COLUNAS_NUMERICAS: Final[list[str]] = [
    nome_coluna
    for nome_coluna, tipo_coluna in TIPOS_BPA_I.items()
    if tipo_coluna.lower() == "int64" or tipo_coluna.lower() == "float64"
]

def transformar_tipos(
    sessao: Session,
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
    periodo_data_inicio: datetime.date,
    tabela_destino: str,
):
    session = Sessao()
    
    try:   
        # Baixar CSV do GCS e carregar em um DataFrame
        path_gcs = f"saude-mental/dados-publicos/siasus/bpa-i-disseminacao/{uf_sigla}/siasus_bpa_i_{uf_sigla}_{periodo_data_inicio:%y%m}.csv"    
        
        bpa_i = download_from_bucket(
            bucket_name="camada-bronze", 
            blob_path=path_gcs)
        
        # Cria um objeto de lista que conterá dados se o parâmetro de competência fornecido na execução 
        # da função (periodo_data_inicio) for igual a alguma data de processamento já presente na tabela  
        tabela_fonte = tabelas[tabela_destino]          
        query = session.query(tabela_fonte).filter_by(processamento_periodo_data_inicio = periodo_data_inicio)
        existing_data = query.first()


        if existing_data:
            # Filtra os dados do GCS que não possuem `processamento_periodo_data_inicio` igual ao parâmetro periodo_data_inicio
            bpa_i_filtrada = pa[pa['processamento_periodo_data_inicio'] != periodo_data_inicio.strftime("%Y-%m-%d")]

            if bpa_i_filtrada.empty:
                raise RuntimeError("Todos os dados possuem uma data de processamento que já existe na "
                                   + "tabela destino, portanto não foram inseridos.")
            
            else: 
                pa = bpa_i_filtrada            
                logging.warning(f"Haviam {len(bpa_i)} registros na competência de processamento {periodo_data_inicio}, " 
                                + f"mas {len(bpa_i) - len(bpa_i_filtrada)} registros não foram incluídos porque apresentavam "
                                + "data de processamento que já foi inserida na tabela destino.")            


        # Obtem tamanho do lote de processamento
        passo = int(os.getenv("IMPULSOETL_LOTE_TAMANHO", 100000))

        # Divide o DataFrame em lotes
        num_lotes = len(bpa_i) // passo + 1
        bpa_i_lotes = np.array_split(bpa_i, num_lotes)

        contador = 0
        for bpa_i_lote in bpa_i_lotes:
            bpa_i_transformada = transformar_tipos(
                sessao=session, 
                bpa_i=bpa_i_lote,
            )

            carregamento_status = carregar_dataframe(
                sessao=session,
                df=bpa_i_transformada,
                tabela_destino=tabela_destino,
                passo=None,
            )
            if carregamento_status != 0:
                session.rollback()
                raise RuntimeError(
                    "Execução interrompida em razão de um erro no "
                    + "carregamento."
                )
            contador += len(bpa_i_lote)

        # Se tudo ocorreu sem erros, commita a transação
        session.commit()

    except Exception as e:
        # Em caso de erro, faz rollback da transação
        session.rollback()
        raise RuntimeError(f"Erro durante a inserção no banco de dados: {format(str(e))}")

    finally:
        # Independentemente de sucesso ou falha, fecha a sessão
        session.close()

    response = {
        "status": "OK",
        "estado": uf_sigla,
        "periodo": f"{periodo_data_inicio:%y%m}",
        "insercoes": contador,
    }

    return response


# RODAR LOCALMENTE
if __name__ == "__main__":
    from datetime import datetime

    # Defina os parâmetros de teste
    uf_sigla = "AC"
    periodo_data_inicio = datetime.strptime("2023-02-01", "%Y-%m-%d").date()
    tabela_destino = "dados_publicos.siasus_bpa_i_testeloading"

    # Chame a função principal com os parâmetros de teste
    inserir_bpa_i_postgres(uf_sigla, periodo_data_inicio, tabela_destino)
