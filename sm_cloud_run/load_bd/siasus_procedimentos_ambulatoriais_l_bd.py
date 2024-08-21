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
from sqlalchemy.orm import Session
from sqlalchemy import select, or_, null
from utilitarios.bd_config import Sessao, tabelas
from utilitarios.airflow_utilitarios import inserir_timestamp_ftp_metadados, verificar_e_executar

from utilitarios.cloud_storage import download_from_bucket
from utilitarios.bd_utilitarios import carregar_dataframe, validar_dataframe
from utilitarios.logger_config import logger_config


# set up logging to file
logger_config()

TIPOS_PA: Final[frozendict] = frozendict(
    {
        "estabelecimento_id_scnes": "object",
        "gestao_unidade_geografica_id_sus": "object",
        "gestao_condicao_id_siasus": "object",
        "unidade_geografica_id_sus": "object",
        "regra_contratual_id_scnes": "object",
        "incremento_outros_id_sigtap": "object",
        "incremento_urgencia_id_sigtap": "object",
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
        "instrumento_registro_id_siasus": "object",
        "autorizacao_id_siasus": "object",
        "profissional_id_cns": "object",
        "profissional_vinculo_ocupacao_id_cbo2002": "object",
        "desfecho_motivo_id_siasus": "object",
        "obito": "bool",
        "encerramento": "bool",
        "permanencia": "bool",
        "alta": "bool",
        "transferencia": "bool",
        "condicao_principal_id_cid10": "object",
        "condicao_secundaria_id_cid10": "object",
        "condicao_associada_id_cid10": "object",
        "carater_atendimento_id_siasus": "object",
        "usuario_idade": "Int64",
        "procedimento_idade_minima": "Int64",
        "procedimento_idade_maxima": "Int64",
        "compatibilidade_idade_id_siasus": "object",
        "usuario_sexo_id_sigtap": "object",
        "usuario_raca_cor_id_siasus": "object",
        "usuario_residencia_municipio_id_sus": "object",
        "quantidade_apresentada": "Int64",
        "quantidade_aprovada": "Int64",
        "valor_apresentado": "Float64",
        "valor_aprovado": "Float64",
        "atendimento_residencia_ufs_distintas": "bool",
        "atendimento_residencia_municipios_distintos": "bool",
        "procedimento_valor_diferenca_sigtap": "Float64",
        "procedimento_valor_vpa": "Float64",
        "procedimento_valor_sigtap": "Float64",
        "aprovacao_status_id_siasus": "object",
        "ocorrencia_id_siasus": "object",
        "erro_quantidade_apresentada_id_siasus": "object",
        "erro_apac": "object",
        "usuario_etnia_id_sus": "object",
        "complemento_valor_federal": "Float64",
        "complemento_valor_local": "Float64",
        "incremento_valor": "Float64",
        "servico_id_sigtap": "object",
        "servico_classificacao_id_sigtap": "object",
        "equipe_id_ine": "object",
        "estabelecimento_natureza_juridica_id_scnes": "object",
        "id": "object",
        "periodo_id": "object",
        "unidade_geografica_id": "object",
        "criacao_data": "datetime64[ns]",
        "atualizacao_data": "datetime64[ns]",
        "ftp_arquivo_nome": "object",
    },
)

COLUNAS_NUMERICAS: Final[list[str]] = [
    nome_coluna
    for nome_coluna, tipo_coluna in TIPOS_PA.items()
    if tipo_coluna.lower() == "int64" or tipo_coluna.lower() == "float64"
]


def transformar_tipos(
    sessao: Session,
    pa: pd.DataFrame,
) -> pd.DataFrame:
    """
    """    
    logging.info(
        f"Forçando tipos para colunas "
    )
    pa_transformada = (
        pa  
        # garantir tipos
        .change_type(
            # HACK: ver https://github.com/pandas-dev/pandas/issues/25472
            COLUNAS_NUMERICAS,
            "float",
        )
        .astype(TIPOS_PA)
    )
    return pa_transformada


def inserir_pa_postgres(
    uf_sigla: str,
    periodo_data_inicio: datetime.date
):
    sessao = Sessao()

    tabela_destino = "saude_mental.siasus_procedimentos_ambulatoriais_sm_municipios"
    
    try:   
        # Baixar CSV do GCS e carregar em um DataFrame
        path_gcs = f"saude-mental/dados-publicos/siasus/procedimentos-disseminacao/{uf_sigla}/siasus_procedimentos_disseminacao_{uf_sigla}_{periodo_data_inicio:%y%m}.csv"    
        
        pa = download_from_bucket(
            bucket_name="camada-bronze", 
            blob_path=path_gcs)
        

        logging.info("Verificando necessidade de exclusão de registros da tabela destino...")
        # Obtem valor do nome do arquivo baixado do FTP e gravado no dataframe        
        ftp_arquivo_nome_df = pa['ftp_arquivo_nome'].iloc[0]
        tabela_ref = tabelas[tabela_destino]
  

        # Deleta linhas conflitantes
        delete_result = sessao.execute(
            tabela_ref.delete()
            .where(tabela_ref.c.ftp_arquivo_nome == ftp_arquivo_nome_df)
        )
        num_deleted = delete_result.rowcount

        # Verifica se alguma linha foi deletada
        if num_deleted > 0:
            logging.info(f"Número de linhas deletadas: {num_deleted}")
        else:
            logging.info("Nenhum registro foi deletado.")


        # Tamanho do lote de processamento
        # passo = int(os.getenv("IMPULSOETL_LOTE_TAMANHO", 100000))
        passo = 100000

        # Divide o DataFrame em lotes
        num_lotes = len(pa) // passo + 1
        pa_lotes = np.array_split(pa, num_lotes)

        contador = 0
        for pa_lote in pa_lotes:
            pa_transformada = transformar_tipos(
                sessao=sessao, 
                pa=pa_lote,
            )
            try:
                validar_dataframe(pa_transformada)
            except AssertionError as mensagem:
                sessao.rollback()
                raise RuntimeError(
                    "Dados inválidos encontrados após a transformação:"
                    + " {}".format(mensagem),
                )

            carregamento_status = carregar_dataframe(
                sessao=sessao,
                df=pa_transformada,
                tabela_destino=tabela_destino,
                passo=None,
            )
            if carregamento_status != 0:
                sessao.rollback()
                raise RuntimeError(
                    "Execução interrompida em razão de um erro no "
                    + "carregamento."
                )
            contador += len(pa_lote)


        # Registrar na tabela de metadados do FTP
        logging.info("Inserindo timestamp na tabela de metadados do FTP...")
        inserir_timestamp_ftp_metadados(
            sessao, 
            uf_sigla, 
            periodo_data_inicio, 
            coluna_atualizar='timestamp_load_bd',
            tipo='PA'
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
        "tipo": "PA",
        "estado": uf_sigla,
        "periodo": f"{periodo_data_inicio:%y%m}",
        "insercoes": contador,
    }

    return response



# RODAR LOCALMENTE
if __name__ == "__main__":
    from datetime import datetime

    # Defina os parâmetros de teste
    uf_sigla = "PI"
    periodo_data_inicio = datetime.strptime("2024-06-01", "%Y-%m-%d").date()

    # Chame a função principal com os parâmetros de teste
    verificar_e_executar(
        uf_sigla=uf_sigla, 
        periodo_data_inicio=periodo_data_inicio, 
        tipo="PA", 
        acao="inserir"
    )


