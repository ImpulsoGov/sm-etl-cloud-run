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

import janitor
from frozendict import frozendict
from sqlalchemy.orm import Session
from utilitarios.bd_config import Sessao, tabelas

from utilitarios.cloud_storage import download_from_bucket
from utilitarios.bd_utilitarios import carregar_dataframe
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

def validar_pa(pa_transformada: pd.DataFrame) -> pd.DataFrame:
    assert isinstance(pa_transformada, pd.DataFrame), "Não é um DataFrame"
    assert len(pa_transformada) > 0, "DataFrame vazio."


def inserir_pa_postgres(
    uf_sigla: str,
    periodo_data_inicio: datetime.date,
    tabela_destino: str,
):
    session = Sessao()
    
    try:   
        # Baixar CSV do GCS e carregar em um DataFrame
        path_gcs = f"saude-mental/dados-publicos/siasus/procedimentos-disseminacao/{uf_sigla}/siasus_procedimentos_disseminacao_{uf_sigla}_{periodo_data_inicio:%y%m}.csv"    
        
        pa = download_from_bucket(
            bucket_name="camada-bronze", 
            blob_path=path_gcs)
        
        # Cria um objeto de lista que conterá dados se o parâmetro de competência fornecido na execução 
        # da função (periodo_data_inicio) for igual a alguma data de processamento já presente na tabela  
        tabela_fonte = tabelas[tabela_destino]          
        query = session.query(tabela_fonte).filter_by(processamento_periodo_data_inicio = periodo_data_inicio)
        existing_data = query.first()


        if existing_data:
            # Filtra os dados do GCS que não possuem `processamento_periodo_data_inicio` igual ao parâmetro periodo_data_inicio
            pa_filtrada = pa[pa['processamento_periodo_data_inicio'] != periodo_data_inicio.strftime("%Y-%m-%d")]

            if pa_filtrada.empty:
                raise RuntimeError("Todos os dados possuem uma data de processamento que já existe na "
                                   + "tabela destino, portanto não foram inseridos.")
            
            else: 
                pa = pa_filtrada            
                logging.warning(f"Haviam {len(pa)} registros na competência de processamento {periodo_data_inicio}, " 
                                + f"mas {len(pa) - len(pa_filtrada)} registros não foram incluídos porque apresentavam "
                                + "data de processamento que já foi inserida na tabela destino.")            


        # Tamanho do lote de processamento
        # passo = int(os.getenv("IMPULSOETL_LOTE_TAMANHO", 100000))
        passo = 100000

        # Divide o DataFrame em lotes
        num_lotes = len(pa) // passo + 1
        pa_lotes = np.array_split(pa, num_lotes)

        contador = 0
        for pa_lote in pa_lotes:
            pa_transformada = transformar_tipos(
                sessao=session, 
                pa=pa_lote,
            )
            try:
                validar_pa(pa_transformada)
            except AssertionError as mensagem:
                session.rollback()
                raise RuntimeError(
                    "Dados inválidos encontrados após a transformação:"
                    + " {}".format(mensagem),
                )

            carregamento_status = carregar_dataframe(
                sessao=session,
                df=pa_transformada,
                tabela_destino=tabela_destino,
                passo=None,
            )
            if carregamento_status != 0:
                session.rollback()
                raise RuntimeError(
                    "Execução interrompida em razão de um erro no "
                    + "carregamento."
                )
            contador += len(pa_lote)

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
    periodo_data_inicio = datetime.strptime("2024-04-01", "%Y-%m-%d").date()
    tabela_destino = "dados_publicos.siasus_pa_testeloading"

    # Chame a função principal com os parâmetros de teste
    inserir_pa_postgres(uf_sigla, periodo_data_inicio, tabela_destino)
