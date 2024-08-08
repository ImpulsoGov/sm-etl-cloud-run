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
from utilitarios.bd_utilitarios import carregar_dataframe

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
    },
)

COLUNAS_NUMERICAS: Final[list[str]] = [
    nome_coluna
    for nome_coluna, tipo_coluna in TIPOS_RAAS_PS.items()
    if tipo_coluna == "Int64"
]



# Baixa arquivo do GCS
def download_csv_from_gcs(bucket_name, blob_path):
    """
    Baixa um arquivo CSV do Google Cloud Storage e retorna como um objeto dataframe do pandas.
    """
    bucket = storage.Client().bucket(bucket_name)
    blob = bucket.blob(blob_path)
    content = blob.download_as_text()
    raas_ps = pd.read_csv(StringIO(content), dtype=str, index_col=0)
    return raas_ps

def transformar_tipos(
    raas_ps: pd.DataFrame,
) -> pd.DataFrame:
    """
    """    
    logging.info(
        f"Forçando tipos para colunas "
    )

    raas_ps_transformada = (
        raas_ps 
        # garantir tipos
        .change_type(
            # HACK: ver https://github.com/pandas-dev/pandas/issues/25472
            COLUNAS_NUMERICAS,
            "float",
        )
        .astype(TIPOS_RAAS_PS)
    )
    memoria_usada = raas_ps_transformada.memory_usage(deep=True).sum() / 10 ** 6
    logging.debug(        
        f"Memória ocupada pelo DataFrame transformado: {memoria_usada:.2f} mB."
    )
    return raas_ps_transformada

def validar_raas_ps(raas_ps_transformada: pd.DataFrame) -> pd.DataFrame:
    assert isinstance(raas_ps_transformada, pd.DataFrame), "Não é um DataFrame"
    assert len(raas_ps_transformada) > 0, "DataFrame vazio."
    nulos_por_coluna = raas_ps_transformada.applymap(pd.isna).sum()
    assert nulos_por_coluna["quantidade_apresentada"] == 0, (
        "A quantidade apresentada é um valor nulo."
    )
    assert nulos_por_coluna["quantidade_aprovada"] == 0, (
        "A quantidade aprovada é um valor nulo."
    )
    assert nulos_por_coluna["realizacao_periodo_data_inicio"] == 0, (
        "A competência de realização é um valor nulo."
    )

def inserir_raas_ps_postgres(
    uf_sigla: str,
    periodo_data_inicio: datetime.date,
    tabela_destino: str,
):
    session = Sessao()
    
    try:   
        # Baixar CSV do GCS e carregar em um DataFrame
        path_gcs = f"saude-mental/dados-publicos/siasus/raas-psicossocial/{uf_sigla}/siasus_raas_ps_disseminacao_{uf_sigla}_{periodo_data_inicio:%y%m}.csv"    
        
        pa = download_csv_from_gcs(
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


        # Obtem tamanho do lote de processamento
        passo = int(os.getenv("IMPULSOETL_LOTE_TAMANHO", 100000))

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
    periodo_data_inicio = datetime.strptime("2023-02-01", "%Y-%m-%d").date()
    tabela_destino = "dados_publicos.siasus_pa_testeloading"

    # Chame a função principal com os parâmetros de teste
    inserir_pa_postgres(uf_sigla, periodo_data_inicio, tabela_destino)
