# NECESSARIO PARA RODAR LOCALMENTE: Adiciona o caminho do diretório `sm_cloud_run` ao sys.path
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'sm_cloud_run')))
###

import re
from ftplib import FTP
import pandas as pd
import janitor  
from frozendict import frozendict
from typing import Final
from psycopg2.extensions import register_adapter, AsIs
from sqlalchemy import and_, Column, Integer, String, DateTime, Date
from sqlalchemy.schema import MetaData
from sqlalchemy.ext.declarative import declarative_base
from utilitarios.bd_config import Sessao, tabelas
from utilitarios.datas import agora_gmt_menos3
from utilitarios.logger_config import logger_config
from utilitarios.config_painel_sm import estados_painel
import logging



# Adaptar pd.NA
def adapt_pd_na(na):
    return AsIs('NULL')
register_adapter(pd._libs.missing.NAType, adapt_pd_na)

# Adaptar pd.NaT
def adapt_pd_nat(nat):
    return AsIs('NULL')
register_adapter(pd.NaT.__class__, adapt_pd_nat)

logger_config()

TIPOS_METADADOS_FTP: Final[frozendict] = frozendict(
    {
        "tipo": "object",		
        "sigla_uf": "object",
        "ano": "object",		
        "mes": "object",	
        "nome": "object",	       	
        "particao": "object",		
        "tamanho": "int64",
        "processamento_periodo_data_inicio": "datetime64[ns]",	
        "timestamp_modificacao_ftp": "datetime64[ns]",	
        "timestamp_etl_ftp_metadados": "datetime64[ns]",
        "timestamp_etl_gcs": "datetime64[ns]",
        "timestamp_load_bd": "datetime64[ns]"
    },
)

COLUNAS_NUMERICAS: Final[list[str]] = [
    nome_coluna
    for nome_coluna, tipo_coluna in TIPOS_METADADOS_FTP.items()
    if tipo_coluna.lower() == "int64" or tipo_coluna.lower() == "float64"
]

COLUNAS_ORDEM = [
  'tipo', 
  'sigla_uf', 
  'ano', 
  'mes', 
  'nome', 
  'particao', 
  'tamanho', 
  'processamento_periodo_data_inicio', 
  'timestamp_modificacao_ftp', 
  'timestamp_etl_ftp_metadados', 
  'timestamp_etl_gcs',
  'timestamp_load_bd'
]

Base = declarative_base()

class MetaData(Base):
    __tablename__ = "sm_metadados_ftp"
    __table_args__ = {'schema': 'saude_mental'}
    tipo = Column(String)
    sigla_uf = Column(String)
    ano = Column(String)
    mes = Column(String)
    nome = Column(String, primary_key=True)
    particao = Column(String)
    tamanho = Column(Integer)
    processamento_periodo_data_inicio = Column(Date)
    timestamp_modificacao_ftp = Column(DateTime)
    timestamp_etl_ftp_metadados = Column(DateTime)
    timestamp_etl_gcs = Column(DateTime)
    timestamp_load_bd = Column(DateTime)

def extrair_metadados_ftp(diretorio: str, prefixos: tuple): 
    """
    Conecta-se a um servidor FTP, navega até o diretório especificado e extrai metadados dos arquivos.

    Args:
        diretorio (str): Caminho do diretório no servidor FTP.
        prefixos (tuple): Prefixos que os nomes dos arquivos devem ter para serem considerados.

    Returns:
        pd.DataFrame: DataFrame contendo os metadados extraídos da pasta de arquivos.
    """ 
    ftp = FTP('ftp.datasus.gov.br')
    ftp.login()
    ftp.cwd(diretorio)
    lines = []
    ftp.retrlines('LIST', lines.append)
    ftp.quit()

    parsed_data = []

    for line in lines:
        match = re.match(r'(\d{2}-\d{2}-\d{2})\s+(\d{2}:\d{2}[APM]{2})\s+(\d+)\s+(.+)', line)
        if match:
            modification_date, hour, size, name = match.groups()
            if name.startswith(prefixos):
                parsed_data.append({
                    'data_modificacao_ftp': modification_date,
                    'hora': hour,
                    'tamanho': size,
                    'nome': name
                })

    # Create a pandas DataFrame from the list of dictionaries
    return pd.DataFrame(parsed_data)


def processar_particoes(df):   
    """
    Processa o DataFrame para lidar com partições, agrupando e agregando informações.

    Args:
        df (pd.DataFrame): DataFrame contendo metadados a serem processados.

    Returns:
        pd.DataFrame: DataFrame processado com partições agrupadas e agregadas.
    """

    # Separate rows with and without partitions
    com_particoes = df[df['particao'].notna()]
    sem_particoes = df[df['particao'].isna()]
    
    # Discard rows without partitions if there are partitioned rows
    if not com_particoes.empty:
        sem_particoes = pd.DataFrame()

    if not com_particoes.empty:
        # Group and aggregate partitioned rows
        com_particoes_grouped = com_particoes.groupby(['tipo', 'sigla_uf', 'ano', 'mes', 'nome']).agg(
            particao=('particao', lambda x: x.tolist()),
            tamanho=('tamanho', 'sum'),
            processamento_periodo_data_inicio=('processamento_periodo_data_inicio', 'first'),
            timestamp_modificacao_ftp=('timestamp_modificacao_ftp', 'max'),
            timestamp_etl_ftp_metadados=('timestamp_etl_ftp_metadados', 'max'),
            timestamp_etl_gcs=('timestamp_etl_gcs', 'min'),
            timestamp_load_bd=('timestamp_load_bd', 'min')
        ).reset_index()
        return pd.concat([sem_particoes, com_particoes_grouped], ignore_index=True)
    
    return df


def transformar_metadados(
    metadados_combinados: pd.DataFrame,
) -> pd.DataFrame:    
    """
    Aplica transformações aos metadados combinados, como extração de informações, conversão de tipos e filtragem.

    Args:
        metadados_combinados (pd.DataFrame): DataFrame contendo metadados a serem transformados.

    Returns:
        pd.DataFrame: DataFrame transformado pronto para inserção no banco de dados.
    """
    
    logging.info("Aplicando transformações no DataFrame...")

    meta_transformado = (
        metadados_combinados
        .assign(
            tipo=metadados_combinados['nome'].str[:2],
            sigla_uf=metadados_combinados['nome'].str[2:4],
            ano=metadados_combinados['nome'].str[4:6],
            mes=metadados_combinados['nome'].str[6:8],
            particao=metadados_combinados['nome'].str.extract(r'^\w{8}(.*)\.dbc$')[0].replace('', pd.NA),
            processamento_periodo_data_inicio=metadados_combinados['nome'].str[4:6] + metadados_combinados['nome'].str[6:8],
            timestamp_modificacao_ftp=pd.to_datetime(
                metadados_combinados['data_modificacao_ftp'].astype(str) + ' ' + metadados_combinados['hora'],
                format='%m-%d-%y %I:%M%p'
            ),
            timestamp_etl_ftp_metadados=agora_gmt_menos3().strftime("%Y-%m-%d %H:%M:%S"),            
            timestamp_etl_gcs=pd.NA, 
            timestamp_load_bd=pd.NA,
        )
        .assign(
            processamento_periodo_data_inicio=lambda x: pd.to_datetime(x['processamento_periodo_data_inicio'], format="%y%m", errors="coerce"),
            timestamp_modificacao_ftp=lambda x: x['timestamp_modificacao_ftp'].dt.strftime("%Y-%m-%d %H:%M:%S"),
            nome=lambda x: x['nome'].str[:8]
        )
        .drop(columns=['data_modificacao_ftp', 'hora'])
        
        # Filtra para manter apenas os últimos 13 meses
        .groupby('tipo')
        .apply(lambda df: df[df['processamento_periodo_data_inicio'] >= df['processamento_periodo_data_inicio'].max() - pd.DateOffset(months=13)])
        .reset_index(drop=True) 
        
        # Garantir tipos corretos para inserção no Postgres
        .change_type(
            # HACK: ver https://github.com/pandas-dev/pandas/issues/25472
            COLUNAS_NUMERICAS,
            "float",
        )
        .astype(TIPOS_METADADOS_FTP)

        # Processar partições
        .groupby(['tipo', 'sigla_uf', 'ano', 'mes', 'nome']).apply(processar_particoes).reset_index(drop=True)
        
        .reorder_columns(COLUNAS_ORDEM)
    )

    filtragem_estados = f"(sigla_uf in {estados_painel})"
    meta_transformado = meta_transformado.query(filtragem_estados, engine="python")

    return meta_transformado

def obter_metadados_combinados():   
    """
    Obtém e combina metadados de diferentes diretórios FTP, aplicando transformações necessárias.

    Returns:
        pd.DataFrame: DataFrame contendo metadados combinados e transformados.
    """    
    logging.info("Conexão com FTP aberta...")
    metadados_siasus = extrair_metadados_ftp('/dissemin/publicos/SIASUS/200801_/Dados', ('BI', 'PS', 'PA'))
    metadados_sihsus = extrair_metadados_ftp('/dissemin/publicos/SIHSUS/200801_/Dados', ('RD',))
    metadados_cnes_hb = extrair_metadados_ftp('/dissemin/publicos/CNES/200508_/Dados/HB', ('HB',))
    metadados_cnes_pf = extrair_metadados_ftp('/dissemin/publicos/CNES/200508_/Dados/PF', ('PF',))
    logging.info("Conexão com FTP encerrada.")

    logging.info("Concatenando DataFrames do SIASUS, SIHSUS e CNES...")
    metadados_combinados = pd.concat([metadados_siasus, metadados_sihsus, metadados_cnes_hb, metadados_cnes_pf], ignore_index=True)

    logging.info("Aplicando tratamentos...")
    meta_transformado = transformar_metadados(metadados_combinados)
    logging.info("Transformações realizadas com sucesso.")

    return meta_transformado



def upsert_dados_no_postgres():
    """
    Realiza operações de upsert (inserção e atualização) dos dados na tabela de metadados 
    do FTP do DataSUS no banco de dados PostgreSQL.

    Esta função realiza as seguintes operações:
    1. Obtém os metadados mais recentes dos arquivos a partir de servidores FTP.
    2. Transforma os metadados obtidos para o formato adequado para inserção no banco de dados.
    3. Conecta-se ao banco de dados PostgreSQL usando uma sessão SQLAlchemy.
    4. Para cada registro no DataFrame de metadados:
        - Verifica se o registro já existe na tabela de metadados.
            - Se o existe, compara data de modificação do arquivo no FTP:
                - Se data é igual, atualiza apenas coluna de última atualização do etl
                - Se data é diferente, atualiza todos os campos relevantes
            - Se não existe, cria um novo registro com os dados fornecidos e o adiciona à tabela.
    5. Faz commit das alterações no banco de dados para garantir que as inserções e atualizações sejam salvas.
    6. Em caso de erro durante o processamento de um registro, reverte as alterações e registra um erro.
    
    Observações:
        - O DataFrame de metadados é obtido e transformado pela função `obter_metadados_combinados()`.
        - A função utiliza a classe `MetaData` para mapear a estrutura da tabela no banco de dados.
        - A função assume que a conexão e a configuração do banco de dados foram previamente configuradas na classe `Sessao`.

    Exceptions:
        - Pode levantar exceções se houver problemas na conexão com o banco de dados ou ao processar os registros.
    """
    df = obter_metadados_combinados()
    sessao = Sessao()
    tabela_fonte_orm = MetaData  # Use the ORM model here
    tabela_fonte = tabelas["_saude_mental_configuracoes.sm_metadados_ftp"]   
    logging.info(f"Iniciando operações de atualização/deleção/upsert dos dados na {tabela_fonte}...")

    # Inicializa contadores
    linhas_deletadas = 0
    linhas_modificadas = 0
    linhas_atualizadas = 0
    linhas_inseridas = 0

    # Adapta tipos para evitar erro "psycopg2.ProgrammingError: can't adapt type 'NAType'".
    register_adapter(pd.NaT, adapt_pd_na)

    try:
        # Remove entradas antigas (anteriores a 13 meses)
        for (tipo, sigla_uf), group in df.groupby(['tipo', 'sigla_uf']):
            oldest_date = group['processamento_periodo_data_inicio'].min()
            contagem_deletadas = sessao.query(tabela_fonte).filter(
                and_(
                    tabela_fonte.c.tipo == tipo,
                    tabela_fonte.c.sigla_uf == sigla_uf,
                    tabela_fonte.c.processamento_periodo_data_inicio < oldest_date
                )
            ).delete(synchronize_session=False)
            linhas_deletadas += contagem_deletadas

        # Upsert comparando tabela gerada por obter_metadados_combinados() com tabela do banco 
        for _, row in df.iterrows():
            nomes_existentes = sessao.query(tabela_fonte).filter_by(nome=row['nome']).first()
            
            if nomes_existentes:
                # Se os arquivos do FTP estão com data de modificação diferente da última vez que buscamos, atualiza todas as colunas pertinentes
                if nomes_existentes.timestamp_modificacao_ftp != row['timestamp_modificacao_ftp']:
                    sessao.query(tabela_fonte).filter_by(nome=row['nome']).update({
                        # 'tipo': row['tipo'],
                        # 'sigla_uf': row['sigla_uf'],
                        # 'ano': row['ano'],
                        # 'mes': row['mes'],
                        'particao': row['particao'],
                        'tamanho': row['tamanho'], 
                        # 'processamento_periodo_data_inicio': row['processamento_periodo_data_inicio'],
                        'timestamp_modificacao_ftp': row['timestamp_modificacao_ftp'],
                        'timestamp_etl_ftp_metadados': row['timestamp_etl_ftp_metadados'],                        
                        # 'timestamp_etl_gcs': row['timestamp_etl_gcs']
                        # 'timestamp_load_bd': row['timestamp_load_bd']
                    })
                    linhas_modificadas += 1
                # Se os arquivos do FTP estão com data de modificação igual a da última vez que buscamos, atualiza apenas a coluna timestamp_etl_ftp_metadados
                elif nomes_existentes.timestamp_modificacao_ftp == row['timestamp_modificacao_ftp']:
                    sessao.query(tabela_fonte).filter_by(nome=row['nome']).update({
                        'timestamp_etl_ftp_metadados': row['timestamp_etl_ftp_metadados']
                    })
                    linhas_atualizadas += 1
                    
            else:      
                nova_entrada = tabela_fonte_orm(
                    tipo=row['tipo'],
                    sigla_uf=row['sigla_uf'],
                    ano=row['ano'],
                    mes=row['mes'],
                    nome=row['nome'],
                    particao=row['particao'],
                    tamanho=row['tamanho'],
                    processamento_periodo_data_inicio=row['processamento_periodo_data_inicio'],
                    timestamp_modificacao_ftp=row['timestamp_modificacao_ftp'],
                    timestamp_etl_ftp_metadados=row['timestamp_etl_ftp_metadados'],
                    timestamp_etl_gcs=row['timestamp_etl_gcs'],
                    timestamp_load_bd=row['timestamp_load_bd']
                )
                sessao.add(nova_entrada)
                linhas_inseridas += 1
        
        sessao.commit()

        logging.info(f"Operação de atualização completa para {tabela_fonte}.")
        logging.info(f"Linhas deletadas: {linhas_deletadas}")
        logging.info(f"Linhas modificadas: {linhas_modificadas}")
        logging.info(f"Linhas atualizadas: {linhas_atualizadas}")
        logging.info(f"Linhas inseridas: {linhas_inseridas}")

    except Exception as e:
        sessao.rollback()
        raise RuntimeError(f"Erro durante a inserção no banco de dados: {format(str(e))}")
    finally:
        sessao.close()

    response = {
        "status": "OK",
        "linhas_deletadas": linhas_deletadas,
        "linhas_modificadas": linhas_modificadas,
        "linhas_atualizadas": linhas_atualizadas,
        "linhas_inseridas": linhas_inseridas,
    }

    logging.info(
        f"Processamento concluído." 
        f"{linhas_deletadas} linhas_deletadas,"
        f"{linhas_modificadas} linhas_modificadas,"
        f"{linhas_atualizadas} linhas_atualizadas,"
        f"{linhas_inseridas} linhas_inseridas"
    )

    return response


if __name__ == "__main__":
    upsert_dados_no_postgres()

