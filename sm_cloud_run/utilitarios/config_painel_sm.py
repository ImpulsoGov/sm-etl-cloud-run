from __future__ import annotations

from datetime import datetime
from typing import Literal
from sqlalchemy.orm.session import Session
from utilitarios.bd_config import tabelas
from sqlalchemy.sql import update, and_


import logging


municipios_painel = [
    "150140",
    "150680",
    "220840",
    "230190",
    "230440",
    "230970",
    "231130",
    "231140",
    "231290",
    "250970",
    "251230",
    "261160",
    "261390",
    "270630",
    "280030",
    "280350",
    "292740",
    "315780",
    "320500",
    "320520",
    "351640",
    "352590",
    "410690",
    "431440",
    "520140"
]

estados_painel = [
    "AL",
    "BA",
    "CE",
    "ES",
    "GO",
    "MG",
    "PA",
    "PB",
    "PE",
    "PI",
    "PR",
    "RS",
    "SE",
    "SP"
]

condicoes_pa = "(PA_TPUPS == '70') or PA_PROC_ID.str.startswith('030106') or PA_PROC_ID.str.startswith('030107') or PA_PROC_ID.str.startswith('030108') or PA_CIDPRI.str.startswith('F') or PA_CIDPRI.str.startswith('F') or PA_CIDPRI.str.startswith('X6') or PA_CIDPRI.str.startswith('X7') or PA_CIDPRI.str.contains('^X8[0-4][0-9]*') or PA_CIDPRI.str.startswith('R78') or PA_CIDPRI.str.startswith('T40') or (PA_CIDPRI == 'Y870') or PA_CIDPRI.str.startswith('Y90') or PA_CIDPRI.str.startswith('Y91') or (PA_CBOCOD in ['223905', '223915', '225133', '223550', '239440', '239445', '322220']) or PA_CBOCOD.str.startswith('2515') or (PA_CATEND == '02')"

condicoes_bpa_i = "(TPUPS == '70') or PROC_ID.str.startswith('030106') or PROC_ID.str.startswith('030107') or PROC_ID.str.startswith('030108') or CIDPRI.str.startswith('F') or CIDPRI.str.startswith('F') or CIDPRI.str.startswith('X6') or CIDPRI.str.startswith('X7') or CIDPRI.str.contains('^X8[0-4][0-9]*') or CIDPRI.str.startswith('R78') or CIDPRI.str.startswith('T40') or (CIDPRI == 'Y870') or CIDPRI.str.startswith('Y90') or CIDPRI.str.startswith('Y91') or (CBOPROF in ['223905', '223915', '225133', '223550', '239440', '239445', '322220']) or CBOPROF.str.startswith('2515') or (CATEND == '02')"


def inserir_timestamp_ftp_metadados(
    sessao: Session,
    uf_sigla: str, 
    periodo_data_inicio: datetime.date,
    coluna_atualizar: Literal['timestamp_etl_gcs', 'timestamp_load_bd', 'timestamp_etl_ftp_metadados'],
    tipo: Literal['PA', 'BI', 'PS', 'RD', 'HB', 'PF']
):
    """
    Insere um timestamp na tabela saude_mental.sm_metadados_ftp quando os 
    parâmetros uf_sigla e periodo_data_inicio são iguais aos das colunas sigla_uf
    e processamento_periodo_data_inicio.

    Argumentos:
        sessao: objeto [`sqlalchemy.orm.session.Session`][] que permite
            acessar a base de dados da ImpulsoGov.
        uf_sigla (str): Sigla da Unidade Federativa.
        periodo_data_inicio (datetime.date): Data de início do período.
        tipo (str): Tipo do ETL a ser utilizado na condição de filtragem. 
            Valores aceitos:  
        coluna_atualizar (str): Nome da coluna a ser atualizada.

    Retorna:
        None
    """

    sm_metadados_ftp = tabelas["saude_mental.sm_metadados_ftp"]

    try:
        timestamp_atual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Cria a requisição de atualização
        requisicao_atualizar = (
            update(sm_metadados_ftp)
            .where(and_(
                sm_metadados_ftp.c.tipo == tipo, 
                sm_metadados_ftp.c.sigla_uf == uf_sigla,
                sm_metadados_ftp.c.processamento_periodo_data_inicio == periodo_data_inicio
                ) 
            )
            .values({coluna_atualizar: timestamp_atual})
        )

        # Executa a atualização
        sessao.execute(requisicao_atualizar)
        sessao.commit()
        logging.info("Metadados do FTP inseridos com sucesso!")
        
    except Exception as e:
        # Em caso de erro, desfaz a transação
        sessao.rollback()
        print(f"Erro ao atualizar timestamp: {e}")




