from __future__ import annotations

# NECESSARIO PARA RODAR LOCALMENTE: Adiciona o caminho do diretório `sm_cloud_run` ao sys.path
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'sm_cloud_run')))
###

from datetime import datetime
from typing import Literal
from sqlalchemy.orm.session import Session
from utilitarios.bd_config import Sessao, tabelas
from sqlalchemy.sql import update, and_
from sqlalchemy import select, or_, null

import logging

from utilitarios.logger_config import logger_config
logger_config()


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



def verificar_e_executar(
    uf_sigla: str,
    periodo_data_inicio: datetime.date,
    tipo: str,
    acao: Literal['baixar', 'inserir']
):
    logging.info(
        f"Verificando se {tipo} de {uf_sigla} ({periodo_data_inicio:%y%m}) precisa ser {'baixado' if acao == 'baixar' else 'inserido no banco'}..."
    )
    
    sessao = Sessao()
    tabela_metadados_ftp = tabelas["saude_mental.sm_metadados_ftp"]

    # Query genérica para consultar necessidade de inserção ou download
    if acao == 'baixar':
        condicao_null = tabela_metadados_ftp.c.timestamp_etl_gcs.is_(null())
        condicao_timestamp = tabela_metadados_ftp.c.timestamp_modificacao_ftp > tabela_metadados_ftp.c.timestamp_etl_gcs
    elif acao == 'inserir':
        condicao_null = tabela_metadados_ftp.c.timestamp_load_bd.is_(null())
        condicao_timestamp = tabela_metadados_ftp.c.timestamp_etl_gcs > tabela_metadados_ftp.c.timestamp_load_bd
    else:
        raise ValueError("Ação inválida. Use 'inserir' ou 'baixar'.")

    consulta = (
        select(tabela_metadados_ftp)
        .where(
            tabela_metadados_ftp.c.tipo == tipo,
            tabela_metadados_ftp.c.sigla_uf == uf_sigla,
            tabela_metadados_ftp.c.processamento_periodo_data_inicio == periodo_data_inicio,
            or_(
                condicao_timestamp,
                condicao_null
            )
        )
    )

    resultado = sessao.execute(consulta).fetchone()

    if resultado:
        logging.info(
            f"Verificação concluída. Iniciando processo de {'inserção/reinserção' if acao == 'inserir' else 'download'} de dados."
        )
        
        # Realiza a ação baseada no tipo e ação escolhida
        if acao == 'baixar':
            if tipo == 'PA':
                from etl.siasus_procedimentos_ambulatoriais import baixar_e_processar_pa
                response = baixar_e_processar_pa(uf_sigla, periodo_data_inicio)
                return response
                
            # elif tipo == 'BI':
            #     from etl.siasus_bpa_individualizado import baixar_e_processar_bpa_i
            #     baixar_e_processar_bpa_i(uf_sigla, periodo_data_inicio)

            # elif tipo == 'PS':
            #     from etl.siasus_raas_ps import baixar_e_processar_raas
            #     baixar_e_processar_raas(uf_sigla, periodo_data_inicio)

            # elif tipo == 'RD':
            #     from etl.sihsus_aih_rd import baixar_e_processar_aih
            #     baixar_e_processar_aih(uf_sigla, periodo_data_inicio)

            # elif tipo == 'HB':
            #     from etl. import baixar_e_processar_
            #     baixar_e_processar_hb(uf_sigla, periodo_data_inicio)

            # elif tipo == 'PF':
            #     from etl. import baixar_e_processar_
            #     baixar_e_processar_pf(uf_sigla, periodo_data_inicio)


            else:
                raise ValueError("Tipo inválido para a ação 'baixar'. Use 'PA', 'BI', 'PS', 'RD', 'HB' ou 'PF'.")
        

        elif acao == 'inserir':
            if tipo == 'PA':
                from load_bd.siasus_procedimentos_ambulatoriais_l_bd import inserir_pa_postgres
                response = inserir_pa_postgres(uf_sigla, periodo_data_inicio)
                return response                

            # elif tipo == 'BI':
            #     from load_bd.siasus_bpa_individualizado_l_bd import inserir_bpa_i_postgres
            #     response = inserir_bpa_i_postgres(uf_sigla, periodo_data_inicio)
            #     return response

            # elif tipo == 'PS':
            #     from load_bd.siasus_raas_ps_l_bd import inserir_raas_postgres
            #     inserir_raas_postgres(uf_sigla, periodo_data_inicio)

            # elif tipo == 'RD':
            #     from load_bd.sihsus_aih_l_bd import inserir_aih_postgres
            #     inserir_aih_postgres(uf_sigla, periodo_data_inicio)

            # elif tipo == 'HB':
            #     from load_bd. import inserir_hab_postgres
            #     inserir_hb_postgres(uf_sigla, periodo_data_inicio)

            # elif tipo == 'PF':
            #     from load_bd. import inserir_vinculos_postgres
            #     inserir_pf_postgres(uf_sigla, periodo_data_inicio)

            else:
                raise ValueError("Tipo inválido para a ação 'inserir'. Use 'PA', 'BI', 'PS', 'RD', 'HB' ou 'PF'.")

        else:
            raise ValueError("Ação inválida. Use 'baixar' ou 'inserir'.")   
            

    else:
        response = {
            "status": "skipped",
            "estado": uf_sigla,
            "periodo": f"{periodo_data_inicio:%y%m}"
        }

        logging.info(
            f"Essa combinação já foi {'inserida' if acao == 'inserir' else 'baixada'} e é a mais atual. "
            f"Nenhum dado foi {'inserido' if acao == 'inserir' else 'baixado'} para {uf_sigla} ({periodo_data_inicio:%y%m})."
        )

        sessao.close()
        return response