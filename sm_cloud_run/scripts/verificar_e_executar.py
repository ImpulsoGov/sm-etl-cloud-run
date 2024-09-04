from __future__ import annotations

# NECESSARIO PARA RODAR LOCALMENTE: Adiciona o caminho do diretório `sm_cloud_run` ao sys.path
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'sm_cloud_run')))
###

from datetime import datetime
from typing import Literal
from utilitarios.bd_config import Sessao, tabelas
from sqlalchemy import select, or_, null

import logging

from utilitarios.logger_config import logger_config
logger_config()



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
    tabela_metadados_ftp = tabelas["_saude_mental_configuracoes.sm_metadados_ftp"]

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
    sessao.close()

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
                
            elif tipo == 'BI':
                from etl.siasus_bpa_individualizado import baixar_e_processar_bpa_i
                response = baixar_e_processar_bpa_i(uf_sigla, periodo_data_inicio)
                return response

            elif tipo == 'PS':
                from etl.siasus_raas_ps import baixar_e_processar_raas_ps
                response = baixar_e_processar_raas_ps(uf_sigla, periodo_data_inicio)
                return response

            elif tipo == 'RD':
                from etl.sihsus_aih_rd import baixar_e_processar_aih_rd
                response = baixar_e_processar_aih_rd(uf_sigla, periodo_data_inicio)
                return response

            elif tipo == 'HB':
                from etl.scnes_habilitacoes import baixar_e_processar_habilitacoes
                response = baixar_e_processar_habilitacoes(uf_sigla, periodo_data_inicio)
                return response
            
            elif tipo == 'PF':
                from etl.scnes_vinculos import baixar_e_processar_vinculos
                response = baixar_e_processar_vinculos(uf_sigla, periodo_data_inicio)
                return response


            else:
                raise ValueError("Tipo inválido para a ação 'baixar'. Use 'PA', 'BI', 'PS', 'RD', 'HB' ou 'PF'.")
        

        elif acao == 'inserir':
            if tipo == 'PA':
                from load_bd.siasus_procedimentos_ambulatoriais_load_bd import inserir_pa_postgres
                response = inserir_pa_postgres(uf_sigla, periodo_data_inicio)
                return response                

            elif tipo == 'BI':
                from load_bd.siasus_bpa_individualizado_load_bd import inserir_bpa_i_postgres
                response = inserir_bpa_i_postgres(uf_sigla, periodo_data_inicio)
                return response

            elif tipo == 'PS':
                from load_bd.siasus_raas_ps_load_bd import inserir_raas_ps_postgres
                response = inserir_raas_ps_postgres(uf_sigla, periodo_data_inicio)
                return response    

            elif tipo == 'RD':
                from load_bd.sihsus_aih_rd_load_bd import inserir_aih_rd_postgres
                response = inserir_aih_rd_postgres(uf_sigla, periodo_data_inicio)
                return response    

            elif tipo == 'HB':
                from load_bd.scnes_habilitacoes_load_bd import inserir_habilitacoes_postgres
                response = inserir_habilitacoes_postgres(uf_sigla, periodo_data_inicio)
                return response    

            elif tipo == 'PF':
                from load_bd.scnes_vinculos_load_bd import inserir_vinculos_postgres
                response = inserir_vinculos_postgres(uf_sigla, periodo_data_inicio)
                return response    

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

        # sessao.close()
        return response
    




   
    
# RODAR LOCALMENTE
if __name__ == "__main__":
    from datetime import datetime

    # Define os parâmetros de teste
    uf_sigla = "PI"
    periodo_data_inicio = datetime.strptime("2024-06-01", "%Y-%m-%d").date()

    # Chama a função principal com os parâmetros de teste
    verificar_e_executar(
        uf_sigla=uf_sigla, 
        periodo_data_inicio=periodo_data_inicio, 
        tipo="PS", 
        acao="baixar"
    )