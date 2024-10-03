from __future__ import annotations

from utilitarios.bd_config import tabelas
from sqlalchemy import select
from sqlalchemy.orm import Query, Session


def municipios_ativos_painel(
    sessao: Session,
):
    tabela_municipios = tabelas["_saude_mental_configuracoes.sm_municipios_painel"]
    query = select(tabela_municipios.c.id_sus).where(tabela_municipios.c.status_ativo == True)

    # Execute the query and fetch all results
    result = sessao.execute(query).fetchall()

    # Extract the id_sus values from the query result into a list
    municipios_painel = [row.id_sus for row in result]

    return municipios_painel

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
