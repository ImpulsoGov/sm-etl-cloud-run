import os
import datetime

from flask import Flask, request, jsonify


from etl.datasus_ftp_metadados import upsert_dados_no_postgres

from etl.siasus_procedimentos_ambulatoriais import verificar_e_executar
from load_bd.siasus_procedimentos_ambulatoriais_l_bd import verificar_e_executar

from etl.siasus_bpa_individualizado import baixar_e_processar_bpa_i
from load_bd.siasus_bpa_individualizado_l_bd import inserir_bpa_i_postgres

app = Flask(__name__)


@app.route("/ftp_metadados", methods=['POST'])
def ftp_metadados():
    content_type = request.headers.get('Content-Type')
    if (content_type != 'application/json'):
        return 'Erro, content-type deve ser json', 400

    return jsonify(upsert_dados_no_postgres())


@app.route("/pa", methods=['POST'])
def sm_pa():
    content_type = request.headers.get('Content-Type')
    if (content_type == 'application/json'):
        json_params = request.json
    else:
        return 'Erro, content-type deve ser json', 400

    data_datetime = datetime.datetime.strptime(json_params['data'], "%Y-%m-%d")

    return jsonify(verificar_e_executar(json_params['UF'], data_datetime))


@app.route("/pa_postgres", methods=['POST'])
def load_pa():
    content_type = request.headers.get('Content-Type')
    if (content_type == 'application/json'):
        json_params = request.json
    else:
        return 'Erro, content-type deve ser json', 400

    data_datetime = datetime.datetime.strptime(json_params['data'], "%Y-%m-%d")

    return jsonify(verificar_e_executar(json_params['UF'], data_datetime, json_params['tabela_destino']))


@app.route("/bpa_i", methods=['POST'])
def sm_bpa_i():
    content_type = request.headers.get('Content-Type')
    if (content_type == 'application/json'):
        json_params = request.json
    else:
        return 'Erro, content-type deve ser json', 400

    data_datetime = datetime.datetime.strptime(json_params['data'], "%Y-%m-%d")

    return jsonify(baixar_e_processar_bpa_i(json_params['UF'], data_datetime))


@app.route("/bpa_i_postgres", methods=['POST'])
def load_bpa_i():
    content_type = request.headers.get('Content-Type')
    if (content_type == 'application/json'):
        json_params = request.json
    else:
        return 'Erro, content-type deve ser json', 400

    data_datetime = datetime.datetime.strptime(json_params['data'], "%Y-%m-%d")

    return jsonify(inserir_bpa_i_postgres(json_params['UF'], data_datetime, json_params['tabela_destino']))




if __name__ == "__main__":
    from waitress import serve
    serve(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
