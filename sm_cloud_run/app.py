import os
import datetime

from flask import Flask, request, jsonify


from etl.datasus_ftp_metadados import upsert_dados_no_postgres
from scripts.verificar_e_executar import verificar_e_executar

app = Flask(__name__)


@app.route("/ftp_metadados", methods=['POST'])
def ftp_metadados():
    content_type = request.headers.get('Content-Type')
    if (content_type != 'application/json'):
        return 'Erro, content-type deve ser json', 400

    return jsonify(upsert_dados_no_postgres())


@app.route("/datasus_etl_e_load", methods=['POST'])
def sm_etl_gcs():
    content_type = request.headers.get('Content-Type')
    if (content_type == 'application/json'):
        json_params = request.json
    else:
        return 'Erro, content-type deve ser json', 400

    data_datetime = datetime.datetime.strptime(json_params['data'], "%Y-%m-%d")

    return jsonify(verificar_e_executar(json_params['UF'], data_datetime, json_params['ETL'], json_params['acao']))



if __name__ == "__main__":
    from waitress import serve
    serve(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
