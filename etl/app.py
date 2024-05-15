import os
import datetime

from flask import Flask, request, jsonify

from procedimentos_ambulatoriais import baixar_e_processar_pa

app = Flask(__name__)


@app.route("/pa", methods=['POST'])
def sm_pa():
    content_type = request.headers.get('Content-Type')
    if (content_type == 'application/json'):
        json_params = request.json
    else:
        return 'Erro, content-type deve ser json', 400

    data_datetime = datetime.datetime.strptime(json_params['data'], "%Y-%m-%d")

    return jsonify(baixar_e_processar_pa(json_params['UF'], data_datetime))


# @app.route("/bpai", methods=['POST'])
# def sm_bpai():
#     content_type = request.headers.get('Content-Type')
#     if (content_type == 'application/json'):
#         json_params = request.json
#     else:
#         return 'Erro, content-type deve ser json', 400

#     data_datetime = datetime.datetime.strptime(json_params['data'], "%Y-%m-%d")

#     return jsonify(baixar_e_processar_bpai(json_params['UF'], data_datetime))


if __name__ == "__main__":
    from waitress import serve
    serve(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
