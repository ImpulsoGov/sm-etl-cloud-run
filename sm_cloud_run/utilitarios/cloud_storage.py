import io
import pandas as pd
from io import StringIO
from google.cloud import storage


# Faz upload de dados no GCS
def upload_to_bucket(bucket_name, blob_path, dados):
    """
    Faz o upload de dados para o Google Cloud Storage (GCS).

    Args:
        bucket_name (str): Nome do bucket no GCS onde os dados serão enviados.
        blob_path (str): Caminho do blob (arquivo) dentro do bucket onde os dados serão armazenados.
        dados (str): Dados a serem enviados, representados como uma string.

    Returns:
        str: URL do blob no GCS onde os dados foram armazenados.

    A função se conecta ao GCS usando a biblioteca `google-cloud-storage`, cria um blob no bucket especificado,
    e carrega os dados fornecidos para este blob. O tipo de conteúdo do arquivo enviado é especificado como
    "text/plain" para indicar que os dados são texto simples. Após o upload bem-sucedido, a função retorna
    o URL completo do blob no formato 'gs://bucket_name/blob_path'.
    """
    bucket = storage.Client().bucket(bucket_name)
    blob = bucket.blob(blob_path)

    with io.BytesIO(dados.encode('utf8')) as file_obj:
        blob.upload_from_file(file_obj, content_type='text/plain', timeout=3600)

    url = f"gs://{bucket_name}/{blob_path}"
    return url


# Baixa arquivo do GCS
def download_from_bucket(bucket_name, blob_path):
    """
    Baixa um arquivo CSV do Google Cloud Storage e retorna como um objeto DataFrame do pandas.

    Args:
        bucket_name (str): Nome do bucket no Google Cloud Storage de onde o arquivo será baixado.
        blob_path (str): Caminho do blob (arquivo) dentro do bucket que será baixado.

    Returns:
        pandas.DataFrame: Objeto DataFrame contendo os dados do arquivo CSV baixado.

    A função se conecta ao Google Cloud Storage usando a biblioteca `google-cloud-storage`, acessa o blob
    especificado pelo `bucket_name` e `blob_path`, e baixa o conteúdo do arquivo CSV como texto.
    Em seguida, utiliza `pd.read_csv` do pandas para ler o conteúdo do CSV diretamente a partir do texto baixado,
    convertendo-o em um objeto DataFrame.
    O DataFrame resultante contém os dados do arquivo CSV e é retornado como resultado da função.

    """
    bucket = storage.Client().bucket(bucket_name)
    blob = bucket.blob(blob_path)
    content = blob.download_as_text()
    arquivo = pd.read_csv(StringIO(content), dtype=str)
    return arquivo


## TODO: unificar funções abaixo com as acima e adicionar encoding como parâmetro
def sisab_upload_to_bucket(bucket_name, blob_path, dados):
    """
    Faz o upload de dados para o Google Cloud Storage (GCS).

    Args:
        bucket_name (str): Nome do bucket no GCS onde os dados serão enviados.
        blob_path (str): Caminho do blob (arquivo) dentro do bucket onde os dados serão armazenados.
        dados (str): Dados a serem enviados, representados como uma string.

    Returns:
        str: URL do blob no GCS onde os dados foram armazenados.

    A função se conecta ao GCS usando a biblioteca `google-cloud-storage`, cria um blob no bucket especificado,
    e carrega os dados fornecidos para este blob. O tipo de conteúdo do arquivo enviado é especificado como
    "text/plain" para indicar que os dados são texto simples. Após o upload bem-sucedido, a função retorna
    o URL completo do blob no formato 'gs://bucket_name/blob_path'.
    """
    bucket = storage.Client().bucket(bucket_name)
    blob = bucket.blob(blob_path)

    with io.BytesIO(dados.encode('iso-8859-1')) as file_obj:
        blob.upload_from_file(file_obj, content_type='text/plain', timeout=3600)

    url = f"gs://{bucket_name}/{blob_path}"
    return url

def sisab_download_from_bucket(bucket_name, blob_path):
    """
    Baixa um arquivo CSV do Google Cloud Storage e retorna como um objeto DataFrame do pandas.

    Args:
        bucket_name (str): Nome do bucket no Google Cloud Storage de onde o arquivo será baixado.
        blob_path (str): Caminho do blob (arquivo) dentro do bucket que será baixado.

    Returns:
        pandas.DataFrame: Objeto DataFrame contendo os dados do arquivo CSV baixado.

    A função se conecta ao Google Cloud Storage usando a biblioteca `google-cloud-storage`, acessa o blob
    especificado pelo `bucket_name` e `blob_path`, e baixa o conteúdo do arquivo CSV como texto.
    Em seguida, utiliza `pd.read_csv` do pandas para ler o conteúdo do CSV diretamente a partir do texto baixado,
    convertendo-o em um objeto DataFrame.
    O DataFrame resultante contém os dados do arquivo CSV e é retornado como resultado da função.

    """
    bucket = storage.Client().bucket(bucket_name)
    blob = bucket.blob(blob_path)
    content = blob.download_as_text(encoding="iso-8859-1")
    arquivo = pd.read_csv(StringIO(content), dtype=str)
    return arquivo