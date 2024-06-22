import pandas as pd
from io import StringIO
from google.cloud import storage


# Inserir resultados no GCS
def upload_to_bucket(bucket_name, blob_path, plain_text):
    bucket = storage.Client().bucket(bucket_name)
    blob = bucket.blob(blob_path)
    url = blob.upload_from_string(plain_text.encode('utf8'))
    return url


# Baixa arquivo do GCS
def download_from_bucket(bucket_name, blob_path):
    """
    Baixa um arquivo CSV do Google Cloud Storage e retorna como um objeto dataframe do pandas.
    """
    bucket = storage.Client().bucket(bucket_name)
    blob = bucket.blob(blob_path)
    content = blob.download_as_text()
    arquivo = pd.read_csv(StringIO(content), dtype=str, index_col=0)
    return arquivo