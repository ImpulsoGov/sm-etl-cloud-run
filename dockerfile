FROM python:3.10

COPY sm_cloud_run .
COPY .env .

RUN pip install -r requirements.txt

ENTRYPOINT ["python", "app.py"]
