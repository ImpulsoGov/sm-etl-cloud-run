FROM python:3.10

COPY etl/* etl/

RUN pip install -r etl/requirements.txt

WORKDIR etl

ENTRYPOINT ["python", "app.py"]