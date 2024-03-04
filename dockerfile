FROM python:3.10

COPY sm_teste_pa.py .

COPY requirements.txt .

RUN pip install -r requirements.txt

ENTRYPOINT ["python", "sm_teste_pa.py"]