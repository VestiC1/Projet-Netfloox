# app/Dockerfile

FROM python:3.13-slim

WORKDIR /code

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

COPY . /code

EXPOSE 8501

ENTRYPOINT ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]