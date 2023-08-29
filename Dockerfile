FROM apache/airflow:2.0.0
RUN pip install apache-airflow-providers-amazon \
  && pip install awscli