FROM bitnami/spark:latest

USER root

COPY requirements.txt /ingestion/

RUN pip install -r /ingestion/requirements.txt

WORKDIR /opt/bitnami/spark
USER 1001
ENTRYPOINT [ "/opt/bitnami/scripts/spark/entrypoint.sh" ]
CMD [ "/opt/bitnami/scripts/spark/run.sh" ]
