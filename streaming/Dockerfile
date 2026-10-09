FROM apache/spark:3.5.3-python3

USER root

RUN python3 -m pip install --no-cache-dir \
    psycopg2-binary==2.9.10 \
    jupyterlab==4.2.5 \
    ipykernel==6.29.5 \
    pandas==2.0.3 \
    numpy==1.24.4 \
    matplotlib==3.7.1

# Rendre les bibliothèques Python de Spark accessibles à Jupyter.
ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip

# Préparer les dossiers utilisés par Jupyter.
ENV HOME=/home/spark
RUN mkdir -p /home/spark \
    && chown spark /home/spark

USER spark

WORKDIR /opt/spark/work-dir