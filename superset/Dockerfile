ARG SUPERSET_VERSION=4.1.1
FROM apache/superset:${SUPERSET_VERSION}

USER root
RUN pip install --no-cache-dir clickhouse-connect psycopg2-binary requests gevent redis
USER superset
