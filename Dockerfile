RUN kind create cluster --name trino
RUN helm install -f conf/conf.yaml trino trino/trino
