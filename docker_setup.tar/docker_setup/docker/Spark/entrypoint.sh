#!/bin/bash
#


echo "Waiting for metastore on ${METASTORE_HOSTNAME} to launch on ${METASTORE_PORT} ..."
while ! nc -z ${METASTORE_HOSTNAME} ${METASTORE_PORT}; do
   sleep 1
done

start-master.sh -i ${SPARK_HOST_NAME} -p ${SPARK_PORT} --webui-port ${SPARK_WEB_PORT} 
start-worker.sh spark://${SPARK_HOST_NAME}:${SPARK_PORT}
start-history-server.sh


tail -f /dev/null