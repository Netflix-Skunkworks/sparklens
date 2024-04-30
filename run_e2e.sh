#!/bin/bash

# Runs an end to end test, updates the output

set -ex

rm -rf /tmp/spark-events

source env_setup.sh


sql_file=e2e/partioned_table_join.sql
EXTENSIONS=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions

# Run a simple local e2e example

mkdir -p /tmp/spark-events
${SPARK_HOME}/bin/spark-sql --master local[5] \
	     --conf spark.eventLog.enabled=true \
	    --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
	    --conf spark.sql.catalog.spark_catalog.type=hive \
	    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
	    --conf spark.sql.catalog.local.type=hadoop \
	    --conf "spark.sql.catalog.local.warehouse=$PWD/warehouse" \
	    --name "fresh" \
	    -f "${sql_file}"


cp /tmp/spark-events/* ./src/test/event-history-test-files/local-fresh

# Same example but with dynamic allocation turned on IF AND ONLY IF we have a kube cluster? idk.

source setup_micro_k8s.sh

if [ -n "${kube_host}" ]; then

  rm -rf /tmp/spark-events
  mkdir -p /tmp/spark-events
  SPARK_LOCAL_IP=127.0.0.1 \
  ${SPARK_HOME}/bin/spark-sql --master "k8s://${kube_host}" \
	       --conf spark.eventLog.enabled=true \
	       --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
	       --conf spark.sql.catalog.spark_catalog.type=hive \
	       --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
	       --conf spark.sql.catalog.local.type=hadoop \
	       --conf spark.dynamicAllocation.enabled=true \
	       --conf "spark.sql.catalog.local.warehouse=$PWD/warehouse" \
	       --conf spark.kubernetes.container.image=${image} \
	       --conf spark.kubernetes.authenticate.caCertFile=${cert_path} \
	       --conf spark.kubernetes.authenticate.submission.caCertFile=${cert_path} \
	       --name "fresh-kube" \
	       -f "${sql_file}"

  cp /tmp/spark-events/* ./src/test/event-history-test-files/local-fresh
  cp ~/.kube/${backup_config} ~/.kube/config
fi


sbt -DsparkVersion=${SPARK_VERSION} ";clean;compile;testOnly com.qubole.sparklens.app.EventHistoryFileReportingSuite -- -z fresh"

