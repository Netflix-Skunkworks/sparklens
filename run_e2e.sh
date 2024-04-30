#!/bin/bash

# Runs an end to end test, updates the output

set -ex

rm -rf /tmp/spark-events

source env_setup.sh


sql_file=e2e/partioned_table_join.sql
EXTENSIONS=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
${SPARK_HOME}/bin/spark-sql --master local[5] \
	     --conf spark.eventLog.enabled=true \
	    --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
	    --conf spark.sql.catalog.spark_catalog.type=hive \
	    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
	    --conf spark.sql.catalog.local.type=hadoop \
	    --conf "spark.sql.catalog.local.warehouse=$PWD/warehouse" \
	    --name "${sql_file}" \
	    -f "${sql_file}"


cp /tmp/spark-events/* ./src/test/event-history-test-files/local-fresh

sbt -DsparkVersion=${SPARK_VERSION} ";clean;compile;test"

