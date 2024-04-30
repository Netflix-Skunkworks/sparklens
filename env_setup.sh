#!/bin/bash

set -ex

# Download Spark and iceberg if not present
SPARK_MAJOR=${SPARK_MAJOR:-"3.3"}
SPARK_VERSION=${SPARK_VERSION:-"${SPARK_MAJOR}.4"}
SCALA_VERSION=${SCALA_VERSION:-"2.12"}
HADOOP_VERSION="3"
SPARK_PATH="$(pwd)/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}"
SPARK_FILE="spark-${SPARK_VERSION}-bin-hadoop3.tgz"
ICEBERG_VERSION=${ICEBERG_VERSION:-"1.4.0"}
if [ ! -f "${SPARK_FILE}" ]; then
  SPARK_DIST_URL="https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/${SPARK_FILE}"
  SPARK_ARCHIVE_DIST_URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_FILE}"
  if command -v axel &> /dev/null
  then
    (axel "$SPARK_DIST_URL" || axel "$SPARK_ARCHIVE_DIST_URL") &
  else
    (wget "$SPARK_DIST_URL" || wget "$SPARK_ARCHIVE_DIST_URL") &
  fi
fi
# Download Icberg if not present
ICEBERG_FILE="iceberg-spark-runtime-${SPARK_MAJOR}_${SCALA_VERSION}-${ICEBERG_VERSION}.jar"
if [ ! -f "${ICEBERG_FILE}" ]; then
  wget "https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime-${SPARK_MAJOR}_${SCALA_VERSION}/${ICEBERG_VERSION}/${ICEBERG_FILE}" -O "${ICEBERG_FILE}" &
fi
# See https://www.mail-archive.com/dev@spark.apache.org/msg27796.html
BC_FILE="bcpkix-jdk15on-1.68.jar"
BC_PROV_FILE="bcprov-jdk15on-1.68.jar"
if [ ! -f "${BC_FILE}" ]; then
  wget https://repo1.maven.org/maven2/org/bouncycastle/bcpkix-jdk15on/1.68/bcpkix-jdk15on-1.68.jar
fi
if [ ! -f "${BC_PROV_FILE}" ]; then
  wget https://repo1.maven.org/maven2/org/bouncycastle/bcprov-jdk15on/1.68/bcprov-jdk15on-1.68.jar
fi
wait
sleep 1
# Setup the env
if [ ! -d "${SPARK_PATH}" ]; then
  tar -xf "${SPARK_FILE}"
fi

SPARK_HOME="${SPARK_PATH}"
export SPARK_HOME

if [ ! -f "${SPARK_PATH}/jars/${ICEBERG_FILE}" ]; then
  # Delete the old JAR first.
  rm "${SPARK_PATH}/jars/iceberg-spark-runtime*.jar" || echo "No old version to delete."
  cp "${ICEBERG_FILE}" "${SPARK_PATH}/jars/${ICEBERG_FILE}"
fi

# Copy boncy castle for Kube
cp bc*jdk*.jar ${SPARK_PATH}/jars/

# Set up for running pyspark and friends
export PATH="${SPARK_PATH}:${SPARK_PATH}/python:${SPARK_PATH}/bin:${SPARK_PATH}/sbin:${PATH}"

# Make sure we have a history directory
mkdir -p /tmp/spark-events

mkdir -p ./data/fetched/
if [ ! -f ./data/fetched/2021 ]; then
  wget "https://gender-pay-gap.service.gov.uk/viewing/download-data/2021" -O ./data/fetched/2021
fi

