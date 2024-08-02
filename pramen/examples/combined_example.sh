#!/bin/bash

# Prerequisites:
# 1. Download Spark 3.4.1 (Scala 2.12) and install it in /opt/spark/spark-3.4.1 or some other directory
# 2. At repo_root/pramen, run
#    sbt -DSPARK_VERSION="3.4.1" ++2.12.19 assembly
# 3. Run
#    ./examples/combined_example.sh

cd $(dirname $(readlink -f $0))

export SCALA_VERSION="2.12"
export SPARK_VERSION="3.3.2"
export PRAMEN_VERSION="1.5.1-SNAPSHOT"

# SPECIFY SPARK LOCATION
export SPARK_HOME="/opt/spark/spark-3.3.2"

# SPECIFY PRAMEN JAR LOCATION
RUNNER_JAR="../runner/target/scala-${SCALA_VERSION}/pramen-runner_${SCALA_VERSION}_${SPARK_VERSION}-${PRAMEN_VERSION}.jar"
EXTRAS_JAR="../extras/target/scala-${SCALA_VERSION}/pramen-extras_${SCALA_VERSION}_${SPARK_VERSION}-${PRAMEN_VERSION}.jar"

RUNNER_ABSOLUTE_PATH_JAR=$(readlink -f ${RUNNER_JAR})
EXTRAS_ABSOLUTE_PATH_JAR=$(readlink -f ${EXTRAS_JAR})

DATE=`date +%Y_%m_%d-%H_%M_%S`
ME=`basename "$0" .sh`
USER=`whoami`

export PRAMEN_TEMP_DIR="/tmp/pramen_example_${USER}"

set -euxo pipefail

# Ensure the default JDK is 1.8
export JAVA_HOME=$(/usr/libexec/java_home -v 1.8)

## GENERATE CONFIG
rm -rf  "${PRAMEN_TEMP_DIR}"
mkdir -p "${PRAMEN_TEMP_DIR}" || true
cd "$PRAMEN_TEMP_DIR"

cat <<EOT > pramen_example.conf
pramen {
  environment.name = "DEV"
  pipeline.name = "EXAMPLE"

  parallel.tasks = 1

  bookkeeping.enabled = false

  temporary.directory = ${PRAMEN_TEMP_DIR}
}

mail {
  # CHANGE THIS TO YOUR SMTP SERVER IF YOU WANT TO RECEIVE EMAIL NOTIFICATIONS
  smtp.host = "some.sftp.server.com"
  smtp.port = "25"
  smtp.auth = "false"
  smtp.starttls.enable = "false"
  smtp.EnableSSL.enable = "false"
  debug = "false"

  send.from = "Pramen <pramen.noreply@absa.africa>"

  # THE COMMA SEPARATED LIST OF EMAIL ADDRESSES
  #send.to = "address@example.com"
}

# This is to run properly in local mode
spark.conf {
    spark.ui.enabled = "false"
    spark.driver.bindAddress = "127.0.0.1"
    spark.driver.host = "127.0.0.1"
    spark.sql.session.timeZone = "Africa/Johannesburg"
    spark.sql.shuffle.partitions = "1"
}

pramen.metastore {
  tables = [
    {
      name = "example_table1"
      format = "delta"
      path = ${PRAMEN_TEMP_DIR}"/metastore/example_table1"
    }
  ]
}

pramen.sources = [
  {
    name = "my_source"
    factory.class = "za.co.absa.pramen.core.source.SparkSource"

    format = "csv"

    has.information.date.column = false

    option {
      header = true
      delimiter = ","
    }
  }
]

pramen.sinks = [
  {
    name = "my_sink"
    factory.class = "za.co.absa.pramen.core.sink.SparkSink"

    format = "parquet"
    mode = "overwrite"
    number.of.partitions = 1
    save.empty = true

    partition.by = [ pramen_info_date ]
  }
]

pramen.operations = [
  {
    name = "Ingestion"
    type = "ingestion"

    schedule.type = "daily"

    source = "my_source"

    tables = [
      {
        input.path = ${PRAMEN_TEMP_DIR}"/landing/example_table1"
        output.metastore.table = "example_table1"

        source.schema = "id int, name string"

        transformations = [
          { col = "test_column", expr = "'test'" }
        ]
      }
    ]
  },
  {
    name = "Sink"
    type = "sink"
    sink = "my_sink"
    #disabled = "true"

    schedule.type = "daily"

    tables = [
      {
        input.metastore.table = "example_table1"
        output.path = ${PRAMEN_TEMP_DIR}"/sink/example_table1"
      }
    ]
  }
]
EOT

## GENERATE DATA

mkdir -p "${PRAMEN_TEMP_DIR}/landing/example_table1" || true

TEMP_CSV="$PRAMEN_TEMP_DIR/example.csv"

cat <<EOT > "$TEMP_CSV"
id,name
1,John
2,Mary
3,ABC
4,XYZ
EOT

cp "${TEMP_CSV}" "${PRAMEN_TEMP_DIR}/landing/example_table1/"

## RUN PRAMEN

$SPARK_HOME/bin/spark-submit \
  --master local \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --jars $EXTRAS_ABSOLUTE_PATH_JAR \
  --class za.co.absa.pramen.runner.PipelineRunner \
  $RUNNER_ABSOLUTE_PATH_JAR --workflow pramen_example.conf --override-log-level INFO $@

