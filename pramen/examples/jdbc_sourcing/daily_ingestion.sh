#!/bin/bash
ME=`basename "$0" .sh`

cd $(dirname $(readlink -f $0))

BUILT_IN_JAR="bultin-jobs-0.13.0.jar"
SYNC_WATCHER_JAR="pipeline-runner-0.13.0.jar"

set -euxo pipefail

$SPARK_HOME/bin/spark-submit \
  --master yarn \
  --deploy-mode client \
  --jars $BUILT_IN_JAR \
  --class za.co.absa.pramen.runner.PipelineRunner \
  $SYNC_WATCHER_JAR --check-new-only --workflow ${ME}.conf $@

cd -
