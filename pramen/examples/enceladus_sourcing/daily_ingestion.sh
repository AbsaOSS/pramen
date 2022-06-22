#!/bin/bash

# Copyright 2022 ABSA Group Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
  $SYNC_WATCHER_JAR --workflow ${ME}.conf $@

cd -
