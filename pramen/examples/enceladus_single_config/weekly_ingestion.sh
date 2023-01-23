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

#!/bin/bash
ME=`basename "$0" .sh`

cd $(dirname $(readlink -f $0))

SCALA_VERSION="2.11"

EXTRAS_JAR="pramen-extras_${SCALA_VERSION}-1.0.0.jar"
RUNNER_JAR="pramen-runner_${SCALA_VERSION}-1.0.0.jar"

set -euxo pipefail

$SPARK_HOME/bin/spark-submit \
  --master yarn \
  --deploy-mode client \
  --jars $EXTRAS_JAR \
  --class za.co.absa.pramen.runner.PipelineRunner \
  $RUNNER_JAR --workflow ${ME}.conf $@

cd -
