/*
 * Copyright 2020 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.pramen.mocks

import java.time.LocalDate

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import za.co.absa.pramen.api._

class DummyAggregationJob extends SyncWatcherAggregationJob {
  var inputTables: Seq[TableDataFrame] = Nil
  var infoDateBegin: LocalDate = _
  var infoDateEnd: LocalDate = _
  var infoDateOutput: LocalDate = _

  override def runTask(taskDependencies: Seq[TaskDependency],
                       infoDateBegin: LocalDate,
                       infoDateEnd: LocalDate,
                       infoDateOutput: LocalDate): Either[Reason, Long] = {

    Left(Reason.NotReady("test"))
  }
}

object DummyAggregationJob extends SyncWatcherJobFactory[DummyAggregationJob] {
  override def apply(conf: Config, spark: SparkSession): DummyAggregationJob =
    new DummyAggregationJob
}
