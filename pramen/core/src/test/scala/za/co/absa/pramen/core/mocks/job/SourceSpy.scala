/*
 * Copyright 2022 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.pramen.core.mocks.job

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import za.co.absa.pramen.api.{ExternalChannelFactory, Query, Source, SourceResult}
import za.co.absa.pramen.core.mocks.reader.ReaderSpy

import java.time.LocalDate
import scala.collection.mutable.ListBuffer

class SourceSpy(sourceConfig: Config = ConfigFactory.empty(),
                numberOfRecords: Int = 5,
                getCountException: Throwable = null,
                getDataException: Throwable = null
)(implicit spark: SparkSession) extends Source {
  var connectCalled: Int = 0
  var closeCalled: Int = 0
  var writeCalled: Int = 0
  val dfs: ListBuffer[DataFrame] = new ListBuffer()
  var specialOption: Option[String] = None

  override val config: Config = sourceConfig

  def getRecordCount(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate): Long = {
    if (getCountException != null) {
      throw getCountException
    }

    numberOfRecords
  }

  def getData(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate, columns: Seq[String]): SourceResult = {
    import spark.implicits._

    if (getDataException != null) {
      throw getDataException
    }

    val generatedList = Range(0, numberOfRecords).toList
    val df = generatedList.toDF("v")

    SourceResult(df)
  }
}

object SourceSpy extends ExternalChannelFactory[SourceSpy] {
  override def apply(conf: Config, parentPath: String, spark: SparkSession): SourceSpy = {
    new SourceSpy()(spark)
  }
}
