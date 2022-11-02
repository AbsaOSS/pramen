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

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import za.co.absa.pramen.api.{ExternalChannelFactory, MetastoreReader, Query, Sink, Source, TableReader}
import za.co.absa.pramen.core.mocks.reader.ReaderSpy

import java.time.LocalDate
import scala.collection.mutable.ListBuffer

class SourceSpy(query: Query = Query.Table("table1"),
                numberOfRecords: Int = 5,
                hasInformationDate: Boolean = false,
                readerCountException: Throwable = null,
                readerDataException: Throwable = null
)(implicit spark: SparkSession) extends Source {
  var connectCalled: Int = 0
  var closeCalled: Int = 0
  var writeCalled: Int = 0
  val dfs: ListBuffer[DataFrame] = new ListBuffer()
  var specialOption: Option[String] = None

  override def hasInfoDate: Boolean = hasInformationDate

  override def getReader(query: Query, columns: Seq[String]): TableReader = new ReaderSpy(numberOfRecords, readerCountException, readerDataException)
}

object SourceSpy extends ExternalChannelFactory[SourceSpy] {
  override def apply(conf: Config, parentPath: String, spark: SparkSession): SourceSpy = {
    new SourceSpy()(spark)
  }
}
