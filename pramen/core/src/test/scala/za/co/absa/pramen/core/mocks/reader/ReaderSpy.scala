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

package za.co.absa.pramen.core.mocks.reader

import org.apache.spark.sql.{DataFrame, SparkSession}
import za.co.absa.pramen.api.offset.OffsetValue
import za.co.absa.pramen.api.{Query, TableReader}

import java.time.LocalDate
import scala.collection.mutable.ListBuffer

class ReaderSpy(numRecords: Long,
                getCountException: Throwable = null,
                getDataException: Throwable = null)(implicit spark: SparkSession) extends TableReader {

  val infoDatePeriodsCountCalled = new ListBuffer[(LocalDate, LocalDate)]
  val infoDatePeriodsDataRead = new ListBuffer[(LocalDate, LocalDate)]

  override def getRecordCount(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate): Long = {
    if (getCountException != null) {
      throw getCountException
    }
    infoDatePeriodsCountCalled += infoDateBegin -> infoDateEnd
    numRecords
  }

  override def getData(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate, columns: Seq[String]): DataFrame = {
    import spark.implicits._

    if (getDataException != null) {
      throw getDataException
    }

    infoDatePeriodsDataRead += infoDateBegin -> infoDateEnd

    val generatedList = Range(0, numRecords.toInt).toList
    val df = generatedList.toDF("v")
    df
  }

  override def getIncrementalData(query: Query, onlyForInfoDate: Option[LocalDate], offsetFromOpt: Option[OffsetValue], offsetToOpt: Option[OffsetValue], columns: Seq[String]): DataFrame = ???
}
