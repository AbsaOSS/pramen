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

package za.co.absa.pramen.extras.mocks

import java.time.LocalDate
import org.apache.spark.sql.{DataFrame, SparkSession}
import za.co.absa.pramen.api.{Query, TableReader}

import scala.collection.mutable.ListBuffer

class ReaderSpy(numRecords: Long = 0L)(implicit spark: SparkSession)
  extends TableReader {

  val infoDatePeriodsCountCalled = new ListBuffer[(LocalDate, LocalDate)]
  val infoDatePeriodsDataRead = new ListBuffer[(LocalDate, LocalDate)]

  override def getRecordCount(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate): Long = {
    infoDatePeriodsCountCalled += infoDateBegin -> infoDateEnd
    numRecords
  }

  override def getData(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate, columns: Seq[String]): DataFrame = {
    import spark.implicits._

    infoDatePeriodsDataRead += infoDateBegin -> infoDateEnd

    val generatedList = Range(0, numRecords.toInt).toList
    val df = generatedList.toDF("v")
    df
  }
}
