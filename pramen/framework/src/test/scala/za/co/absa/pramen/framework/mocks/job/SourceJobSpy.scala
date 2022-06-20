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

package za.co.absa.pramen.framework.mocks.job

import java.time.LocalDate

import org.apache.spark.sql.{DataFrame, SparkSession}
import za.co.absa.pramen.api.reader.TableReader
import za.co.absa.pramen.api.writer.TableWriter
import za.co.absa.pramen.api.{SourceJob, TableDataFrame}
import za.co.absa.pramen.framework.mocks.reader.ReaderSpy
import za.co.absa.pramen.framework.mocks.writer.WriterSpy

class SourceJobSpy(val numOfRecords: Long)(implicit spark: SparkSession) extends SourceJob {

  var getTablesCalled = 0
  var getReaderCalled = 0
  var getWriterCalled = 0
  var runTaskCalled = 0

  val reader = new ReaderSpy(numOfRecords)
  val writer = new WriterSpy(numOfRecords)

  override def getTables: Seq[String] = {
    getTablesCalled += 1

    Seq("dummy_table")
  }

  override def getReader(tableName: String): TableReader = {
    getReaderCalled += 1
    reader
  }

  override def getWriter(tableName: String): TableWriter = {
    getWriterCalled += 1
    writer
  }

  override def runTask(inputTable: TableDataFrame, infoDateBegin: LocalDate, infoDateEnd: LocalDate, infoDateOutput: LocalDate): DataFrame = {
    runTaskCalled += 1
    super.runTask(inputTable, infoDateBegin, infoDateEnd, infoDateOutput)
  }

  override def name: String = "Dummy"
}



