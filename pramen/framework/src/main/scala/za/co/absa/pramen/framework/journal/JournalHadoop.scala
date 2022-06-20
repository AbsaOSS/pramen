/*
 * Copyright 2022 ABSA Group Limited
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

package za.co.absa.pramen.framework.journal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import za.co.absa.pramen.framework.model.Constants
import za.co.absa.pramen.framework.notify.TaskCompleted
import za.co.absa.pramen.framework.utils.{CsvUtils, FsUtils, SparkUtils}

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate}

object JournalHadoop {
  val journalFileName = "journal.csv"
  val separator = '|'
}

class JournalHadoop(journalPath: String)
                   (implicit spark: SparkSession) extends Journal {

  import JournalHadoop._

  private val hdfsConfig: Configuration = spark.sparkContext.hadoopConfiguration
  private val fsUtils = new FsUtils(hdfsConfig, journalPath)
  private val journalFilePath = new Path(journalPath, journalFileName)

  private val headers = CsvUtils.getHeaders[TaskCompletedCsv](separator)
  private val schema = SparkUtils.getStructType[TaskCompletedCsv]

  private val dateFormatter = DateTimeFormatter.ofPattern(Constants.DATE_FORMAT_INTERNAL)

  init()

  override def addEntry(entry: TaskCompleted): Unit = {
    val taskStr = serializeCompletedTaskCsv(entry)
    fsUtils.appendFile(journalFilePath, taskStr)
  }

  override def getEntries(from: Instant, to: Instant): Seq[TaskCompleted] = {
    import spark.implicits._

    val df = spark
      .read
      .option("header", "true")
      .option("sep", s"$separator")
      .schema(schema)
      .csv(journalFilePath.toUri.toString)

    // Since in Spark 2 encoding/decoding of LocalDate is not supported, using an intermediate case class TaskCompletedCsv.
    df.filter(col("finishedAt") >= from.getEpochSecond && col("finishedAt") <= to.getEpochSecond)
      .orderBy(col("finishedAt"))
      .as[TaskCompletedCsv]
      .collect()
      .map(v => TaskCompleted(
        jobName = v.jobName,
        tableName = v.tableName,
        periodBegin = LocalDate.parse(v.periodBegin, dateFormatter),
        periodEnd = LocalDate.parse(v.periodEnd, dateFormatter),
        informationDate = LocalDate.parse(v.informationDate, dateFormatter),
        inputRecordCount = v.inputRecordCount,
        inputRecordCountOld = v.inputRecordCountOld,
        outputRecordCount = v.outputRecordCount,
        outputRecordCountOld = v.outputRecordCountOld,
        outputSize = v.outputSize,
        startedAt = v.startedAt,
        finishedAt = v.finishedAt,
        status = v.status,
        failureReason = v.failureReason
      ))
  }

  private def serializeCompletedTaskCsv(t: TaskCompleted): String = {
    val periodBegin = t.periodBegin.format(dateFormatter)
    val periodEnd = t.periodEnd.format(dateFormatter)
    val infoDate = t.informationDate.format(dateFormatter)

    val outputRecordCount = t.outputRecordCount.map(_.toString).getOrElse("")
    val outputRecordCountOld = t.outputRecordCountOld.map(_.toString).getOrElse("")
    val outputSize = t.outputSize.map(_.toString).getOrElse("")

    val record = removeSeparators(t.jobName) ::
      removeSeparators(t.tableName) ::
      periodBegin ::
      periodEnd ::
      infoDate ::
      t.inputRecordCount ::
      t.inputRecordCountOld ::
      outputRecordCount ::
      outputRecordCountOld ::
      outputSize ::
      t.startedAt ::
      t.finishedAt ::
      t.status ::
      removeSeparators(t.failureReason.getOrElse("")) ::
      Nil
    record.mkString("", s"$separator", "\n")
  }

  private def removeSeparators(s: String): String = {
    s.replace(separator, ' ')
      .replace('\n', ' ')
      .replace('\r', ' ')
  }

  private def init(): Unit = {
    fsUtils.createDirectoryRecursive(new Path(journalPath))

    // Create CSV headers
    if (!fsUtils.exists(journalFilePath)) {
      fsUtils.writeFile(journalFilePath, s"$headers\n")
    }
  }

}
