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

package za.co.absa.pramen.builtin.infofile

import com.typesafe.config.Config
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.framework.utils.ConfigUtils

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate, ZoneId, ZonedDateTime}

object InfoFileGeneration {
  val PLUGINS_GENERATE_INFO_FILE = "info.file.generate"
  val SOURCE_APPLICATION_KEY = "info.file.source.application"
  val COUNTRY_KEY = "info.file.country"
  val HISTORY_TYPE_KEY = "info.file.history.type"
  val TIMESTAMP_FORMAT_KEY = "info.file.timestamp.format"
  val DATE_FORMAT_KEY = "info.file.date.format"

  private val log = LoggerFactory.getLogger(this.getClass)

  def generateInfoFile(pramenVersion: String,
                       timezoneId: ZoneId,
                       sourceCount: Long,
                       rawDf: DataFrame,
                       outputPartitionPath: Path,
                       infoDate: LocalDate,
                       sourceStart: Instant,
                       rawStart: Instant)
                      (implicit spark: SparkSession, conf: Config): Unit = {
    val fs = outputPartitionPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val rawCount = rawDf.count()

    val infoFileContents = renderInfoFile(pramenVersion, timezoneId, sourceCount, rawCount, infoDate, sourceStart, rawStart, Instant.now())

    val infoFilePath = new Path(outputPartitionPath, "_INFO")

    val out = fs.create(infoFilePath)
    out.write(infoFileContents.getBytes())
    out.close()

    log.info(s"The info file is saved to $infoFilePath:\n$infoFileContents")
  }

  def renderInfoFile(pramenVersion: String,
                     timezoneId: ZoneId,
                     sourceCount: Long,
                     rawCount: Long,
                     infoDate: LocalDate,
                     sourceStart: Instant,
                     rawStart: Instant,
                     rawFinish: Instant)
                    (implicit conf: Config): String = {
    ConfigUtils.validatePathsExistence(conf, "", Seq(SOURCE_APPLICATION_KEY, COUNTRY_KEY, HISTORY_TYPE_KEY, TIMESTAMP_FORMAT_KEY, DATE_FORMAT_KEY))

    val sourceApplication = conf.getString(SOURCE_APPLICATION_KEY)
    val country = conf.getString(COUNTRY_KEY)
    val historyType = conf.getString(HISTORY_TYPE_KEY)
    val timestampFormat = conf.getString(TIMESTAMP_FORMAT_KEY)
    val dateFormat = conf.getString(DATE_FORMAT_KEY)

    val fmtTimestamp = DateTimeFormatter.ofPattern(timestampFormat)
    val fmtDate = DateTimeFormatter.ofPattern(dateFormat)

    val infoDateStr = fmtDate.format(infoDate)

    val sourceStarted = ZonedDateTime.ofInstant(sourceStart, timezoneId).format(fmtTimestamp)
    val rawStarted = ZonedDateTime.ofInstant(rawStart, timezoneId).format(fmtTimestamp)
    val rawFinished = ZonedDateTime.ofInstant(rawFinish, timezoneId).format(fmtTimestamp)

    s"""{
       |  "metadata" : {
       |    "sourceApplication" : "$sourceApplication",
       |    "country" : "$country",
       |    "historyType" : "$historyType",
       |    "dataFilename" : "JDBC",
       |    "sourceType" : "Source",
       |    "version" : 1,
       |    "informationDate" : "$infoDateStr",
       |    "additionalInfo" : { }
       |  },
       |  "checkpoints" : [ {
       |    "name" : "Source",
       |    "software" : "pramen",
       |    "version" : "$pramenVersion",
       |    "processStartTime" : "$sourceStarted",
       |    "processEndTime" : "$rawStarted",
       |    "workflowName" : "Source",
       |    "order" : 1,
       |    "controls" : [ {
       |      "controlName" : "recordCount",
       |      "controlType" : "count",
       |      "controlCol" : "*",
       |      "controlValue" : "$sourceCount"
       |    } ]
       |  }, {
       |    "name" : "Raw",
       |    "software" : "pramen",
       |    "version" : "$pramenVersion",
       |    "processStartTime" : "$rawStarted",
       |    "processEndTime" : "$rawFinished",
       |    "workflowName" : "Source",
       |    "order" : 2,
       |    "controls" : [ {
       |      "controlName" : "recordCount",
       |      "controlType" : "count",
       |      "controlCol" : "*",
       |      "controlValue" : "$rawCount"
       |    } ]
       |  } ]
       |}
       |""".stripMargin

  }
}
