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

package za.co.absa.pramen.framework.job

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api._
import za.co.absa.pramen.api.metastore.Metastore
import za.co.absa.pramen.api.reader.TableReader
import za.co.absa.pramen.api.schedule.Schedule
import za.co.absa.pramen.api.v2.Source
import za.co.absa.pramen.api.writer.TableWriter
import za.co.absa.pramen.framework.AppContextFactory
import za.co.absa.pramen.framework.expr.DateExprEvaluator
import za.co.absa.pramen.framework.source.SourceManager
import za.co.absa.pramen.framework.utils.SparkUtils.{addProcessingTimestamp, applyTransformations}

import java.time.LocalDate

class IngestionJob(jobName: String,
                   schedule: Schedule,
                   metastore: Metastore,
                   sourceGetter: SourceTable => Source,
                   tables: List[SourceTable],
                   outputInfoDateExpression: Option[String],
                   processingTimestampCol: Option[String])
                  (implicit spark: SparkSession, conf: Config)
  extends SourceJob with LazyReadJob with SchemaTransformJob {
  private val log = LoggerFactory.getLogger(this.getClass)

  override def name: String = jobName

  override def getSchedule: Schedule = schedule

  override def getTables: Seq[String] = tables.map(_.metaTableName)

  override def transformOutputInfoDate(infoDate: LocalDate): LocalDate = {
    outputInfoDateExpression match {
      case Some(expr) =>
        val evaluator = new DateExprEvaluator
        evaluator.setValue("infoDate", infoDate)
        val result = evaluator.evalDate(expr)
        val q = "\""
        log.info(s"Given @infoDate = '$infoDate', $q$expr$q => '$result'")
        result
      case None       =>
        infoDate
    }
  }

  override def getReader(tableName: String): TableReader = {
    val table = getSourceTable(tableName)

    val source = sourceGetter(table)

    source.getReader(table.query, table.columns)
  }

  override def getWriter(tableName: String): TableWriter = {
    metastore.getWriter(tableName)
  }

  override def runTask(inputTable: TableDataFrame,
                       infoDateBegin: LocalDate,
                       infoDateEnd: LocalDate,
                       infoDateOutput: LocalDate): DataFrame = {
    processingTimestampCol match {
      case Some(timestampCol) => addProcessingTimestamp(inputTable.dataFrame, timestampCol)
      case None               => inputTable.dataFrame
    }
  }

  override def schemaTransformation(inputTable: TableDataFrame, infoDate: LocalDate): DataFrame = {
    val table = getSourceTable(inputTable.tableName)

    if (table.transformations.nonEmpty) {
      applyTransformations(inputTable.dataFrame, table.transformations)
    } else {
      inputTable.dataFrame
    }
  }

  private def getSourceTable(tableName: String): SourceTable = {
    tables.find(t => t.metaTableName.equalsIgnoreCase(tableName)) match {
      case Some(table) => table
      case None        => throw new IllegalStateException(s"Source table not defined: $tableName")
    }
  }
}

object IngestionJob extends JobFactory[IngestionJob] {
  val PREFIX = "pramen.ingestion"

  override def apply(conf: Config, spark: SparkSession): IngestionJob = {
    val metastore = AppContextFactory.get.metastore
    val sc = IngestionContext.fromConfig(conf, PREFIX)

    val sourceGetter = (table: SourceTable) => {
      SourceManager.getSourceByName(sc.sourceName, conf, table.overrideConf)(spark)
    }

    new IngestionJob(sc.jobName, sc.schedule, metastore, sourceGetter, sc.tables, sc.outputInfoDateExpression, sc.processingTimestampCol)(spark, conf)
  }

}
