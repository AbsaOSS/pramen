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

package za.co.absa.pramen.framework.job

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api._
import za.co.absa.pramen.api.metastore.Metastore
import za.co.absa.pramen.api.schedule.Schedule
import za.co.absa.pramen.api.v2.Sink
import za.co.absa.pramen.framework.AppContextFactory
import za.co.absa.pramen.framework.job.SinkJob.EXTRA_INPUT_TABLES_KEY
import za.co.absa.pramen.framework.sink.SinkManager

import java.time.LocalDate

class SinkJob(jobName: String,
              schedule: Schedule,
              metastore: Metastore,
              sinkGetter: SinkTable => Sink,
              sinkName: String,
              tables: Seq[SinkTable])
             (implicit spark: SparkSession, conf: Config) extends AggregationJob {
  private val log = LoggerFactory.getLogger(this.getClass)

  override def name: String = jobName

  override def getSchedule: Schedule = schedule

  override def getDependencies: Seq[JobDependency] = tables.map(t => JobDependency(Seq(t.metaTableName), s"${t.metaTableName}->$sinkName"))

  override def runTask(inputTables: Seq[String],
                       infoDateBegin: LocalDate,
                       infoDateEnd: LocalDate,
                       infoDateOutput: LocalDate): Option[Long] = {
    val table = getSinkTable(inputTables.head)
    val sink = sinkGetter(table)

    sink.connect()

    try {
      val additionalTables = table.options.get(EXTRA_INPUT_TABLES_KEY).map(_.split(",").toList).getOrElse(List.empty)
      val transformationMap = getTransformationMap(table)
      val metastoreReader = new MetastoreSchemaTransformer(metastore.getMetastoreReader(Seq(table.metaTableName) ++ additionalTables, infoDateOutput), transformationMap)

      log.info(s"Processing ${table.metaTableName} for $infoDateOutput...")
      val df = metastoreReader.getTable(table.metaTableName, Option(infoDateOutput), Option(infoDateOutput))

      val recordsWritten = sink.send(df, table.metaTableName, metastoreReader, infoDateOutput, table.options)
      Option(recordsWritten)
    }
    finally {
      sink.close()
    }
  }

  def getSinkTable(tableName: String): SinkTable = {
    tables.find(t => t.metaTableName.equalsIgnoreCase(tableName)) match {
      case Some(table) => table
      case None        => throw new IllegalStateException(s"Sink table not defined: $tableName")
    }
  }

  def getTransformationMap(table: SinkTable): Map[String, Seq[TransformExpression]] = {
    Map(
      table.metaTableName -> table.transformations
    )
  }
}

object SinkJob extends JobFactory[SinkJob] {
  val PREFIX = "pramen.sink"
  val EXTRA_INPUT_TABLES_KEY = "extra.input.tables"

  override def apply(conf: Config, spark: SparkSession): SinkJob = {
    val metastore = AppContextFactory.get.metastore
    val sc = SinkContext.fromConfig(conf, PREFIX)

    val sinkGetter = (sinkTable: SinkTable) => {
      SinkManager.getSinkByName(sc.sinkName, conf, sinkTable.overrideConf)(spark)
    }

    new SinkJob(sc.jobName, sc.schedule, metastore, sinkGetter, sc.sinkName, sc.tables)(spark, conf)
  }
}
