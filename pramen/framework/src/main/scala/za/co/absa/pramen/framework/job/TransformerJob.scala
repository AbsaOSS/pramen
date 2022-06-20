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
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api._
import za.co.absa.pramen.api.metastore.Metastore
import za.co.absa.pramen.api.reader.TableReader
import za.co.absa.pramen.api.schedule.Schedule
import za.co.absa.pramen.api.v2.Transformer
import za.co.absa.pramen.api.writer.TableWriter
import za.co.absa.pramen.framework.AppContextFactory
import za.co.absa.pramen.framework.exceptions.ReasonException
import za.co.absa.pramen.framework.expr.DateExprEvaluator
import za.co.absa.pramen.framework.utils.ClassLoaderUtils

import java.time.LocalDate

class TransformerJob(job: Transformer,
                     metastore: Metastore,
                     jobName: String,
                     schedule: Schedule,
                     inputTables: List[String],
                     outputTable: String,
                     outputInfoDateExpression: Option[String],
                     options: Map[String, String])
                    (implicit spark: SparkSession, conf: Config)
  extends za.co.absa.pramen.api.TransformationJob with LazyReadJob {
  private val log = LoggerFactory.getLogger(this.getClass)

  override def name: String = jobName

  override def getSchedule: Schedule = schedule

  override def getDependencies: Seq[JobDependency] = JobDependency(inputTables, outputTable) :: Nil

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
    metastore.getReader(tableName)
  }

  override def getWriter(tableName: String): Option[TableWriter] = {
    Some(metastore.getWriter(tableName))
  }

  override def runTask(inputTablesIn: Seq[TableDataFrame],
                       infoDateBegin: LocalDate,
                       infoDateEnd: LocalDate,
                       infoDateOutput: LocalDate): DataFrame = {
    val metastoreReader = metastore.getMetastoreReader(inputTables :+ outputTable, infoDateEnd)

    val reason = job.validate(metastoreReader, infoDateOutput, options)

    reason match {
      case Reason.Ready         => job.run(metastoreReader, infoDateOutput, options)
      case Reason.NotReady(msg) => throw new ReasonException(reason, msg)
      case Reason.Skip(msg)     => throw new ReasonException(reason, msg)
    }
  }
}


object TransformerJob extends JobFactory[TransformerJob] {
  val PREFIX = "pramen.transformer"

  override def apply(conf: Config, spark: SparkSession): TransformerJob = {
    val metastore = AppContextFactory.get.metastore
    val jobContext = TransformerContext.fromConfig(conf, PREFIX)

    val transformer: Transformer = ClassLoaderUtils.loadConfigurableClass[Transformer](jobContext.factoryClass, conf)

    validateTables(metastore, jobContext.inputTables :+ jobContext.outputTable)

    new TransformerJob(transformer,
      metastore,
      jobContext.jobName,
      jobContext.schedule,
      jobContext.inputTables,
      jobContext.outputTable,
      jobContext.outputInfoDateExpression,
      jobContext.options)(spark, conf)
  }

  def validateTables(metastore: Metastore, tables: Seq[String]): Unit = {
    val registeredTables = metastore.getRegisteredTables.toSet
    val notRegistered = tables.filter(tbl => !registeredTables.contains(tbl))

    if (notRegistered.nonEmpty) {
      throw new IllegalArgumentException(s"Tables not registered in the metastore: ${notRegistered.mkString(", ")}")
    }
  }
}
