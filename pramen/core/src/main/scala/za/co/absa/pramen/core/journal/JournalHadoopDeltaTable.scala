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

package za.co.absa.pramen.core.journal

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import za.co.absa.pramen.core.journal.model.TaskCompleted

import java.time.Instant

class JournalHadoopDeltaTable(database: Option[String],
                              tablePrefix: String)
                             (implicit spark: SparkSession) extends Journal {
  import JournalHadoopDeltaTable._
  import spark.implicits._

  override def addEntry(entry: TaskCompleted): Unit = {
    val recordDf = Seq(entry).toDS().toDF()

    if (spark.version.split('.').head.toInt < 3) {
      throw new IllegalArgumentException("Delta Lake for bookkeeping is only available in Spark 3+")
    }

    recordDf
      .write
      .mode(SaveMode.Append)
      .option("format", "delta")
      .option("mergeSchema", "true")
      .saveAsTable(getFullTableName(database, tablePrefix))
  }

  override def getEntries(from: Instant, to: Instant): Seq[TaskCompleted] = {
    import spark.implicits._

    if (!spark.catalog.tableExists(getFullTableName(database, tablePrefix))) {
      return Seq.empty[TaskCompleted]
    }

    val df = spark
      .table(getFullTableName(database, tablePrefix))

    df.filter(col("finishedAt") >= from.getEpochSecond && col("finishedAt") <= to.getEpochSecond)
      .orderBy(col("finishedAt"))
      .as[TaskCompleted]
      .collect()
  }
}

object JournalHadoopDeltaTable {
  def getFullTableName(databaseOpt: Option[String], tablePrefix: String): String = {
    databaseOpt match {
      case Some(db) => s"$db.${tablePrefix}journal"
      case None     => s"${tablePrefix}journal"
    }
  }
}