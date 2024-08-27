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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import za.co.absa.pramen.core.journal.model.TaskCompleted
import za.co.absa.pramen.core.utils.FsUtils

import java.time.Instant

class JournalHadoopDeltaPath(journalPath: String)
                            (implicit spark: SparkSession) extends Journal {
  import spark.implicits._

  private val hadoopConfig: Configuration = spark.sparkContext.hadoopConfiguration
  private val fsUtils = new FsUtils(hadoopConfig, journalPath)

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
      .save(journalPath)
  }

  override def getEntries(from: Instant, to: Instant): Seq[TaskCompleted] = {
    import spark.implicits._

    if (!fsUtils.exists(new Path(journalPath))) {
      return Seq.empty[TaskCompleted]
    }

    val df = spark
      .read
      .option("format", "delta")
      .load(journalPath)

    df.filter(col("finishedAt") >= from.getEpochSecond && col("finishedAt") <= to.getEpochSecond)
      .orderBy(col("finishedAt"))
      .as[TaskCompleted]
      .collect()
  }
}
