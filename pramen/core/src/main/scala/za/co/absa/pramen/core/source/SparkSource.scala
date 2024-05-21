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

package za.co.absa.pramen.core.source

import com.typesafe.config.Config
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api._
import za.co.absa.pramen.core.config.Keys.KEYS_TO_REDACT
import za.co.absa.pramen.core.reader.TableReaderSpark
import za.co.absa.pramen.core.reader.model.TableReaderJdbcConfig.{HAS_INFO_DATE, INFORMATION_DATE_COLUMN, getInfoDateFormat}
import za.co.absa.pramen.core.utils.{ConfigUtils, FsUtils}

import java.time.LocalDate

class SparkSource(val format: Option[String],
                  val schema: Option[String],
                  val hasInfoDateCol: Boolean,
                  val infoDateColumn: String,
                  val infoDateFormat: String,
                  val sourceConfig: Config,
                  val options: Map[String, String])(implicit spark: SparkSession) extends Source {
  private val log = LoggerFactory.getLogger(this.getClass)

  override val config: Config = sourceConfig

  override def hasInfoDateColumn(query: Query): Boolean = hasInfoDateCol

  override def getRecordCount(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate): Long = {
    val reader = getReader(query)

    reader.getRecordCount(query, infoDateBegin, infoDateEnd)
  }

  override def getData(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate, columns: Seq[String]): SourceResult = {
    val reader = getReader(query)

    val df = reader.getData(query, infoDateBegin, infoDateEnd, columns)

    val filesRead = query match {
      case _: Query.Table   => df.inputFiles
      case _: Query.Sql     => Array.empty[String]
      case Query.Path(path) =>
        val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, path)
        fsUtils.getHadoopFiles(new Path(path)).map(_.getPath.toString).sorted
      case other            => throw new IllegalArgumentException(s"'${other.name}' is not supported by the Spark source. Use 'path', 'table' or 'sql' instead.")
    }

    SourceResult(df, filesRead)
  }

  def getReader(query: Query): TableReader = {
    val tableReader = query match {
      case Query.Table(table) =>
        log.info(s"Using TableReaderSpark to read table: $table")
        new TableReaderSpark(format, schema, hasInfoDateCol, infoDateColumn, infoDateFormat, options)
      case Query.Sql(sql)     =>
        log.info(s"Using TableReaderSpark to read SQL for: $sql")
        new TableReaderSpark(format, schema, hasInfoDateCol, infoDateColumn, infoDateFormat, options)
      case Query.Path(path)   =>
        log.info(s"Using TableReaderSpark to read '$format' from: $path")
        new TableReaderSpark(format, schema, hasInfoDateCol, infoDateColumn, infoDateFormat, options)
      case other              => throw new IllegalArgumentException(s"'${other.name}' is not supported by the Spark source. Use 'path', 'table' or 'sql' instead.")
    }

    if (options.nonEmpty) {
      log.info(s"Options passed for '$format':")
      ConfigUtils.renderExtraOptions(options, KEYS_TO_REDACT)(s => log.info(s))
    }

    schema.foreach(s => log.info(s"Using schema: $s"))
    tableReader
  }
}

object SparkSource extends ExternalChannelFactory[SparkSource] {
  val FORMAT = "format"
  val SCHEMA = "schema"

  override def apply(conf: Config, parentPath: String, spark: SparkSession): SparkSource = {
    val format = ConfigUtils.getOptionString(conf, FORMAT)
    val schema = ConfigUtils.getOptionString(conf, SCHEMA)

    val hasInfoDate = conf.hasPath(HAS_INFO_DATE) && conf.getBoolean(HAS_INFO_DATE)
    val (infoDateColumn, infoDateFormat) = if (hasInfoDate) {
      (conf.getString(INFORMATION_DATE_COLUMN), getInfoDateFormat(conf))
    } else {
      ("", "")
    }

    val options = ConfigUtils.getExtraOptions(conf, "option")

    new SparkSource(format, schema, hasInfoDate, infoDateColumn, infoDateFormat, conf, options)(spark)
  }
}
