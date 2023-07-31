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

package za.co.absa.pramen.core.mocks.transformer

import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.{MetastoreReader, Reason, Transformer}

import java.time.LocalDate

class FormatConversionTransformer extends Transformer {
  private val log = LoggerFactory.getLogger(this.getClass)

  override def validate(metastore: MetastoreReader, infoDate: LocalDate, options: Map[String, String]): Reason = {
    val table = options("input_table")

    val filesDf = metastore.getTable(table)

    if (filesDf.schema.fields.head.name != "path") {
      throw new IllegalArgumentException(s"Table $table should be in 'raw' format so the for each file the metastore returns a file path.")
    }

    Reason.Ready
  }

  override def run(metastore: MetastoreReader, infoDate: LocalDate, options: Map[String, String]): DataFrame = {
    val table = options("input_table")
    val inputFormat = options("input_format")
    val useDf = options.get("use_dataframe").exists(_.toBoolean)

    val filesDf = metastore.getTable(table)
    val spark = filesDf.sparkSession

    if (useDf) {
      log.info("Using the input dataframe to get the list of files to load")
      val files = filesDf.collect().map(_.getString(0))

      spark.read
        .format(inputFormat)
        .options(options)
        .load(files: _*)
    } else {
      log.info("Using the extra options field to get the list of files to load")
      val basePath = options(s"$table.path")
      val path = s"$basePath/pramen_info_date=${infoDate.toString}"

      spark.read
        .format(inputFormat)
        .options(options)
        .load(path)
    }
  }
}
