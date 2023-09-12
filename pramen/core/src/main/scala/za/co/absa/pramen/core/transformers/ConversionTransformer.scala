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

package za.co.absa.pramen.core.transformers

import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.{DataFormat, MetastoreReader, Reason, Transformer}
import za.co.absa.pramen.core.utils.SparkUtils.getPartitionPath

import java.time.LocalDate

/**
  * This transformer allows converting data from a metastore table having 'raw' format to a DataFrame,
  * and consequently to a new table in th metastore having wither 'parquet' or 'delta' format.
  *
  * This transformer is useful mainly as an example of implementing file-based ingestion use cases,
  * when these files can be converted to one of formats that the metastore supports by returning a DataFrame.
  *
  * Example usage:
  * {{{
  *   pramen.sources = [
  *     {
  *       name = "file_source"
  *       factory.class = "za.co.absa.pramen.core.source.RawFileSource"
  *     }
  *   ]
  *
  *   pramen.metastore {
  *     tables = [
  *       {
  *         name = "table1"
  *         description = "Table 1 (file based)"
  *         format = "raw"
  *         path = /bigdata/raw/table1
  *       },
  *       {
  *         name = "table2"
  *         description = "Table 2 (parquet)"
  *         format = "delta"
  *         path = /bigdata/lakehouse/table2
  *       }
  *     ]
  *   }
  *
  *   pramen.operations = [
  *     {
  *       name = "Sourcing from a folder"
  *       type = "ingestion"
  *       schedule.type = "daily"
  *
  *       source = "file_source"
  *
  *       info.date.expr = "@runDate"
  *
  *       tables = [
  *         {
  *           input.path = /bigdata/landing
  *           output.metastore.table = table1
  *         }
  *       ]
  *     },
  *     {
  *       name = "Converting to Delta Lake"
  *       type = "transformation"
  *
  *       class = "za.co.absa.pramen.core.transformers.ConversionTransformer"
  *       schedule.type = "daily"
  *
  *       output.table = "table2"
  *
  *       dependencies = [
  *         {
  *           tables = [ table1 ]
  *           date.from = "@infoDate"
  *           optional = true # Since no bookkeeping available the table will be seen as empty for the dependency manager
  *         }
  *       ]
  *
  *       option {
  *         input.table = "table1"
  *         input.format = "csv"
  *         use.file.list = true
  *
  *         header = true
  *       }
  *     }
  *   ]
  * }}}
  */
class ConversionTransformer extends Transformer {
  import ConversionTransformer._

  private val log = LoggerFactory.getLogger(this.getClass)

  private val allKeys = Seq(INPUT_TABLE_KEY, INPUT_FORMAT_KEY, USE_FILE_LIST_KEY)
  private val mandatoryKeys = Seq(INPUT_TABLE_KEY, INPUT_FORMAT_KEY)

  override def validate(metastore: MetastoreReader, infoDate: LocalDate, options: Map[String, String]): Reason = {
    val missingKeys = mandatoryKeys.filter(!options.contains(_))
    if (missingKeys.nonEmpty) {
      throw new IllegalArgumentException(s"Missing mandatory keys for ConversionTransformer: ${missingKeys.mkString(", ")}")
    }

    val inputTable = options(INPUT_TABLE_KEY)

    val filesDf = metastore.getTable(inputTable, Option(infoDate), Option(infoDate))

    if (!metastore.getTableDef(inputTable).format.isInstanceOf[DataFormat.Raw]) {
      throw new IllegalArgumentException(s"Table $inputTable should be in 'raw' format so the for each file the metastore returns a file path.")
    }

    if (filesDf.isEmpty) {
      Reason.SkipOnce(s"Table $inputTable has no data for $infoDate")
    } else {
      Reason.Ready
    }
  }

  override def run(metastore: MetastoreReader, infoDate: LocalDate, options: Map[String, String]): DataFrame = {
    val inputTable = options(INPUT_TABLE_KEY)
    val inputFormat = options(INPUT_FORMAT_KEY)
    val useListOfFiles = options.get(USE_FILE_LIST_KEY).exists(_.toBoolean)

    val filesDf = metastore.getTable(inputTable)
    val spark = filesDf.sparkSession

    val formatOptions = options
      .filter {
        case (k, _) => !allKeys.contains(k)
      }

    if (useListOfFiles) {
      log.info("Using the input dataframe to get the list of files to load")
      val files = filesDf.collect().map(_.getString(0))

      spark.read
        .format(inputFormat)
        .options(formatOptions)
        .load(files: _*)
    } else {
      log.info("Using table's metadata to get the list of files to load")

      val metaTable = metastore.getTableDef(inputTable)
      val path = metaTable.format.asInstanceOf[DataFormat.Raw].path
      val partitionPath = getPartitionPath(infoDate, metaTable.infoDateColumn, metaTable.infoDateFormat, path).toString

      spark.read
        .format(inputFormat)
        .options(formatOptions)
        .load(partitionPath)
    }
  }
}

object ConversionTransformer {
  val INPUT_TABLE_KEY = "input.table"
  val INPUT_FORMAT_KEY = "input.format"
  val USE_FILE_LIST_KEY = "use.file.list"
}
