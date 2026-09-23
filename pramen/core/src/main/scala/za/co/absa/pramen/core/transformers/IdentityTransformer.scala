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
import za.co.absa.pramen.api.{MetastoreReader, Reason, Transformer}
import za.co.absa.pramen.core.transformers.IdentityTransformer._

import java.time.LocalDate

/**
  * The transformer does not do any actual transformation and just returns the input DataFrame.
  *
  * It can be used to copy data between metastore tables located in different storages.
  *
  * The transformer supports incremental processing.
  *
  * Example usage:
  * {{{
  *   pramen.operations = [
  *     {
  *       name = "Copy table"
  *       type = "transformation"
  *
  *       class = "za.co.absa.pramen.core.transformers.IdentityTransformer"
  *       schedule.type = "daily"
  *
  *       dependencies = [
  *         {
  *           tables = [ table_from ]
  *           date.from = "@infoDate"
  *         }
  *       ]
  *
  *       option {
  *         input.table = "table_from"
  *         empty.allowed = true
  *       }
  *
  *       output.table = "table_to"
  *     }
  *   ]
  * }}}
  */
class IdentityTransformer extends Transformer {
  override def validate(metastore: MetastoreReader, infoDate: LocalDate, options: Map[String, String]): Reason = {
    if (!options.contains(INPUT_TABLE_KEY) && !options.contains(INPUT_TABLE_LEGACY_KEY)) {
      throw new IllegalArgumentException(s"Option '$INPUT_TABLE_KEY' is not defined")
    }

    val emptyAllowed = if (metastore.isIncremental)
      true
    else
      options.getOrElse(EMPTY_ALLOWED_KEY, "true").toBoolean

    val tableName = options.getOrElse(INPUT_TABLE_KEY, options(INPUT_TABLE_LEGACY_KEY))

    val df = metastore.getCurrentBatch(tableName)

    if (emptyAllowed || !df.isEmpty) {
      Reason.Ready
    } else {
      Reason.SkipOnce(s"No data for '$tableName' at $infoDate")
    }
  }

  override def run(metastore: MetastoreReader,
                   infoDate: LocalDate,
                   options: Map[String, String]): DataFrame = {
    val tableName = options.getOrElse(INPUT_TABLE_KEY, options(INPUT_TABLE_LEGACY_KEY))

    metastore.getCurrentBatch(tableName)
  }
}

object IdentityTransformer {
  val INPUT_TABLE_KEY = "input.table"
  val INPUT_TABLE_LEGACY_KEY = "table"
  val EMPTY_ALLOWED_KEY = "empty.allowed"
}
