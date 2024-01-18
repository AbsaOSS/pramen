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
import za.co.absa.pramen.api.{MetastoreReader, Reason, Transformer}
import za.co.absa.pramen.core.transformers.IdentityTransformer.{INPUT_TABLE_KEY, INPUT_TABLE_LEGACY_KEY}

import java.time.LocalDate

/**
  * This transformer does not have dependencies, but always generates some data.
  */
class RangedTransformer extends Transformer {
  override def validate(metastore: MetastoreReader,
                        infoDate: LocalDate,
                        options: Map[String, String]): Reason = {
    Reason.Ready
  }

  override def run(metastore: MetastoreReader, infoDate: LocalDate, options: Map[String, String]): DataFrame = {
    val tableName = options.getOrElse(INPUT_TABLE_KEY, options(INPUT_TABLE_LEGACY_KEY))

    val useLatest = options.getOrElse("use.latest", "false").toBoolean

    if (useLatest) {
      metastore.getLatest(tableName)
    } else {
      metastore.getTable(tableName, Option(infoDate.minusDays(6)), Option(infoDate))
    }
  }
}
