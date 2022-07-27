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

package za.co.absa.pramen.core.model

import org.apache.spark.sql.types.StructType
import za.co.absa.pramen.core.utils.SparkUtils

import java.time.LocalDate
import scala.util.Try

case class TableSchema(tableName: String,
                       infoDate: String, /* Use String to workaround serialization issues */
                       schemaJson: String
                      )

object TableSchema {
  def toSchemaAndDate(tableSchema: TableSchema): Option[(StructType, LocalDate)] = {
    SparkUtils.schemaFromJson(tableSchema.schemaJson)
      .map(schema => (schema, Try {
        LocalDate.parse(tableSchema.infoDate)
      }.getOrElse(LocalDate.of(1970, 1, 1))))
  }
}



