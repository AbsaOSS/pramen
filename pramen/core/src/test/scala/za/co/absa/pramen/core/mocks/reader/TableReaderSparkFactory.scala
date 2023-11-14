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

package za.co.absa.pramen.core.mocks.reader

import org.apache.spark.sql.SparkSession
import za.co.absa.pramen.core.reader.TableReaderSpark

object TableReaderSparkFactory {
  def getDummyReader(formatOpt: Option[String] = None,
                     schemaOpt: Option[String] = None,
                     hasInfoDateColumn: Boolean = false,
                     infoDateColumn: String = "info_date",
                     infoDateFormat: String = "yyyy-MM-dd",
                     options: Map[String, String] = Map.empty[String, String]
                    )(implicit spark: SparkSession): TableReaderSpark = {
    new TableReaderSpark(formatOpt, schemaOpt, hasInfoDateColumn, infoDateColumn, infoDateFormat, options)
  }
}
