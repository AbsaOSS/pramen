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

package za.co.absa.pramen.core.metastore.peristence

import org.apache.spark.sql.DataFrame
import za.co.absa.pramen.core.metastore.peristence.MetastorePersistenceRaw.{RAW_COPY_FIELD_KEY, RAW_PATH_FIELD_KEY}

case class RawFile(filePath: String, needsCopying: Boolean)


object RawFile {

  /**
    * Converts a data frame of the 'raw' persistent format, which is just a list of files, to a Seq[RawFile].
    *
    * The data frame must contain the file path column. If the flag column determining whether a file
    * needs to be copied is present, its value is used, otherwise all returned files are marked as
    * requiring copying.
    *
    * @param df A data frame in the 'raw' persistent format containing at least the file path column.
    * @return A sequence of raw files collected from the data frame.
    * @throws IllegalArgumentException if the data frame does not contain the file path column.
    */
  def fromDf(df: DataFrame): Seq[RawFile] = {
    if (!df.schema.exists(_.name == RAW_PATH_FIELD_KEY)) {
      throw new IllegalArgumentException("The 'raw' persistent format data frame should have 'path' column.")
    }

    if (df.schema.exists(_.name == RAW_COPY_FIELD_KEY)) {
      df.select(RAW_PATH_FIELD_KEY, RAW_COPY_FIELD_KEY).collect().map(row => RawFile(row.getString(0), row.getBoolean(1)))
    } else {
      df.select(RAW_PATH_FIELD_KEY).collect().map(row => RawFile(row.getString(0), needsCopying = true))
    }
  }
}