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

package za.co.absa.pramen.core.utils

import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import za.co.absa.pramen.api.CatalogTable

object CatalogUtils {
  /** Checks if a catalog table exists. Supports Iceberg, Delta, Parquet tables. */
  def doesTableExist(fullTableName: String)(implicit spark: SparkSession): Boolean = {
    getExistingTable(fullTableName).isDefined
  }

  /** Checks if a catalog table exists. The table is a structured object. Supports Iceberg, Delta, Parquet tables. */
  def doesTableExist(catalogTable: CatalogTable)(implicit spark: SparkSession): Boolean = {
    getExistingTable(catalogTable).isDefined
  }

  /**
    * Fetches an existing table as a DataFrame based on the provided `CatalogTable`.
    * If the table is not found, returns `None`. If an unsupported catalog is used,
    * throws an `IllegalArgumentException`.
    *
    * Supports Iceberg, Delta, Parquet tables
    *
    * @param fullTableName The catalog table representing the metadata of the desired table.
    * @param spark         An implicit SparkSession instance used to interact with the table.
    * @return An `Option[DataFrame]` containing the table as a DataFrame if it exists,
    *         or `None` if the table is not found.
    */
  def getExistingTable(fullTableName: String)(implicit spark: SparkSession): Option[DataFrame] = {
    try {
      val df = spark.table(fullTableName)
      // Force analysis to surface TABLE_OR_VIEW_NOT_FOUND at this point.
      // Technically, not needed, but Spark can potentially skip analysis until the schema is requested.
      val _ = df.schema
      Some(df)
    } catch {
      // This is a common error
      case ex: AnalysisException if ex.getMessage().contains("Table or view not found") || ex.getMessage().contains("TABLE_OR_VIEW_NOT_FOUND") =>
        None
      // This is the exception, needs to be re-thrown. This happens when attempting to read Iceberg table when Spark catalog is not configured
      // properly.
      case ex: AnalysisException if ex.getMessage().contains("TableType cannot be null for table:") =>
        throw new IllegalArgumentException("Attempt to use a catalog not supported by the file format. " +
          "Ensure you are using the iceberg catalog and/or it is set as the default catalog with (spark.sql.defaultCatalog) " +
          "or the catalog is specified explicitly as the table name.", ex)
    }
  }

  /** Same as above, but uses a structured catalog table name. */
  def getExistingTable(catalogTable: CatalogTable)(implicit spark: SparkSession): Option[DataFrame] = {
    getExistingTable(catalogTable.getFullTableName)
  }
}
