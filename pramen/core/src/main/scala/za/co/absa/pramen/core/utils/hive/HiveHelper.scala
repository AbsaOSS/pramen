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

package za.co.absa.pramen.core.utils.hive

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory
import za.co.absa.pramen.core.metastore.model.{HiveApi, HiveConfig}
import za.co.absa.pramen.core.reader.JdbcUrlSelector

import java.sql.SQLException

abstract class HiveHelper {
  /**
    * Creates a Hive table pointing to the data stored at the specified location (base path).
    *
    * The table is defined using the provided schema, storage format and partitioning columns.
    * If the table is partitioned and the format requires it (for example, Parquet), the table
    * partitions can be automatically repaired after creation so that existing partition
    * directories are registered in the metastore.
    *
    * If the table already exists an exception will be thrown, usually `SQLException`, but depends on implementation.
    *
    * @param path                 The location of the data files the Hive table should point to (base path).
    * @param format               The storage format of the data (only Parquet is supported at the moment).
    * @param schema               The Spark schema of the data used to define the table columns.
    * @param partitionBy          The list of columns the table is partitioned by. An empty list means a non-partitioned table.
    * @param databaseName         An optional database name in which the table should be created. If not specified, the current database is used.
    * @param tableName            The name of the Hive table to create.
    * @param autoRepairPartitions If true, partitions are repaired (discovered and registered) after the table is created when the format requires it.
    * @throws SQLException        if there is an issue with the query that does the creation or update
    */
  @throws[SQLException]
  def createHiveTable(path: String,
                      format: HiveFormat,
                      schema: StructType,
                      partitionBy: Seq[String],
                      databaseName: Option[String],
                      tableName: String,
                      autoRepairPartitions: Boolean = true): Unit

  /**
    * Creates a Hive table pointing to the data stored at the specified location (base path).
    *
    * The table is defined using the provided schema, storage format and partitioning columns.
    * If the table is partitioned and the format requires it (for example, Parquet), the table
    * partitions can be automatically repaired after creation so that existing partition
    * directories are registered in the metastore.
    *
    * If the table already exists, no exceptions will be thrown. Partitions can be repaired on request for existing tables.
    *
    * @param path                 The location of the data files the Hive table should point to (base path).
    * @param format               The storage format of the data (only Parquet is supported at the moment).
    * @param schema               The Spark schema of the data used to define the table columns.
    * @param partitionBy          The list of columns the table is partitioned by. An empty list means a non-partitioned table.
    * @param databaseName         An optional database name in which the table should be created. If not specified, the current database is used.
    * @param tableName            The name of the Hive table to create.
    * @param autoRepairPartitions If true, partitions are repaired (discovered and registered) after the table is created when the format requires it.
    * @throws SQLException        if there is an issue with the query that does the creation or update
    */
  @throws[SQLException]
  def createOrUpdateHiveTable(path: String,
                              format: HiveFormat,
                              schema: StructType,
                              partitionBy: Seq[String],
                              databaseName: Option[String],
                              tableName: String,
                              autoRepairPartitions: Boolean = true): Unit

  /**
    * Replaces the schema of an existing Hive table with the provided Spark schema.
    *
    * The column definitions of the table are updated to match the given schema, while the
    * partitioning columns are preserved and excluded from the regular column list. This is
    * useful when the structure of the underlying data has changed (for example, columns were
    * added, removed or their types were modified) and the Hive table metadata needs to reflect
    * the new structure without recreating the table.
    *
    * It is up to the client to ensure new schema is read-compatible with data already exist in the table.
    * If not conformed to Hive+Parquet schema on read compatibility matrix, Hive clients can experience
    * casting or serialization errors.
    *
    * The table is expected to exist. If it does not, an exception will be thrown, usually
    * `SQLException`, but this depends on the implementation.
    *
    * @param schema       The new Spark schema of the data used to redefine the table columns.
    * @param partitionBy  The list of columns the table is partitioned by. An empty list means a non-partitioned table.
    * @param databaseName An optional database name in which the table resides. If not specified, the current database is used.
    * @param tableName    The name of the Hive table whose schema should be replaced.
    * @throws SQLException   if there is an issue with the query that does the replacement
    */
  @throws[SQLException]
  def replaceHiveTableSchema(schema: StructType,
                             partitionBy: Seq[String],
                             databaseName: Option[String],
                             tableName: String): Unit

  /**
    * Replaces the schema of a specific partition of an existing Hive table with the provided Spark schema.
    *
    * The column definitions of the partition identified by the given partition values are updated
    * to match the given schema, while the partitioning columns are excluded from the regular column list.
    * The partition is associated with the specified location. This is useful when the structure of the
    * data in a particular partition has changed and the partition-level metadata needs to reflect the
    * new structure without recreating the table or the partition.
    *
    * Also this method must be called after `replaceHiveTableSchema()` in some Hive implementations, e.g. Hive 1.0.
    * Otherwise, the schema changed won't take effect.
    *
    * It is up to the client to ensure the new schema is read-compatible with the data already stored in
    * the partition. If it does not conform to the Hive+Parquet schema on read compatibility matrix, Hive
    * clients can experience casting or serialization errors.
    *
    * The table and the partition are expected to exist. If they do not, an exception will be thrown,
    * usually `SQLException`, but this depends on the implementation.
    *
    * @param schema          The new Spark schema of the data used to redefine the partition columns.
    * @param partitionBy     The list of columns the table is partitioned by.
    * @param partitionValues The values of the partitioning columns identifying the partition, in the same order as `partitionBy`.
    * @param databaseName    An optional database name in which the table resides. If not specified, the current database is used.
    * @param tableName       The name of the Hive table containing the partition whose schema should be replaced.
    * @param partitionPath   The path of the data files of the partition. Important! This is not the base path of the table, but
    *                        a path to the underlying partition: /base/bath/my_partitoin=2026-10-05
    * @throws SQLException   if there is an issue with the query that does the replacement
    */
  @throws[SQLException]
  def replaceHivePartitionSchema(schema: StructType,
                                 partitionBy: Seq[String],
                                 partitionValues: Seq[String],
                                 databaseName: Option[String],
                                 tableName: String,
                                 partitionPath: String): Unit

  /**
    * Repairs the metadata of a Hive table so that it matches the data actually
    * present in its storage location. This typically recovers partitions that
    * exist on the file system but are missing from the Hive metastore (for
    * example, by issuing an `MSCK REPAIR TABLE` statement or an equivalent
    * operation suited to the given table format).
    *
    * @param databaseName the optional name of the database containing the table;
    *                     if `None`, the current or default database is used
    * @param tableName    the name of the Hive table to repair
    * @param format       the storage format of the Hive table, which may determine
    *                     how the repair operation is performed
    * @throws SQLException if a database access error occurs or the repair
    *                      statement fails to execute
    */
  @throws[SQLException]
  def repairHiveTable(databaseName: Option[String],
                      tableName: String,
                      format: HiveFormat): Unit

  /**
    * Adds a new partition to the specified table, using the given partition columns and their
    * corresponding values, with partition data stored at the provided location.
    *
    * @param databaseName    optional name of the database containing the table; if `None`,
    *                        the current or default database is used
    * @param tableName       name of the table to which the partition is added
    * @param partitionBy     names of the partition columns, in the order they are defined for the table
    * @param partitionValues values for each partition column, matching the order of `partitionBy`
    * @param location        storage path where the data for the new partition resides
    * @throws SQLException if a database access error occurs or the partition cannot be added
    */
  @throws[SQLException]
  def addPartition(databaseName: Option[String],
                   tableName: String,
                   partitionBy: Seq[String],
                   partitionValues: Seq[String],
                   location: String): Unit

  /**
    * Checks whether a table with the given name exists, optionally within a specific database.
    *
    * If a database name is provided, the lookup is restricted to that database. Otherwise, the
    * table is looked up in the current or default database of the connection.
    *
    * @param databaseName an optional name of the database in which to look for the table;
    *                     if `None`, the current or default database is used
    * @param tableName    the name of the table whose existence is to be checked
    * @return `true` if the table exists, `false` otherwise
    * @throws java.sql.SQLException if a database access error occurs while checking for the table
    */
  @throws[SQLException]
  def doesTableExist(databaseName: Option[String],
                     tableName: String): Boolean

  /**
    * Drops the specified table from the given database. If no database name is provided,
    * the table is resolved against the current or default database of the connection.
    *
    * @param databaseName optional name of the database containing the table to drop;
    *                     if `None`, the current or default database is used
    * @param tableName    name of the table to drop
    * @return `Unit`, as this method is executed only for its side effect of removing the table
    * @throws SQLException if a database access error occurs, the table does not exist,
    *                      or the drop operation fails
    */
  @throws[SQLException]
  def dropTable(databaseName: Option[String],
                tableName: String): Unit
}

object HiveHelper {
  private val log = LoggerFactory.getLogger(this.getClass)

  def fromHiveConfig(hiveConfig: HiveConfig)
                    (implicit spark: SparkSession): HiveHelper = {
    hiveConfig.hiveApi match {
      case HiveApi.Sql          =>
        val queryExecutor = hiveConfig.jdbcConfig match {
          case Some(jdbcConfig) =>
            log.info(s"Using Hive SQL API by connecting to the Hive metastore via JDBC.")
            new QueryExecutorJdbc(JdbcUrlSelector(jdbcConfig), hiveConfig.existenceCheckStrategy)
          case None             =>
            log.info(s"Using Hive SQL API by connecting to the Hive metastore via Spark.")
            new QueryExecutorSpark()
        }
        new HiveHelperSql(queryExecutor, hiveConfig.templates, hiveConfig.alwaysEscapeColumnNames)
      case HiveApi.SparkCatalog =>
        log.info(s"Using Hive via Spark Catalog API and configuration.")
        new HiveHelperSparkCatalog(spark)
    }
  }

  def fromQueryExecutor(api: HiveApi,
                        templates: HiveQueryTemplates,
                        queryExecutor: QueryExecutor)
                       (implicit spark: SparkSession): HiveHelper = {
    api match {
      case HiveApi.Sql => new HiveHelperSql(queryExecutor, templates, true)
      case _ => new HiveHelperSparkCatalog(spark)
    }
  }

  def getFullTable(databaseName: Option[String],
                   tableName: String): String = {

    databaseName match {
      case Some(dbName) => s"`$dbName`.`$tableName`"
      case None         => tableName
    }
  }
}
