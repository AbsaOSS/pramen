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

package za.co.absa.pramen.core.metastore.model

import com.typesafe.config.Config
import za.co.absa.pramen.core.reader.model.JdbcConfig
import za.co.absa.pramen.core.utils.ConfigUtils
import za.co.absa.pramen.core.utils.hive.HiveQueryTemplates

/**
  * Hive configuration for Pramen.
  *
  * It works like this. The pipeline can define a global Hive configuration and query templates like this:
  * {{{
  * pramen {
  *   spark.conf = {
  *     hive.metastore.uris = "thrift://host1:9083,thrift://host2:9083"
  *     spark.sql.warehouse.dir = "/hive/warehouse"
  *   }
  *
  *   hive {
  *     database = "my_db"
  *
  *     escape.column.names = true
  *     ignore.failures = false
  *
  *     # Optional, use only if you want to use JDBC rather than Spark metastore to query Hive
  *     hive.jdbc {
  *       driver = "com.cloudera.hive.jdbc41.HS2Driver"
  *       url = "jdbc:hive2://myhivehost:10000/"
  *       user = "..."
  *       password = "..."
  *     }
  *
  *     # Optional, use only if you want to override default templates
  *     conf.parquet = {
  *        create.table.template = "..."
  *        repair.table.template = "..."
  *        drop.table.template = "..."
  *     }
  *     conf.delta = {
  *        create.table.template = "..."
  *        repair.table.template = "..."
  *        drop.table.template = "..."
  *     }
  *   }
  * }
  * }}}
  *
  * Each meta table can then define its own Hive configuration and query templates like this:
  * {{{
  * pramen.metastore {
  *   tables = [
  *      {
  *         name = my_table
  *         format = parquet
  *         path = /a/b/c
  *         hive.table = my_hive_table
  *         hive.database = my_hive_db
  *
  *         # Override the table creation query for this table
  *         hive.conf.create.table.template = "..."
  *
  *         # Override Hive JDBC for this table
  *         hive.jdbc {
  *
  *         }
  *      }
  *   ]
  * }
  * }}}
  *
  * @param hiveApi             The Hive API to use (SQL or Spark Catalog).
  * @param database            The database database to use. If omitted, you can use full table names for each table.
  * @param templates           Query templates for each output format
  * @param jdbcConfig          Hive JDBC configuration to use instead of Spark metastore if needed
  * @param ignoreFailures      Whether to ignore errors when creating or repairing tables. If true, only warnings will be emitted on Hive errors.
  * @param optimizeExistQuery  If true, Pramen uses Hive-specific SQL dialect to check table existence to ensure data won't be touched.
  */
case class HiveDefaultConfig(
                              hiveApi: HiveApi,
                              database: Option[String],
                              templates: Map[String, HiveQueryTemplates],
                              jdbcConfig: Option[JdbcConfig],
                              ignoreFailures: Boolean,
                              alwaysEscapeColumnNames: Boolean,
                              optimizeExistQuery: Boolean
                            )

object HiveDefaultConfig {
  val HIVE_CONFIG_JDBC_PREFIX = "jdbc"
  val HIVE_TEMPLATE_CONFIG_PREFIX = "conf"

  val HIVE_API_KEY = "api"
  val HIVE_IGNORE_FAILURES_KEY = "ignore.failures"
  val HIVE_ALWAYS_ESCAPE_COLUMN_NAMES = "escape.column.names"
  val HIVE_DATABASE_KEY = "database"
  val HIVE_OPTIMIZE_EXIST_QUERY_KEY = "optimize.exist.query"

  /**
    * Gets global Hive configuration and query templates for all supported formats.
    *
    * Currently this config goes under 'pramen.hive', but can be used for any other prefix.
    *
    * @param conf   The hive configuration section (usually under 'pramen.hive' prefix)
    * @param parent The parent path of the hive configuration section (for including full config path in exceptions)
    * @return
    */
  def fromConfig(conf: Config, parent: String = ""): HiveDefaultConfig = {
    val hiveApi = if (conf.hasPath(HIVE_DATABASE_KEY)) HiveApi.fromString(conf.getString(HIVE_API_KEY)) else HiveApi.Sql
    val database = if (conf.hasPath(HIVE_DATABASE_KEY)) Some(conf.getString(HIVE_DATABASE_KEY)) else None
    val ignoreFailures = ConfigUtils.getOptionBoolean(conf, HIVE_IGNORE_FAILURES_KEY).getOrElse(false)
    val alwaysEscapeColumnNames = ConfigUtils.getOptionBoolean(conf, HIVE_ALWAYS_ESCAPE_COLUMN_NAMES).getOrElse(true)
    val hiveOptimizeExistQuery = ConfigUtils.getOptionBoolean(conf, HIVE_OPTIMIZE_EXIST_QUERY_KEY).getOrElse(true)

    val jdbcConfig = if (conf.hasPath(HIVE_CONFIG_JDBC_PREFIX))
      Option(JdbcConfig.load(conf, parent))
    else
      None

    val formats = List("parquet", "delta")

    val templates = formats.map(format => {
      val prefix = s"$HIVE_TEMPLATE_CONFIG_PREFIX.$format"

      (format, HiveQueryTemplates.fromConfig(ConfigUtils.getOptionConfig(conf, prefix)))
    }).toMap

    HiveDefaultConfig(hiveApi, database, templates, jdbcConfig, ignoreFailures, alwaysEscapeColumnNames, hiveOptimizeExistQuery)
  }

  def getNullConfig: HiveDefaultConfig = HiveDefaultConfig(
    HiveApi.Sql,
    None,
    Map(),
    None,
    ignoreFailures = false,
    alwaysEscapeColumnNames = true,
    optimizeExistQuery = true
  )
}
