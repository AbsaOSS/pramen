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
import za.co.absa.pramen.api.DataFormat
import za.co.absa.pramen.core.metastore.model.HiveDefaultConfig._
import za.co.absa.pramen.core.reader.model.JdbcConfig
import za.co.absa.pramen.core.utils.ConfigUtils
import za.co.absa.pramen.core.utils.hive.HiveQueryTemplates
import za.co.absa.pramen.core.utils.hive.HiveQueryTemplates._

/**
  * Hive configuration for Pramen.
  *
  * The default configuration is defined in [[za.co.absa.pramen.core.metastore.model.HiveDefaultConfig]].
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
  * @param hiveApi                 The Hive API to use (SQL or Spark Catalog).
  * @param database                T he database database to use. If omitted, you can use full table names for each table.
  * @param templates               Query templates for generating Hive queries.
  * @param jdbcConfig              Hive JDBC configuration to use instead of Spark metastore if needed
  * @param ignoreFailures          Whether to ignore errors when creating or repairing tables. If true, only warnings will be emitted on Hive errors.
  * @param alwaysEscapeColumnNames If true, column names are always escaped when executing SQL against Hive.
  * @param optimizeExistQuery      If true, Pramen uses Hive-specific SQL dialect to check table existence to ensure data won't be touched.
  */
case class HiveConfig(
                       hiveApi: HiveApi,
                       database: Option[String],
                       templates: HiveQueryTemplates,
                       jdbcConfig: Option[JdbcConfig],
                       ignoreFailures: Boolean,
                       alwaysEscapeColumnNames: Boolean,
                       optimizeExistQuery: Boolean
                     )

object HiveConfig {
  /**
    * This method is needed to allow easily specify default Hive query templates for each supported format, while
    * also allowing custom templates to be used for a metastore table.
    *
    * @param conf     The hive configuration section (usually under 'hive.' prefix)
    * @param defaults Global Pramen defaults for Hive Configuration
    * @param format   The format of the table ('parquet', 'delta' at th moment)
    * @param parent   The parent path of the hive configuration section (for including full config path in exceptions)
    * @return
    */
  def fromConfigWithDefaults(conf: Config, defaults: HiveDefaultConfig, format: DataFormat, parent: String = ""): HiveConfig = {
    val defaultTemplates = defaults.templates.getOrElse(format.name, HiveQueryTemplates(
      DEFAULT_CREATE_TABLE_TEMPLATE,
      DEFAULT_REPAIR_TABLE_TEMPLATE,
      DEFAULT_ADD_PARTITION_TEMPLATE,
      DEFAULT_DROP_TABLE_TEMPLATE
    ))

    val hiveApi = if (conf.hasPath(HIVE_API_KEY))
      HiveApi.fromString(conf.getString(HIVE_API_KEY))
    else
      defaults.hiveApi

    val database = ConfigUtils.getOptionString(conf, HIVE_DATABASE_KEY).orElse(defaults.database)
    val ignoreFailures = ConfigUtils.getOptionBoolean(conf, HIVE_IGNORE_FAILURES_KEY).getOrElse(defaults.ignoreFailures)
    val alwaysEscapeColumnNames = ConfigUtils.getOptionBoolean(conf, HIVE_ALWAYS_ESCAPE_COLUMN_NAMES).getOrElse(defaults.alwaysEscapeColumnNames)

    val jdbcConfig = if (conf.hasPath(HIVE_CONFIG_JDBC_PREFIX))
      Option(JdbcConfig.load(conf, parent))
    else
      defaults.jdbcConfig


    val createTableTemplate = ConfigUtils.getOptionString(conf, s"$HIVE_TEMPLATE_CONFIG_PREFIX.$CREATE_TABLE_TEMPLATE_KEY")
      .getOrElse(defaultTemplates.createTableTemplate)

    val repairTableTemplate = ConfigUtils.getOptionString(conf, s"$HIVE_TEMPLATE_CONFIG_PREFIX.$REPAIR_TABLE_TEMPLATE_KEY")
      .getOrElse(defaultTemplates.repairTableTemplate)

    val addPartitionTableTemplate = ConfigUtils.getOptionString(conf, s"$HIVE_TEMPLATE_CONFIG_PREFIX.$ADD_PARTITION_TEMPLATE_KEY")
      .getOrElse(defaultTemplates.addPartitionTemplate)

    val dropTableTemplate = ConfigUtils.getOptionString(conf, s"$HIVE_TEMPLATE_CONFIG_PREFIX.$DROP_TABLE_TEMPLATE_KEY")
      .getOrElse(defaultTemplates.dropTableTemplate)

    val hiveOptimizeExistQuery = ConfigUtils.getOptionBoolean(conf, s"$HIVE_TEMPLATE_CONFIG_PREFIX.$HIVE_OPTIMIZE_EXIST_QUERY_KEY")
      .getOrElse(defaults.optimizeExistQuery)

    HiveConfig(
      hiveApi = hiveApi,
      database = database,
      templates = HiveQueryTemplates(createTableTemplate, repairTableTemplate, addPartitionTableTemplate, dropTableTemplate),
      jdbcConfig = jdbcConfig,
      ignoreFailures,
      alwaysEscapeColumnNames,
      hiveOptimizeExistQuery
    )
  }

  /**
    * Get default templates for the specified format.
    *
    * @param defaults Global Pramen defaults for Hive Configuration
    * @param format   The format of the table ('parquet', 'delta' at th moment)
    * @return Hive configuration with default query templates for the given format.
    */
  def fromDefaults(defaults: HiveDefaultConfig, format: DataFormat): HiveConfig = {
    val templates = defaults.templates.getOrElse(format.name, HiveQueryTemplates(
      DEFAULT_CREATE_TABLE_TEMPLATE,
      DEFAULT_REPAIR_TABLE_TEMPLATE,
      DEFAULT_ADD_PARTITION_TEMPLATE,
      DEFAULT_DROP_TABLE_TEMPLATE
    ))

    HiveConfig(defaults.hiveApi, defaults.database, templates, defaults.jdbcConfig, defaults.ignoreFailures, alwaysEscapeColumnNames = true, optimizeExistQuery = true)
  }

  def getNullConfig: HiveConfig = HiveConfig(
    HiveApi.Sql,
    None,
    HiveQueryTemplates(DEFAULT_CREATE_TABLE_TEMPLATE, DEFAULT_REPAIR_TABLE_TEMPLATE, DEFAULT_ADD_PARTITION_TEMPLATE, DEFAULT_DROP_TABLE_TEMPLATE),
    None,
    ignoreFailures = false,
    alwaysEscapeColumnNames = true,
    optimizeExistQuery = true)
}
