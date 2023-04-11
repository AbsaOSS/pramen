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
import za.co.absa.pramen.core.utils.hive.HiveQueryTemplates._

/**
  * Hive configuration for Pramen.
  *
  * It works like this. The pipeline can define a global Hive configuration and query templates like this:
  * {{{
  * pramen {
  *   spark.conf.option = {
  *     hive.metastore.uris = "thrift://host1:9083,thrift://host2:9083"
  *     spark.sql.warehouse.dir = "/hive/warehouse"
  *   }
  *
  *   hive {
  *     database = "my_db"
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
  * @param database    The database database to use. If omitted, you can use full table names for each table.
  * @param templates   Query templates for each output format
  * @param jdbcConfig  Hive JDBC configuration to use instead of Spark metastore if needed
  */
case class HiveConfig(
                       database: Option[String],
                       templates: Map[String, HiveQueryTemplates],
                       jdbcConfig: Option[JdbcConfig]
                     )

object HiveConfig {
  val HIVE_CONFIG_JDBC_PREFIX = "jdbc"
  val HIVE_TEMPLATE_CONFIG_PREFIX = "conf"

  /**
    * Gets global Hive configuration and query templates for all supported formats.
    *
    * Currently this config goes under 'pramen.hive', but can be used for any other prefix.
    *
    * @param conf   The hive configuration section (usually under 'pramen.hive' prefix)
    * @param parent The parent path of the hive configuration section (for including full config path in exceptions)
    * @return
    */
  def fromConfig(conf: Config, parent: String = ""): HiveConfig = {
    val database = if (conf.hasPath("database")) Some(conf.getString("database")) else None

    val jdbcConfig = if (conf.hasPath(HIVE_CONFIG_JDBC_PREFIX))
      Option(JdbcConfig.load(conf, parent))
    else
      None

    val formats = List("parquet", "delta")

    val templates = formats.map(format => {
      val prefix = s"$HIVE_TEMPLATE_CONFIG_PREFIX.$format"

      (format, HiveQueryTemplates.fromConfig(ConfigUtils.getOptionConfig(conf, prefix)))
    }).toMap

    HiveConfig(database, templates, jdbcConfig)
  }

  /**
    * This method is needed to allow easily specify default Hive query templates for each suppported format, while
    * also allowing custom templates to be used for a metastore table.
    *
    * @param conf     The hive configuration section (usually under 'hive.' prefix)
    * @param defaults Global Pramen defaults for Hive Configuration
    * @param format   The format of the table ('parquet', 'delta' at th moment)
    * @param parent   The parent path of the hive configuration section (for including full config path in exceptions)
    * @return
    */
  def fromConfigWithDefaults(conf: Config, defaults: HiveConfig, format: DataFormat, parent: String = ""): HiveConfig = {
    val hiveOverride = fromConfig(conf, parent)

    val defaultTemplates = defaults.templates.getOrElse(format.name, HiveQueryTemplates(
      DEFAULT_CREATE_TABLE_TEMPLATE,
      DEFAULT_REPAIR_TABLE_TEMPLATE,
      DEFAULT_DROP_TABLE_TEMPLATE
    ))

    val createTableTemplate = ConfigUtils.getOptionString(conf, s"$HIVE_TEMPLATE_CONFIG_PREFIX.$CREATE_TABLE_TEMPLATE_KEY")
      .getOrElse(defaultTemplates.createTableTemplate)

    val repairTableTemplate = ConfigUtils.getOptionString(conf, s"$HIVE_TEMPLATE_CONFIG_PREFIX.$REPAIR_TABLE_TEMPLATE_KEY")
      .getOrElse(defaultTemplates.repairTableTemplate)

    val dropTableTemplate = ConfigUtils.getOptionString(conf, s"$HIVE_TEMPLATE_CONFIG_PREFIX.$DROP_TABLE_TEMPLATE_KEY")
      .getOrElse(defaultTemplates.dropTableTemplate)

    HiveConfig(
      database = hiveOverride.database.orElse(defaults.database),
      templates = defaults.templates + (format.name -> HiveQueryTemplates(createTableTemplate, repairTableTemplate, dropTableTemplate)),
      jdbcConfig = hiveOverride.jdbcConfig.orElse(defaults.jdbcConfig)
    )
  }

  def getNullConfig: HiveConfig = HiveConfig(None, Map(), None)
}
