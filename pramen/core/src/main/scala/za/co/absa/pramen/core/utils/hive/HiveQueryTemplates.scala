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

import com.typesafe.config.Config
import za.co.absa.pramen.core.utils.ConfigUtils

case class HiveQueryTemplates(
                               createTableTemplate: String,
                               repairTableTemplate: String,
                               addPartitionTemplate: String,
                               dropTableTemplate: String
                             )

object HiveQueryTemplates {
  val TEMPLATES_DEFAULT_PREFIX = "hive.conf"

  val CREATE_TABLE_TEMPLATE_KEY = "create.table.template"
  val REPAIR_TABLE_TEMPLATE_KEY = "repair.table.template"
  val ADD_PARTITION_TEMPLATE_KEY = "add.partition.template"
  val DROP_TABLE_TEMPLATE_KEY = "drop.table.template"

  val DEFAULT_CREATE_TABLE_TEMPLATE: String =
    """CREATE EXTERNAL TABLE IF NOT EXISTS
      |@fullTableName ( @schema )
      |@partitionedBy
      |ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
      |STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
      |OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
      |LOCATION '@path';""".stripMargin

  val DEFAULT_REPAIR_TABLE_TEMPLATE: String = "MSCK REPAIR TABLE @fullTableName"

  val DEFAULT_ADD_PARTITION_TEMPLATE: String =
    """ALTER TABLE @fullTableName ADD IF NOT EXISTS PARTITION (@partitionClause) LOCATION '@partitionPath';""".stripMargin

  val DEFAULT_DROP_TABLE_TEMPLATE: String = "DROP TABLE IF EXISTS @fullTableName"

  def fromConfig(conf: Config): HiveQueryTemplates = {
    val createTableTemplate = ConfigUtils.getOptionString(conf, CREATE_TABLE_TEMPLATE_KEY)
      .getOrElse(DEFAULT_CREATE_TABLE_TEMPLATE)

    val repairTableTemplate = ConfigUtils.getOptionString(conf, REPAIR_TABLE_TEMPLATE_KEY)
      .getOrElse(DEFAULT_REPAIR_TABLE_TEMPLATE)

    val addPartitionTemplate = ConfigUtils.getOptionString(conf, ADD_PARTITION_TEMPLATE_KEY)
      .getOrElse(DEFAULT_ADD_PARTITION_TEMPLATE)

    val dropTableTemplate = ConfigUtils.getOptionString(conf, DROP_TABLE_TEMPLATE_KEY)
      .getOrElse(DEFAULT_DROP_TABLE_TEMPLATE)

    HiveQueryTemplates(
      createTableTemplate = createTableTemplate,
      repairTableTemplate = repairTableTemplate,
      addPartitionTemplate = addPartitionTemplate,
      dropTableTemplate = dropTableTemplate
    )
  }

  def getDefaultQueryTemplates: HiveQueryTemplates = {
    HiveQueryTemplates(
      createTableTemplate = DEFAULT_CREATE_TABLE_TEMPLATE,
      repairTableTemplate = DEFAULT_REPAIR_TABLE_TEMPLATE,
      addPartitionTemplate = DEFAULT_ADD_PARTITION_TEMPLATE,
      dropTableTemplate = DEFAULT_DROP_TABLE_TEMPLATE
    )
  }
}
