/*
 * Copyright 2022 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.pramen.framework.source

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.{Query, Source, TableReader}
import za.co.absa.pramen.framework.ExternalChannelFactory
import za.co.absa.pramen.framework.reader.model.TableReaderJdbcConfig
import za.co.absa.pramen.framework.reader.{TableReaderJdbc, TableReaderJdbcNative}

class JdbcSource(hasInfoDateCol: Boolean,
                 sourceConfig: Config,
                 sourceConfigParentPath: String,
                 val jdbcReaderConfig: TableReaderJdbcConfig)(implicit spark: SparkSession) extends Source {
  private val log = LoggerFactory.getLogger(this.getClass)

  override def hasInfoDate: Boolean = hasInfoDateCol

  override def getReader(query: Query, columns: Seq[String]): TableReader = {
    query match {
      case Query.Table(dbTable) =>
        log.info(s"Using TableReaderJdbc to read the table: $dbTable")
        new TableReaderJdbc(dbTable, columns, jdbcReaderConfig, sourceConfig)
      case Query.Sql(sql)       =>
        log.info(s"Using TableReaderJdbcNative to read the query: $sql")
        new TableReaderJdbcNative(sql, jdbcReaderConfig.jdbcConfig, jdbcReaderConfig.connectionRetries)
      case Query.Path(_)          =>
        throw new IllegalArgumentException(s"Unexpected 'path' spec for the JDBC reader. Only 'table' or 'sql' are supported. Config path: $sourceConfigParentPath")
    }
  }
}

object JdbcSource extends ExternalChannelFactory[JdbcSource] {
  override def apply(conf: Config, parentPath: String, spark: SparkSession): JdbcSource = {
    val tableReaderJdbc = TableReaderJdbcConfig.load(conf)

    new JdbcSource(tableReaderJdbc.hasInfoDate, conf, parentPath, tableReaderJdbc)(spark)
  }
}
