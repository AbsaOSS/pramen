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

package za.co.absa.pramen.core.sql

import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.sql.{SqlConfig, SqlGenerator}

object SqlGeneratorLoader {
  private val log = LoggerFactory.getLogger(this.getClass)

  /**
    * Loads an SQL generator, If SQL configuration contains a generator class name, it will be loaded.
    * If not, the generator will be selected based on the driver name based on the internal mapping.
    * @param driver    The driver class.
    * @param sqlConfig The SQL configuration.
    * @return The SQL generator.
    */
  def getSqlGenerator(driver: String, sqlConfig: SqlConfig): SqlGenerator = {
    val sqlGenerator = sqlConfig.sqlGeneratorClass match {
      case Some(clazz) => fromClass(clazz, sqlConfig)
      case None        => fromDriverName(driver, sqlConfig)
    }

    log.info(s"Using SQL generator: ${sqlGenerator.getClass.getCanonicalName}")
    sqlGenerator
  }

  private def fromDriverName(driver: String, sqlConfig: SqlConfig): SqlGenerator = {
    driver match {
      case "org.postgresql.Driver"                        => new SqlGeneratorPostgreSQL(sqlConfig)
      case "com.mysql.cj.jdbc.Driver"                     => new SqlGeneratorMySQL(sqlConfig)
      case "oracle.jdbc.OracleDriver"                     => new SqlGeneratorOracle(sqlConfig)
      case "net.sourceforge.jtds.jdbc.Driver"             => new SqlGeneratorMicrosoft(sqlConfig)
      case "com.microsoft.sqlserver.jdbc.SQLServerDriver" => new SqlGeneratorMicrosoft(sqlConfig)
      case "com.denodo.vdp.jdbc.Driver"                   => new SqlGeneratorDenodo(sqlConfig)
      case "com.sas.rio.MVADriver"                        => new SqlGeneratorSas(sqlConfig)
      case "com.cloudera.hive.jdbc41.HS2Driver"           => new SqlGeneratorHive(sqlConfig)
      case "com.simba.hive.jdbc41.HS2Driver"              => new SqlGeneratorHive(sqlConfig)
      case "com.simba.spark.jdbc.Driver"                  => new SqlGeneratorHive(sqlConfig)
      case "org.hsqldb.jdbc.JDBCDriver"                   => new SqlGeneratorHsqlDb(sqlConfig)
      case "com.ibm.db2.jcc.DB2Driver"                    => new SqlGeneratorDb2(sqlConfig)
      case d                                              =>
        log.warn(s"Unsupported JDBC driver: '$d'. Trying to use a generic SQL generator.")
        new SqlGeneratorGeneric(sqlConfig)
    }
  }

  private def fromClass(clazz: String, sqlConfig: SqlConfig): SqlGenerator = {
    Class.forName(clazz)
      .getConstructor(classOf[SqlConfig])
      .newInstance(sqlConfig)
      .asInstanceOf[SqlGenerator]
  }
}
