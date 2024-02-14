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

package za.co.absa.pramen.core.sql.dialects

import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.types.{DataType, MetadataBuilder}
import org.slf4j.LoggerFactory

/**
  * This is required for Spark to be able to handle data that comes from Hive JDBC drivers
  */
object HiveDialect extends JdbcDialect {
  private val logger = LoggerFactory.getLogger(this.getClass)

  override def canHandle(url: String): Boolean = url.startsWith("jdbc:hive2")

  override def quoteIdentifier(colName: String): String = {
    colName.split('.').map(sub => s"`$sub`").mkString(".")
  }

  override def getCatalystType(sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    // Initially, I thought that a workaround like this is needed.
    // But it seems it works without it.
    // Leaving it here commented out.
    //    val inferredTypeOpt = if (sqlType == -5 /*BIGINT*/ && size <=19) {
    //      Some(LongType)
    //    } else {
    //      super.getCatalystType(sqlType, typeName, size, md)
    //    }
    //    log.info(s"sqlType=$sqlType, typeName=$typeName, size=$size, TYPE=$inferredTypeOpt")
    //    inferredTypeOpt

    val inferredTypeOpt = super.getCatalystType(sqlType, typeName, size, md)

    logger.debug(s"sqlType=$sqlType, typeName=$typeName, size=$size, TYPE=$inferredTypeOpt")

    inferredTypeOpt
  }
}
