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

package za.co.absa.pramen.api.v2

import com.typesafe.config.Config

sealed trait Query {
  def query: String
}

object Query {
  val SQL_KEY = "sql"
  val PATH_KEY = "path"
  val TABLE_KEY = "table"
  val DB_TABLE_KEY = "db.table" // Same as table - for backwards compatibility and config readability

  case class Sql(sql: String) extends Query {
    override def query: String = sql
  }

  case class Table(dbTable: String) extends Query {
    override def query: String = dbTable
  }

  case class Path(path: String) extends Query {
    override def query: String = path
  }

  def fromConfig(conf: Config, prefix: String, parentPath: String): Query = {
    val p = if (prefix.isEmpty) "" else s"$prefix."

    val hasSql = conf.hasPath(s"$p$SQL_KEY")
    val hasPath = conf.hasPath(s"$p$PATH_KEY")
    val hasDbTable = conf.hasPath(s"$p$TABLE_KEY") || conf.hasPath(s"$p$DB_TABLE_KEY")

    val tableDef = if (conf.hasPath(s"$p$TABLE_KEY")) {
      Some(conf.getString(s"$p$TABLE_KEY"))
    } else if (conf.hasPath(s"$p$DB_TABLE_KEY")) {
      Some(conf.getString(s"$p$DB_TABLE_KEY"))
    } else {
      None
    }

    (hasSql, hasPath, hasDbTable) match {
      case (true, false, false)  => Sql(conf.getString(s"$p$SQL_KEY"))
      case (false, true, false)  => Path(conf.getString(s"$p$PATH_KEY"))
      case (false, false, true)  => Table(tableDef.get)
      case (false, false, false) => throw new IllegalArgumentException(s"Either '$p$SQL_KEY', '$p$PATH_KEY', '$p$TABLE_KEY', '$p$DB_TABLE_KEY' is required at $parentPath.")
      case _                     => throw new IllegalArgumentException(s"Only one of: '$p$SQL_KEY', '$p$PATH_KEY', '$p$TABLE_KEY', '$p$DB_TABLE_KEY' are allowed at $parentPath.")
    }
  }
}
