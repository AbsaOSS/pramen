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

package za.co.absa.pramen.api

/**
  * Query is an abstraction that unifies the way data can be retrieved from a source.
  */
sealed trait Query {
  def name: String

  def query: String
}

object Query {
  /** A SQL query. */
  case class Sql(sql: String) extends Query {
    override def name: String = "sql"

    override def query: String = sql
  }

  /** A database table name. */
  case class Table(dbTable: String) extends Query {
    override def name: String = "table"

    override def query: String = dbTable
  }

  /** A path to a directory. */
  case class Path(path: String) extends Query {
    override def name: String = "path"

    override def query: String = path
  }

  /** A custom way of specifying query options. */
  case class Custom(options: Map[String, String]) extends Query {
    override def name: String = "custom"

    override def query: String = options.map { case (k, v) => s"$k=$v" }.mkString("(", ", ", ")")
  }
}
