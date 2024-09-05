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

package za.co.absa.pramen.api.sql

import scala.collection.mutable.ListBuffer

/**
  * This class contains implementation of methods that are common across all SQL dialects.
  *
  * @param sqlConfig A SQL generator configuration
  */
abstract class SqlGeneratorBase(sqlConfig: SqlConfig) extends SqlGenerator {
  import SqlGeneratorBase._

  /**
    * This returns characters used for escaping into a mode that allows special characters in identifiers.
    * For example,
    *  - in Hive, column name 'my column' should be escaped with back quotes `my column`,
    *  - in MS SQL server square braces are used instead [my column].
    */
  def beginEndEscapeChars: (Char, Char)

  def quoteSingleIdentifier(identifier: String): String = {
    val (escapeBegin, escapeEnd) = beginEndEscapeChars

    if (identifier.startsWith(s"$escapeBegin") && identifier.endsWith(s"$escapeEnd")) {
      identifier
    } else {
      s"$escapeBegin$identifier$escapeEnd"
    }
  }

  def unquoteSingleIdentifier(identifier: String): String = {
    val (escapeBegin, escapeEnd) = beginEndEscapeChars

    if (identifier.startsWith(s"$escapeBegin") && identifier.endsWith(s"$escapeEnd") && identifier.length > 2) {
      identifier.substring(1, identifier.length - 1)
    } else {
      identifier
    }
  }

  override def getAliasExpression(expression: String, alias: String): String = {
    s"$expression AS ${escape(alias)}"
  }

  override final def quote(identifier: String): String = {
    validateIdentifier(identifier)
    splitComplexIdentifier(identifier).map(quoteSingleIdentifier).mkString(".")
  }

  override final def unquote(identifier: String): String = {
    validateIdentifier(identifier)
    splitComplexIdentifier(identifier).map(unquoteSingleIdentifier).mkString(".")
  }

  override final def escape(identifier: String): String = {  
    if (needsEscaping(sqlConfig.identifierQuotingPolicy, identifier)) {
      quote(identifier)
    } else {
      identifier
    }
  }

  /**
    * An expression for the list of configured columns.
    * @return A part of SQL expression listing column names.
    */
  protected def columnExpr(columns: Seq[String]): String = {
    if (columns.isEmpty) {
      "*"
    } else {
      columns.map(col => escape(col)).mkString(", ")
    }
  }

  final def splitComplexIdentifier(identifier: String): Seq[String] = {
    val trimmedIdentifier = identifier.trim

    if (trimmedIdentifier.isEmpty) {
      throw new IllegalArgumentException(f"Found an empty table name or column name ('$identifier').")
    }

    val (escapeBegin, escapeEnd) = beginEndEscapeChars
    val sameEscapeChar = escapeBegin == escapeEnd

    val output = new ListBuffer[String]
    val curColumn = new StringBuffer()
    val len = trimmedIdentifier.length
    var nestingLevel = 0
    var i = 0

    while (i < len) {
      val c = trimmedIdentifier(i)
      val nextChar = if (i == len - 1) ' ' else trimmedIdentifier(i + 1)

      if (nestingLevel == 0 && c == '.') {
        output += curColumn.toString
        curColumn.setLength(0)
      } else {
        curColumn.append(c)
      }

      if (sameEscapeChar) {
        if (c == escapeBegin) {
          if (curColumn.length() > 1 && i < len - 1 && nextChar != '.')
            throw new IllegalArgumentException(f"Invalid character '$escapeBegin' in the identifier '$identifier', position $i.")
          if (nestingLevel == 0) {
            nestingLevel += 1
          } else
            nestingLevel -= 1
        }
      } else {
        if (c == escapeBegin) {
          nestingLevel += 1
          if (curColumn.length() != 1)
            throw new IllegalArgumentException(f"Invalid character '$escapeBegin' in the identifier '$identifier', position $i.")
        } else if (c == escapeEnd)
          nestingLevel -= 1
      }

      if (nestingLevel < 0) {
        throw new IllegalArgumentException(f"Found not matching '$escapeEnd' in the identifier '$identifier'.")
      }
      i += 1
    }

    if (nestingLevel != 0)
      throw new IllegalArgumentException(f"Found not matching '$escapeBegin' in the identifier '$identifier'.")

    if (curColumn.toString.nonEmpty)
      output += curColumn.toString

    output.toSeq
  }

  /**
    * This escapes the information date column properly.
    */
  final protected def infoDateColumn: String = {
    escape(sqlConfig.infoDateColumn)
  }
}

object SqlGeneratorBase {
  val forbiddenCharacters = ";'\\"
  val normalCharacters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_."

  final def validateIdentifier(identifier: String): Unit = {
    identifier.foreach { c =>
      if (forbiddenCharacters.contains(c) || c.toInt < 32)
        throw new IllegalArgumentException(f"The character '$c' (0x${c.toInt}%02X) cannot be used as part of column name in '$identifier'.")
    }
  }

  final def needsEscaping(policy: QuotingPolicy, identifier: String): Boolean = {
    policy match {
      case QuotingPolicy.Always => true
      case QuotingPolicy.Never => false
      case QuotingPolicy.Auto => !identifier.forall(normalCharacters.contains(_))
    }
  }
}
