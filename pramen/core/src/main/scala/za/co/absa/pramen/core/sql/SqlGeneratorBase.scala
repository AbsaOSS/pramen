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

/**
  * This class contains implementation of methods that are common across all SQL dialects.
  *
  * @param sqlConfig A SQL generator configuration
  */
abstract class SqlGeneratorBase(sqlConfig: SqlConfig) extends SqlGenerator {
  import SqlGeneratorBase._

  /** This Wraps the column name with escaping characters. The escape logic varies among SQL dialects. */
  def wrapIdentifier(identifier: String): String

  /** This validates and escapes the column name is needed. Escaping does not happen always to maintain backwards compatibility. */
  final override def escapeIdentifier(identifier: String): String = {
    validateIdentifier(identifier)

    if (identifierNeedsEscaping(identifier))
      wrapIdentifier(identifier)
    else
      identifier
  }

  /**
    * An expression for the list of configured columns.
    * @return A part of SQL expression listing column names.
    */
  protected def columnExpr(columns: Seq[String]): String = {
    if (columns.isEmpty) {
      "*"
    } else {
      columns.map(col => escapeIdentifier(col)) .mkString(", ")
    }
  }

  /**
    * This escapes the information date column properly.
    */
  final protected def infoDateColumn: String = {
    escapeIdentifier(sqlConfig.infoDateColumn)
  }
}

object SqlGeneratorBase {
  val forbiddenCharacters = ";'\""
  val normalCharacters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_."
  val sqlKeywords: Set[String] = Set(
    "ABORT", "ABS", "ABSOLUTE", "ACCESS", "ADMIN", "AFTER", "AGGREGATE", "ALIAS", "ALL", "ALLOCATE",
    "ALSO", "ALTER", "ALWAYS", "ANALYSE", "ANALYZE", "AND", "ANY", "ARE", "ARRAY", "AS", "ASC", "ASENSITIVE", "ASSERTION",
    "AT", "ATOMIC", "ATTRIBUTE", "ATTRIBUTES", "AUTHORIZATION", "BACKWARD", "BEFORE", "BEGIN", "BETWEEN", "BIGINT", "BINARY",
    "BIT", "BITVAR", "BLOB", "BOOLEAN", "BOTH", "BY", "CACHE", "CALL", "CALLED", "CARDINALITY", "CASCADE", "CASCADED",
    "CASE", "CAST", "CATALOG", "CATALOG_NAME", "CHAIN", "CHAR", "CHARACTER", "CHARACTERISTICS", "CHARACTERS", "CHARACTER_LENGTH",
    "CHARACTER_SET_CATALOG", "CHARACTER_SET_NAME", "CHARACTER_SET_SCHEMA", "CHAR_LENGTH", "CHECK", "CHECKED", "CHECKPOINT",
    "CLASS", "CLASS_ORIGIN", "CLOSE", "CLUSTER", "COALESCE", "COLLATE", "COLLATION", "COLLATION_CATALOG", "COLLATION_NAME",
    "COLLATION_SCHEMA", "COLLECT", "COLUMN", "COLUMN_NAME", "COMMAND_FUNCTION", "COMMAND_FUNCTION_CODE", "COMMENT",
    "COMMIT", "COMMITTED", "COMPLETION", "CONDITION", "CONNECT", "CONSTRAINT", "CONSTRAINTS", "CONTAINS", "CONTINUE",
    "CONVERT", "COPY", "CORR", "COUNT", "CREATE", "CREATEDB", "CREATEROLE", "CREATEUSER", "CROSS", "CURRENT", "CURRENT_DATE",
    "CURRENT_PATH", "CURRENT_ROLE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURRENT_USER", "CURSOR", "CURSOR_NAME", "DATA",
    "DATABASE", "DATE", "DATETIME", "DAY", "DEALLOCATE", "DEC", "DECIMAL", "DECLARE", "DEFAULT", "DEFAULTS", "DEGREE",
    "DELETE", "DELIMITER", "DELIMITERS", "DESC", "DESCRIBE", "DESCRIPTOR", "DESTROY", "DICTIONARY", "DISABLE", "DISCONNECT",
    "DISTINCT", "DO", "DOMAIN", "DOUBLE", "DROP", "EACH", "ELEMENT", "ELSE", "ENABLE", "ENCODING", "ENCRYPTED", "END",
    "EQUALS", "ESCAPE", "EVERY", "EXCEPT", "EXCEPTION", "EXCLUDE", "EXCLUDING", "EXEC", "EXECUTE", "EXISTS", "EXP",
    "EXPLAIN", "EXTERNAL", "EXTRACT", "FALSE", "FETCH", "FILTER", "FINAL", "FIRST", "FOR", "FORCE", "FREE", "FROM",
    "FUNCTION", "GET", "GLOBAL", "GO", "GOTO", "GRANT", "GRANTED", "GROUP", "HANDLER", "HAVING", "HOLD", "HOST", "HOUR",
    "IDENTITY", "ILIKE", "IMMEDIATE", "IN", "INCREMENT", "INDEX", "INNER", "INOUT", "INPUT", "INSENSITIVE", "INSERT", "INSTANCE",
    "INSTEAD", "INTERSECT", "INTERSECTION", "INTERVAL", "INTO", "INVOKER", "IS", "ISNULL", "JOIN", "KEY", "KEY_MEMBER",
    "KEY_TYPE", "LANGUAGE", "LAST", "LEAST", "LEFT", "LENGTH", "LESS", "LEVEL", "LIKE", "LIMIT", "LOAD", "LOCAL", "LOCALTIME",
    "LOCALTIMESTAMP", "LOCATION", "LOCK", "LOGIN", "MATCH", "MAX", "MAXVALUE", "MEMBER", "MERGE", "METHOD", "MIN", "MINUTE",
    "MODE", "MODIFIES", "MODIFY", "MODULE", "MONTH", "MORE", "MOVE", "NATIONAL", "NATURAL", "NESTING", "NEW", "NEXT", "NO",
    "NOLOCK", "NOLOGIN", "NONE", "NORMALIZE", "NORMALIZED", "NOSUPERUSER", "NOT", "NOTHING", "NOTIFY", "NOTNULL", "NOWAIT",
    "NULL", "NULLABLE", "NULLIF", "NULLS", "OBJECT", "OF", "OFF", "OFFSET", "OIDS", "OLD", "ON", "ONLY", "OPEN", "OPERATION",
    "OPTION", "OPTIONS", "OR", "ORDER", "ORDERING", "OUT", "OUTPUT", "OVER", "OWNER", "PARAMETER", "PARAMETERS",
    "PARTIAL", "PARTITION", "PASSWORD", "PATH", "POWER", "PRECEDING", "PRECISION", "PREFIX", "PREPARE", "PRESERVE", "PRIMARY",
    "PRIOR", "PRIVILEGES", "PUBLIC", "QUOTE", "RANGE", "RANK", "READ", "READS", "RECURSIVE", "REF", "REFERENCES", "REFERENCING",
    "RELATIVE", "RELEASE", "RENAME", "REPLACE", "RESET", "RESTART", "RESTRICT", "RESULT", "RETURN", "RETURNS", "REVOKE",
    "RIGHT", "ROLE", "ROLLBACK", "ROW", "ROWS", "ROW_COUNT", "ROW_NUMBER", "RULE", "SAVEPOINT", "SCALE", "SCHEMA", "SCHEMA_NAME",
    "SCOPE", "SCROLL", "SEARCH", "SECOND", "SECTION", "SECURITY", "SELECT", "SELF", "SENSITIVE", "SEQUENCE", "SERIALIZABLE",
    "SERVER_NAME", "SESSION", "SESSION_USER", "SET", "SETOF", "SETS", "SHARE", "SHOW", "SIZE", "SOME", "SOURCE", "SPACE",
    "SQL", "SQLCODE", "SQLERROR", "SQLEXCEPTION", "SQLSTATE", "SQLWARNING", "STABLE", "START", "STATE", "STATEMENT", "STATIC",
    "STATISTICS", "STDIN", "STDOUT", "STORAGE", "STRICT", "STRUCTURE", "STYLE", "SUBSTRING", "SUM", "SUPERUSER", "SYMMETRIC",
    "SYSID", "SYSTEM", "SYSTEM_USER", "TABLE", "TABLESPACE", "TABLE_NAME", "TEMP", "TEMPLATE", "TEMPORARY", "TERMINATE",
    "THAN", "THEN", "TIES", "TIME", "TIMESTAMP", "TIMEZONE_MINUTE", "TO", "TOAST", "TRAILING", "TRANSACTION")

  final def validateIdentifier(identifier: String): Unit = {
    identifier.foreach { c =>
      if (forbiddenCharacters.contains(c))
        throw new IllegalArgumentException(s"The character '$c' cannot be used as part of column name in '$identifier'.")
    }
  }

  final def identifierNeedsEscaping(identifier: String): Boolean = {
    identifier.length < 2 || !identifier.forall(c => normalCharacters.contains(c)) || sqlKeywords.contains(identifier.toUpperCase)
  }
}
