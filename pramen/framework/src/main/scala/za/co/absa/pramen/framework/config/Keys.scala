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

package za.co.absa.pramen.framework.config

object Keys {
  val INFORMATION_DATE_COLUMN = "pramen.information.date.column"
  val INFORMATION_DATE_FORMAT_APP = "pramen.information.date.format"
  val INFORMATION_DATE_START = "pramen.information.date.start"

  val PARQUET_CUSTOM_PATTERN = "pramen.parquet.custom.partition.pattern"

  val TEMPORARY_DIRECTORY = "pramen.temporary.directory"

  val WARN_THROUGHPUT_RPS = "pramen.warn.throughput.rps"
  val GOOD_THROUGHPUT_RPS = "pramen.good.throughput.rps"

  val JDBC_SYNC_TABLE_DAILY_PREFIX="jdbc.sync.daily.table"
  val JDBC_SYNC_TABLE_WEEKLY_PREFIX="jdbc.sync.weekly.table"

  val JDBC_SYNC_TABLE_WEEKLY_OUTPUT_INFO_DATE_DELAY = "jdbc.sync.weekly.info.date.delay.days"

  val JOB_FACTORIES = "pramen.jobs"

  val SYNC_WEEKLY_DAYS_OF_WEEK = "jdbc.sync.weekly.days.of.week"

  val MAIL_FROM = "mail.send.from"
  val MAIL_TO = "mail.send.to"
  val MAIL_FAILURES_TO = "mail.send.failures.to"

  // Snapshot job
  val SNAP_DATE_FIELD = "jdbc.snap.weekly.snapshot.date.column"

  val HAS_INFO_DATE_COLUMN = "jdbc.snap.weekly.has.information.date.column"
  val JDBC_SNAP_INFO_DATE_COLUMN = "jdbc.snap.weekly.information.date.column"

  val JDBC_SNAP_INFO_DATE_DELAY = "jdbc.snap.weekly.info.date.delay.days"
  val JDBC_SNAP_FIELD_DELAY = "jdbc.snap.weekly.snapshot.date.delay.days"

  val JDBC_SNAP_TABLE_WEEKLY_PREFIX="jdbc.snap.weekly.table"

  val SNAP_WEEKLY_DAYS_OF_WEEK = "jdbc.snap.weekly.days.of.week"

  val HADOOP_REDACT_TOKENS = "hadoop.redacted.tokens"
  val HADOOP_OPTION_PREFIX = "hadoop.option"

  val EXTRA_OPTIONS_PREFIX = "pramen.spark.conf.option"

  val STOP_SPARK_SESSION = "pramen.stop.spark.session"

  val EXIT_CODE_ENABLED = "pramen.exit.code.enabled"

  val TIMEZONE = "pramen.timezone"
}
