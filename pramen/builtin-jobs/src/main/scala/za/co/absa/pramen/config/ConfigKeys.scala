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

package za.co.absa.pramen.config

object ConfigKeys {

  //// JDBC to Parquet jobs
  val JDBC_SYNC_PREFIX = "jdbc.sync"
  val JDBC_SYNC_TABLE_PREFIX = "jdbc.sync.table"

  val PARQUET_WRITER_PREFIX = "jdbc.sync.writer.parquet"
  val DELTA_WRITER_PREFIX = "jdbc.sync.writer.delta"

  val JDBC_SYNC_TABLE_NEW_PREFIX = "jdbc.sync.tables.table"

  // Sourcing parameters
  val JDBC_SYNC_JOB_NAME = s"$JDBC_SYNC_PREFIX.job.name"
  val JDBC_SYNC_TABLE_OUTPUT_INFO_DATE_DELAY = s"$JDBC_SYNC_PREFIX.info.date.delay.days"
  val JDBC_SYNC_PROCESSING_TIMESTAMP_COL = s"$JDBC_SYNC_PREFIX.processing.timestamp.col"
  val JDBC_SYNC_SNAPSHOT_DATE = s"$JDBC_SYNC_PREFIX.snapshot.date.col"
  val JDBC_SYNC_SNAP_FIELD_DELAY = s"$JDBC_SYNC_PREFIX.snapshot.date.delay.days"

  //// Parquet to Kafka jobs

  // Parquet sourcing parameters
  val PARQUET_TO_KAFKA_SYNC_PREFIX = "parquet.kafka.sync"
  val PARQUET_TO_KAFKA_SYNC_TABLE_PREFIX = "parquet.kafka.sync.table"
  val PARQUET_TO_KAFKA_SYNC_JOB_NAME = s"$PARQUET_TO_KAFKA_SYNC_PREFIX.job.name"
  val PARQUET_TO_KAFKA_WRITER_PREFIX = s"$PARQUET_TO_KAFKA_SYNC_PREFIX.writer.kafka"

  //// Generic Sync job
  val SYNC_JOB_NAME = "sync.job.name"
  val SYNC_TABLE_PREFIX = "sync.table"

  //// Command line job
  val CMD_LINE_PREFIX = "cmd.line"
  val CMD_LINE_JOB_NAME = "cmd.line.job.name"
  val CMD_LINE_INPUT_TABLES = "cmd.line.input.table.names"
  val CMD_LINE_OUTPUT_TABLE = "cmd.line.output.table.name"
  val CMD_LINE_OUTPUT_INFO_DATE_DELAY = s"cmd.line.info.date.delay.days"
  val CMD_LINE_OUTPUT_RECORDS_REGEX = "cmd.line.record.count.regex"
  val CMD_LINE_OUTPUT_ZERO_RECORD_SUCCESS_REGEX = "cmd.line.zero.records.success.regex"
  val CMD_LINE_OUTPUT_FAILURE_NO_DATA = "cmd.line.no.data.failure.regex"
  val CMD_LINE_OUTPUT_FILTER_REGEX = "cmd.line.output.filter.regex"
  val CMD_LINE_COMMAND = "cmd.line.command"
  val CMD_LINE_LOGS_TO_INCLUDE = "cmd.line.log.lines.to.include"

}
