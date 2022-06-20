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

package za.co.absa.pramen.mocks

import java.time.DayOfWeek

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import za.co.absa.pramen.api.reader.TableReader
import za.co.absa.pramen.api.schedule.Weekly
import za.co.absa.pramen.api.writer.TableWriter
import za.co.absa.pramen.builtin.JDBCToParquetSyncJob
import za.co.absa.pramen.builtin.model.JdbcToParquetSyncTable

class JdbcToParquetSyncWeeklySpy(tablesToSync: List[JdbcToParquetSyncTable],
                                 daysOfWeek: Seq[DayOfWeek],
                                 numberOfRecords: Long = 0L)
                                (implicit spark: SparkSession, conf: Config)
  extends JDBCToParquetSyncJob("Dummy Job", Weekly(daysOfWeek), tablesToSync) {

  override def getReader(tableName: String): TableReader = new ReaderSpy(numberOfRecords)

  override def getWriter(tableName: String): TableWriter = new WriterSpy(numberOfRecords)

}
