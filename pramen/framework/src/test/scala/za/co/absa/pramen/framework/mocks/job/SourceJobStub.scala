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

package za.co.absa.pramen.framework.mocks.job

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import za.co.absa.pramen.api.reader.TableReader
import za.co.absa.pramen.api.writer.TableWriter
import za.co.absa.pramen.api.{JobFactory, SourceJob}
import za.co.absa.pramen.framework.mocks.reader.ReaderStub
import za.co.absa.pramen.framework.mocks.writer.WriterStub

class SourceJobStub(val dummyOption: Option[Boolean]) extends SourceJob {
  override def getTables: Seq[String] = Seq("dummy_table")

  override def getReader(tableName: String): TableReader = new ReaderStub

  override def getWriter(tableName: String): TableWriter = new WriterStub

  override def name: String = "Dummy"
}

object SourceJobStub extends JobFactory[SourceJobStub] {
  override def apply(conf: Config, spark: SparkSession): SourceJobStub = {
    val dummyOpt = if (conf.hasPath("dummy.option")) {
      Option(conf.getBoolean("dummy.option"))
    } else {
      None
    }
    new SourceJobStub(dummyOpt)
  }
}
