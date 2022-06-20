/*
 * Copyright 2020 ABSA Group Limited
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

class ThrowJobDummy extends SourceJob {
  override def getTables: Seq[String] = throw new IllegalArgumentException("getTables is called")
  override def getReader(tableName: String): TableReader = throw new IllegalArgumentException("getReader is called")
  override def getWriter(tableName: String): TableWriter = throw new IllegalArgumentException("getWriter is called")
  override def name: String = throw new IllegalArgumentException("name is called")
}

object ThrowJobDummy extends JobFactory[ThrowJobDummy] {
  override def apply(conf: Config, spark: SparkSession): ThrowJobDummy = throw new IllegalArgumentException("Factory is called")
}
