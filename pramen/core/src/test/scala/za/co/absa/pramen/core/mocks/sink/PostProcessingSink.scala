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

package za.co.absa.pramen.core.mocks.sink

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.{ExternalChannelFactory, MetastoreReader, Sink, SinkResult}

import java.time.LocalDate

class PostProcessingSink(sinkConfig: Config) extends Sink {
  private val log = LoggerFactory.getLogger(this.getClass)

  override def send(df: DataFrame, tableName: String, metastore: MetastoreReader, infoDate: LocalDate, options: Map[String, String])(implicit spark: SparkSession): SinkResult = {
    log.info(s"Input df is empty = ${df.isEmpty}")

    val path = options("path")

    val count = spark.read.parquet(path).count()

    SinkResult(count)
  }

  override def config: Config = sinkConfig
}

object PostProcessingSink extends ExternalChannelFactory[PostProcessingSink] {
  override def apply(conf: Config, parentPath: String, spark: SparkSession): PostProcessingSink = {
    new PostProcessingSink(conf)
  }
}
