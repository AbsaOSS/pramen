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
import org.apache.spark.sql.{DataFrame, SparkSession}
import za.co.absa.pramen.api.{MetastoreReader, Sink}
import za.co.absa.pramen.framework.ExternalChannelFactory

import java.time.LocalDate
import scala.collection.mutable.ListBuffer

class SinkSpy(val customConfig: String = "",
              connectException: Throwable = null,
              writeException: Throwable = null,
              closeException: Throwable = null) extends Sink {
  var connectCalled: Int = 0
  var closeCalled: Int = 0
  var writeCalled: Int = 0
  val dfs: ListBuffer[DataFrame] = new ListBuffer()
  var specialOption: Option[String] = None

  override def connect(): Unit = {
    connectCalled += 1
    if (connectException != null) {
      throw connectException
    }
  }

  override def close(): Unit = {
    closeCalled += 1
    if (closeException != null) {
      throw closeException
    }
  }

  override def send(df: DataFrame,
                    tableName: String,
                    metastore: MetastoreReader,
                    infoDate: LocalDate,
                    options: Map[String, String])
                   (implicit spark: SparkSession): Long = {
    writeCalled += 1

    if (writeException != null) {
      throw writeException
    }

    specialOption = options.get("specialOption")

    dfs += df

    df.count()
  }
}

object SinkSpy extends ExternalChannelFactory[SinkSpy] {
  val TOPIC_NAME_KEY = "topic.name"

  override def apply(conf: Config, parentPath: String, spark: SparkSession): SinkSpy = {
    val customConfig = if (conf.hasPath("custom.config")) {
      conf.getString("custom.config")
    } else {
      ""
    }
    new SinkSpy(customConfig)
  }
}
