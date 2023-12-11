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

package za.co.absa.pramen.core.mocks.transformer

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.{MetastoreReader, Reason, Transformer}
import za.co.absa.pramen.core.source.SourceManager
import za.co.absa.pramen.core.utils.ConfigUtils

import java.time.LocalDate
import scala.collection.JavaConverters.asScalaBufferConverter

/**
  * This transformer does not have dependencies, but always generates some data.
  */
class InnerSourceTransformer(conf: Config) extends Transformer {
  private val log = LoggerFactory.getLogger(this.getClass)

  override def validate(metastore: MetastoreReader,
                        infoDate: LocalDate,
                        options: Map[String, String]): Reason = {
    Reason.Ready
  }

  override def run(metastore: MetastoreReader, infoDate: LocalDate, options: Map[String, String]): DataFrame = {
    implicit val spark: SparkSession = SparkSession.builder().getOrCreate()

    import spark.implicits._

    val sourceName = options("source")

    val sourceConfig = conf.getConfig(options("config.key"))

    log.info(ConfigUtils.renderEffectiveConfigHocon(sourceConfig))

    val src = SourceManager.getSourceByName(sourceName, sourceConfig, None)

    val ar = src.config.getStringList("array.list").asScala.toList

    ar.toDF("value")
  }
}
