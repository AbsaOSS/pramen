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

import org.apache.spark.sql.{DataFrame, SparkSession}
import za.co.absa.pramen.api.{MetastoreReader, Reason, Transformer}

import java.time.LocalDate

/**
  * This transformer does not have dependencies, but always generates some data.
  */
class GeneratingTransformer extends Transformer {
  override def validate(metastore: MetastoreReader,
                        infoDate: LocalDate,
                        options: Map[String, String]): Reason = {
    Reason.Ready
  }

  override def run(metastore: MetastoreReader, infoDate: LocalDate, options: Map[String, String]): DataFrame = {
    implicit val spark: SparkSession = SparkSession.builder().getOrCreate()

    import spark.implicits._

    List(("D", 4), ("E", 5), ("F", 6)).toDF("a", "b")
  }
}
