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

import org.apache.spark.sql.DataFrame
import za.co.absa.pramen.api.{MetastoreReader, Reason, Transformer}

import java.time.LocalDate

class FailingTransformer extends Transformer {
  override def validate(metastore: MetastoreReader, infoDate: LocalDate, options: Map[String, String]): Reason = {
    if (options.contains("fail.validation") && options("fail.validation").toBoolean) {
      Reason.NotReady("Validation failed")
    } else {
      Reason.Ready
    }
  }

  override def run(metastore: MetastoreReader, infoDate: LocalDate, options: Map[String, String]): DataFrame = {
    if (options.contains("fatal.exception") && options("fatal.exception").toBoolean) {
      throw new AbstractMethodError("Error in test")
    } else {
      throw new RuntimeException("Error in test")
    }
  }
}
