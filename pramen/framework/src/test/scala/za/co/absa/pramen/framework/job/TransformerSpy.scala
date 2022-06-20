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

package za.co.absa.pramen.framework.job

import org.apache.spark.sql.DataFrame
import za.co.absa.pramen.api.Reason
import za.co.absa.pramen.api.metastore.MetastoreReader
import za.co.absa.pramen.api.v2.Transformer

import java.time.LocalDate

class TransformerSpy(validateFunction: () => Reason = () => Reason.Ready,
                     runFunction: () => DataFrame = () => null ) extends Transformer {
  var validationCalled = 0
  var runCalled = 0

  override def validate(metastore: MetastoreReader,
                        infoDate: LocalDate,
                        options: Map[String, String]): Reason = {
    validationCalled += 1
    validateFunction()
  }

  override def run(metastore: MetastoreReader, infoDate: LocalDate, options: Map[String, String]): DataFrame = {
    runCalled += 1
    runFunction()
  }
}
