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

package za.co.absa.pramen.core.validator

import com.typesafe.config.Config
import za.co.absa.pramen.core.utils.ConfigUtils

case class ValidationCheck(
                          tablesAll: Seq[String],
                          tablesOneOf: Seq[String],
                          dataFromExpr: Option[String],
                          dataToExpr: Option[String]
                          )

object ValidationCheck {
  val TABLES_ALL_KEY = "tables.all"
  val TABLES_ONE_OF_KEY = "tables.one.of"

  val DATE_FROM_KEY = "date.from"
  val DATE_TO_KEY = "date.to"

  def load(conf: Config, parent: String): ValidationCheck = {
    ConfigUtils.validateOneOfPathsExistence(conf, parent, TABLES_ALL_KEY :: TABLES_ONE_OF_KEY :: Nil)
    ConfigUtils.validatePathsExistence(conf, parent, DATE_FROM_KEY :: Nil)

    ValidationCheck (
      ConfigUtils.getOptListStrings(conf, TABLES_ALL_KEY),
      ConfigUtils.getOptListStrings(conf, TABLES_ONE_OF_KEY),
      ConfigUtils.getOptionString(conf, DATE_FROM_KEY),
      ConfigUtils.getOptionString(conf, DATE_TO_KEY)
    )
  }

}