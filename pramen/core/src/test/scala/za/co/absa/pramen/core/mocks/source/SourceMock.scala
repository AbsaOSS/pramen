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

package za.co.absa.pramen.core.mocks.source

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import za.co.absa.pramen.api._
import za.co.absa.pramen.core.utils.ConfigUtils

import java.time.LocalDate

class SourceMock(
                  conf: Config = ConfigFactory.empty(),
                  mockDataAlwaysAvailable: Boolean = false,
                  throwConnectionException: Boolean = false,
                  throwValidationException: Boolean = false,
                  throwCloseException: Boolean = false,
                  recordCountToReturn: Int = 1,
                  validationReasonToReturn: Reason = Reason.Ready
                )(implicit spark: SparkSession) extends Source {
  var connectCalled = 0
  var closeCaller = 0
  var getRecordCountCalled = 0
  var getDataCalled = 0

  override def isDataAlwaysAvailable: Boolean = mockDataAlwaysAvailable

  @throws[Exception]
  override def connect(): Unit = {
    connectCalled += 1
    if (throwConnectionException)
      throw new RuntimeException("Connection test exception")
  }

  override def close(): Unit = {
    closeCaller += 1
    if (throwCloseException)
      throw new RuntimeException("Close test exception")
  }

  override def config: Config = conf

  override def validate(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate): Reason = {
    if (throwValidationException)
      throw new RuntimeException("Validation test exception")
    validationReasonToReturn
  }

  override def getRecordCount(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate): Long = {
    getRecordCountCalled += 1
    recordCountToReturn
  }

  override def getData(query: Query, infoDateBegin: LocalDate, infoDateEnd: LocalDate, columns: Seq[String]): SourceResult = {
    getDataCalled += 1

    import spark.implicits._

    val df = List(("A", 1), ("B", 2), ("C", 3)).toDF("a", "b")
    SourceResult(df, Nil, Nil)
  }
}

object SourceMock extends ExternalChannelFactory[SourceMock] {
  val MOCK_ALWAYS_AVAILABLE = "mock.always.available"

  val THROW_CONNECTION_EXCEPTION = "throw.connection.exception"
  val THROW_CLOSE_EXCEPTION = "throw.close.exception"
  val THROW_VALIDATION_EXCEPTION = "throw.validation.exception"
  val RECORD_COUNT_TO_RETURN = "record.count.to.return"
  val VALIDATION_WARNING = "validation.warning"

  override def apply(conf: Config, parentPath: String, spark: SparkSession): SourceMock = {
    val mockAlwaysAvailable = ConfigUtils.getOptionBoolean(conf, MOCK_ALWAYS_AVAILABLE).getOrElse(false)

    val throwConnectionException = ConfigUtils.getOptionBoolean(conf, THROW_CONNECTION_EXCEPTION).getOrElse(false)
    val throwCloseException = ConfigUtils.getOptionBoolean(conf, THROW_CLOSE_EXCEPTION).getOrElse(false)
    val throwValidationException = ConfigUtils.getOptionBoolean(conf, THROW_VALIDATION_EXCEPTION).getOrElse(false)
    val recordCountToReturn = ConfigUtils.getOptionInt(conf, RECORD_COUNT_TO_RETURN).getOrElse(1)

    val validationReasonToReturn = ConfigUtils.getOptionString(conf, VALIDATION_WARNING) match {
      case Some(warning) => Reason.Warning(Seq(warning))
      case None => Reason.Ready
    }

    val source = new SourceMock(
      conf,
      mockAlwaysAvailable,
      throwConnectionException,
      throwValidationException,
      throwCloseException,
      recordCountToReturn,
      validationReasonToReturn)(spark)

    source
  }
}
