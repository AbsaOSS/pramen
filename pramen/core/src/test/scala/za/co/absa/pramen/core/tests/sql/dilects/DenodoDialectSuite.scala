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

package za.co.absa.pramen.core.tests.sql.dilects

import org.apache.spark.sql.types.{MetadataBuilder, TimestampType}
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.sql.dialects.DenodoDialect

import java.sql.Types.TIMESTAMP_WITH_TIMEZONE

class DenodoDialectSuite extends AnyWordSpec {
  "canHandle" should {
    "be able to handle denodo jdbc url" in {
      val url = "jdbc:denodo://localhost:9999"

      DenodoDialect.canHandle(url) shouldBe true
    }
  }

  "getCatalystType" should {
    "convert timestamp with timezone to timestamp type" in {
      val sqlType = TIMESTAMP_WITH_TIMEZONE
      val typeName = "TIMESTAMP WITH TIME ZONE"
      val size = 0
      val md = new MetadataBuilder

      val result = DenodoDialect.getCatalystType(sqlType, typeName, size, md)

      result shouldBe Some(TimestampType)
    }

    "not convert other types" in {
      val sqlType = 0
      val typeName = "UNKNOWN"
      val size = 0
      val md = new MetadataBuilder

      val result = DenodoDialect.getCatalystType(sqlType, typeName, size, md)

      result shouldBe None
    }
  }
}
