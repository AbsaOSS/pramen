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

package za.co.absa.pramen.core.config

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.PartitionScheme

class PartitionSchemeParserSuite extends AnyWordSpec {
  "fromConfig" should {
    "return all None for an empty config" in {
      val conf = ConfigFactory.empty()
      val partitionSchemeOpt = PartitionSchemeParser.fromConfig(conf, "info_date")

      assert(partitionSchemeOpt.isEmpty)
    }

    "return non-partitioned when specified" in {
      val conf = ConfigFactory.parseString("partition.by = false")

      val partitionSchemeOpt = PartitionSchemeParser.fromConfig(conf, "info_date")

      assert(partitionSchemeOpt.contains(PartitionScheme.NotPartitioned))
    }

    "return daily partitioning when specified" in {
      val conf = ConfigFactory.parseString("partition.period = day")

      val partitionSchemeOpt = PartitionSchemeParser.fromConfig(conf, "info_date")

      assert(partitionSchemeOpt.contains(PartitionScheme.PartitionByDay))
    }

    "return monthly with a standard column name when specified" in {
      val conf = ConfigFactory.parseString("partition.period = month")

      val partitionSchemeOpt = PartitionSchemeParser.fromConfig(conf, "info_date")

      assert(partitionSchemeOpt.contains(PartitionScheme.PartitionByMonth("info_date_month", "info_date_year", isVisible = true)))
    }

    "return monthly with a custom column name when specified" in {
      val conf = ConfigFactory.parseString("partition.period = month\npartition.month.column=aaa\npartition.year.column=bbb")

      val partitionSchemeOpt = PartitionSchemeParser.fromConfig(conf, "info_date")

      assert(partitionSchemeOpt.contains(PartitionScheme.PartitionByMonth("aaa", "bbb", isVisible = true)))
    }

    "return year-month with a standard column name when specified" in {
      val conf = ConfigFactory.parseString("partition.period = year_month")

      val partitionSchemeOpt = PartitionSchemeParser.fromConfig(conf, "info_date")

      assert(partitionSchemeOpt.contains(PartitionScheme.PartitionByYearMonth("info_date_month", isVisible = true)))
    }

    "return year-month with a custom column name when specified" in {
      val conf = ConfigFactory.parseString("partition.period = year_month\npartition.month.column=aaa")

      val partitionSchemeOpt = PartitionSchemeParser.fromConfig(conf, "info_date")

      assert(partitionSchemeOpt.contains(PartitionScheme.PartitionByYearMonth("aaa", isVisible = true)))
    }

    "return yearly with a standard column name when specified" in {
      val conf = ConfigFactory.parseString("partition.period = year")

      val partitionSchemeOpt = PartitionSchemeParser.fromConfig(conf, "info_date")

      assert(partitionSchemeOpt.contains(PartitionScheme.PartitionByYear("info_date_year", isVisible = true)))
    }

    "return yearly with a custom column name when specified" in {
      val conf = ConfigFactory.parseString("partition.period = year\npartition.year.column=aaa")

      val partitionSchemeOpt = PartitionSchemeParser.fromConfig(conf, "info_date")

      assert(partitionSchemeOpt.contains(PartitionScheme.PartitionByYear("aaa", isVisible = true)))
    }

    "fail on incompatible options" in {
      val confStr =
        s"""partition.period = week""".stripMargin

      val conf = ConfigFactory.parseString(confStr)

      val ex = intercept[IllegalArgumentException] {
        PartitionSchemeParser.fromConfig(conf, "info_date")
      }

      assert(ex.getMessage == "Invalid value 'week' of 'partition.period'. Valid values are: day, month, year_month, year.")
    }
  }
}

