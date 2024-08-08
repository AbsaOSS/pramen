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

package za.co.absa.pramen.core.metastore.model

import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.DataFormat._
import za.co.absa.pramen.api.{CachePolicy, Query}
import za.co.absa.pramen.core.metastore.model.DataFormatParser.{PATH_KEY, TABLE_KEY}

class DataFormatSuite extends AnyWordSpec {
  "fromConfig()" should {
    "use 'parquet' as the default format" in {
      val conf = ConfigFactory.parseString("path = /a/b/c")

      val format = DataFormatParser.fromConfig(conf, conf)

      assert(format.name == "parquet")
      assert(!format.isTransient)
      assert(format.isInstanceOf[Parquet])
      assert(format.asInstanceOf[Parquet].path == "/a/b/c")
      assert(format.asInstanceOf[Parquet].recordsPerPartition.isEmpty)
    }

    "use 'parquet' when specified explicitly" in {
      val conf = ConfigFactory.parseString(
        """format = parquet
          |path = /a/b/c
          |records.per.partition = 100
          |""".stripMargin)

      val format = DataFormatParser.fromConfig(conf, conf)

      assert(format.name == "parquet")
      assert(!format.isTransient)
      assert(format.isInstanceOf[Parquet])
      assert(format.asInstanceOf[Parquet].path == "/a/b/c")
      assert(format.asInstanceOf[Parquet].recordsPerPartition.contains(100))
    }

    "use 'delta' when specified explicitly" in {
      val conf = ConfigFactory.parseString(
        """format = delta
          |path = /a/b/c
          |records.per.partition = 200
          |""".stripMargin)

      val format = DataFormatParser.fromConfig(conf, conf)

      assert(format.name == "delta")
      assert(!format.isTransient)
      assert(format.isInstanceOf[Delta])
      assert(format.asInstanceOf[Delta].query.isInstanceOf[Query.Path])
      assert(format.asInstanceOf[Delta].query.query == "/a/b/c")
      assert(format.asInstanceOf[Delta].recordsPerPartition.contains(200))
    }

    "use 'raw' when specified explicitly" in {
      val conf = ConfigFactory.parseString(
        """format = raw
          |path = /a/b/c
          |""".stripMargin)

      val format = DataFormatParser.fromConfig(conf, conf)

      assert(format.name == "raw")
      assert(!format.isTransient)
      assert(format.isInstanceOf[Raw])
      assert(format.asInstanceOf[Raw].path == "/a/b/c")
    }

    "use 'transient_eager' when specified explicitly" in {
      val conf = ConfigFactory.parseString("format = transient_eager")

      val format = DataFormatParser.fromConfig(conf, conf)

      assert(format.name == "transient_eager")
      assert(format.isTransient)
      assert(!format.isLazy)
      assert(format.isInstanceOf[TransientEager])
      assert(format.asInstanceOf[TransientEager].cachePolicy == CachePolicy.NoCache)
    }

    "support cache policies for 'transient_eager' format" in {
      val conf = ConfigFactory.parseString(
        """format = transient_eager
          |cache.policy = cache
          |""".stripMargin)

      val format = DataFormatParser.fromConfig(conf, conf)

      assert(format.name == "transient_eager")
      assert(format.isTransient)
      assert(!format.isLazy)
      assert(format.isInstanceOf[TransientEager])
      assert(format.asInstanceOf[TransientEager].cachePolicy == CachePolicy.Cache)
    }

    "use 'transient' when specified explicitly" in {
      val conf = ConfigFactory.parseString("format = transient")

      val format = DataFormatParser.fromConfig(conf, conf)

      assert(format.name == "transient")
      assert(format.isTransient)
      assert(format.isLazy)
      assert(format.isInstanceOf[Transient])
      assert(format.asInstanceOf[Transient].cachePolicy == CachePolicy.NoCache)
    }

    "support cache policies for 'transient' format" in {
      val conf = ConfigFactory.parseString(
        """format = transient
          |cache.policy = cache
          |""".stripMargin)

      val format = DataFormatParser.fromConfig(conf, conf)

      assert(format.name == "transient")
      assert(format.isTransient)
      assert(format.isInstanceOf[Transient])
      assert(format.asInstanceOf[Transient].cachePolicy == CachePolicy.Cache)
    }

    "use default records per partition" in {
      val conf = ConfigFactory.parseString(
        """format = delta
          |path = /a/b/c
          |""".stripMargin)

      val appConf = ConfigFactory.parseString("pramen.default.records.per.partition = 100")

      val format = DataFormatParser.fromConfig(conf, appConf)

      assert(format.name == "delta")
      assert(!format.isTransient)
      assert(format.isInstanceOf[Delta])
      assert(format.asInstanceOf[Delta].query.isInstanceOf[Query.Path])
      assert(format.asInstanceOf[Delta].query.query == "/a/b/c")
      assert(format.asInstanceOf[Delta].recordsPerPartition.contains(100))
    }

    "throw an exception on unknown format" in {
      val conf = ConfigFactory.parseString("format = iceberg")

      val ex = intercept[IllegalArgumentException] {
        DataFormatParser.fromConfig(conf, conf)
      }

      assert(ex.getMessage.contains("Unknown format: iceberg"))
    }

    "throw an exception on mandatory options missing" in {
      val conf = ConfigFactory.parseString("format = parquet")

      val ex = intercept[IllegalArgumentException] {
        DataFormatParser.fromConfig(conf, conf)
      }

      assert(ex.getMessage.contains("Mandatory option missing: path"))
    }

    "throw an exception when path is not specified for 'raw'" in {
      val conf = ConfigFactory.parseString("format = raw")

      val ex = intercept[IllegalArgumentException] {
        DataFormatParser.fromConfig(conf, conf)
      }

      assert(ex.getMessage.contains("Mandatory option for a metastore table having 'raw' format"))
    }
  }

  "getCachePolicy" should {
    "return None when no policy is defined" in {
      val conf = ConfigFactory.empty()

      val policy = DataFormatParser.getCachePolicy(conf)

      assert(policy.isEmpty)
    }

    "return the proper object for policy 'cache'" in {
      val conf = ConfigFactory.parseString("cache.policy = cache")

      val policy = DataFormatParser.getCachePolicy(conf)

      assert(policy.isDefined)
      assert(policy.get == CachePolicy.Cache)
    }

    "return the proper object for policy 'no_cache'" in {
      val conf = ConfigFactory.parseString("cache.policy = no_cache")

      val policy = DataFormatParser.getCachePolicy(conf)

      assert(policy.isDefined)
      assert(policy.get == CachePolicy.NoCache)
    }

    "return the proper object for policy 'transient'" in {
      val conf = ConfigFactory.parseString("cache.policy = persist")

      val policy = DataFormatParser.getCachePolicy(conf)

      assert(policy.isDefined)
      assert(policy.get == CachePolicy.Persist)
    }

    "throw wn exception on invalid cache policy string" in {
      val conf = ConfigFactory.parseString("cache.policy = dummy")

      val ex = intercept[IllegalArgumentException] {
        DataFormatParser.getCachePolicy(conf)
      }

      assert(ex.getMessage.contains("Incorrect cache policy: dummy"))
    }
  }

  "getQuery" should {
    "return the proper object for path" in {
      val conf = ConfigFactory.parseString("path = /a/b/c")

      val query = DataFormatParser.getQuery(conf)

      assert(query.isInstanceOf[Query.Path])
      assert(query.asInstanceOf[Query.Path].path == "/a/b/c")
    }

    "return the proper object for table" in {
      val conf = ConfigFactory.parseString("table = db.table")

      val query = DataFormatParser.getQuery(conf)

      assert(query.isInstanceOf[Query.Table])
      assert(query.asInstanceOf[Query.Table].dbTable == "db.table")
    }

    "throw an exception when neither path nor table is specified" in {
      val conf = ConfigFactory.empty()

      val ex = intercept[IllegalArgumentException] {
        DataFormatParser.getQuery(conf)
      }

      assert(ex.getMessage.contains(s"Mandatory option missing: $PATH_KEY or $TABLE_KEY"))
    }
  }
}
