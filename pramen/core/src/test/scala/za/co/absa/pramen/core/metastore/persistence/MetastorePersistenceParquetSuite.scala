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

package za.co.absa.pramen.core.metastore.persistence

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.mockito.Mockito.{mock, when}
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.fixtures.{TempDirFixture, TextComparisonFixture}
import za.co.absa.pramen.core.metastore.peristence.MetastorePersistenceParquet
import za.co.absa.pramen.core.utils.FsUtils

/**
  * This test suite tests only parquet-specific functionality. The general behavior is tested in [[MetastorePersistenceSuite]].
  */
class MetastorePersistenceParquetSuite extends AnyWordSpec with SparkTestBase with TempDirFixture with TextComparisonFixture {
  import spark.implicits._

  private val exampleDf: DataFrame = List(("A", 1), ("B", 2), ("C", 3)).toDF("a", "b")
  private val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, "file:///tmp")

  "writeAndCleanOnFailure()" should {
    "write data to the specific partition folder" in {
      withTempDirectory("metastore_parquet") { tempDir =>
        val outputPath = new Path(tempDir, "partition=10")

        val persistence = new MetastorePersistenceParquet(tempDir, "ignore", "yyyy-MM-dd", None, None, Map.empty, Map.empty)

        persistence.writeAndCleanOnFailure(exampleDf, outputPath.toString, fsUtils, SaveMode.Overwrite)

        assert(fsUtils.exists(outputPath))
        assert(fsUtils.getFilesRecursive(outputPath, "*.parquet").nonEmpty)
      }
    }

    "append data when save mode is append" in {
      withTempDirectory("metastore_parquet") { tempDir =>
        val outputPath = new Path(tempDir, "partition=10")

        val persistence = new MetastorePersistenceParquet(tempDir, "ignore", "yyyy-MM-dd", None, Some(SaveMode.Append), Map.empty, Map.empty)

        persistence.writeAndCleanOnFailure(exampleDf, outputPath.toString, fsUtils, SaveMode.Append)
        persistence.writeAndCleanOnFailure(exampleDf, outputPath.toString, fsUtils, SaveMode.Append)

        assert(fsUtils.exists(outputPath))
        assert(fsUtils.getFilesRecursive(outputPath, "*.parquet").nonEmpty)

        val df = spark.read.parquet(outputPath.toString)

        assert(df.count() == 6)
      }
    }

    "re-throw on failure" in {
      val df = mock(classOf[DataFrame])

      when(df.write).thenThrow(new RuntimeException("test exception"))

      val persistence = new MetastorePersistenceParquet("dummy", "ignore", "yyyy-MM-dd", None, None, Map.empty, Map.empty)

      assertThrows[RuntimeException] {
        persistence.writeAndCleanOnFailure(df, "dummy", fsUtils, SaveMode.Overwrite)
      }
    }

    "clean the empty directory if created" in {
      val df = mock(classOf[DataFrame])

      when(df.write).thenThrow(new RuntimeException("test exception"))

      withTempDirectory("metastore_parquet") { tempDir =>
        val outputPath = new Path(tempDir, "partition=10")

        fsUtils.createDirectoryRecursive(outputPath)

        val persistence = new MetastorePersistenceParquet("dummy", "ignore", "yyyy-MM-dd", None, None, Map.empty, Map.empty)

        val ex = intercept[RuntimeException] {
          persistence.writeAndCleanOnFailure(df, outputPath.toString, fsUtils, SaveMode.Overwrite)
        }

        assert(ex.getMessage.contains("test exception"))
        assert(!fsUtils.exists(outputPath))
      }
    }
  }
}
