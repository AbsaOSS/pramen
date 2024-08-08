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
import org.apache.spark.sql.SaveMode
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.fixtures.{TempDirFixture, TextComparisonFixture}
import za.co.absa.pramen.core.metastore.peristence.MetastorePersistenceRaw
import za.co.absa.pramen.core.utils.FsUtils

import java.time.LocalDate

/**
  * This test suite for the raw format, which is just keeping partitioned files without looking in its contants.
  */
class MetastorePersistenceRawSuite extends AnyWordSpec with SparkTestBase with TempDirFixture with TextComparisonFixture {
  import spark.implicits._

  private val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, "file:///tmp")
  private val infoDate = LocalDate.of(2022, 2, 18)
  private val infoDateTo = LocalDate.of(2022, 2, 19)
  private val infoDateColumn = "pramen_info_date"
  private val infoDateFormat = "yyyy-MM-dd"

  "loadTable()" should {
    "be able to read files in a partitioned directory" in {
      withTempDirectory("metastore_raw") { tempDir =>

        val dataPath = new Path(tempDir, s"$infoDateColumn=$infoDate")

        fsUtils.createDirectoryRecursive(dataPath)
        fsUtils.fs.create(new Path(dataPath, "1.dat")).close()
        fsUtils.fs.create(new Path(dataPath, "2.dat")).close()

        val persistence = new MetastorePersistenceRaw(tempDir, infoDateColumn, infoDateFormat, None)

        val actual = persistence.loadTable(Some(infoDate), Some(infoDate)).orderBy("path").collect().map(_.getString(0))

        assert(actual.length == 2)
        assert(actual.exists(_.contains("/pramen_info_date=2022-02-18/1.dat")))
        assert(actual.exists(_.contains("/pramen_info_date=2022-02-18/2.dat")))
      }
    }

    "be able to read files in a partitioned range" in {
      withTempDirectory("metastore_raw") { tempDir =>

        val dataPath1 = new Path(tempDir, s"$infoDateColumn=$infoDate")
        val dataPath2 = new Path(tempDir, s"$infoDateColumn=$infoDateTo")

        fsUtils.createDirectoryRecursive(dataPath1)
        fsUtils.createDirectoryRecursive(dataPath2)
        fsUtils.fs.create(new Path(dataPath1, "1.dat")).close()
        fsUtils.fs.create(new Path(dataPath2, "2.dat")).close()

        val persistence = new MetastorePersistenceRaw(tempDir, infoDateColumn, infoDateFormat, None)

        val actual = persistence.loadTable(Some(infoDate), Some(infoDateTo)).orderBy("path").collect().map(_.getString(0))

        assert(actual.length == 2)
        assert(actual.exists(_.contains("/pramen_info_date=2022-02-18/1.dat")))
        assert(actual.exists(_.contains("/pramen_info_date=2022-02-19/2.dat")))
      }
    }

    "return an empty dataframe if the dat range is wrong" in {
      withTempDirectory("metastore_raw") { tempDir =>

        val dataPath = new Path(tempDir, s"$infoDateColumn=$infoDate")

        fsUtils.createDirectoryRecursive(dataPath)
        fsUtils.fs.create(new Path(dataPath, "1.dat")).close()
        fsUtils.fs.create(new Path(dataPath, "2.dat")).close()

        val persistence = new MetastorePersistenceRaw(tempDir, infoDateColumn, infoDateFormat, None)

        val actual = persistence.loadTable(Some(infoDate.plusDays(1)), Some(infoDate)).orderBy("path").collect().map(_.getString(0))

        assert(actual.isEmpty)
      }
    }

    "throw an exception is no date range is provided" in {
      withTempDirectory("metastore_raw") { tempDir =>

        val dataPath = new Path(tempDir, s"$infoDateColumn=$infoDate")

        fsUtils.createDirectoryRecursive(dataPath)

        val persistence = new MetastorePersistenceRaw(tempDir, infoDateColumn, infoDateFormat, None)

        assertThrows[IllegalArgumentException] {
          persistence.loadTable(None, None)
        }
      }
    }
  }

  "saveTable()" should {
    "do nothing on an empty dataset" in {
      withTempDirectory("metastore_raw") { tempDir =>
        val persistence = new MetastorePersistenceRaw(tempDir, infoDateColumn, infoDateFormat, None)

        persistence.saveTable(infoDate, Seq.empty[String].toDF("path"), None)

        val partitionPath = new Path(tempDir, s"$infoDateColumn=$infoDate")
        assert(fsUtils.exists(partitionPath))

        assert(fsUtils.getHadoopFiles(partitionPath).isEmpty)
      }
    }

    "copy files to the target directory" in {
      withTempDirectory("metastore_raw") { tempDir =>
        val file1 = new Path(tempDir, "1.dat")
        val file2 = new Path(tempDir, "2.dat")

        fsUtils.fs.create(file1).close()
        fsUtils.fs.create(file2).close()

        val files = Seq(file1, file2).map(_.toUri.toString).toDF("path")

        val persistence = new MetastorePersistenceRaw(tempDir, infoDateColumn, infoDateFormat, Some(SaveMode.Overwrite))

        persistence.saveTable(infoDate, files, None)

        val partitionPath = new Path(tempDir, s"$infoDateColumn=$infoDate")

        assert(fsUtils.exists(new Path(partitionPath, "1.dat")))
        assert(fsUtils.exists(new Path(partitionPath, "2.dat")))
      }
    }

    "delete existing partition directory if it exists" in {
      withTempDirectory("metastore_raw") { tempDir =>
        val file1 = new Path(tempDir, "1.dat")
        val file2 = new Path(tempDir, "2.dat")

        fsUtils.fs.create(file1).close()
        fsUtils.fs.create(file2).close()

        val files = Seq(file1, file2).map(_.toUri.toString).toDF("path")

        val partitionPath = new Path(tempDir, s"$infoDateColumn=$infoDate")

        fsUtils.fs.create(new Path(partitionPath, "3.dat"))

        val persistence = new MetastorePersistenceRaw(tempDir, infoDateColumn, infoDateFormat, None)

        persistence.saveTable(infoDate, files, None)

        assert(fsUtils.exists(new Path(partitionPath, "1.dat")))
        assert(fsUtils.exists(new Path(partitionPath, "2.dat")))
        assert(!fsUtils.exists(new Path(partitionPath, "3.dat")))

        persistence.saveTable(infoDate, Seq.empty[String].toDF("path"), None)

        assert(!fsUtils.exists(new Path(partitionPath, "1.dat")))
      }
    }

    "append existing partition directory if it exists for save mode append" in {
      withTempDirectory("metastore_raw") { tempDir =>
        val file1 = new Path(tempDir, "1.dat")
        val file2 = new Path(tempDir, "2.dat")

        fsUtils.fs.create(file1).close()
        fsUtils.fs.create(file2).close()

        val files = Seq(file1, file2).map(_.toUri.toString).toDF("path")

        val partitionPath = new Path(tempDir, s"$infoDateColumn=$infoDate")

        fsUtils.fs.create(new Path(partitionPath, "3.dat"))

        val persistence = new MetastorePersistenceRaw(tempDir, infoDateColumn, infoDateFormat, Some(SaveMode.Append))

        persistence.saveTable(infoDate, files, None)

        assert(fsUtils.exists(new Path(partitionPath, "1.dat")))
        assert(fsUtils.exists(new Path(partitionPath, "2.dat")))
        assert(fsUtils.exists(new Path(partitionPath, "3.dat")))

        persistence.saveTable(infoDate, Seq.empty[String].toDF("path"), None)

        assert(fsUtils.exists(new Path(partitionPath, "1.dat")))
        assert(fsUtils.exists(new Path(partitionPath, "2.dat")))
        assert(fsUtils.exists(new Path(partitionPath, "3.dat")))
      }
    }

    "throw an exception if the dataframe does not contain the required column" in {
      withTempDirectory("metastore_raw") { tempDir =>

        val persistence = new MetastorePersistenceRaw(tempDir, infoDateColumn, infoDateFormat, None)

        assertThrows[IllegalArgumentException] {
          persistence.saveTable(infoDate, spark.emptyDataFrame, None)
        }
      }
    }
  }

  "getStats" should {
    "return the number of files and the total size" in {
      withTempDirectory("metastore_raw") { tempDir =>
        val partitionPath = new Path(tempDir, s"$infoDateColumn=$infoDate")

        val file1 = new Path(partitionPath, "1.dat")
        val file2 = new Path(partitionPath, "2.dat")

        fsUtils.writeFile(file1, "123")
        fsUtils.writeFile(file2, "4567")

        val persistence = new MetastorePersistenceRaw(tempDir, infoDateColumn, infoDateFormat, None)

        val stats = persistence.getStats(infoDate)

        assert(stats.recordCount == 2)
        assert(stats.dataSizeBytes.contains(7L))
      }
    }
  }

  "createOrUpdateHiveTable" should {
    "throw the unsupported exception" in {
      val persistence = new MetastorePersistenceRaw("", "", "", None)
      assertThrows[UnsupportedOperationException] {
        persistence.createOrUpdateHiveTable(infoDate, "table", null, null)
      }
    }
  }

  "repairHiveTable" should {
    "throw the unsupported exception" in {
      val persistence = new MetastorePersistenceRaw("", "", "", None)
      assertThrows[UnsupportedOperationException] {
        persistence.repairHiveTable("table", null, null)
      }
    }
  }
}
