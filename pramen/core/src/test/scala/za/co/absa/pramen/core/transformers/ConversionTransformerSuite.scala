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

package za.co.absa.pramen.core.transformers

import org.apache.hadoop.fs.Path
import org.mockito.Mockito.{mock, when => whenMock}
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.{DataFormat, MetastoreReader, Reason}
import za.co.absa.pramen.core.MetaTableDefFactory.getDummyMetaTableDef
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.fixtures.{TempDirFixture, TextComparisonFixture}
import za.co.absa.pramen.core.utils.FsUtils

import java.time.LocalDate

class ConversionTransformerSuite extends AnyWordSpec with SparkTestBase with TempDirFixture with TextComparisonFixture {
  import spark.implicits._

  private val infoDateWithData = LocalDate.of(2023, 1, 18)
  private val infoDateWithEmptyDf = LocalDate.of(2023, 1, 19)

  private val conversionOptions = Map(
    "input.table" -> "table1",
    "input.format" -> "csv",
    "header" -> "true"
  )

  "validate()" should {
    "pass when everything is in order" in {
      withTempDirectory("comparison_transformer") { tempDir =>
        val (transformer, metastoreReader) = getUseCase(tempDir)

        val result = transformer.validate(metastoreReader, infoDateWithData, conversionOptions)

        assert(result == Reason.Ready)
      }
    }

    "return SkipOnce when there is no data" in {
      withTempDirectory("comparison_transformer") { tempDir =>
        val (transformer, metastoreReader) = getUseCase(tempDir)

        val result = transformer.validate(metastoreReader, infoDateWithEmptyDf, conversionOptions)

        assert(result.isInstanceOf[Reason.SkipOnce])
      }
    }

    "fail if a mandatory option is missing" in {
      withTempDirectory("comparison_transformer") { tempDir =>
        val (transformer, metastoreReader) = getUseCase(tempDir)

        assertThrows[IllegalArgumentException] {
          transformer.validate(metastoreReader, infoDateWithData, conversionOptions.filter(_._1 != "input.table"))
        }
      }
    }

    "fail if a wrong format is specified option is missing" in {
      withTempDirectory("comparison_transformer") { tempDir =>
        val (transformer, metastoreReader) = getUseCase(tempDir, useWrongFormat = true)

        assertThrows[IllegalArgumentException] {
          transformer.validate(metastoreReader, infoDateWithData, conversionOptions)
        }
      }
    }
  }

  "run()" should {
    val expected =
      """{"id":"1","name":"John"}
        |{"id":"2","name":"Jack"}
        |{"id":"3","name":"Jill"}
        |{"id":"4","name":"Mary"}
        |{"id":"5","name":"Jane"}
        |{"id":"6","name":"Kate"}
        |""".stripMargin

    "convert files to a dataframe when inputs are loaded from a list of files dataframe" in {
      withTempDirectory("comparison_transformer") { tempDir =>
        val (transformer, metastoreReader) = getUseCase(tempDir)

        val df = transformer.run(metastoreReader, infoDateWithData, conversionOptions + ("use.file.list" -> "true"))

        val actual = df.orderBy("id").toJSON.collect().mkString("\n")

        assert(df.count() == 6)

        compareText(actual, expected)
      }
    }

    "convert files to a dataframe when inputs are loaded from the table metadata" in {
      withTempDirectory("comparison_transformer") { tempDir =>
        val (transformer, metastoreReader) = getUseCase(tempDir)

        val df = transformer.run(metastoreReader, infoDateWithData, conversionOptions)

        val actual = df.orderBy("id").toJSON.collect().mkString("\n")

        assert(df.count() == 6)

        compareText(actual, expected)
      }
    }
  }

  def getUseCase(tempDir: String, useWrongFormat: Boolean = false): (ConversionTransformer, MetastoreReader) = {
    val metastoreReadeMock = mock(classOf[MetastoreReader])

    val basePath = new Path(tempDir, "table1")
    val partitionDir = new Path(basePath, "info_date=2023-01-18")
    val file1 = new Path(partitionDir, "file1.csv")
    val file2 = new Path(partitionDir, "file2.csv")

    val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, tempDir)

    fsUtils.createDirectoryRecursive(partitionDir)
    fsUtils.writeFile(file1, "id,name\n1,John\n2,Jack\n3,Jill\n")
    fsUtils.writeFile(file2, "id,name\n4,Mary\n5,Jane\n6,Kate\n")

    val filesDf = Seq(file1.toString, file2.toString).toDF("path")
    val emptyDf = filesDf.filter($"path" === "_")

    val tableFormat = if (useWrongFormat) {
      DataFormat.Parquet(basePath.toString, None)
    } else {
      DataFormat.Raw(basePath.toString)
    }

    val metaTableDef = getDummyMetaTableDef(name = "table1", format = tableFormat)

    whenMock(metastoreReadeMock.getTable("table1")).thenReturn(filesDf)
    whenMock(metastoreReadeMock.getTableDef("table1")).thenReturn(metaTableDef)
    whenMock(metastoreReadeMock.getTable("table1", Some(infoDateWithData), Some(infoDateWithData))).thenReturn(filesDf)
    whenMock(metastoreReadeMock.getTable("table1", Some(infoDateWithEmptyDf), Some(infoDateWithEmptyDf))).thenReturn(emptyDf)

    (new ConversionTransformer(), metastoreReadeMock)
  }
}
