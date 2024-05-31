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

package za.co.absa.pramen.extras.utils

import org.apache.hadoop.fs.Path
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.extras.base.SparkTestBase
import za.co.absa.pramen.extras.fixtures.TempDirFixture

import java.nio.file.Paths

class FsUtilsSuite extends AnyWordSpec with SparkTestBase with TempDirFixture {
  private val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, "/tmp")

  "getDirectorySize" should {
    "return the file size in bytes" in {
      withTempDirectory("FsUtilsSuite") {
        tempDir =>
          createBinFile(tempDir, "data1.bin", 541)

          assert(fsUtils.getDirectorySize(Paths.get(tempDir, "data1.bin").toString) == 541)
      }
    }
  }

  "splitUriPath" should {
    "split a relative path" in {
      assert(fsUtils.splitUriPath(new Path("a/b/c")) == ("", "a/b/c"))
    }

    "split an absolute path" in {
      assert(fsUtils.splitUriPath(new Path("/a/b/c")) == ("", "/a/b/c"))
    }

    "split a path with schema" in {
      assert(fsUtils.splitUriPath(new Path("s3:///a/b/c")) == ("s3://", "/a/b/c"))
    }

    "split a path with authority" in {
      intercept[IllegalArgumentException] {
        fsUtils.splitUriPath(new Path("hostname:888/a/b/c")) == ("hostname:888", "/a/b/c")
      }
    }

    "split a path with schema and authority" in {
      assert(fsUtils.splitUriPath(new Path("hdfs://hostname:888/a/b/c")) == ("hdfs://hostname:888", "/a/b/c"))
      assert(fsUtils.splitUriPath(new Path("s3://bucket/a/b/c")) == ("s3://bucket", "/a/b/c"))
      assert(fsUtils.splitUriPath(new Path("s3://bucket")) == ("s3://bucket", ""))
    }
  }

  "getFilesRecursive" should {
    "return a single file" in {
      withTempDirectory("FsUtilsSuite") { tempDir =>
        val path = new Path(tempDir, "data1.bin")

        fsUtils.appendFile(path, "0123456789")

        val lst = fsUtils.getFilesRecursive(path, "*")

        assert(lst.nonEmpty)
        assert(lst.head == path)
      }
    }

    "all files from a directory" in {
      withTempDirectory("FsUtilsSuite") { tempDir =>
        val path1 = new Path(tempDir, "data1.bin")
        val path2 = new Path(tempDir, "data2.txt")
        val path3 = new Path(tempDir, "data3")
        val path4 = new Path(tempDir, "_data4")
        val path5 = new Path(tempDir, ".data5")

        fsUtils.appendFile(path1, "0123456789")
        fsUtils.appendFile(path2, "0123456789")
        fsUtils.appendFile(path3, "0123456789")
        fsUtils.appendFile(path4, "0123456789")
        fsUtils.appendFile(path5, "0123456789")

        val lst1 = fsUtils.getFilesRecursive(new Path(tempDir))
        val lst2 = fsUtils.getFilesRecursive(new Path(tempDir), "*", includeHiddenFiles = true)

        assert(lst1.nonEmpty)
        assert(lst1.exists(_.getName == path1.getName))
        assert(lst1.exists(_.getName == path2.getName))
        assert(lst1.exists(_.getName == path3.getName))
        assert(!lst1.exists(_.getName == path4.getName))
        assert(!lst1.exists(_.getName == path5.getName))
        assert(lst2.nonEmpty)
        assert(lst2.exists(_.getName == path1.getName))
        assert(lst2.exists(_.getName == path2.getName))
        assert(lst2.exists(_.getName == path3.getName))
        assert(lst2.exists(_.getName == path4.getName))
        assert(lst2.exists(_.getName == path5.getName))
      }
    }

    "filtered files from a directory" in {
      withTempDirectory("FsUtilsSuite") { tempDir =>
        val path1 = new Path(tempDir, "data1.bin")
        val path2 = new Path(tempDir, "data2.txt")
        val path3 = new Path(tempDir, "data3")

        fsUtils.appendFile(path1, "0123456789")
        fsUtils.appendFile(path2, "0123456789")
        fsUtils.appendFile(path3, "0123456789")

        val lst = fsUtils.getFilesRecursive(new Path(tempDir), "*.txt")

        assert(lst.nonEmpty)
        assert(!lst.exists(_.getName == path1.getName))
        assert(lst.exists(_.getName == path2.getName))
        assert(!lst.exists(_.getName == path3.getName))
      }
    }

    "files from subdirectories" in {
      withTempDirectory("FsUtilsSuite") { tempDir =>
        val subPath1 = new Path(tempDir, "a")
        val subPath2 = new Path(subPath1, "b")
        val subPath3 = new Path(tempDir, "c")
        val subPath4 = new Path(tempDir, "_d")

        val file1 = new Path(subPath1, "data1.bin")
        val file2 = new Path(subPath1, "data2.txt")
        val file3 = new Path(subPath2, "data3.bin")
        val file4 = new Path(subPath2, "data4.txt")
        val file5 = new Path(subPath3, "data5.bin")
        val file6 = new Path(subPath3, "data6.txt")
        val file7 = new Path(subPath1, "_data7.txt")
        val file8 = new Path(subPath2, ".data8.txt")
        val file9 = new Path(subPath3, "_data9.txt")
        val file10 = new Path(subPath4, "data10.txt")
        val file11 = new Path(subPath4, "_data11.txt")

        fsUtils.appendFile(file1, "0123456789")
        fsUtils.appendFile(file2, "0123456789")
        fsUtils.appendFile(file3, "0123456789")
        fsUtils.appendFile(file4, "0123456789")
        fsUtils.appendFile(file5, "0123456789")
        fsUtils.appendFile(file6, "0123456789")
        fsUtils.appendFile(file7, "0123456789")
        fsUtils.appendFile(file8, "0123456789")
        fsUtils.appendFile(file9, "0123456789")
        fsUtils.appendFile(file10, "0123456789")
        fsUtils.appendFile(file11, "0123456789")

        val lst1 = fsUtils.getFilesRecursive(new Path(tempDir), "*.txt")
        val lst2 = fsUtils.getFilesRecursive(new Path(tempDir), "*.txt", includeHiddenFiles = true)

        assert(lst1.nonEmpty)
        assert(!lst1.exists(_.getName == file1.getName))
        assert(lst1.exists(_.getName == file2.getName))
        assert(!lst1.exists(_.getName == file3.getName))
        assert(lst1.exists(_.getName == file4.getName))
        assert(!lst1.exists(_.getName == file5.getName))
        assert(lst1.exists(_.getName == file6.getName))
        assert(!lst1.exists(_.getName == file7.getName))
        assert(!lst1.exists(_.getName == file8.getName))
        assert(!lst1.exists(_.getName == file9.getName))
        assert(!lst1.exists(_.getName == file10.getName))
        assert(!lst1.exists(_.getName == file11.getName))

        assert(lst2.nonEmpty)
        assert(!lst2.exists(_.getName == file1.getName))
        assert(lst2.exists(_.getName == file2.getName))
        assert(!lst2.exists(_.getName == file3.getName))
        assert(lst2.exists(_.getName == file4.getName))
        assert(!lst2.exists(_.getName == file5.getName))
        assert(lst2.exists(_.getName == file6.getName))
        assert(lst2.exists(_.getName == file7.getName))
        assert(lst2.exists(_.getName == file8.getName))
        assert(lst2.exists(_.getName == file9.getName))
        assert(lst2.exists(_.getName == file10.getName))
        assert(lst2.exists(_.getName == file11.getName))

      }
    }
  }

  "appendFile" should {
    "append to a new  file" in {
      withTempDirectory("FsUtilsSuite") { tempDir =>
        val path = new Path(tempDir, "data1.bin")

        fsUtils.appendFile(path, "0123456789")

        assert(fsUtils.getDirectorySize(path.toString) == 10)
      }
    }

    "append new data to an existing file" in {
      withTempDirectory("FsUtilsSuite") { tempDir =>
        createBinFile(tempDir, "data1.bin", 100)

        val path = new Path(tempDir, "data1.bin")

        fsUtils.appendFile(path, "0123456789")

        assert(fsUtils.getDirectorySize(path.toString) == 110)
      }
    }
  }

  "safeWriteFile" should {
    "write the data to a file" in {
      withTempDirectory("FsUtilsSuite") { tempDir =>
        val path = new Path(tempDir, "data1.bin")

        fsUtils.safeWriteFile(path, "0123456789")

        assert(fsUtils.getDirectorySize(path.toString) == 10)
      }
    }
  }

  "renamePath" should {
    "rename a file" in {
      withTempDirectory("FsUtilsSuite") { tempDir =>
        createBinFile(tempDir, "data1.bin", 100)

        val pathSrc = new Path(tempDir, "data1.bin")
        val pathDst = new Path(tempDir, "data2.bin")

        fsUtils.renamePath(pathSrc, pathDst)

        assert(!fsUtils.exists(pathSrc))
        assert(fsUtils.exists(pathDst))
      }
    }

    "rename a file when the target already exists" in {
      withTempDirectory("FsUtilsSuite") { tempDir =>
        createBinFile(tempDir, "data1.bin", 100)
        createBinFile(tempDir, "data2.bin", 100)

        val pathSrc = new Path(tempDir, "data1.bin")
        val pathDst = new Path(tempDir, "data2.bin")

        fsUtils.renamePath(pathSrc, pathDst)

        assert(!fsUtils.exists(pathSrc))
        assert(fsUtils.exists(pathDst))
      }
    }
  }

}
