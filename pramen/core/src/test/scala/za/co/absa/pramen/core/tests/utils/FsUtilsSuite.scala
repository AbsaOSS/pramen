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

package za.co.absa.pramen.core.tests.utils

import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.{Logger, LoggerFactory}
import za.co.absa.pramen.core.base.SparkTestBase
import za.co.absa.pramen.core.fixtures.TempDirFixture
import za.co.absa.pramen.core.utils.{DateUtils, FsUtils}

import java.io.{File, IOException}
import java.nio.file.Paths
import java.time.{Instant, ZoneId}

class FsUtilsSuite extends AnyWordSpec with SparkTestBase with TempDirFixture {
  private val timezoneId = ZoneId.of("Africa/Johannesburg")
  private val fsUtils = new FsUtils(spark.sparkContext.hadoopConfiguration, "/tmp")

  "getDirectorySizeMb" should {
    "return the file size in megabytes" in {
      withTempDirectory("FsUtilsSuite") {
        tempDir =>
          createBinFile(tempDir, "data1.bin", 1024 * 1024 * 5)

          assert(fsUtils.getDirectorySizeMb(Paths.get(tempDir, "data1.bin").toString) == 5)
      }
    }
  }

  "getDirectorySize" should {
    "return the file size in bytes" in {
      withTempDirectory("FsUtilsSuite") {
        tempDir =>
          createBinFile(tempDir, "data1.bin", 541)

          assert(fsUtils.getDirectorySize(Paths.get(tempDir, "data1.bin").toString) == 541)
      }
    }
  }

  "createDirectoryRecursive" should {
    "do nothing if the storage is in object store" in {
      fsUtils.createDirectoryRecursive(new Path("s3://some-non-existing-bucket/a/b/c"))
    }

    "throw an exception if force to create on object storage for a bucket that does not exist" in {
      assertThrows[RuntimeException] {
        fsUtils.createDirectoryRecursive(new Path("s3://some-non-existing-bucket/a/b/c"), skipOnObjectStorage = false)
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

  "getSubPath" should {
    "work with absolute paths" in {
      assert(fsUtils.getSubPath(new Path("/a/b/c/d"), new Path("/a/x/y/z")) == "x/y/z")
    }

    "work with relative paths" in {
      assert(fsUtils.getSubPath(new Path("../a/b/c"), new Path("../a/x/y/z")) == "x/y/z")
    }

    "work with similar paths" in {
      assert(fsUtils.getSubPath(new Path("../a/b/path1"), new Path("../a/b/path2")) == "path2")
    }

    "work with second path longer" in {
      assert(fsUtils.getSubPath(new Path("/a/b/path/c/d/e"), new Path("/a/b/path")) == "")
    }

    "work with paths that have schemas and authorities" in {
      assert(fsUtils.getSubPath(new Path("hdfs://server1:123/a/b/c"), new Path("/a/x/y/z")) == "x/y/z")
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

  "getFilesWithSubdirs" should {
    "generate the list of expected files" in {
      withTempDirectory("FsUtilsSuite") { tempDir =>
        val subPath1 = new Path(tempDir, "a")
        val subPath2 = new Path(subPath1, "b")
        val subPath3 = new Path(tempDir, "c")

        val path1 = new Path(tempDir, "data1.bin")
        val path2 = new Path(tempDir, "data2.txt")
        val path3 = new Path(subPath1, "data3.bin")
        val path4 = new Path(subPath1, "data4.txt")
        val path5 = new Path(subPath2, "data5.bin")
        val path6 = new Path(subPath2, "data6.txt")
        val path7 = new Path(subPath3, "data7.bin")
        val path8 = new Path(subPath3, "data8.txt")

        val files = Seq(path1, path2, path3, path4, path5, path6, path7, path8)

        files.foreach(f => fsUtils.appendFile(f, "0123456789"))

        val lst1 = fsUtils.getFilesWithSubdirs(new Path(tempDir), "*.txt")
        val lst2 = fsUtils.getFilesWithSubdirs(new Path(tempDir))

        assert(lst1.nonEmpty)
        assert(lst2.nonEmpty)
        assert(lst1.length == 4)
        assert(lst2.length == 8)
        assert(lst1.exists { case (p, f) => p.isEmpty && f.getName == "data2.txt" })
        assert(lst1.exists { case (p, f) => p == Seq("a") && f.getName == "data4.txt" })
        assert(lst1.exists { case (p, f) => p == Seq("a", "b") && f.getName == "data6.txt" })
        assert(lst1.exists { case (p, f) => p == Seq("c") && f.getName == "data8.txt" })
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

    "append to a filesystem that supports it" in {
      val basePath = new Path("/a/b/c")
      val fsMock = mock(classOf[FileSystem])
      val ofsMock = mock(classOf[FSDataOutputStream])

      when(fsMock.append(basePath)) thenReturn ofsMock
      doNothing().when(ofsMock).write("123".getBytes())
      doNothing().when(ofsMock).close()

      val fsUtilsMock = getFsUtilsMock(fsMock)

      fsUtilsMock.appendFile(basePath, "123")

      verify(ofsMock, times(1)).write("123".getBytes())
      verify(ofsMock, times(1)).close()
    }

    "re-throw the exception" in {
      val basePath = new Path("/a/b/c")
      val fsMock = mock(classOf[FileSystem])

      when(fsMock.append(basePath)) thenThrow new IllegalArgumentException
      when(fsMock.create(any())) thenThrow new IllegalArgumentException

      val fsUtilsMock = getFsUtilsMock(fsMock)

      intercept[IllegalArgumentException] {
        fsUtilsMock.appendFile(basePath, "123", 1, 1)
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

  "isFileGuardOwned" should {
    "create a new file guard if the file doesn't exist" in {
      withTempDirectory("FsUtilsSuite") { tempDir =>
        val fileGuard = new Path(tempDir, "lock.txt")

        assert(fsUtils.isFileGuardOwned(fileGuard, 1))
        assert(fsUtils.exists(fileGuard))
        assert(fsUtils.isFile(fileGuard))
      }
    }

    "fail to acquire a file that is already acquired" in {
      withTempDirectory("FsUtilsSuite") { tempDir =>
        val fileGuard = new Path(tempDir, "lock.txt")

        assert(fsUtils.isFileGuardOwned(fileGuard, 1))
        assert(!fsUtils.isFileGuardOwned(fileGuard, 1))
      }
    }

    "acquire an expired file" in {
      withTempDirectory("FsUtilsSuite") { tempDir =>
        val fileGuard = new Path(tempDir, "lock.txt")

        fsUtils.writeFile(fileGuard, "10000" /* very old epoch */)

        assert(fsUtils.isFileGuardOwned(fileGuard, 1))
      }
    }

    "return false if unable to write to the file" in {
      val path = new Path("/a/b/c")
      val fsMock = mock(classOf[FileSystem])

      when(fsMock.exists(path)).thenReturn(false)
      when(fsMock.create(path, false)).thenThrow(new IOException)

      val fsUtilsMock = getFsUtilsMock(fsMock)

      assert(!fsUtilsMock.isFileGuardOwned(path, 1))
    }

    "throw an exception if a directory is provided" in {
      withTempDirectory("FsUtilsSuite") { tempDir =>
        val fileGuard = new Path(tempDir, "lock_dir")

        fsUtils.createDirectoryRecursive(fileGuard)

        val ex = intercept[IllegalArgumentException] {
          fsUtils.isFileGuardOwned(fileGuard, 1)
        }

        assert(ex.getMessage.contains("is not a file"))
      }
    }

    "re-throw an unknown exception" in {
      val path = new Path("/a/b/c")
      val fsMock = mock(classOf[FileSystem])

      when(fsMock.exists(path)).thenReturn(false)
      when(fsMock.create(path, false)).thenThrow(new IllegalStateException)

      val fsUtilsMock = getFsUtilsMock(fsMock)

      intercept[IllegalStateException] {
        fsUtilsMock.isFileGuardOwned(path, 1)
      }
    }

  }

  "updateFileGuard" should {
    "write the timed ticked to a file" in {
      withTempDirectory("FsUtilsSuite") { tempDir =>
        val file = new Path(tempDir, "lock.txt")
        fsUtils.updateFileGuard(file, 10)

        val ticketSec = fsUtils.readFile(file).toLong

        val nowSec = Instant.now().getEpochSecond

        assert(nowSec - ticketSec < 10)
      }
    }
  }

  "copyFile" should {
    "copy a file" in {
      withTempDirectory("FsUtilsSuite") { tempDir =>
        createBinFile(tempDir, "data1.bin", 100)

        val pathSrc = new Path(tempDir, "data1.bin")
        val pathDst = new Path(tempDir, "data2.bin")

        fsUtils.copyFile(pathSrc, pathDst)

        assert(fsUtils.exists(pathSrc))
        assert(fsUtils.exists(pathDst))
      }
    }
  }

  "copyToLocal" should {
    "copy a file from Hadoop to local filesystem" in {
      withTempDirectory("FsUtilsSuite") { tempDir =>
        // This test assumes it is running in local mode and the same file system is used for both local and hadoop
        val pathSrc = new Path(tempDir, "hadoop")
        val pathDst = new Path(tempDir, "local")
        val pathSrcFile = new Path(pathSrc, "data1.bin")
        val pathDstFile = new Path(pathDst, "data2.bin")

        fsUtils.createDirectoryRecursive(pathSrc)
        fsUtils.createDirectoryRecursive(pathDst, skipOnObjectStorage = false)

        createBinFile(pathSrc.toString, "data1.bin", 100)

        fsUtils.copyToLocal(pathSrcFile, pathDstFile)

        assert(fsUtils.exists(pathSrcFile))
        assert(fsUtils.exists(pathDstFile))
      }
    }
  }

  "copyFromLocal" should {
    "copy a file from the local filesystem to Hadoop" in {
      withTempDirectory("FsUtilsSuite") { tempDir =>
        // This test assumes it is running in local mode and the same file system is used for both local and hadoop
        val pathSrc = new Path(tempDir, "local")
        val pathDst = new Path(tempDir, "hadoop")

        val pathSrcFile = new Path(pathSrc, "data1.bin")
        val pathDstFile = new Path(pathDst, "data2.bin")

        fsUtils.createDirectoryRecursive(pathSrc)
        fsUtils.createDirectoryRecursive(pathDst)

        createBinFile(pathSrc.toString, "data1.bin", 100)

        fsUtils.copyFromLocal(pathSrcFile, pathDstFile)

        assert(fsUtils.exists(pathSrcFile))
        assert(fsUtils.exists(pathDstFile))
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

  "deleteFile" should {
    "be able to delete a single file" in {
      withTempDirectory("FsUtilsSuite") { tempDir =>
        createBinFile(tempDir, "data1.bin", 100)
        createBinFile(tempDir, "data2.bin", 100)

        fsUtils.deleteFile(new Path(tempDir, "data1.bin"))

        assert(!fsUtils.exists(new Path(tempDir, "data1.bin")))
        assert(fsUtils.exists(new Path(tempDir, "data2.bin")))
      }
    }
  }

  "deleteDirectoryRecursively()" should {
    "be able to delete a single file" in {
      withTempDirectory("FsUtilsSuite") { tempDir =>
        createBinFile(tempDir, "data1.bin", 100)

        assert(fsUtils.deleteDirectoryRecursively(new Path(tempDir, "data1.bin")))
      }
    }

    "be able to delete multiple files" in {
      withTempDirectory("FsUtilsSuite") { tempDir =>
        createBinFile(tempDir, "data1.bin", 100)
        createBinFile(tempDir, "data2.bin", 100)

        assert(fsUtils.deleteDirectoryRecursively(new Path(tempDir)))
      }
    }

    "be able to delete multiple files and directories" in {
      withTempDirectory("FsUtilsSuite") { tempDir =>
        val path1 = new Path(tempDir, "a")
        val path2 = new Path(path1, "b")
        val path3 = new Path(path1, "c")

        fsUtils.createDirectoryRecursive(path1)
        fsUtils.createDirectoryRecursive(path2)
        fsUtils.createDirectoryRecursive(path3)

        createBinFile(path3.toString, "data1.bin", 100)
        createBinFile(path2.toString, "data2.bin", 100)

        assert(fsUtils.exists(new Path(path3, "data1.bin")))
        assert(fsUtils.exists(new Path(path2, "data2.bin")))

        val isDeleted = fsUtils.deleteDirectoryRecursively(path1)

        assert(isDeleted)
        assert(!fsUtils.exists(path1))
      }
    }

    "return false when the path does not exist" in {
      val path = new Path("/dummy/dummy/dummy")
      if (!fsUtils.exists(path)) {
        assert(!fsUtils.deleteDirectoryRecursively(path))
      }
    }
  }

  "getTempPath" should {
    "return a temporary path in a given path" in {
      withTempDirectory("FsUtilsSuite") { tempDir =>
        val path = new Path(tempDir)

        val tempSubDir = fsUtils.getTempPath(path)

        assert(fsUtils.exists(tempSubDir))
        assert(!fsUtils.isFile(tempSubDir))

        val tempSubDirWithPlatformSeparator = tempSubDir.toString.replace('/', File.separatorChar)
        assert(tempSubDirWithPlatformSeparator.startsWith(tempDir))
      }
    }

    "throw an exception is unable to create the base path" in {
      val basePath = new Path("/a/b")
      val fsMock = mock(classOf[FileSystem])

      when(fsMock.exists(basePath)).thenReturn(false)
      when(fsMock.mkdirs(basePath)).thenReturn(false)

      val fsUtilsMock = getFsUtilsMock(fsMock)

      val ex = intercept[IllegalStateException] {
        fsUtilsMock.getTempPath(basePath)
      }

      assert(ex.getMessage.contains("Unable to create"))
    }

    "ensure that there are no name conflicts" in {
      val basePath = new Path("/a/b")
      val tmpPath1 = new Path(basePath, "_1")
      val tmpPath2 = new Path(basePath, "_2")
      val fsMock = mock(classOf[FileSystem])

      when(fsMock.exists(basePath)).thenReturn(true)
      when(fsMock.exists(tmpPath1)).thenReturn(true)
      when(fsMock.exists(tmpPath2)).thenReturn(false)
      when(fsMock.mkdirs(tmpPath2)).thenReturn(true)

      val fsUtilsMock = getFsUtilsMock(fsMock)

      val tmpPath = fsUtilsMock.getTempPath(basePath)

      assert(tmpPath == tmpPath2)
    }
  }

  "withTempDirectory" should {
    "be able to access the temporary directory from teh closuer" in {
      withTempDirectory("FsUtilsSuite") { tempDir =>
        var subdirOutOfScope: Path = null

        fsUtils.withTempDirectory(new Path(tempDir)) { tempSubDir =>
          subdirOutOfScope = tempSubDir

          assert(fsUtils.exists(tempSubDir))
          assert(!fsUtils.isFile(tempSubDir))

          val tempSubDirWithPlatformSeparator = tempSubDir.toString.replace('/', File.separatorChar)
          assert(tempSubDirWithPlatformSeparator.startsWith(tempDir))
        }

        assert(!fsUtils.exists(subdirOutOfScope))
      }
    }

    "throw an exception is unable to create the base path" in {
      val basePath = new Path("/a/b")
      val fsMock = mock(classOf[FileSystem])

      when(fsMock.exists(basePath)).thenReturn(false)
      when(fsMock.mkdirs(basePath)).thenReturn(false)

      val fsUtilsMock = getFsUtilsMock(fsMock)

      var innerExecuted = false

      val ex = intercept[IllegalStateException] {
        fsUtilsMock.withTempDirectory(basePath) { _ =>
          innerExecuted = true
        }
      }

      assert(!innerExecuted)
      assert(ex.getMessage.contains("Unable to create"))
    }
  }

  "deleteObsolete" should {
    "delete old files recursively" in {
      withTempDirectory("FsUtilsSuite") { tempDir =>
        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

        val date = DateUtils.fromIsoStrToDate("2020-07-01")

        // Creating directory structure
        // tempDir
        //      data0.bin (old)
        //    + inner11
        //        + inner12
        //          data1.bin (old)
        //    + inner21
        //        + inner22
        //          data2.bin (new)
        //    + inner31
        //      data3.bin (new)
        //        + inner 32
        //    * _inner4
        //        + _data41 (old)
        //        + _data42 (old)
        //    * _inner5
        //        + _data5 (new)

        val dir1 = new Path(new Path(tempDir, "inner11"), "inner12")
        fsUtils.createDirectoryRecursive(dir1)

        val dir2 = new Path(new Path(tempDir, "inner21"), "inner22")
        fsUtils.createDirectoryRecursive(dir2)

        val dir31 = new Path(tempDir, "inner31")
        val dir32 = new Path(dir31, "inner32")
        fsUtils.createDirectoryRecursive(dir32)

        val dir4 = new Path(tempDir, "_inner4")
        fsUtils.createDirectoryRecursive(dir4)

        val dir5 = new Path(tempDir, "_inner5")
        fsUtils.createDirectoryRecursive(dir5)

        createBinFile(tempDir, "data0.bin", 100)
        val file0 = new Path(tempDir, "data0.bin")
        fs.setTimes(file0, DateUtils.fromIsoStrToTimestampMs("2020-06-01", timezoneId), -1)

        createBinFile(dir1.toString, "data1.bin", 100)
        val file1 = new Path(dir1, "data1.bin")
        fs.setTimes(file1, DateUtils.fromIsoStrToTimestampMs("2020-06-15", timezoneId), -1)

        createBinFile(dir2.toString, "data2.bin", 100)
        val file2 = new Path(dir2, "data2.bin")
        fs.setTimes(file2, DateUtils.fromIsoStrToTimestampMs("2020-06-25", timezoneId), -1)

        createBinFile(dir31.toString, "data3.bin", 100)
        val file3 = new Path(dir31, "data3.bin")
        fs.setTimes(file3, DateUtils.fromIsoStrToTimestampMs("2020-06-28", timezoneId), -1)

        createBinFile(dir4.toString, "_data41", 100)
        val file41 = new Path(dir4, "_data41")
        fs.setTimes(file41, DateUtils.fromIsoStrToTimestampMs("2020-06-19", timezoneId), -1)

        createBinFile(dir4.toString, ".data42", 100)
        val file42 = new Path(dir4, ".data42")
        fs.setTimes(file42, DateUtils.fromIsoStrToTimestampMs("2020-06-19", timezoneId), -1)

        createBinFile(dir5.toString, ".data5", 100)
        val file5 = new Path(dir5, ".data5")
        fs.setTimes(file5, DateUtils.fromIsoStrToTimestampMs("2020-06-30", timezoneId), -1)

        fsUtils.deleteObsolete(Seq(tempDir), 10, date, dryRun = true, timezoneId)
        fsUtils.deleteObsolete(Seq(tempDir), 11, date, dryRun = false, timezoneId)

        // After the transformation only new files should remain
        // tempDir
        //    + inner21
        //        + inner22
        //          data2.bin (new)
        //    + inner31
        //      data3.bin (new)
        //        + inner 32 (empty directory that was empty before the method started)
        //    * _inner5
        //        + _data5 (new)
        assert(fsUtils.exists(file5))
        assert(fsUtils.exists(file3))
        assert(fsUtils.exists(file2))
        assert(fsUtils.exists(dir32))

        assert(!fsUtils.exists(file0))
        assert(!fsUtils.exists(file1))
        assert(!fsUtils.exists(file41))
        assert(!fsUtils.exists(file42))
        assert(!fsUtils.exists(new Path(tempDir, "inner11")))
        assert(!fsUtils.exists(dir4))
      }
    }
  }

  "moveObsolete" should {
    "move old files recursively" in {
      withTempDirectory("FsUtilsSuite") { tempDirBase =>

        val tempDir = new Path(tempDirBase, "data").toString
        val trashDir = new Path(tempDirBase, "trash").toString

        val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

        val date = DateUtils.fromIsoStrToDate("2020-07-01")

        // Creating directory structure
        // tempDir
        //      data0.bin (old)
        //    + inner11
        //        + inner12
        //          data1.bin (old)
        //    + inner21
        //        + inner22
        //          data2.bin (new)
        //    + inner31
        //      data3.bin (new)
        //        + inner 32
        //    * _inner4
        //        + _data41 (old)
        //        + _data42 (old)
        //    * _inner5
        //        + _data5 (new)

        val dir1 = new Path(new Path(tempDir, "inner11"), "inner12")
        val dir1r = new Path(new Path(new Path(trashDir, "data"), "inner11"), "inner12")
        fsUtils.createDirectoryRecursive(dir1)

        val dir2 = new Path(new Path(tempDir, "inner21"), "inner22")
        fsUtils.createDirectoryRecursive(dir2)

        val dir31 = new Path(tempDir, "inner31")
        val dir32 = new Path(dir31, "inner32")
        fsUtils.createDirectoryRecursive(dir32)

        val dir4 = new Path(tempDir, "_inner4")
        fsUtils.createDirectoryRecursive(dir4)

        val dir5 = new Path(tempDir, "_inner5")
        fsUtils.createDirectoryRecursive(dir5)

        createBinFile(tempDir, "data0.bin", 100)
        val file0 = new Path(tempDir, "data0.bin")
        fs.setTimes(file0, DateUtils.fromIsoStrToTimestampMs("2020-06-01", timezoneId), -1)

        createBinFile(dir1.toString, "data1.bin", 100)
        val file1 = new Path(dir1, "data1.bin")
        val file1r = new Path(dir1r, "data1.bin")
        fs.setTimes(file1, DateUtils.fromIsoStrToTimestampMs("2020-06-15", timezoneId), -1)

        createBinFile(dir2.toString, "data2.bin", 100)
        val file2 = new Path(dir2, "data2.bin")
        fs.setTimes(file2, DateUtils.fromIsoStrToTimestampMs("2020-06-25", timezoneId), -1)

        createBinFile(dir31.toString, "data3.bin", 100)
        val file3 = new Path(dir31, "data3.bin")
        fs.setTimes(file3, DateUtils.fromIsoStrToTimestampMs("2020-06-28", timezoneId), -1)

        createBinFile(dir4.toString, "_data41", 100)
        val file41 = new Path(dir4, "_data41")
        fs.setTimes(file41, DateUtils.fromIsoStrToTimestampMs("2020-06-19", timezoneId), -1)

        createBinFile(dir4.toString, ".data42", 100)
        val file42 = new Path(dir4, ".data42")
        fs.setTimes(file42, DateUtils.fromIsoStrToTimestampMs("2020-06-19", timezoneId), -1)

        createBinFile(dir5.toString, ".data5", 100)
        val file5 = new Path(dir5, ".data5")
        fs.setTimes(file5, DateUtils.fromIsoStrToTimestampMs("2020-06-30", timezoneId), -1)

        fsUtils.moveObsolete(Seq(tempDir), trashDir, 10, date, dryRun = true, timezoneId)
        fsUtils.moveObsolete(Seq(tempDir), trashDir, 11, date, dryRun = false, timezoneId)

        // After the transformation only new files should remain
        // tempDir
        //    + inner21
        //        + inner22
        //          data2.bin (new)
        //    + inner31
        //      data3.bin (new)
        //        + inner 32 (empty directory that was empty before the method started)
        //    * _inner5
        //        + _data5 (new)
        assert(fsUtils.exists(file5))
        assert(fsUtils.exists(file3))
        assert(fsUtils.exists(file2))
        assert(fsUtils.exists(dir32))

        assert(!fsUtils.exists(file0))
        assert(!fsUtils.exists(file1))
        assert(!fsUtils.exists(file41))
        assert(!fsUtils.exists(file42))
        assert(!fsUtils.exists(new Path(tempDir, "inner11")))
        assert(!fsUtils.exists(dir4))

        // Moved files should exist
        assert(fsUtils.exists(file1r))
      }
    }
  }

  "getHadoopFiles" should {
    "thrown an exception if the file does not exist" in {
      withTempDirectory("get_hadoop_files") { tempDir =>
        val file = new File(tempDir, "innerDir1").getAbsolutePath
        val ex = intercept[IllegalArgumentException] {
          fsUtils.getHadoopFiles(new Path(file))
        }
        assert(ex.getMessage.contains("Input path does not exist"))
      }
    }

    "not retrieve files starting with . and _" in {
      withTempDirectory("get_hadoop_files") { tempDir =>

        val innerPath = new Path(tempDir, "inner")

        fsUtils.createDirectoryRecursive(innerPath)

        createBinFile(innerPath.toString, "data1.bin", 100)
        createBinFile(innerPath.toString, ".data2.bin", 100)
        createBinFile(innerPath.toString, "_data3.bin", 100)

        val files = fsUtils.getHadoopFiles(innerPath)

        assert(files.length == 1)
        assert(files.head.getPath.toString.endsWith("data1.bin"))
      }
    }

    "retrieve only one level of recursion" in {
      withTempDirectory("get_hadoop_files") { tempDir =>
        val innerPath = new Path(tempDir, "inner")
        val innerHiddenPath = new Path(tempDir, "_inner2")
        val innerLevel2Path = new Path(innerPath, "level2")

        fsUtils.createDirectoryRecursive(innerLevel2Path)
        fsUtils.createDirectoryRecursive(innerHiddenPath)

        createBinFile(innerPath.toString, "data1.bin", 100)
        createBinFile(innerPath.toString, ".data2.bin", 100)
        createBinFile(innerPath.toString, "_data3.bin", 100)
        createBinFile(innerPath.toString, "_SUCCESS", 100)
        createBinFile(innerPath.toString, "_INFO", 100)

        createBinFile(innerHiddenPath.toString, "data4.bin", 100)
        createBinFile(innerHiddenPath.toString, ".data5.bin", 100)
        createBinFile(innerHiddenPath.toString, "_data6.bin", 100)
        createBinFile(innerHiddenPath.toString, "_SUCCESS", 100)
        createBinFile(innerHiddenPath.toString, "_INFO", 100)

        createBinFile(innerLevel2Path.toString, "data7.bin", 100)
        createBinFile(innerLevel2Path.toString, ".data8.bin", 100)
        createBinFile(innerLevel2Path.toString, "_data9.bin", 100)
        createBinFile(innerLevel2Path.toString, "_SUCCESS", 100)
        createBinFile(innerLevel2Path.toString, "_INFO", 100)

        val filesByPath = fsUtils.getHadoopFiles(new Path(tempDir))
        val filesWithMask = fsUtils.getHadoopFiles(new Path(tempDir, "*"))

        assert(filesByPath.isEmpty)
        assert(filesWithMask.length == 1)
        assert(filesWithMask.head.getPath.toString.endsWith("data1.bin"))
      }
    }

    "support a dir" in {
      withTempDirectory("get_hadoop_files") { tempDir =>

        val innerPath = new Path(tempDir, "inner")

        fsUtils.createDirectoryRecursive(innerPath)

        createBinFile(innerPath.toString, "data1.bin", 100)

        val files = fsUtils.getHadoopFiles(innerPath)

        assert(files.length == 1)
        assert(files.head.getPath.toString.endsWith("data1.bin"))
      }
    }

    "support a file" in {
      withTempDirectory("get_hadoop_files") { tempDir =>

        val innerPath = new Path(tempDir, "inner")
        val innerFile = new Path(innerPath, "data1.bin")

        fsUtils.createDirectoryRecursive(innerPath)

        createBinFile(innerPath.toString, "data1.bin", 100)

        val files = fsUtils.getHadoopFiles(innerFile)

        assert(files.length == 1)
        assert(files.head.getPath.toString.endsWith("data1.bin"))
      }
    }
  }

  "getHadoopFilesCaseInsensitive" should {
    "thrown an exception if the file does not exist" in {
      withTempDirectory("get_hadoop_files") { tempDir =>
        val file = new File(tempDir, "innerDir1").getAbsolutePath
        val ex = intercept[IllegalArgumentException] {
          fsUtils.getHadoopFilesCaseInsensitive(new Path(file))
        }
        assert(ex.getMessage.contains("Input path does not exist"))
      }
    }

    "support case insensitive patterns" in {
      withTempDirectory("get_hadoop_files") { tempDir =>

        val innerPath = new Path(tempDir, "inner")
        val filePattern = new Path(innerPath, "?data?.{bin,csv}")

        fsUtils.createDirectoryRecursive(innerPath)

        createBinFile(innerPath.toString, "Adata1.BIN", 100)
        createBinFile(innerPath.toString, ".data2.bin", 100)
        createBinFile(innerPath.toString, "_data3.BiN", 100)

        val files = fsUtils.getHadoopFilesCaseInsensitive(filePattern, includeHiddenFiles = true)
          .map(_.getPath.toString)

        assert(files.length == 3)
        assert(files.exists(_.endsWith("Adata1.BIN")))
        assert(files.exists(_.endsWith(".data2.bin")))
        assert(files.exists(_.endsWith("_data3.BiN")))
      }
    }

    "support case insensitive patterns with hidden filter" in {
      withTempDirectory("get_hadoop_files") { tempDir =>

        val innerPath = new Path(tempDir, "inner")
        val filePattern = new Path(innerPath, "?data?.{bin,csv}")

        fsUtils.createDirectoryRecursive(innerPath)

        createBinFile(innerPath.toString, "Adata1.BiN", 100)
        createBinFile(innerPath.toString, ".data2.bin", 100)
        createBinFile(innerPath.toString, "_data3.BIN", 100)

        val files = fsUtils.getHadoopFilesCaseInsensitive(filePattern)

        assert(files.length == 1)
        assert(files.head.getPath.toString.endsWith("Adata1.BiN"))
      }
    }

    "support a dir" in {
      withTempDirectory("get_hadoop_files") { tempDir =>

        val innerPath = new Path(tempDir, "inner")

        fsUtils.createDirectoryRecursive(innerPath)

        createBinFile(innerPath.toString, "data1.bin", 100)

        val files = fsUtils.getHadoopFilesCaseInsensitive(innerPath)

        assert(files.length == 1)
        assert(files.head.getPath.toString.endsWith("data1.bin"))
      }
    }

    "support a file" in {
      withTempDirectory("get_hadoop_files") { tempDir =>

        val innerPath = new Path(tempDir, "inner")
        val innerFile = new Path(innerPath, "data1.bin")

        fsUtils.createDirectoryRecursive(innerPath)

        createBinFile(innerPath.toString, "data1.bin", 100)

        val files = fsUtils.getHadoopFilesCaseInsensitive(innerFile)

        assert(files.length == 1)
        assert(files.head.getPath.toString.endsWith("data1.bin"))
      }
    }
  }

  private def getFsUtilsMock(fsMock: FileSystem): FsUtils = {
    new FsUtils(spark.sparkContext.hadoopConfiguration, "/tmp") {
      var i = 0
      override protected val log: Logger = LoggerFactory.getLogger("null.logger")
      override val fs: FileSystem = fsMock

      override def getTimedToken: String = {
        i += 1
        s"_$i"
      }
    }
  }

}
