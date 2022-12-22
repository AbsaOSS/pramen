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

package za.co.absa.pramen.core.tests.utils.traverser

import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.core.fixtures.{TempDirFixture, TextComparisonFixture}
import za.co.absa.pramen.core.utils.LocalFsUtils
import za.co.absa.pramen.core.utils.traverser.FsTraverserLocal

import java.io.File
import java.nio.file.{Files, Paths}
import scala.collection.mutable.ListBuffer

class FsTraverserLocalSuite extends AnyWordSpec with BeforeAndAfterAll with TempDirFixture with TextComparisonFixture {
  val tempDir: String = createTempDir("fs_traverser")

  override def beforeAll(): Unit = {
    super.beforeAll()

    val innedDir1 = Paths.get(tempDir, "inner1")
    val innedDir2 = Paths.get(tempDir, "inner2.csv")
    val innedDir3 = Paths.get(innedDir1.toString, "inner3")
    val innedDir4 = Paths.get(tempDir, "_inner4")

    Files.createDirectory(innedDir1)
    Files.createDirectory(innedDir2)
    Files.createDirectory(innedDir3)
    Files.createDirectory(innedDir4)

    Files.createFile(Paths.get(tempDir, "file1.csv"))
    Files.createFile(Paths.get(tempDir, "file2.json"))
    Files.createFile(Paths.get(tempDir, "_file3.csv"))
    Files.createFile(Paths.get(innedDir1.toString, "file4.csv"))
    Files.createFile(Paths.get(innedDir2.toString, "file5.csv"))
    Files.createFile(Paths.get(innedDir3.toString, "file6.csv"))
    Files.createFile(Paths.get(innedDir1.toString, "file7.json"))
    Files.createFile(Paths.get(innedDir2.toString, "file8.json"))
    Files.createFile(Paths.get(innedDir3.toString, "file9.json"))
    Files.createFile(Paths.get(innedDir3.toString, "_hidden"))
    Files.createFile(Paths.get(innedDir3.toString, "_file10.csv"))
    Files.createFile(Paths.get(innedDir4.toString, "_file11.csv"))
    Files.createFile(Paths.get(innedDir4.toString, "file12.csv"))
  }

  override def afterAll(): Unit = {
    super.afterAll()

    LocalFsUtils.deleteTemp(new File(tempDir))
  }

  "traverse" should {
    "work with a single file" in {
      val traverser = new FsTraverserLocal()

      var count = 0

      traverser.traverse(Paths.get(tempDir, "file1.csv").toString, "*", isRecursive = true, includeHiddenFiles = true) { f =>
        assert(f.path.getAbsolutePath == Paths.get(tempDir, "file1.csv").toString)
        count += 1
      }

      assert(count == 1)
    }

    "work with a single hidden file" in {
      val traverser = new FsTraverserLocal()

      var count = 0

      traverser.traverse(Paths.get(tempDir, "_file3.csv").toString, "*", isRecursive = true, includeHiddenFiles = false) { f =>
        assert(f.path.getAbsolutePath == Paths.get(tempDir, "_file3.csv").toString)
        count += 1
      }

      assert(count == 1)
    }

    "mask is not specified" when {
      "recursive traversal" when {
        "with hidden files enabled" in {
          val count = runTraverser(tempDir, "*", isRecursive = true, includeHiddenFiles = true).size

          assert(count == 13)
        }

        "with hidden files disabled" in {
          val count = runTraverser(tempDir, "*", isRecursive = true, includeHiddenFiles = false).size

          assert(count == 8)
        }
      }

      "non-recursive traversal" when {
        "with hidden files enabled" in {
          val count = runTraverser(tempDir, "*", isRecursive = false, includeHiddenFiles = true).size

          assert(count == 3)
        }

        "with hidden files disabled" in {
          val count = runTraverser(tempDir, "*", isRecursive = false, includeHiddenFiles = false).size

          assert(count == 2)
        }
      }
    }

    "mask is specified" when {
      "recursive traversal" when {
        "with hidden files enabled" in {
          val count = runTraverser(tempDir, "file*.csv", isRecursive = true, includeHiddenFiles = true).size

          assert(count == 5)
        }

        "with hidden files disabled" in {
          val count = runTraverser(tempDir, "file*.csv", isRecursive = true, includeHiddenFiles = false).size

          assert(count == 4)
        }
      }

      "non-recursive traversal" when {
        "with hidden files enabled" in {
          val count = runTraverser(tempDir, "*file*.csv", isRecursive = false, includeHiddenFiles = true).size

          assert(count == 2)
        }

        "with hidden files disabled" in {
          val count = runTraverser(tempDir, "*file*.csv", isRecursive = false, includeHiddenFiles = false).size

          assert(count == 1)
        }
      }
    }
  }

  def runTraverser(dir: String, mask: String, isRecursive: Boolean, includeHiddenFiles: Boolean): List[String] = {
    val listFiles = new ListBuffer[String]

    val traverser = new FsTraverserLocal()

    traverser.traverse(dir, mask, isRecursive, includeHiddenFiles) { f =>
      listFiles += f.path.getAbsolutePath
    }

    listFiles.toList
  }

}
