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

package za.co.absa.pramen.core.fixtures

import org.apache.commons.io.FileUtils

import java.io.{File, PrintWriter, RandomAccessFile}
import java.nio.file.{Files, Paths}

/**
  * This fixture adds ability for a unit test to use temporary directories provided by the test env.
  */
trait TempDirFixture {

  /**
    * Creates a temporary directory and provides it to the test.
    *
    * @param prefix  The prefix string to be used in generating directory name;
    *                must be at least three characters long.
    * @return The full path to the temporary directory.
    */
  def withTempDirectory(prefix: String)(f: String => Unit): Unit = {
    val pathStr = createTempDir(prefix)

    f(pathStr)

    deleteDir(pathStr)
  }

  /**
    * Creates a temporary directory and returns it to the caller.
    *
    * @param prefix  The prefix string to be used in generating directory name;
    *                must be at least three characters long.
    * @return The full path to the temporary directory.
    */
  def createTempDir(prefix: String): String = {
    val tmpPath = Files.createTempDirectory(prefix)
    tmpPath.toAbsolutePath.toString
  }

  /**
    * Deletes a directory recursively.
    *
    * @param path Path to a directory to delete.
    */
  def deleteDir(path: String): Unit = {
    FileUtils.deleteDirectory(new File(path))
  }

  /**
    * Creates a file having the given size.
    *
    * @return The full path to the file.
    */
  def createBinFile(path: String, fileName: String, size: Int): Unit = {
    val fullPathToTheFile = Paths.get(path, fileName).toString
    val f = new RandomAccessFile(fullPathToTheFile, "rw")
    f.setLength(size)
    f.close()
  }

  /**
    * Creates a file having the specified contents.
    *
    * @return The full path to the file.
    */
  def createTextFile(path: String, fileName: String, contents: String): Unit = {
    val out = new PrintWriter(Paths.get(path, fileName).toAbsolutePath.toString)
    out.println(contents)
    out.close()
  }

}
