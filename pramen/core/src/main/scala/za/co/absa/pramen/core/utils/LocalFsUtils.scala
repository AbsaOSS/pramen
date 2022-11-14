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

package za.co.absa.pramen.core.utils

import java.io.File
import java.nio.file._
import java.util.function.Consumer
import scala.collection.mutable.ListBuffer

object LocalFsUtils {

  /**
    * Enumerates all files in the directory and returns files that matches the given mask (glob syntax).
    *
    * @param path        A path
    * @param mask        An optional file mask. Will enumerate all files if not specified.
    * @param includeDirs If true, directories will also be included in the list.
    * @return The list of files
    */
  def getListOfFiles(path: Path, mask: String = "", includeDirs: Boolean = false): Seq[Path] = {
    val dirStream: DirectoryStream[Path] = if (mask.isEmpty) {
      Files.newDirectoryStream(path)
    } else {
      Files.newDirectoryStream(path, mask)
    }
    val outputList = new ListBuffer[Path]

    dirStream.forEach(new Consumer[Path] {
      override def accept(p: Path): Unit = {
        if (includeDirs || p.toFile.isFile) {
          outputList += p
        }
      }
    })

    outputList.toSeq
  }

  /**
    * Copies all files from one directory to another.
    *
    * @param srcPath The source path.
    * @param trgPath The target path.
    * @return The list of files
    */
  def copyDirectory(srcPath: Path, trgPath: Path): Unit = {
    getListOfFiles(srcPath)
      .foreach(filePath => {
        Files.copy(filePath, Paths.get(trgPath.toAbsolutePath.toString, filePath.getFileName.toString), StandardCopyOption.REPLACE_EXISTING)
      })
  }

  /**
    * Parses a path and extracts a file mask if the path contains a wildcard.
    *
    * @param path a path string.
    * @return A pair containing the path without the wildcard and the file mask containing the wildcard.
    */
  def splitPathAndMask(path: String): (String, String) = {
    val slash = FileSystems.getDefault.getSeparator

    val pathArray = path.split(slash.head)
    if (path.isEmpty) {
      return (Paths.get(".").toString, "*")
    }

    if (pathArray.isEmpty) {
      // This happens if the path is '/' or '//' or '///'
      return (Paths.get(path).toString, "*")
    }

    val lastElement = pathArray.last
    if (lastElement.contains("*") || lastElement.contains("?")) {
      if (pathArray.length == 1) {
        (Paths.get(".").toString, path)
      } else {
        val pathWithoutMask = pathArray.dropRight(1).mkString(slash)
        val fixedPath = if (pathWithoutMask.isEmpty) slash else pathWithoutMask
        (Paths.get(fixedPath).toString, pathArray.last)
      }
    } else {
      (Paths.get(path).toString, "*")
    }
  }

  private[core] def deleteTemp(f: File): Unit = {
    if (f.isDirectory) {
      for (c <- f.listFiles) {
        deleteTemp(c)
      }
    }
  }


}
