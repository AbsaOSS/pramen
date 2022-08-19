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

import java.nio.file._
import java.util.function.Consumer
import scala.collection.mutable.ListBuffer

object LocalFsUtils {

  /**
    * Enumerates all files in the directory and returns files that matches the given mask (glob syntax).
    *
    * @param path A path
    * @param mask An optional file mask. Will enumerate all files if not specified.
    * @return The list of files
    */
  def getListOfFiles(path: Path, mask: String = ""): Seq[Path] = {
    val dirStream: DirectoryStream[Path] = if (mask.isEmpty) {
      Files.newDirectoryStream(path)
    } else {
      Files.newDirectoryStream(path, mask)
    }
    val outputList = new ListBuffer[Path]

    dirStream.forEach(new Consumer[Path]{
      override def accept(p: Path): Unit = {
        if (p.toFile.isFile) {
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

}
