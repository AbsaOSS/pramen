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

package za.co.absa.pramen.core.utils.traverser

import org.apache.commons.io.filefilter.WildcardFileFilter

import java.io.{File, FilenameFilter}

class FsTraverserLocal extends FsTraverser {
  override def traverse(path: String,
               fileMask: String,
               isRecursive: Boolean,
               includeHiddenFiles: Boolean)(action: FileStatus => Unit): Unit = {
    val filter: FilenameFilter = new WildcardFileFilter(fileMask)

    val listAll = fileMask.isEmpty || fileMask == "*"

    def actionFile(file: File): Unit = {
      if (includeHiddenFiles || !isFileHidden(file.getName)) {
        action(FileStatus(file, file.length()))
      }
    }

    def traverseSubPath(subPath: File): Unit = {
      if (subPath.isDirectory) {
        subPath.listFiles().foreach { f =>
          if (f.isDirectory) {
            if (isRecursive && (includeHiddenFiles || !isFileHidden(f.getName))) {
              traverseSubPath(f)
            }
          } else {
            if (listAll || filter.accept(f.getParentFile, f.getName)) {
              actionFile(f)
            }
          }
        }
      } else {
        action(FileStatus(subPath, subPath.length()))
      }
    }

    traverseSubPath(new File(path))
  }

  private def isFileHidden(fileName: String): Boolean = {
    fileName.startsWith(".") || fileName.startsWith("_")
  }
}
