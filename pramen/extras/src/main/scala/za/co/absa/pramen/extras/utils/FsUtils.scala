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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, GlobFilter, Path}
import org.slf4j.{Logger, LoggerFactory}

import java.io.IOException
import java.nio.file.{Files, Paths}
import java.time._
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal
import scala.util.{Failure, Random, Success, Try}

class FsUtils(conf: Configuration, pathBase: String) {
  protected val log: Logger = LoggerFactory.getLogger(this.getClass)
  val fs: FileSystem = new Path(pathBase).getFileSystem(conf)

  /**
    * Returns directory size in bytes
    */
  def getDirectorySize(path: String): Long = {
    val hadoopPath = new Path(path)

    fs.getContentSummary(hadoopPath).getLength
  }

  def createDirectoryRecursiveButLast(path: Path): Unit = {
    val (prefix, rawPath) = splitUriPath(path)

    val tokens = rawPath.split("/").filter(_.nonEmpty).dropRight(1)

    var currPath = prefix
    tokens.foreach({ dir =>
      currPath = currPath + "/" + dir
      val p = new Path(currPath)
      if (!fs.exists(p)) {
        log.info(s"Creating ${p.toUri.toString}...")
        fs.mkdirs(p)
      }
    })
  }

  /**
    * Split path URI by separating scheme+server and path part
    * Example:
    * hdfs://server:8020/user/data/input -> (hdfs://server:8020, /user/data/input)
    * /user/data/input -> ("", /user/data/input)
    */
  def splitUriPath(path: Path): (String, String) = {
    val uri = path.toUri
    val scheme = uri.getScheme
    val authority = uri.getAuthority
    val prefix = if (scheme == null) "" else {
      if (authority == null) {
        scheme + "://"
      } else {
        scheme + "://" + authority
      }
    }
    val rawPath = uri.getRawPath
    (prefix, rawPath)
  }

  /**
    * Checks if a path points to a file.
    *
    * @param path a path.
    * @return true if the path is a file.
    */
  def isFile(path: Path): Boolean = fs.getFileStatus(path).isFile

  /**
    * Checks if a path exists.
    *
    * @param path a path.
    * @return true if the path exists.
    */
  def exists(path: Path): Boolean = fs.exists(path)

  /**
    * Gets the list of files that match the given pattern (recursively).
    *
    * @param path     The base path to search
    * @param fileMask The file mask to apply
    * @return
    */
  def getFilesRecursive(path: Path, fileMask: String = "*", includeHiddenFiles: Boolean = false): Seq[Path] = {
    val filter = new GlobFilter(fileMask)
    val files = new ListBuffer[Path]

    def isHidden(fileName: String): Boolean = {
      fileName.startsWith("_") || fileName.startsWith(".")
    }

    def addToListRecursive(searchPath: Path): Unit = {
      val statuses = fs.globStatus(new Path(searchPath, "*"))
      statuses.foreach { status =>
        val p = status.getPath
        if (status.isDirectory) {
          if (includeHiddenFiles || !isHidden(p.getName)) {
            addToListRecursive(p)
          }
        } else {
          if (filter.accept(status.getPath)) {
            if (includeHiddenFiles || !isHidden(p.getName)) {
              files += status.getPath
            }
          }
        }
      }
    }

    if (isFile(path)) {
      Seq(path)
    } else {
      addToListRecursive(path)
      files.toSeq
    }
  }

  /**
    * Gets the list of directories
    *
    * @param path     The base path to search
    * @return
    */
  def getDirectories(path: Path, includeHiddenFiles: Boolean = false): Seq[Path] = {
    val dirs = new ListBuffer[Path]

    def isHidden(fileName: String): Boolean = {
      fileName.startsWith("_") || fileName.startsWith(".")
    }

    def addToListRecursive(searchPath: Path): Unit = {
      val statuses = fs.globStatus(new Path(searchPath, "*"))
      statuses.foreach { status =>
        val p = status.getPath
        if (status.isDirectory) {
          if (includeHiddenFiles || !isHidden(p.getName)) {
            dirs += status.getPath
          }
        }
      }
    }

    if (isFile(path)) {
      Nil
    } else {
      addToListRecursive(path)
      dirs.toSeq
    }
  }

  /**
    * Appends an UTF-8 string to a file.
    *
    * @param filePath a path to a file.
    * @param content  content to write.
    */
  def appendFile(filePath: Path, content: String): Unit = {
    try {
      val out = fs.append(filePath)
      log.info("Appending using the filesystem routine")
      out.write(content.getBytes())
      out.close()
    } catch {
      // It seems not all filesystems support append(). And Filesystem.append() just throws an exception in this case.
      // This is a workaround for this particular case, so the append can be done anyway.
      // At the same time, all other exceptions will be re-thrown. Since IOException is too broad, the code relies on
      // the particular message. It's a hack, sorry. HDFS supports append().
      case ex: Throwable if ex.getMessage != null && ex.getMessage.toLowerCase.contains("not supported") =>
        log.info("Appending using full overwrite")
        val originalContent = if (exists(filePath)) {
          readFile(filePath)
        } else ""
        safeWriteFile(filePath, originalContent + content)
      case ex: Throwable                                              => throw ex
    }
  }

  /**
    * Writes a string as a UTF-8 text file atomically so either full file is written or nothing is written.
    *
    * @param filePath a path to a file.
    * @param content  content to write.
    */
  def safeWriteFile(filePath: Path, content: String): Unit = {
    val tmpPath = new Path(s"${filePath.toUri}.tmp")
    writeFile(tmpPath, content)
    renamePath(tmpPath, filePath)
  }

  /**
    * Reads an entire file as a UTF-8 string.
    *
    * @param filePath a path to a file.
    * @return contents of the file as string.
    */
  def readFile(filePath: Path): String = {
    val in = fs.open(filePath)
    val content = Array.fill(in.available())(0.toByte)
    in.readFully(content)
    in.close()
    new String(content, "UTF-8")
  }

  /**
    * Writes a string as a UTF-8 text file.
    *
    * @param filePath a path to a file.
    * @param content  content to write.
    */
  def writeFile(filePath: Path, content: String): Unit = {
    val out = fs.create(filePath)
    out.write(content.getBytes())
    out.close()
  }

  /**
    * Renames/Moves a path.
    *
    * @param pathSrc   source path.
    * @param pathTrg   target path.
    * @param overwrite overwrite the target file is exists.
    * @return true if the rename succeeded.
    */
  def renamePath(pathSrc: Path, pathTrg: Path, overwrite: Boolean = true): Boolean = {
    log.debug(s"fs.exists($pathTrg) = ${fs.exists(pathTrg)}")
    if (overwrite && fs.exists(pathTrg)) {
      log.debug(s"Renaming is not succeeded. Deleting '$pathTrg'...")
      fs.delete(pathTrg, true)
      log.debug(s"Renaming $pathSrc to '$pathTrg'...")
      fs.rename(pathSrc, pathTrg)
    } else {
      log.debug(s"Renaming $pathSrc to '$pathTrg'...")
      fs.rename(pathSrc, pathTrg)
    }
  }

  /**
    * Deletes a directory and all its contents recursively.
    *
    * @param path path to a file.
    */
  def deleteDirectoryRecursively(path: Path): Boolean = {
    if (fs.exists(path)) {
      log.info(s"Deleting recursively '$path'...")
      fs.delete(path, true)
    } else {
      false
    }
  }
}
