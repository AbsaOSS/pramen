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

import org.slf4j.Logger

import scala.annotation.tailrec
import scala.collection.mutable

object AlgorithmUtils {
  /** Finds which strings are encountered multiple times (case insensitive). */
  def findDuplicates(seq: Seq[String]): Seq[String] = {
    val existingElements: mutable.Set[String] = new mutable.HashSet[String]()

    seq.filter(str => {
      val lcaseStr = str.toLowerCase()
      if (existingElements.contains(lcaseStr)) {
        true
      } else {
        existingElements += lcaseStr
        false
      }
    }).distinct
  }

  /**
    * Finds a cycle in a sequence of dependencies.
    *
    * Based on https://github.com/AbsaOSS/cobrix/blob/e5b5a9caaaf1b1fad703487dc39beade58229c27/cobol-parser/src/main/scala/za/co/absa/cobrix/cobol/parser/CopybookParser.scala#L996
    *
    * @param deps List of dependencies where _1 depends on _2.
    * @return A list of fields in a cycle if there is one, an empty list otherwise
    */
  def findCycle(deps: Seq[(String, String)]): List[String] = {
    def findCycleHelper(field: String, fieldsInPath: List[String]): List[String] = {
      val i = fieldsInPath.indexOf(field)
      if (i >= 0) {
        fieldsInPath.take(i + 1).reverse :+ field
      } else {
        val fieldDependencies = deps.filter(_._1 == field).map(_._2)
        val path = field :: fieldsInPath

        fieldDependencies.collectFirst {
          case d if findCycleHelper(d, path).nonEmpty => findCycleHelper(d, path)
        }.getOrElse(Nil)
      }
    }

    deps.view
      .map({ case (k, _) =>
        findCycleHelper(k, Nil)
      })
      .find(_.nonEmpty)
      .getOrElse(List[String]())
  }

  @tailrec
  final def actionWithRetry(attempts: Int, log: Logger)(action: => Unit): Unit = {
    def getErrorMessage(ex: Throwable): String = {
      if (ex.getCause == null) {
        ex.getMessage
      } else {
        s"${ex.getMessage}(${ex.getCause.getMessage})"
      }
    }

    try {
      action
    } catch {
      case ex: Throwable =>
        val attemptsLeft = attempts - 1

        if (attemptsLeft < 1) {
          throw ex
        } else {
          log.warn(s"Attempt failed: ${getErrorMessage(ex)}. Attempts left: $attemptsLeft. Retrying...")
          actionWithRetry(attemptsLeft, log)(action)
        }
    }
  }
}
