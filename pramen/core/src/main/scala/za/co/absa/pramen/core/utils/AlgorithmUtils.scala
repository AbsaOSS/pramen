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

object AlgorithmUtils {
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
}
