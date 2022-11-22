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

import scala.language.reflectiveCalls
import scala.reflect.runtime.universe

object MainRunner {
  type ScalaApplication = {
    def main(args: Array[String]): Unit
  }

  /**
    * Runs the main method of the given object.
    *
    * The object must have a main method with the signature `def main(args: Array[String]): Unit`.
    * The object must be on the classpath.
    *
    * The implementation is based on: https://stackoverflow.com/a/34260983/1038282
    *
    * @param objectName The name of the object to run.
    * @param args       The arguments to pass to the main method.
    */
  @throws[ReflectiveOperationException]
  @throws[ScalaReflectionException]
  def runMain(objectName: String, args: Array[String]): Unit = {
    val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
    val module = runtimeMirror.staticModule(objectName)
    val obj = runtimeMirror.reflectModule(module)
    val scalaApp: ScalaApplication = obj.instance.asInstanceOf[ScalaApplication]

    scalaApp.main(args)
  }
}
