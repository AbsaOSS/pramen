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

import com.typesafe.config.Config

import java.lang.reflect.Constructor
import scala.reflect.ClassTag
import scala.reflect.runtime.universe
import scala.util.{Failure, Success, Try}

object ClassLoaderUtils {

  @throws[IllegalArgumentException]
  def loadSingletonClassOfType[T:ClassTag:universe.TypeTag](fullyQualifiedName: String): T = {
    val mirror = universe.runtimeMirror(getClass.getClassLoader)
    Try(mirror.staticModule(fullyQualifiedName)) match {
      case Success(module) => val reflectiveMirror = mirror.reflectModule(module)
        Try(reflectiveMirror.instance) match {
          case Success(instance) =>
            instance match {
              case singleton: T => singleton
              case _ => throw new IllegalArgumentException(s"Class '$fullyQualifiedName' is not an instance of '${universe.typeOf[T]}'")
            }
          case Failure(exception) => throw new IllegalArgumentException(s"Class '$fullyQualifiedName' is not a singleton", exception)
        }
      case Failure(exception) => throw new IllegalArgumentException(s"Class '$fullyQualifiedName' could not be found", exception)
    }
  }

  @throws[ClassNotFoundException]
  @throws[ClassCastException]
  @throws[NoSuchMethodException]
  def loadConfigurableClass[T:ClassTag:universe.TypeTag](fullyQualifiedName: String, config: Config): T = {
    // There are 2 types of constructors supported. If there is a one that takes a config - use it.
    // Otherwise, try the default constructor.
    val confCtor = Try[Constructor[_]](Class.forName(fullyQualifiedName).getConstructor(classOf[Config]))

    confCtor
      .map(ctor => ctor.newInstance(config).asInstanceOf[T])
      .recoverWith{
        case _: NoSuchMethodException =>
          val defCtor = Try[Constructor[_]](Class.forName(fullyQualifiedName).getConstructor())
          defCtor.map(ctor => ctor.newInstance().asInstanceOf[T])
      }.get
  }

  @throws[ClassNotFoundException]
  @throws[ClassCastException]
  @throws[NoSuchMethodException]
  def loadEntityConfigurableClass[T: ClassTag : universe.TypeTag](fullyQualifiedName: String, entityConfig: Config, appConfig: Config): T = {
    // There are 3 types of constructors supported. If there is a one that takes a config - use it.
    // Otherwise, try the default constructor.
    val confCtor = Try[Constructor[_]](Class.forName(fullyQualifiedName).getConstructor(classOf[Config], classOf[Config]))

    confCtor
      .map(ctor => ctor.newInstance(entityConfig, appConfig).asInstanceOf[T])
      .recoverWith {
        case _: NoSuchMethodException =>
          val defCtor = Try[Constructor[_]](Class.forName(fullyQualifiedName).getConstructor(classOf[Config]))
          defCtor.map(ctor => ctor.newInstance(entityConfig).asInstanceOf[T])
      }
      .recoverWith {
        case _: NoSuchMethodException =>
          val defCtor = Try[Constructor[_]](Class.forName(fullyQualifiedName).getConstructor())
          defCtor.map(ctor => ctor.newInstance().asInstanceOf[T])
      }
      .get
  }

}
