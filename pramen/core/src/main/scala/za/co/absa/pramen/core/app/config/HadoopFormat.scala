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

package za.co.absa.pramen.core.app.config

trait HadoopFormat

case object HadoopFormat {
  case object Text extends HadoopFormat
  case object Delta extends HadoopFormat

  def apply(format: String): HadoopFormat = format.toLowerCase match {
    case "text" => Text
    case "delta" => Delta
    case _ => throw new IllegalArgumentException(s"Unknown Hadoop format: $format")
  }
}
