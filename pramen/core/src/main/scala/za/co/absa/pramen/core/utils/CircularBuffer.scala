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

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

class CircularBuffer[T: ClassTag](maxSize: Int) {
  require(maxSize > 0)

  var actualSize = 0
  var cursor = 0
  val content = new Array[T](maxSize)

  def add(el: T): Unit = {
    content(cursor) = el
    cursor += 1
    if (cursor >= maxSize) {
      cursor = 0
    }
    if (actualSize < maxSize) {
      actualSize += 1
    }
  }

  def get(): Array[T] = {
    ListBuffer
    val outputArray = new Array[T](actualSize)
    if (actualSize < maxSize) {
      System.arraycopy(content, 0, outputArray, 0, actualSize)
    } else {
      System.arraycopy(content, cursor, outputArray, 0, actualSize - cursor)
      System.arraycopy(content, 0, outputArray, actualSize - cursor, cursor)
    }
    outputArray
  }

  def length: Int = actualSize

  def size: Int = actualSize
}
