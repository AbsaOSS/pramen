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

package za.co.absa.pramen.api

import com.typesafe.config.Config

/**
  * This is a trait tha allows a unified way of creating new instances of sources and sinks
  * from a fully qualified class name.
  */
trait ExternalChannel extends AutoCloseable {

  /** Channels can optionally have a method to close it when saving is done or in case of an error. */
  @throws[Exception]
  override def close(): Unit = {}

  /** Channels can optionally have a method to connect to it. */
  @throws[Exception]
  def connect(): Unit = {}

  /** The configuration used to create the channel. */
  def config: Config
}
