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

import za.co.absa.pramen.api.common.FactoryLoader
import za.co.absa.pramen.api.notification.{NotificationBuilderFactory, NotificationEntry}

/**
  * Pramen provides an instance of notification builder to custom sources, transformers and sinks so that
  * they can add entries to email notifications and [in the future] other types of user notifications.
  */
trait PramenNotificationBuilder {
  /**
    * Adds entries for email notification, and [in the future] other use notifications.
    *
    * The method can be accessed concurrently, so you need to group all related entries in a single call.
    *
    * @param entries Entries to add.
    */
  def addEntries(entries: NotificationEntry*): Unit
}

/**
  * Custom code can get the notification builder to send custom notifications any time using
  * {{{
  *   val notificationBuilder = NotificationBuilder.instance
  *
  *   notificationBuilder.addEntries(...)
  * }}}
  */
object PramenNotificationBuilder {
  val PRAMEN_NOTIFICATION_BUILDER_FACTORY_CLASS = "za.co.absa.pramen.core.state.PramenNotificationBuilderFactory"

  lazy val instance: NotificationBuilderFactory = FactoryLoader.loadSingletonFactoryOfType[NotificationBuilderFactory](PRAMEN_NOTIFICATION_BUILDER_FACTORY_CLASS)
}
