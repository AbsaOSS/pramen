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

package za.co.absa.pramen.core.state

import org.slf4j.LoggerFactory
import za.co.absa.pramen.api.NotificationBuilder
import za.co.absa.pramen.api.notification.NotificationEntry

import scala.collection.mutable.ListBuffer

class NotificationBuilderImpl extends NotificationBuilder {
  private val log = LoggerFactory.getLogger(this.getClass)

  private val notificationEntries = new ListBuffer[NotificationEntry]

  override def addEntries(entries: NotificationEntry*): Unit = synchronized {
    entries.foreach(entry => if (isEntryValid(entry)) notificationEntries += entry)
  }

  def entries: Seq[NotificationEntry] = synchronized {
    notificationEntries.toSeq
  }

  private def isEntryValid(entry: NotificationEntry): Boolean = entry match {
    case NotificationEntry.Paragraph(_) => true
    case NotificationEntry.Table(headers, cells) =>
      if (headers.isEmpty) {
        log.error("Table entry has no headers - skipping adding it to the notification")
        false
      } else if (cells.exists(_.size != headers.size)) {
        log.error("Table entry has cells with different number of elements than the number of headers - skipping adding it to the notification")
        false
      } else {
        true
      }
  }
}
