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

import org.apache.spark.sql.DataFrame
import za.co.absa.pramen.api.notification.{NotificationEntry, Style, TextElement}

/**
  * Pramen provides an instance of notification builder to custom sources, transformers and sinks so that
  * they can add entries to email notifications and [in the future] other types of user notifications.
  *
  * Custom code can get the notification builder to send custom notifications any time using
  * {{{
  *   val notificationBuilder = Pramen.instance.notificationBuilder
  *
  *   notificationBuilder.addEntries(...)
  * }}}
  */
trait NotificationBuilder {
  /**
    * Adds entries for email notification, and [in the future] other use notifications.
    *
    * The method can be accessed concurrently, so you need to group all related entries in a single call.
    *
    * @param entries Entries to add.
    */
  def addEntries(entries: NotificationEntry*): Unit

  /**
    * Adds a data frame as a table in email notification, and [in the future] other use notifications.
    *
    * @param df               The DataFrame containing the table data,
    * @param description      The test that goes before the table.
    * @param descriptionStyle The style of the text of the description.
    * @param maxRecords       The maximum number of records to add to the notification.
    * @param align            Optionally specify text alignment of each column as a caracter of 'L', 'C', 'R'.
    *                         For example Seq('C', 'C', 'R') means first 3 columns are centered and the last one is
    *                         right aligned.
    */
  def addDataFrameTable(df: DataFrame,
                        description: String,
                        descriptionStyle: Style = Style.Normal,
                        maxRecords: Int = 200,
                        align: Option[Seq[Char]] = None): Unit

  /** Sets a custom notification signature at runtime. Can be used from the custom startup/shutdown hook. */
  def setSignature(text: TextElement*): Unit
}
