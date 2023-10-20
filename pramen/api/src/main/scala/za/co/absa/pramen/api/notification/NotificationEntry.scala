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

package za.co.absa.pramen.api.notification

sealed trait NotificationEntry

object NotificationEntry {
  /**
    * A text to be added to the email notification. If a notification targets supports it, it can be used there as well
    * (for example, a text to put to a chat application)
    *
    * @param text A formatted text
    */
  case class Paragraph(text: Seq[TextElement]) extends NotificationEntry

  /**
    * A table to be added to the email notification. If a notification targets supports it, it can be used there as well
    * (for example, a table to put to a chat application)
    *
    * @param headers Table headers
    * @param cells   Table cells. The number of elements in each row must match the number of headers.
    */
  case class Table(headers: Seq[TableHeader], cells: Seq[Seq[TextElement]]) extends NotificationEntry

  /**
    * An unformatted text in a separate block.
    *
    * @param text The text to inlude
    */
  case class UnformattedText(text: String) extends NotificationEntry

  /**
    * An unordered (bulleted) list (corresponds to `<ul>`).
    *
    * @param items The list items.
    */
  case class UnorderedList(items: Seq[Paragraph]) extends NotificationEntry

  /**
    * Am ordered list (corresponds to `<ol>`).
    *
    * @param items The list items.
    */
  case class OrderedList(items: Seq[Paragraph]) extends NotificationEntry

  /**
    * A file to attach.
    *
    * @param fileName The name of the file as seed in the email message.
    * @param contents The contents of the file.
    */
  case class AttachedFile(fileName: String, contents: Array[Byte]) extends NotificationEntry

  /**
    * A raw HTML block - you can include anything.
    *
    * @param contents The contents of the HTML block.
    */
  case class Html(contents: String) extends NotificationEntry
}
