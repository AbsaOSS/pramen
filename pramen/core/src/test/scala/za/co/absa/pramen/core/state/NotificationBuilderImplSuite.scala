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

import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.notification.{NotificationEntry, TableHeader, TextElement}
import za.co.absa.pramen.core.base.SparkTestBase

class NotificationBuilderImplSuite extends AnyWordSpec with SparkTestBase {
  import spark.implicits._

  "addEntries()" should {
    "do nothing if no entries provided" in {
      val builder = new NotificationBuilderImpl

      builder.addEntries()

      assert(builder.entries.isEmpty)
    }

    "add notification entries ot the list" in {
      val builder = new NotificationBuilderImpl

      builder.addEntries(NotificationEntry.Paragraph(TextElement("Some text") :: Nil))

      assert(builder.entries.size == 1)
    }

    "skip adding invalid items" in {
      val builder = new NotificationBuilderImpl

      builder.addEntries(NotificationEntry.Table(Seq.empty[TableHeader], Seq(TextElement("Some text") :: Nil)))

      assert(builder.entries.isEmpty)
    }
  }

  "addDataFrameTable()" should {
    "do nothing for an empty data frame" in {
      val df = spark.emptyDataFrame

      val builder = new NotificationBuilderImpl

      builder.addDataFrameTable(df, "My description")

      assert(builder.entries.isEmpty)
    }

    "add a table to notification entries" in {
      val df = List(("A", 1), ("B", 2), ("C", 3)).toDF("a", "b")

      val builder = new NotificationBuilderImpl

      builder.addDataFrameTable(df,"My description")

      assert(builder.entries.size == 2)
      assert(builder.entries.head.isInstanceOf[NotificationEntry.Paragraph])
      assert(builder.entries(1).isInstanceOf[NotificationEntry.Table])
    }

    "do not add table is alignment is invalid" in {
      val df = List(("A", 1), ("B", 2), ("C", 3)).toDF("a", "b")

      val builder = new NotificationBuilderImpl

      builder.addDataFrameTable(df, "My description", align = Some("C".toSeq)) // Should be 2 characters, like or "CC"

      assert(builder.entries.isEmpty)
    }

  }

  "isEntryValid()" should {
    "return true for a paragraph" in {
      val builder = new NotificationBuilderImpl

      val isValid = builder.isEntryValid(NotificationEntry.Paragraph(TextElement("Some text") :: Nil))

      assert(isValid)
    }

    "return true for a unformatted text" in {
      val builder = new NotificationBuilderImpl

      val isValid = builder.isEntryValid(NotificationEntry.UnformattedText("Some text"))

      assert(isValid)
    }

    "return true for a valid table text" in {
      val builder = new NotificationBuilderImpl

      val table = NotificationEntry.Table(
        TableHeader(TextElement("Header")) :: Nil,
        Seq(Seq(
          TextElement("cell")
        ))
      )

      val isValid = builder.isEntryValid(table)

      assert(isValid)
    }

    "return false for a table with empty headers" in {
      val builder = new NotificationBuilderImpl

      val table = NotificationEntry.Table(
        Seq.empty[TableHeader],
        Seq(Seq(
          TextElement("cell")
        ))
      )

      val isValid = builder.isEntryValid(table)

      assert(!isValid)
    }

    "return false for a table with the number of columns does not match the number of headers" in {
      val builder = new NotificationBuilderImpl

      val table = NotificationEntry.Table(
        TableHeader(TextElement("Header")) :: Nil,
        Seq(Seq(
          TextElement("cell 1"), TextElement("cell 2")
        ), Seq(
          TextElement("cell 3")
        ))
      )

      val isValid = builder.isEntryValid(table)

      assert(!isValid)
    }
  }

  "setSignature()" should {
    "have the default empty signature if not set" in {
      val builder = new NotificationBuilderImpl

      assert(builder.signature.isEmpty)
    }

    "set the signature retrievable by the caller" in {
      val builder = new NotificationBuilderImpl

      builder.setSignature(TextElement("Some text"), TextElement("Some more text"))

      assert(builder.signature.size == 2)
    }

    "keep the latest signature when set twice" in {
      val builder = new NotificationBuilderImpl

      builder.setSignature(TextElement("Some text"), TextElement("Some more text"))
      builder.setSignature(TextElement("Some text"))

      assert(builder.signature.size == 1)
    }
  }
}
