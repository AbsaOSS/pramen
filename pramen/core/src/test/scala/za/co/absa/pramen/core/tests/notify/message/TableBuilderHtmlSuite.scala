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

package za.co.absa.pramen.core.tests.notify.message

import org.scalatest.wordspec.AnyWordSpec
import za.co.absa.pramen.api.notification.{Align, Style, TableHeader, TextElement}
import za.co.absa.pramen.core.fixtures.TextComparisonFixture
import za.co.absa.pramen.core.notify.message._

class TableBuilderHtmlSuite extends AnyWordSpec with TextComparisonFixture {
  "TableBuilderHtml" should {
    "create an empty string if no headers not records" in {
      val builder = new TableBuilderHtml

      val actual = builder.renderTable

      assert(actual == "")
    }

    "create an empty table" in {
      val expected =
        """<div class="datagrid" style="width:fit-content"><table style="width:100%">
          |<thead><tr><th>a</th>
          |<th>b</th>
          |<th>c</th></tr></thead>
          |<tbody></tbody>
          |</table></div>
          |""".stripMargin

      val builder = new TableBuilderHtml

      val actual = builder.withHeaders(Seq(TableHeader(TextElement("a")), TableHeader(TextElement("b")), TableHeader(TextElement("c"))))
        .renderTable

      compareText(actual, expected)
    }

    "create a simple table" in {
      val expected =
        """<div class="datagrid" style="width:fit-content"><table style="width:100%">
          |<thead><tr><th>a</th>
          |<th>b</th>
          |<th>c</th></tr></thead>
          |<tbody><tr><td>1</td>
          |<td>2</td>
          |<td>3</td></tr></tbody>
          |</table></div>
          |""".stripMargin

      val builder = new TableBuilderHtml

      val actual = builder.withHeaders(Seq(TableHeader(TextElement("a")), TableHeader(TextElement("b")), TableHeader(TextElement("c"))))
        .withRow(Seq(TextElement("1"), TextElement("2"), TextElement("3")))
        .renderTable

      compareText(actual, expected)
    }

    "create a multirow, multistage table" in {
      val expected =
        """<div class="datagrid" style="width:fit-content"><table style="width:100%">
          |<thead><tr><th>a</th>
          |<th>b</th>
          |<th>c</th></tr></thead>
          |<tbody><tr><td class="tdwarn">1</td>
          |<td class="tdgreen" style="text-align:center">2</td>
          |<td style="text-align:right">3</td></tr>
          |<tr class="alt"><td style="font-style: italic;">444</td>
          |<td class="tdgreen" style="text-align:center">555</td>
          |<td style="text-align:right">666</td></tr>
          |<tr><td style="font-style: bold;">7</td>
          |<td class="tdwarn" style="text-align:center">8</td>
          |<td class="tderr" style="text-align:right">9</td></tr></tbody>
          |</table></div>
          |""".stripMargin

      val builder = new TableBuilderHtml

      val actual = builder.withHeaders(Seq(TableHeader(TextElement("a", Style.Bold), Align.Left), TableHeader(TextElement("b", Style.Warning), Align.Center), TableHeader(TextElement("c", Style.Exception), Align.Right)))
        .withRow(Seq(TextElement("1", Style.Warning), TextElement("2", Style.Success), TextElement("3", Style.Normal)))
        .withRow(Seq(TextElement("444", Style.Italic), TextElement("555", Style.Success), TextElement("666", Style.Normal)))
        .withRow(Seq(TextElement("7", Style.Bold), TextElement("8", Style.Warning), TextElement("9", Style.Exception)))
        .renderTable

      compareText(actual, expected)
    }

    "throw an exception on empty headers" in {
      val builder = new TableBuilderHtml

      val ex = intercept[IllegalArgumentException] {
        builder.withHeaders(Nil)
      }

      assert(ex.getMessage.contains("Empty headers are not allowed"))
    }

    "throw an exception on an attempt to add records when headers are not initialized" in {
      val builder = new TableBuilderHtml

      val ex = intercept[IllegalStateException] {
        builder.withRow(Seq(TextElement("a")))
      }

      assert(ex.getMessage.contains("Create headers first"))
    }

    "throw an exception on an attempt to initialize headers twice" in {
      val builder = new TableBuilderHtml

      val ex = intercept[IllegalStateException] {
        builder.withHeaders(Seq(TableHeader(TextElement("a"))))
          .withHeaders(Seq(TableHeader(TextElement("a"))))
      }

      assert(ex.getMessage.contains("Headers already created"))
    }

    "throw an exception on an attempt to add records that do not match header count" in {
      val builder = new TableBuilderHtml

      val ex = intercept[IllegalArgumentException] {
        builder.withHeaders(Seq(TableHeader(TextElement("a"))))
          .withRow(Seq(TextElement("a"), TextElement("b")))
      }

      assert(ex.getMessage.contains("Column count in the row (2) does not match header columns 1"))
    }

  }
}
