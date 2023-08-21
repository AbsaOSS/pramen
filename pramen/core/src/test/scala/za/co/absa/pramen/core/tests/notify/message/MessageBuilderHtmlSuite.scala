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
import za.co.absa.pramen.api.notification.{Style, TableHeader, TextElement}
import za.co.absa.pramen.core.fixtures.TextComparisonFixture
import za.co.absa.pramen.core.notify.message._
import za.co.absa.pramen.core.utils.ResourceUtils

class MessageBuilderHtmlSuite extends AnyWordSpec with TextComparisonFixture {
  "EmailBuilderHtml" should {
    "render an email with the requested elements" in {
      val expected = ResourceUtils.getResourceString("/test/notify/expectedMessage.dat")

      val builder = new MessageBuilderHtml
      val tb = new TableBuilderHtml

      val actual = builder
        .withParagraph(TextElement("Hi, ") :: TextElement("user", Style.Bold) :: TextElement(". This is a test") :: Nil)
        .withParagraph(TextElement("This is an") :: TextElement("error", Style.Error) :: Nil)
        .withParagraph(TextElement("This is a") :: TextElement("warning", Style.Warning) :: Nil)
        .withParagraph(ParagraphBuilder()
          .withText("This is a")
          .withText("success", Style.Success)
        )
        .withUnformattedText("This is some unformatted text\nLine 2\n")
        .withException("This is an exception:",
          new CustomException("Dummy Exception")
        )
        .withException("This is an exception with a cause:",
          new CustomException("Dummy Exception", new CustomException("Dummy Cause"))
        )
        .withTable(tb
          .withHeaders(Seq(TableHeader(TextElement("a")), TableHeader(TextElement("b")), TableHeader(TextElement("c"))))
          .withRow(Seq(TextElement("1"), TextElement("2"), TextElement("3")))
        )
        .withRawParagraph("Regards,<br>MyApp<br>MyVersion")
        .renderBody

      compareText(actual, expected)
    }
  }

}