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

package za.co.absa.pramen.core.notify.message

import za.co.absa.pramen.core.utils.ResourceUtils
import za.co.absa.pramen.core.utils.StringUtils.renderThrowable

import scala.collection.mutable.ListBuffer
import scala.compat.Platform.EOL

class MessageBuilderHtml extends MessageBuilder {
  private val style = ResourceUtils.getResourceString("/email_template/style.css")
  private val body = new ListBuffer[String]

  override def withParagraph(text: Seq[TextElement]): MessageBuilderHtml = {
    val sb = new StringBuilder

    sb.append("<p>")
    text.foreach(te => sb.append(renderTextElement(te)))
    sb.append(s"</p>$EOL")
    body += sb.toString()
    this
  }

  override def withParagraph(text: String): MessageBuilderHtml = {
    withParagraph(Seq(TextElement(text)))
  }

  override def withParagraph(builder: ParagraphBuilder): MessageBuilderHtml = {
    withParagraph(builder.paragraph)
  }

  override def withTable(tableBuilder: TableBuilder): MessageBuilderHtml = {
    body += tableBuilder.renderTable
    this
  }

  override def withException(description: String, ex: Throwable): MessageBuilderHtml = {
    val rendered = renderThrowable(ex)

    withParagraph(Seq(TextElement(description, Style.Exception)))
    withUnformattedText(rendered)
    this
  }

  override def withUnformattedText(text: String): MessageBuilderHtml = {
    body += s"<pre>$text</pre>$EOL"
    this
  }

  override def withRawParagraph(text: String): MessageBuilderHtml = {
    body += s"""<p>$text</p>$EOL"""
    this
  }

  override def renderBody: String = {
    s"<html>$EOL<head>$EOL<style>$EOL$style$EOL</style>$EOL<body>${body.mkString(EOL)}$EOL</body>$EOL</html>"
  }

  private def renderTextElement(te: TextElement): String = {
    val (styleOp, styleCl) = te.style match {
      case Style.Normal => ("", "")
      case Style.Bold => ("<b>", "</b>")
      case Style.Success => ("""<span class="tdgreen">""", "</span>")
      case Style.Warning   => ("""<span class="tdwarn">""", "</span>")
      case Style.Exception => ("""<span class="tdred">""", "</span>")
      case Style.Error     => ("""<span class="tderr">""", "</span>")
    }
    s"$styleOp${te.text}$styleCl"
  }
}
