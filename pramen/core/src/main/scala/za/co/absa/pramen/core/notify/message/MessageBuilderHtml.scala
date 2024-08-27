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

import com.typesafe.config.Config
import za.co.absa.pramen.api.notification.NotificationEntry.Paragraph
import za.co.absa.pramen.api.notification.{Style, TableHeader, TextElement}
import za.co.absa.pramen.core.exceptions.OsSignalException
import za.co.absa.pramen.core.notify.pipeline.PipelineNotificationBuilderHtml.NOTIFICATION_EXCEPTION_MAX_LENGTH_KEY
import za.co.absa.pramen.core.utils.{ConfigUtils, ResourceUtils}
import za.co.absa.pramen.core.utils.StringUtils.{renderThreadDumps, renderThrowable}

import scala.collection.mutable.ListBuffer
import scala.compat.Platform.EOL

class MessageBuilderHtml(conf: Config) extends MessageBuilder {
  private val style = ResourceUtils.getResourceString("/email_template/style.css")
  private val maxExceptionLength = ConfigUtils.getOptionInt(conf, NOTIFICATION_EXCEPTION_MAX_LENGTH_KEY)
  private val body = new ListBuffer[String]

  override def withParagraph(text: Seq[TextElement]): MessageBuilderHtml = {
    body += renderParagraph(text)
    this
  }

  override def withParagraph(text: String): MessageBuilderHtml = {
    withParagraph(Seq(TextElement(text)))
  }

  override def withParagraph(builder: ParagraphBuilder): MessageBuilderHtml = {
    withParagraph(builder.paragraph)
  }

  override def withUnorderedList(items: Seq[Paragraph]): MessageBuilderHtml = {
    body += s"<ul>${items.map(renderListItem).mkString(EOL)}</ul>$EOL"
    this
  }

  override def withOrderedList(items: Seq[Paragraph]): MessageBuilder = {
    body += s"<ol>${items.map(renderListItem).mkString(EOL)}</ol>$EOL"
    this
  }

  override def withTable(headers: Seq[TableHeader], cells: Seq[Seq[TextElement]]): MessageBuilderHtml = {
    val tableBuilder = new TableBuilderHtml
    tableBuilder.withHeaders(headers)
    cells.foreach(tableBuilder.withRow)
    withTable(tableBuilder)
  }

  override def withTable(tableBuilder: TableBuilder): MessageBuilderHtml = {
    body += tableBuilder.renderTable
    this
  }

  override def withException(description: String, ex: Throwable): MessageBuilderHtml = {
    val rendered = ex match {
      case signalException: OsSignalException =>
        withParagraph(Seq(TextElement(s"The application was interrupted by a signal: ", Style.Exception),
          TextElement(signalException.signalName, Style.Error),
          TextElement(".", Style.Exception)))
        renderThreadDumps(signalException.threadStackTraces)
      case other =>
        withParagraph(Seq(TextElement(description, Style.Exception)))
        renderThrowable(other, maximumLength = maxExceptionLength)
    }

    withUnformattedText(rendered)
    this
  }

  override def withUnformattedText(text: String): MessageBuilderHtml = {
    body += s"<pre>$text</pre>$EOL"
    this
  }

  override def withHtmlText(html: String): MessageBuilderHtml = {
    body += s"$html$EOL"
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
      case Style.Italic => ("<i>", "</i>")
      case Style.Success => ("""<span class="tdgreen">""", "</span>")
      case Style.Warning   => ("""<span class="tdwarn">""", "</span>")
      case Style.Exception => ("""<span class="tdred">""", "</span>")
      case Style.Error     => ("""<span class="tderr">""", "</span>")
    }
    s"$styleOp${te.text}$styleCl"
  }

  private def renderParagraph(text: Seq[TextElement]): String = {
    val paragraphText = text.map(renderTextElement).mkString

    s"<p>$paragraphText</p>$EOL"
  }

  private def renderListItem(p: Paragraph): String = {
    s"<li>${renderParagraph(p.text)}</li>"
  }
}
