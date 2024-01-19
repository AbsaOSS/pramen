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

import za.co.absa.pramen.api.notification.{Align, Style, TableHeader, TextElement}

import scala.collection.mutable.ListBuffer

class TableBuilderHtml extends TableBuilder {
  private var headerArray: Array[TableHeader] = _
  private val body = new ListBuffer[String]

  override def withHeaders(headers: Seq[TableHeader]): TableBuilder = {
    if (headerArray != null) {
      throw new IllegalStateException("Headers already created")
    }

    if (headers.isEmpty) {
      throw new IllegalArgumentException("Empty headers are not allowed")
    }

    this.headerArray = headers.toArray

    this
  }

  override def withRow(row: Iterable[TextElement]): TableBuilder = {
    if (headerArray == null) {
      throw new IllegalStateException("Create headers first")
    }
    if (headerArray.length != row.size) {
      throw new IllegalArgumentException(s"Column count in the row (${row.size}) does not match header columns ${headerArray.length}")
    }

    val rowClass = renderRowClass(body.length)
    val rowHtml = row.zipWithIndex.map { case (c, i) =>
      val header = headerArray(i)
      val clazz = renderStyleClass(c.style)
      val align = renderAlign(header.align)

      s"<td$clazz$align>${c.text}</td>"
    }.mkString(s"<tr$rowClass>", "\n", "</tr>")

    body += rowHtml

    this
  }

  def renderTable: String = {
    val headerHtml = if (headerArray != null) {
      headerArray.map(c => {
        s"<th>${c.textElement.text}</th>"
      }).mkString("<tr>", "\n", "</tr>")
    } else ""

    if (headerHtml == "" && body.isEmpty) {
      ""
    } else {
      val styleHeader = """<div class="datagrid" style="width:fit-content"><table style="width:100%">"""
      val bodyHtml = body.mkString("\n")

      s"$styleHeader\n<thead>$headerHtml</thead>\n<tbody>$bodyHtml</tbody>\n</table></div>"
    }
  }

  private def renderAlign(align: Align): String = {
    align match {
      case Align.Left   => ""
      case Align.Right  => """ style="text-align:right""""
      case Align.Center => """ style="text-align:center""""
    }
  }

  private def renderStyleClass(style: Style): String = {
    style match {
      case Style.Normal    => ""
      case Style.Bold      => """ style="font-style: bold;""""
      case Style.Italic    => """ style="font-style: italic;""""
      case Style.Success   => """ class="tdgreen""""
      case Style.Warning   => """ class="tdwarn""""
      case Style.Error     => """ class="tdred""""
      case Style.Exception => """ class="tderr""""
    }
  }

  private def renderRowClass(rowNumber: Int): String = {
    if (rowNumber % 2 == 0) "" else """ class="alt""""
  }
}
