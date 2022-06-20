/*
 * Copyright 2022 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.pramen.framework.notify

import com.typesafe.config.Config
import za.co.absa.pramen.framework.config.Keys.TIMEZONE
import za.co.absa.pramen.framework.exceptions.CmdFailedException
import za.co.absa.pramen.framework.model.TaskStatus
import za.co.absa.pramen.framework.utils.{ConfigUtils, ResourceUtils, StringUtils, TimeUtils}

import java.time._
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ListBuffer
import scala.compat.Platform.EOL

class NotificationBuilderHtml(implicit conf: Config) extends NotificationBuilder {
  import za.co.absa.pramen.framework.utils.DateUtils._

  private val timestampFmt: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm Z")

  private val zoneId = getZoneId

  private var minRps = 0
  private var goodRps = Int.MaxValue

  var appException: Option[Throwable] = None
  var appName: String = "Unspecified Job"
  var envName: String = "Unspecified Environment"
  var appStarted: Instant = Instant.now()
  var appFinished: Instant = Instant.now()
  var isDryRun = false
  var isUndercover = false

  val schemaDifferences = new ListBuffer[SchemaDifference]
  val completedTasks = new ListBuffer[TaskCompleted]

  override def addFailureException(ex: Throwable): Unit = {
    appException = Option(ex)
  }

  override def addAppName(appName: String): Unit = {
    this.appName = appName
  }

  override def addEnvironmentName(envName: String): Unit = {
    this.envName = envName
  }

  override def addAppDuration(appStarted: Instant, appFinished: Instant): Unit = {
    this.appStarted = appStarted
    this.appFinished = appFinished
  }

  override def addDryRun(isDryRun: Boolean): Unit = {
    this.isDryRun = isDryRun
  }

  override def addUndercover(isUndercover: Boolean): Unit = {
    this.isUndercover = isUndercover
  }

  override def addRpsMetrics(minRps: Int, goodRps: Int): Unit = {
    this.minRps = minRps
    this.goodRps = goodRps
  }

  override def addSchemaDifferences(schemaDifference: SchemaDifference): Unit = {
    schemaDifferences += schemaDifference
  }

  override def addCompletedTask(completedTask: TaskCompleted): Unit = {
    completedTasks += completedTask
  }

  def renderSubject(): String = {
    val timeCreatedStr = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"))
    val dryRunStr = if (isDryRun) "(DRY RUN) " else ""
    if (appException.isEmpty) {
      s"${dryRunStr}Notification for $appName at $timeCreatedStr"
    } else {
      s"${dryRunStr}Notification of FAILURE for $appName at $timeCreatedStr"
    }
  }

  def renderBody(): String = {
    val css = ResourceUtils.getResourceString("/email_template/style.css")
    val style = s"<style>$css</style>"

    val dryRunStr = if (isDryRun) "<b>(DRY RUN)</b> " else ""

    val header = renderHeader()

    val dataLoadedTextHtml = if (completedTasks.isEmpty) {
      s"""<p>No new data has been loaded.</p>"""
    } else {
      renderCompletedTasks()
    }

    val schemaDifferencesHtml = schemaDifferences map renderSchemaDifference mkString "\n"

    s"""$style<p>Hi,</p>
       |
       |$dryRunStr$header
       |
       |$dataLoadedTextHtml
       |
       |$schemaDifferencesHtml
       |
       |<p>Regards,<br>
       |Pramen<br>
       |${conf.getString("pramen.application.version")}</p>
       |""".stripMargin
  }

  private def getZoneId: ZoneId = {
    ConfigUtils.getOptionString(conf, TIMEZONE) match {
      case Some(tz) => ZoneId.of(tz)
      case None => ZoneId.systemDefault()
    }
  }

  private def renderHeader(): String = {
    val jobStartedStr = ZonedDateTime.ofInstant(appStarted, zoneId).format(timestampFmt)
    val jobFinishedStr = ZonedDateTime.ofInstant(appFinished, zoneId).format(timestampFmt)

    val jobDurationMillis = Duration.between(appStarted, appFinished).toMillis

    val undercoverMode = if (isUndercover) " The job ran in <i>undercover</i> mode - no updates to bookkeeping tables are saved." else ""

    val jobDuration =
      s"""<p>Job started at <b>$jobStartedStr</b>, finished at <b>$jobFinishedStr</b>.
         |Elapsed time: <b>${TimeUtils.prettyPrintElapsedTime(jobDurationMillis)}</b>.$undercoverMode</p>""".stripMargin

    val allTasksFailed = completedTasks.nonEmpty && completedTasks.forall(_.failureReason.isDefined)
    val someTasksSucceeded = completedTasks.exists(_.failureReason.isEmpty)
    val someTasksFailed = completedTasks.exists(_.failureReason.isDefined)

    appException match {
      case None if allTasksFailed =>
        s"""<p>This is a notification from Pramen for <b>$appName</b>
           | on <b>$envName.</b>
           | The job has <span class="tderr">FAILED</span>.</p>
           | $jobDuration"""
      case None if someTasksSucceeded && someTasksFailed =>
        s"""<p>This is a notification from Pramen for <b>$appName</b>
           | on <b>$envName.</b>
           | The job has <span class="tdwarn">partially succeeded</span>.</p>
           | $jobDuration"""
      case None =>
        s"""<p>This is a notification from Pramen for <b>$appName</b>
           | on <b>$envName.</b>
           | The job has <span class="tdgreen">succeeded</span>.</p>
           | $jobDuration"""
      case Some(ex) =>
        s"""<p>This is a notification from Pramen for <b>$appName</b>
           | on <b>$envName.</b>
           | The job has <span class="tderr">FAILED</span>.</p>
           | $jobDuration
           |
           | <p>The job has failed with the following exception:</p>
           | <pre>${renderException(ex)}</pre>
           |"""
    }
  }

  private def renderException(ex: Throwable): String = {
    ex match {
      case CmdFailedException(msg, logLines) =>
        if (logLines.isEmpty) {
          msg
        } else {
          s"""$msg\nLast log lines:\n${logLines.mkString("", EOL, EOL)}"""
        }
      case ex: Throwable =>
        val base = s"""${ex.toString}\n${ex.getStackTrace.mkString("", EOL, EOL)}"""
        val cause = Option(ex.getCause) match {
          case Some(c) => s"""\nCaused by ${c.toString}\n${c.getStackTrace.mkString("", EOL, EOL)}"""
          case None => ""
        }
        base + cause
    }
  }
  private def renderCompletedTasks(): String = {
    completedTasks
      .groupBy(_.jobName)
      .toArray
      .sortBy(_._1)
      .map { case (jobName, tasks) => renderJob(jobName, tasks) }
      .mkString("\n")
  }

  private def renderJob(jobName: String, tasks: Seq[TaskCompleted]): String = {
    s"""<p>$jobName:</p>
    ${renderJobTable(jobName, tasks)}"""
  }

  private def renderJobTable(jobName: String, tasks: Seq[TaskCompleted]): String = {
    val dataLoaded = tasks
      .filter(_.jobName.equalsIgnoreCase(jobName))
      .sortBy(d => (d.tableName, d.informationDate))

    val outputRecordsKnown = tasks.exists(_.outputRecordCount.isDefined)
    val outputSizeKnown = tasks.exists(_.outputSize.isDefined)
    val haveFailures = tasks.exists(_.failureReason.isDefined)

    val outputRecordHeader = if (outputRecordsKnown) "<th>Output Record Count</th>" else ""
    val outputSizeHeader = if (outputSizeKnown) "<th>Output Size</th>" else ""
    val reasonHeader = if (haveFailures) "<th>Reason</th>" else ""

    val header =
      s"""<div class="datagrid" style="width:fit-content"><table style="width:100%">
         |<thead><tr>
         |<th>Table Name</th>
         |<th>Information Date</th>
         |<th>Input Record Count</th>
         |$outputRecordHeader
         |<th>Elapsed Time</th>
         |$outputSizeHeader
         |<th>RPS</th>
         |<th>Landed at</th>
         |<th>Status</th>
         |$reasonHeader
         |</tr></thead><tbody>
         |""".stripMargin
    val footer = s"""</tbody></table></div>""".stripMargin

    var i = 0
    val contents = dataLoaded.map(chunk => {
      i += 1
      val rowClass = if (i % 2 == 0) """ class="alt"""" else ""
      val statusClass = chunk.status match {
        case TaskStatus.NEW.toString => """class="tdgreen""""
        case TaskStatus.RENEW.toString => """class="tdgreen""""
        case _ => """class="tdwarn""""
      }

      val inputRecordCountText = getRecordCountText(chunk.inputRecordCountOld, chunk.inputRecordCount)
      val outputRecordCountHtml = if (outputRecordsKnown) {
        val outputRecordCountText = getRecordCountText(chunk.outputRecordCountOld.getOrElse(0L), chunk.outputRecordCount.getOrElse(0L))
        s"""<td style="text-align:right">$outputRecordCountText</td>"""
      } else {
        ""
      }

      val elapsedTimeText = TimeUtils.prettyPrintElapsedTimeShort((chunk.finishedAt - chunk.startedAt) * 1000L)
      val elapsedTimeHtml = s"""<td style="text-align:center">$elapsedTimeText</td>"""

      val outputSizeHtml = if (outputSizeKnown) {
        val outputSizeStr = chunk.outputSize match {
          case Some(size) => StringUtils.prettySize(size)
          case None => ""
        }
        s"""<td style="text-align:right">$outputSizeStr</td>"""
      } else {
        ""
      }

      val failureReasonHtml = if (haveFailures) {
        val reason = chunk.failureReason.getOrElse("")
        s"""<td style="text-align:center"> $reason </td> """
      } else {
        ""
      }

      s"""<tr$rowClass>
         |<td>${chunk.tableName}</td>
         |<td>${chunk.informationDate}</td>
         |<td style="text-align:right">$inputRecordCountText</td>
         |$outputRecordCountHtml
         |$elapsedTimeHtml
         |$outputSizeHtml
         |${getThroughputRps(chunk)}
         |<td>${ZonedDateTime.ofInstant(Instant.ofEpochSecond(chunk.finishedAt), zoneId).format(timestampFmt)}</td>
         |<td style="text-align:center" $statusClass>${chunk.status}</td>
         |$failureReasonHtml
         |</tr>""".stripMargin

    }).mkString("\n")

    s"$header $contents $footer"
  }

  private def getThroughputRps(task: TaskCompleted): String = {
    val startedAt = Instant.ofEpochSecond(task.startedAt)
    val finishedAt = Instant.ofEpochSecond(task.finishedAt)
    val jobDuration = Duration.between(startedAt, finishedAt).getSeconds
    val (recordsPerSecStr, rpsClass) = if (jobDuration > 0 && task.outputRecordCount.getOrElse(0L) > 0L) {
      val throughput = task.inputRecordCount / jobDuration
      val rpsClass = throughput match {
        case n if n < minRps => """class="tdwarn""""
        case n if n >= goodRps => """class="tdgreen""""
        case _ => ""
      }
      (throughput.toString, rpsClass)
    } else {
      ("", "")
    }

    s"""<td style="text-align:right" $rpsClass>$recordsPerSecStr</td>"""
  }
  private def getRecordCountText(recordCountOld: Long, recordCountNew: Long): String = {
    if (recordCountOld == 0) {
      recordCountNew.toString
    } else {
      val difference = recordCountNew - recordCountOld
      if (difference > 0) {
        s"$recordCountNew (+$difference)"
      } else {
        s"$recordCountNew ($difference)"
      }
    }
  }

  private def renderSchemaDifference(schemaDifference: SchemaDifference): String = {
    val title = s"<p>Schema change for <b>${schemaDifference.tableName}</b> introduced at <b>${schemaDifference.infoDateNew}</b>. Last schema change was at <b>${schemaDifference.infoDateOld}</b></p>"

    val header =
      s"""<div class="datagrid" style="width:fit-content"><table style="width:100%">
         |  <thead><tr>
         |    <th>Change</th>
         |    <th>Old column</th>
         |    <th>New column</th>
         |  </tr></thead><tbody>
         |""".stripMargin
    val footer = s"""</tbody></table></div>""".stripMargin

    var i = 0
    val contents = schemaDifference.changes.map(change => {
      i += 1
      val rowClass = if (i % 2 == 0) """class="alt"""" else ""
      val changeClass = change match {
        case _: FieldChange.NewField     => """class="tdgreen""""
        case _: FieldChange.DeletedField => """class="tderr""""
        case _: FieldChange.ChangedType  => """class="tdwarn""""
      }

      val changeDescription = change match {
        case _: FieldChange.NewField     => "Added"
        case _: FieldChange.DeletedField => "Deleted"
        case _: FieldChange.ChangedType  => "Changed"
      }

      val changeColumnOld = change match {
        case _: FieldChange.NewField     => ""
        case c: FieldChange.DeletedField => s"<b>${c.columnName}</b> (${c.dataType})"
        case c: FieldChange.ChangedType  => s"<b>${c.columnName}</b> (${c.oldType})"
      }

      val changeColumnNew = change match {
        case c: FieldChange.NewField     => s"<b>${c.columnName}</b> (${c.dataType})"
        case _: FieldChange.DeletedField => ""
        case c: FieldChange.ChangedType  => s"<b>${c.columnName}</b> (${c.newType})"
      }

      s"<tr $rowClass><td $changeClass>$changeDescription</td><td>$changeColumnOld</td><td>$changeColumnNew</td></tr>"
    }).mkString("\n")

    s"$title $header $contents $footer"
  }
}
