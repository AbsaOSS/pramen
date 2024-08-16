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

package za.co.absa.pramen.core.journal

import org.slf4j.LoggerFactory
import slick.jdbc.H2Profile.api._
import za.co.absa.pramen.core.app.config.InfoDateConfig
import za.co.absa.pramen.core.journal.model.{JournalTask, JournalTasks, TaskCompleted}
import za.co.absa.pramen.core.utils.SlickUtils

import java.time.{Instant, LocalDate}
import scala.util.control.NonFatal

class JournalJdbc(db: Database) extends Journal {
  import za.co.absa.pramen.core.utils.FutureImplicits._

  private val log = LoggerFactory.getLogger(this.getClass)
  private val dateFormatter = InfoDateConfig.defaultDateFormatter

  override def addEntry(entry: TaskCompleted): Unit = {
    val periodBegin = entry.periodBegin.format(dateFormatter)
    val periodEnd = entry.periodEnd.format(dateFormatter)
    val infoDate = entry.informationDate.format(dateFormatter)

    val journalTask = JournalTask(entry.jobName,
      entry.tableName,
      periodBegin,
      periodEnd,
      infoDate,
      entry.inputRecordCount,
      entry.inputRecordCountOld,
      entry.outputRecordCount,
      entry.outputRecordCountOld,
      entry.outputSize,
      entry.startedAt,
      entry.finishedAt,
      entry.status,
      entry.failureReason,
      entry.sparkApplicationId,
      entry.pipelineId,
      entry.pipelineName,
      entry.environmentName,
      entry.tenant)

    try {
      db.run(
        JournalTasks.journalTasks += journalTask
      ).execute()
    } catch {
      case NonFatal(ex) => log.error(s"Unable to write to the journal table.", ex)
    }
  }

  override def getEntries(from: Instant, to: Instant): Seq[TaskCompleted] = {
    val fromSec = from.getEpochSecond
    val toSec = to.getEpochSecond

    val entries = SlickUtils.executeQuery(db, JournalTasks.journalTasks.filter(d => d.finishedAt >= fromSec && d.finishedAt <= toSec ))

    entries.map(v => {
      model.TaskCompleted(
        jobName = v.jobName,
        tableName = v.pramenTableName,
        periodBegin = LocalDate.parse(v.periodBegin, dateFormatter),
        periodEnd = LocalDate.parse(v.periodEnd, dateFormatter),
        informationDate = LocalDate.parse(v.informationDate, dateFormatter),
        inputRecordCount = v.inputRecordCount,
        inputRecordCountOld = v.inputRecordCountOld,
        outputRecordCount = v.outputRecordCount,
        outputRecordCountOld = v.outputRecordCountOld,
        outputSize = v.outputSize,
        startedAt = v.startedAt,
        finishedAt = v.finishedAt,
        status = v.status,
        failureReason = v.failureReason,
        sparkApplicationId = v.sparkApplicationId,
        pipelineId = v.pipelineId,
        pipelineName = v.pipelineName,
        environmentName = v.environmentName,
        tenant = v.tenant
      )
    }).toList
  }

}
