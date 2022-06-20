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

package za.co.absa.pramen.framework.samples

import com.typesafe.config.ConfigFactory
import za.co.absa.pramen.framework.bookkeeper.SyncBookKeeper
import za.co.absa.pramen.framework.config.WatcherConfig.TRACK_UPDATES
import za.co.absa.pramen.framework.mocks.bookkeeper.SyncBookkeeperMock
import za.co.absa.pramen.framework.validator.{DataAvailabilityValidator, SyncJobValidator}

import java.time.LocalDate
import scala.collection.mutable.ListBuffer

class DataValidationTestCaseBuilder {
  val bk: SyncBookKeeper = new SyncBookkeeperMock

  private var mandatoryTables = new ListBuffer[String]
  private var optionalTables = new ListBuffer[String]

  private val upToDateDate = LocalDate.of(2020, 10, 8)
  private val outdatedDate = LocalDate.of(2020, 9, 8)

  private var checkNum: Int = 1
  private var checkConfigs: String = ""

  def getInfoDateBegin: LocalDate = LocalDate.of(2020, 10, 1)

  def getInfoDateEnd: LocalDate = LocalDate.of(2020, 10, 10)

  def addOutputTableUpToDate(tableName: String): Unit = {
    bk.setRecordCount(tableName, getInfoDateEnd, getInfoDateEnd, getInfoDateEnd, 100, 100, 2222221L, 2222222L)
  }

  def addOutputTableOutDated(tableName: String): Unit = {
    bk.setRecordCount(tableName, getInfoDateEnd, getInfoDateEnd, getInfoDateEnd, 100, 100, 1111110L, 1111111L)
  }

  def addMandatoryUpToDateTable(tableName: String): Unit = {
    mandatoryTables += tableName
    bk.setRecordCount(tableName, upToDateDate, upToDateDate, upToDateDate, 100, 100, 1234567L, 1234568L)
  }

  def addMandatoryOutdatedTable(tableName: String): Unit = {
    mandatoryTables += tableName
    bk.setRecordCount(tableName, outdatedDate, outdatedDate, outdatedDate, 100, 100, 1234567L, 1234568L)
  }

  def addOptionalUpToDateTable(tableName: String): Unit = {
    optionalTables += tableName
    bk.setRecordCount(tableName, upToDateDate, upToDateDate, upToDateDate, 100, 100, 1234567L, 1234568L)
  }

  def addOptionalOutdatedTable(tableName: String): Unit = {
    optionalTables += tableName
    bk.setRecordCount(tableName, outdatedDate, outdatedDate, outdatedDate, 100, 100, 1234567L, 1234568L)
  }

  def addCheck(dateFrom: String = "minusMonths(@infoDate, 1)", dateTo: String = "@infoDate"): Unit = {
    checkConfigs = checkConfigs + "\n" + getCheckConfig(checkNum, dateFrom, dateTo)
    checkNum += 1
    mandatoryTables = new ListBuffer[String]
    optionalTables = new ListBuffer[String]
  }

  def getTestCase: SyncJobValidator = {
    val confStr =
      s"""
  $TRACK_UPDATES = true
  pramen.input.data.availability {
     $checkConfigs
  }
    """.stripMargin

    val config = ConfigFactory.parseString(confStr)
    DataAvailabilityValidator.fromConfig(config, bk)
  }

  private def getCheckConfig(n: Int, dateFrom: String, dateTo: String): String = {
    val dateFromStr = if (dateFrom.nonEmpty) {
      s"""date.from = "$dateFrom""""
    } else {
      ""
    }

    val dateToStr = if (dateTo.nonEmpty) {
      s"""date.to = "$dateTo""""
    } else {
      ""
    }

    s"""
   check.$n {
     tables.all = [ ${mandatoryTables.mkString(", ")} ]
     tables.one.of = [ ${optionalTables.mkString(", ")} ]

     $dateFromStr
     $dateToStr
   }
   """
  }

}
