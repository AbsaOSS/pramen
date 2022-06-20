/*
 * Copyright 2020 ABSA Group Limited
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

package za.co.absa.pramen.framework.tests.runner

import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterAll, WordSpec}
import za.co.absa.pramen.api.{JobDependency, TaskDependency}
import za.co.absa.pramen.framework.base.SparkTestBase
import za.co.absa.pramen.framework.fixtures.MongoDbFixture
import za.co.absa.pramen.framework.mocks.DummyConfigFactory
import za.co.absa.pramen.framework.mocks.bookkeeper.SyncBookkeeperMock
import za.co.absa.pramen.framework.mocks.job.{AggregationJobSpy, SourceJobSpy}
import za.co.absa.pramen.framework.mocks.lock.TokenLockFactoryMock
import za.co.absa.pramen.framework.mocks.state.RunStateSpy
import za.co.absa.pramen.framework.mocks.validator.DummySyncJobValidator
import za.co.absa.pramen.framework.model.TaskStatus
import za.co.absa.pramen.framework.runner.JobCoordinator

import java.io.File
import java.nio.file.Files
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, LocalTime, ZoneOffset}

class DailyJobRunnerSuite extends WordSpec with SparkTestBase with MongoDbFixture with BeforeAndAfterAll {
  // ToDo Split assert groups into individual tests

  var tempPath: String = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    tempPath = Files.createTempDirectory("testDaily").toAbsolutePath.toString
  }

  override protected def afterAll(): Unit = {
    FileUtils.deleteDirectory(new File(tempPath))
    super.afterAll()
  }

  "A job runner" when {
    val monday = LocalDate.of(2020, 8, 10)
    val tuesday = LocalDate.of(2020, 8, 11)
    val wednesday = LocalDate.of(2020, 8, 12)
    val thursday = LocalDate.of(2020, 8, 13)
    val friday = LocalDate.of(2020, 8, 14)
    val saturday = LocalDate.of(2020, 8, 15)
    val sunday = LocalDate.of(2020, 8, 16)

    val thursdayTimestamp = LocalDateTime.of(thursday, LocalTime.of(14, 22)).toEpochSecond(ZoneOffset.UTC)
    val fridayJobAStarted = LocalDateTime.of(friday, LocalTime.of(10, 0)).toEpochSecond(ZoneOffset.UTC)
    val fridayJobAFinished = LocalDateTime.of(friday, LocalTime.of(10, 30)).toEpochSecond(ZoneOffset.UTC)
    val fridayJobBStarted = LocalDateTime.of(friday, LocalTime.of(11, 1)).toEpochSecond(ZoneOffset.UTC)
    val fridayJobBFinished = LocalDateTime.of(friday, LocalTime.of(11, 59)).toEpochSecond(ZoneOffset.UTC)

    "daily schedule" should {
      "be able to run SourceJob with new data" in {
        val pramenConfig = DummyConfigFactory.getDummyConfig(bookkeepingLocation = Some(tempPath), bookkeepingConnectionString = Some(connectionString))
        val bookkeeperMock = new SyncBookkeeperMock
        val runStateSpy = new RunStateSpy
        val tokenFactory = new TokenLockFactoryMock
        val validator = new DummySyncJobValidator()
        val runner = new JobCoordinator(pramenConfig, tokenFactory, bookkeeperMock, runStateSpy, validator, saturday)
        val job = new SourceJobSpy(100)

        runner.runJob(job)

        assert(runStateSpy.schemaDifferences.isEmpty)

        // Check proper tasks generated
        assert(runStateSpy.completedTasks.size == 4)
        assert(runStateSpy.completedTasks.head.informationDate == tuesday)
        assert(runStateSpy.completedTasks.last.informationDate == friday)
        assert(runStateSpy.completedTasks.head.status == TaskStatus.LATE.toString)
        assert(runStateSpy.completedTasks.last.status == TaskStatus.NEW.toString)
        assert(runStateSpy.completedTasks.head.inputRecordCount == 100L)
        assert(runStateSpy.completedTasks.head.inputRecordCountOld == 0L)
        assert(runStateSpy.completedTasks.head.outputRecordCount.get == 100L)
        assert(runStateSpy.completedTasks.head.outputRecordCountOld.isEmpty)

        // Check readers and writers are caller proper amount of time
        assert(job.getReaderCalled == 1)
        assert(job.getWriterCalled >= 1)
        assert(job.getTablesCalled == 1)
        assert(job.runTaskCalled == 4)

        // Check proper information dates read and written
        val infoDates = job.reader.infoDatePeriodsCountCalled.map(_._1)
        val expectedDates = List("2020-08-11", "2020-08-12", "2020-08-13", "2020-08-14").map(getDate)
        assert(job.reader.infoDatePeriodsCountCalled == job.reader.infoDatePeriodsDataRead)
        assert(infoDates.toList == expectedDates)
        assert(job.writer.infoDatesWritten.toList == expectedDates)

        // Check output records are logged
        val chunks = bookkeeperMock.getChunks.sortBy(_.infoDate)
        assert(chunks.size == 4)
        assert(chunks.forall(_.inputRecordCount == 100L))
        assert(chunks.forall(_.outputRecordCount == 100L))
        assert(chunks.head.infoDate == chunks.head.infoDateBegin && chunks.head.infoDate == chunks.head.infoDateEnd && chunks.head.infoDate == "2020-08-11")
        assert(chunks(1).infoDate == chunks(1).infoDateBegin && chunks(1).infoDate == chunks(1).infoDateEnd && chunks(1).infoDate == "2020-08-12")
        assert(chunks(2).infoDate == chunks(2).infoDateBegin && chunks(2).infoDate == chunks(2).infoDateEnd && chunks(2).infoDate == "2020-08-13")
        assert(chunks(3).infoDate == chunks(3).infoDateBegin && chunks(3).infoDate == chunks(3).infoDateEnd && chunks(3).infoDate == "2020-08-14")
      }

      "be able to run AggregationJob with new data" in {
        val pramenConfig = DummyConfigFactory.getDummyConfig(bookkeepingLocation = Some(tempPath), trackDays = 5, bookkeepingConnectionString = Some(connectionString))
        val bookkeeperMock = new SyncBookkeeperMock
        val runStateSpy = new RunStateSpy
        val tokenFactory = new TokenLockFactoryMock
        val validator = new DummySyncJobValidator()
        val runner = new JobCoordinator(pramenConfig, tokenFactory, bookkeeperMock, runStateSpy, validator, sunday)
        var validations = 0
        var dateSelections = 0
        var datesPassed = Array.empty[LocalDate]
        var latestOutputDatePassed: Option[LocalDate] = None
        val job = new AggregationJobSpy(JobDependency("a" :: "b" :: Nil, "c") :: Nil, Some(100)) {
          override def selectInfoDates(newDataAvailable: Array[LocalDate], latestOutputInfoDate: Option[LocalDate]): Array[LocalDate] = {
            dateSelections += 1
            datesPassed = newDataAvailable
            latestOutputDatePassed = latestOutputInfoDate
            val k = newDataAvailable.filter(d => d != tuesday)
            k
          }

          override def validateTask(taskDependencies: Seq[TaskDependency], infoDateBegin: LocalDate, infoDateEnd: LocalDate, infoDateOutput: LocalDate): Unit = {
            validations += 1
            if (infoDateOutput == thursday) {
              throw new IllegalStateException(s"Fail on that date")
            }
          }
        }

        bookkeeperMock.setRecordCount("a", monday, monday, monday, 100, 50, thursdayTimestamp, thursdayTimestamp)
        bookkeeperMock.setRecordCount("b", monday, monday, monday, 200, 25, thursdayTimestamp, thursdayTimestamp)
        bookkeeperMock.setRecordCount("c", monday, monday, monday, 300, 75, fridayJobAStarted, fridayJobAFinished)

        bookkeeperMock.setRecordCount("a", tuesday, tuesday, tuesday, 200, 150, thursdayTimestamp, thursdayTimestamp)
        bookkeeperMock.setRecordCount("b", tuesday, tuesday, tuesday, 300, 250, thursdayTimestamp, thursdayTimestamp)

        bookkeeperMock.setRecordCount("a", wednesday, wednesday, wednesday, 200, 150, thursdayTimestamp, thursdayTimestamp)
        bookkeeperMock.setRecordCount("b", wednesday, wednesday, wednesday, 300, 250, thursdayTimestamp, thursdayTimestamp)

        bookkeeperMock.setRecordCount("a", thursday, thursday, thursday, 100, 60, thursdayTimestamp, thursdayTimestamp)
        bookkeeperMock.setRecordCount("b", thursday, thursday, thursday, 150, 120, thursdayTimestamp, thursdayTimestamp)

        bookkeeperMock.setRecordCount("a", friday, friday, friday, 75, 75, fridayJobAStarted, fridayJobAFinished)
        bookkeeperMock.setRecordCount("b", friday, friday, friday, 25, 25, fridayJobBStarted, fridayJobBFinished)
        bookkeeperMock.setRecordCount("c", friday, friday, friday, 80, 80, fridayJobAStarted, fridayJobAFinished)

        bookkeeperMock.setRecordCount("a", saturday, saturday, saturday, 55, 44, fridayJobAStarted, fridayJobAFinished)
        bookkeeperMock.setRecordCount("b", saturday, saturday, saturday, 33, 22, fridayJobBStarted, fridayJobBFinished)

        runner.runJob(job)

        // Assert proper pre-processing
        assert(dateSelections == 1)
        assert(validations == 4)

        assert(datesPassed.toList == tuesday :: wednesday :: thursday :: friday :: saturday :: Nil)
        assert(latestOutputDatePassed.get == friday)

        // Check proper tasks generated
        assert(runStateSpy.completedTasks.size == 5)
        val task1 = runStateSpy.completedTasks.head
        val task2 = runStateSpy.completedTasks(1)
        val task3 = runStateSpy.completedTasks(2)
        val task4 = runStateSpy.completedTasks(3)
        val task5 = runStateSpy.completedTasks(4)

        assert(task1.periodBegin == tuesday)
        assert(task1.periodEnd == tuesday)
        assert(task1.informationDate == tuesday)
        assert(task1.status == TaskStatus.SKIPPED.toString)
        assert(task1.inputRecordCount == 400L)
        assert(task1.inputRecordCountOld == 0L)
        assert(task1.outputRecordCount.isEmpty)
        assert(task1.outputRecordCountOld.isEmpty)
        assert(task1.failureReason.isEmpty)

        assert(task2.periodBegin == wednesday)
        assert(task2.periodEnd == wednesday)
        assert(task2.informationDate == wednesday)
        assert(task2.status == TaskStatus.LATE.toString)
        assert(task2.inputRecordCount == 400L)
        assert(task2.inputRecordCountOld == 0L)
        assert(task2.outputRecordCount.get == 100L)
        assert(task2.outputRecordCountOld.isEmpty)
        assert(task2.failureReason.isEmpty)

        assert(task3.periodBegin == thursday)
        assert(task3.periodEnd == thursday)
        assert(task3.informationDate == thursday)
        assert(task3.status == TaskStatus.NOT_READY.toString)
        assert(task3.inputRecordCount == 180L)
        assert(task3.inputRecordCountOld == 0L)
        assert(task3.outputRecordCount.isEmpty)
        assert(task3.outputRecordCountOld.isEmpty)
        assert(task3.failureReason.get == "Fail on that date")

        assert(task4.periodBegin == friday)
        assert(task4.periodEnd == friday)
        assert(task4.informationDate == friday)
        assert(task4.status == TaskStatus.UPDATE.toString)
        assert(task4.inputRecordCount == 100L)
        assert(task4.inputRecordCountOld == 80L)
        assert(task4.outputRecordCount.get == 100L)
        assert(task4.outputRecordCountOld.get == 80L)
        assert(task4.failureReason.isEmpty)

        assert(task5.periodBegin == saturday)
        assert(task5.periodEnd == saturday)
        assert(task5.informationDate == saturday)
        assert(task5.status == TaskStatus.NEW.toString)
        assert(task5.inputRecordCount == 66L)
        assert(task5.inputRecordCountOld == 0L)
        assert(task5.outputRecordCount.get == 100L)
        assert(task5.outputRecordCountOld.isEmpty)
        assert(task5.failureReason.isEmpty)

        // Check output records are logged
        val chunks = bookkeeperMock.getDataChunks("c", monday, sunday).sortBy(_.infoDate)
        assert(chunks.size == 5)

        val chunk1 = chunks.head
        val chunk2 = chunks(1)
        val chunk3 = chunks(2)
        val chunk4 = chunks(3)
        val chunk5 = chunks(4)

        assert(chunk1.infoDate == monday.toString)
        assert(chunk1.infoDateBegin == monday.toString)
        assert(chunk1.infoDateEnd == monday.toString)
        assert(chunk1.inputRecordCount == 300)
        assert(chunk1.outputRecordCount == 75)
        assert(chunk1.jobStarted == fridayJobAStarted)
        assert(chunk1.jobFinished == fridayJobAFinished)

        assert(chunk2.infoDate == tuesday.toString)
        assert(chunk2.infoDateBegin == tuesday.toString)
        assert(chunk2.infoDateEnd == tuesday.toString)
        assert(chunk2.inputRecordCount == 400)
        assert(chunk2.outputRecordCount == 0)
        assert(chunk2.jobStarted != fridayJobAStarted)
        assert(chunk2.jobFinished != fridayJobAFinished)

        assert(chunk3.infoDate == wednesday.toString)
        assert(chunk3.infoDateBegin == wednesday.toString)
        assert(chunk3.infoDateEnd == wednesday.toString)
        assert(chunk3.inputRecordCount == 400)
        assert(chunk3.outputRecordCount == 100)
        assert(chunk3.jobStarted != fridayJobAStarted)
        assert(chunk3.jobFinished != fridayJobAFinished)

        assert(chunk4.infoDate == friday.toString)
        assert(chunk4.infoDateBegin == friday.toString)
        assert(chunk4.infoDateEnd == friday.toString)
        assert(chunk4.inputRecordCount == 100)
        assert(chunk4.outputRecordCount == 100)
        assert(chunk4.jobStarted != fridayJobAStarted)
        assert(chunk4.jobFinished != fridayJobAFinished)

        assert(chunk5.infoDate == saturday.toString)
        assert(chunk5.infoDateBegin == saturday.toString)
        assert(chunk5.infoDateEnd == saturday.toString)
        assert(chunk5.inputRecordCount == 66)
        assert(chunk5.outputRecordCount == 100)
        assert(chunk5.jobStarted != fridayJobAStarted)
        assert(chunk5.jobFinished != fridayJobAFinished)
      }

    }
  }

  private def getDate(dateStr: String): LocalDate = {
    LocalDate.parse(dateStr, dateFormatter)
  }

  private val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

}
