package za.co.absa.pramen.api.v2

import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import za.co.absa.pramen.api.metastore.MetastoreReader
import za.co.absa.pramen.api.schedule.{EveryDay, Schedule}

import java.time.LocalDate

/** This is a base class for all Pramen jobs (new API). */
trait Job {
  /** A name of a job. */
  def name: String

  /** The schedule that defines which days the job should run */
  def getSchedule: Schedule = EveryDay()

  /** Calculates output information date by the run date */
  def getInfoDate(runDate: LocalDate): LocalDate

  /**
    * Dependencies specify which tables in the metastore the job depends on.
    * The job will be executed only if all dependencies are met.
    *
    * Each dependency consists of a list of tables in the metastore and the date range defining the recency of
    * the table required.
    *
    * @return A list of job dependencies.
    */
  def getDependencies: Seq[MetastoreDependency]

  /**
    * This method is invoked for each selected information date.
    * Pramen provides the metastore which can be used to check data availability for date ranges,
    * as well as fetching data and checking properties necessary to run the job
    *
    * If a task is not ready to run it will not be considered done and dependencies for that
    * information date will be checked again when new data is available.
    *
    * If the task is not ready it should thrown an exception derived from RuntimeException
    * with a message to be added to the notification about the reason of the task is not ready.
    *
    * @param metastore The metastore reader to be used to fetch data and check properties.
    * @param infoDate  The date for which the job is being run.
    * @param jobConfig The job configuration.
    * @throws RuntimeException Throws an exception if the task cannot run for some reason.
    */
  def validate(metastore: MetastoreReader,
               infoDate: LocalDate,
               jobConfig: Config): Unit

  def run(metastore: MetastoreReader,
                         infoDate: LocalDate,
                         jobConfig: Config): DataFrame

  def writeOutput(df: DataFrame,
                  infoDate: LocalDate,
                  jobConfig: Config): (Long, Option[Long])
}
