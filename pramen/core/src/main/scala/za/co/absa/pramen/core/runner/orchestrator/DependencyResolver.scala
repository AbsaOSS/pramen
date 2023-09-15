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

package za.co.absa.pramen.core.runner.orchestrator

trait DependencyResolver {
  /**
    * Ensure the pipeline is valid.
    * - Each table used in the pipeline has only one job that generates it
    * - There are no cycles in the pipeline
    *
    * @throws IllegalArgumentException if there are issues in the pipeline
    */
  def validate(): Unit

  /**
    * Updates the state of the dependency resolver setting the state of the table as available.
    *
    * Cancels the effect of setNoDataTable().
    *
    * @param table A table name
    */
  def setAvailableTable(table: String): Unit

  /**
    * Updates the state of the dependency resolver setting that the job that generates data for the table has failed.
    * This will be reflected on the rendered DAG. Dependent jobs cannot run.
    *
    * This method cannot be used after setAvailableTable().
    *
    * @param table A table name
    */
  def setFailedTable(table: String): Unit

  /**
    * Returns if a job that has specific dependent tables can run, e.g. all dependent tables are available.
    *
    * The job is identified by the output table.
    *
    * @return True if the job can run, false otherwise
    */
  def canRun(outputTable: String, alwaysAttempt: Boolean): Boolean

  /**
    * Returns the list of dependencies that are not satisfied in order to run a job identified by its output table
    *
    * @return The list of unsatisfied dependencies
    */
  def getMissingDependencies(outputTable: String): Seq[String]

  /**
    * Get DAG visualization for the current state of the specified list of output tables.
    * @param outputTables The list of output tables to generate DAR for.
    * @return DAG visualization
    */
  def getDag(outputTables: Seq[String]): String
}
