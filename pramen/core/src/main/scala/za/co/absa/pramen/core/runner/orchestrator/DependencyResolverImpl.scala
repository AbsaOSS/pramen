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

import za.co.absa.pramen.core.app.config.GeneralConfig.ENABLE_MULTIPLE_JOBS_PER_OUTPUT_TABLE
import za.co.absa.pramen.core.pipeline.JobDependency
import za.co.absa.pramen.core.utils.AlgorithmUtils

import scala.collection.mutable

class DependencyResolverImpl(deps: Seq[JobDependency], enableMultipleJobsPerTable: Boolean) extends DependencyResolver {
  private val inputTables = deps.flatMap(_.inputTables).toSet
  private val outputTables = deps.map(_.outputTable).toSet
  private val dependentTables = outputTables.intersect(inputTables)

  private val availableTables = new mutable.HashSet[String]()
  private val unavailableTables = new mutable.HashSet[String]()

  override def validate(): Unit = {
    val issues1 = if (enableMultipleJobsPerTable) {
      Seq.empty
    } else {
      ensureHaveOnlyOneTransformation(deps.map(_.outputTable))
    }

    val issues2 = ensureNoCycles()

    val issuesAll = issues1 ++ issues2
    if (issuesAll.nonEmpty) {
      throw new IllegalArgumentException(s"Pipeline validation error: ${issuesAll.mkString("\n")}")
    }
  }

  override def setAvailableTable(table: String): Unit = {
    unavailableTables.remove(table)
    availableTables.add(table)
  }

  override def setFailedTable(table: String): Unit = {
    unavailableTables.add(table)
    availableTables.remove(table)
  }

  override def canRun(outputTable: String, alwaysAttempt: Boolean): Boolean = {
    val relevantTables = getRelevantTables(outputTable)

    if (alwaysAttempt) {
      // Always attempt flag means always try running the job, even if dependent jobs failed
      // But we still need to enforce order, so allow running the job only if all dependent tables
      // were processed, either successfully or with a failure.
      relevantTables.forall(t => availableTables.contains(t) || unavailableTables.contains(t))
    } else {
      relevantTables.forall(t => availableTables.contains(t))
    }
  }

  override def getMissingDependencies(outputTable: String):Seq[String] = {
    val relevantTables = getRelevantTables(outputTable)

    relevantTables.diff(availableTables).toArray.sortBy(a => a)
  }

  override def getDag(outputTables: Seq[String]): String = {
    val tables = if (outputTables.nonEmpty) {
      outputTables.filterNot(tbl => availableTables.contains(tbl))
    } else {
      outputTables
    }

    val dags = deps.filter(dep => tables.contains(dep.outputTable))

    dags.map(renderDag).mkString("\n")
  }

  private def getRelevantTables(outputTable: String): Set[String] = {
    val dependentInputTables = deps
      .filter(_.outputTable == outputTable)
      .flatMap(_.nonRecursiveInputTables)
      .toSet

    dependentTables.intersect(dependentInputTables)
  }

  private def renderDag(dep: JobDependency): String = {
    def availabilityFlag(table: String): String = {
      if (availableTables.contains(table)) {
        "+"
      } else if (unavailableTables.contains(table)) {
        "-"
      } else if (outputTables.contains(table)) {
        ""
      } else {
        "?"
      }
    }

    val parentJobs = dep.nonRecursiveInputTables.map(table => {
      val parentJob = deps.find(_.outputTable == table)
      parentJob match {
        case Some(j) => renderDag(j)
        case None    => s"${availabilityFlag(table)}$table"
      }
    })

    if (parentJobs.isEmpty) {
      s"${availabilityFlag(dep.outputTable)}${dep.outputTable}"
    } else {
      s"${availabilityFlag(dep.outputTable)}${dep.outputTable} <- (${parentJobs.mkString(", ")})"
    }
  }

  private def ensureHaveOnlyOneTransformation(allTables: Seq[String]): Seq[String] = {
    val wrongTables = allTables.filter(table => deps.count(_.outputTable == table) > 1)
    wrongTables.map(t => s"Table is produced my more than 1 job: $t. " +
      s"Please, either make jobs output to different metastore tables, or set '$ENABLE_MULTIPLE_JOBS_PER_OUTPUT_TABLE = true'")
  }

  private def ensureNoCycles(): Seq[String] = {
    val depsMap = deps.flatMap(job => {
      // A job can depend on the same table it produces
      // so for cycle detection, we use a list of input tables without this table
      job.nonRecursiveInputTables.map(table => (job.outputTable, table))
    })

    val cycle = AlgorithmUtils.findCycle(depsMap)

    if (cycle.isEmpty) {
      Nil
    } else {
      Seq(s"Job graph has a cycle: ${cycle.mkString(", ")}")
    }
  }
}
