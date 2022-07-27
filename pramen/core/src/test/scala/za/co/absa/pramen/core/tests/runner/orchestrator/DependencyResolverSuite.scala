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

package za.co.absa.pramen.framework.tests.runner.orchestrator

import org.scalatest.WordSpec
import za.co.absa.pramen.framework.pipeline.JobDependency
import za.co.absa.pramen.framework.runner.orchestrator.{DependencyResolver, DependencyResolverImpl}

class DependencyResolverSuite extends WordSpec {

  "validate" should {
    "do nothing if the pipeline is ok" in {
      val testCase = getTestCase

      testCase.validate()
    }

    "do nothing if a job has the same table as an input table and output table" in {
      val testCase = new DependencyResolverImpl(Seq(
        JobDependency(Nil, "table1"),
        JobDependency(Seq("table2", "table10"), "table2"),
        JobDependency(Seq("table11"), "table4")
      ))

      testCase.validate()
    }

    "throw an exception if there are 2 jobs outputting the same table" in {
      val faultyCase = new DependencyResolverImpl(Seq(
        JobDependency(Nil, "table1"),
        JobDependency(Nil, "table2"),
        JobDependency(Seq("table1", "table10"), "table2"),
        JobDependency(Seq("table11"), "table4")
      ))

      val ex  = intercept[IllegalArgumentException] {
        faultyCase.validate()
      }

      assert(ex.getMessage.contains("Pipeline validation error: Table is produced my more than 1 job: table2"))
    }

    "throw an exception if there is a cycle" in {
      val faultyCase = new DependencyResolverImpl(Seq(
        JobDependency(Nil, "table1"),
        JobDependency(Nil, "table2"),
        JobDependency(Seq("table1", "table4"), "table3"),
        JobDependency(Seq("table3"), "table4")
      ))

      val ex  = intercept[IllegalArgumentException] {
        faultyCase.validate()
      }

      assert(ex.getMessage.contains("Pipeline validation error: Job graph has a cycle: table3, table4, table3"))
    }
  }

  "setAvailableTable" should {
    "make dependent output tables ready to calculate" in {
      val resolver = getTestCase

      resolver.setAvailableTable("table1")

      assert(resolver.canRun("table3" ))
    }
  }

  "setNoDataTable" should {
    "make dependent output tables not ready to calculate" in {
      val resolver = getTestCase

      resolver.setAvailableTable("table1")
      resolver.setFailedTable("table1")

      assert(!resolver.canRun("table3"))
    }
  }

  "canRun" should {
    "return true for tables that do not have dependencies" in {
      val resolver = getTestCase

      assert(resolver.canRun("table1"))
    }

    "return true for tables that have all dependencies satisfied" in {
      val resolver = getTestCase

      resolver.setAvailableTable("table1")
      assert(resolver.canRun("table3"))
    }

    "return true for tables that have table dependencies not part of this pipeline" in {
      val resolver = getTestCase

      assert(resolver.canRun("table4"))
    }

    "return false for tables that have not all dependencies satisfied" in {
      val resolver = getTestCase

      assert(!resolver.canRun("table3"))
    }
  }

  "getMissingDependencies" should {
    "return Nil for tables that do not have dependencies" in {
      val resolver = getTestCase

      assert(resolver.getMissingDependencies("table1").isEmpty)
    }

    "return Nil for tables that have all dependencies satisfied" in {
      val resolver = getTestCase

      resolver.setAvailableTable("table1")
      assert(resolver.getMissingDependencies("table3").isEmpty)
    }

    "return Nil for tables that have table dependencies not part of this pipeline" in {
      val resolver = getTestCase

      assert(resolver.getMissingDependencies("table4").isEmpty)
    }

    "return list of tables for tables that have not all dependencies satisfied" in {
      val resolver = getTestCase

      assert(resolver.getMissingDependencies("table3") == Seq("table1"))
    }
  }

  "getDag" should {
    "visualize a graph before any jobs completed" in {
      val resolver = getTestCase

      val graph = resolver.getDag("table3" :: Nil)

      assert(graph == "table3 <- (table1, ?table10)")
    }

    "visualize a graph after some jobs completed" in {
      val resolver = getTestCase

      resolver.setAvailableTable("table1")

      val graph = resolver.getDag("table3" :: Nil)

      assert(graph == "table3 <- (+table1, ?table10)")
    }
  }

  def getTestCase: DependencyResolver = {
    new DependencyResolverImpl(Seq(
      JobDependency(Nil, "table1"),
      JobDependency(Nil, "table2"),
      JobDependency(Seq("table1", "table10"), "table3"),
      JobDependency(Seq("table11"), "table4")
    ))
  }
}
