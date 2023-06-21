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

package za.co.absa.pramen.core.databricks

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.wordspec.AnyWordSpec

import java.time.LocalDate

class PramenPyJobTemplateSuite extends AnyWordSpec {

  "render()" should {
    "prepare a job configuration to a map" in {
      val conf = ConfigFactory.parseString(
        s"""
           |pramen.py {
           |  databricks {
           |    job {
           |      run_name = "Pramen-Py @pythonClass (@infoDate)"
           |      tasks = [
           |        {
           |           new_cluster {
           |             node_type = "smallest"
           |             spark_version = "3.3.1"
           |           }
           |           notebook_task = {
           |             notebook_path = "/path/to/a/notebook"
           |             base_parameters = {
           |               transformer_class = "@pythonClass",
           |               info_date = "@infoDate"
           |               metastore_config = "@metastoreConfig"
           |             }
           |           }
           |           libraries = {
           |             pypi {
           |               package = "pramen-py"
           |             }
           |           }
           |        }
           |      ]
           |    }
           |  }
           |}
           |""".stripMargin)

      val jobDefinition = PramenPyJobTemplate.render(
        conf,
        "HelloWorldTransformation",
        "/path/to/config",
        LocalDate.of(2023, 1, 1)
      )

      val expectedJobDefinition = Map(
        "run_name" -> "Pramen-Py HelloWorldTransformation (2023-01-01)",
        "tasks" -> Seq(
          Map(
            "new_cluster" -> Map(
              "node_type" -> "smallest",
              "spark_version" -> "3.3.1"
            ),
            "notebook_task" -> Map(
              "notebook_path" -> "/path/to/a/notebook",
              "base_parameters" -> Map(
                "transformer_class" -> "HelloWorldTransformation",
                "info_date" -> "2023-01-01",
                "metastore_config" -> "/path/to/config"
              )
            ),
            "libraries" -> Map(
              "pypi" -> Map(
                "package" -> "pramen-py"
              )
            )
          )
        )
      )
      assert(jobDefinition == expectedJobDefinition)
    }

    "correctly resolve config when overwriting cluster configuration" in {
      val conf = ConfigFactory.parseString(
        """
          |cluster.definition = {
          |  new_cluster = {
          |    node_type = "largest"
          |  }
          |}
          |
          |cluster.definition = {
          |  new_cluster = null
          |  existing_cluster_id = "s0m3-1d"
          |}
          |
          |task = ${cluster.definition} {
          |  notebook_task = {
          |    notebook_path = "/path/to/a/notebook"
          |  }
          |}
          |
          |pramen.py {
          |  databricks {
          |    job {
          |      run_name = "Pramen-Py @pythonClass (@infoDate)"
          |      tasks = [ ${task} ]
          |    }
          |  }
          |}
          |""".stripMargin).resolve()

      val jobDefinition = getUseCase(conf).render()

      val expectedJobDefinition = Map(
        "run_name" -> "Pramen-Py SomeTransformation (2022-01-01)",
        "tasks" -> Seq(
          Map(
            "existing_cluster_id" -> "s0m3-1d",
            "notebook_task" -> Map(
              "notebook_path" -> "/path/to/a/notebook"
            )
          )
        )
      )
      assert(jobDefinition == expectedJobDefinition)
    }
  }

  "replaceVariablesInMap()" should {
    "substitute variables in a map" in {
      val map = Map(
        "a" -> Seq("a", "b", "@pythonClass"),
        "b" -> Map(
          "c" -> "@pythonClass",
          "d" -> 1,
          "@infoDate" -> "@infoDate",
          "e" -> Map(
            "@pythonClass" -> "@pythonClass"
          )
        )
      )
      val template = getUseCase()

      val substitutedMap = template.replaceVariablesInMap(map)

      val expectedSubstitutedMap = Map(
        "a" -> Seq("a", "b", "SomeTransformation"),
        "b" -> Map(
          "c" -> "SomeTransformation",
          "d" -> 1,
          "@infoDate" -> "2022-01-01",
          "e" -> Map(
            "@pythonClass" -> "SomeTransformation"
          )
        )
      )
      assert(substitutedMap == expectedSubstitutedMap)
    }
  }

  def getUseCase(conf: Config = ConfigFactory.empty(),
                 pythonClass: String = "SomeTransformation",
                 metastoreConfig: String = "/path/to/config",
                 infoDate: LocalDate = LocalDate.of(2022, 1, 1)
                ): PramenPyJobTemplate = {
    new PramenPyJobTemplate(conf, pythonClass, metastoreConfig, infoDate)
  }
}
