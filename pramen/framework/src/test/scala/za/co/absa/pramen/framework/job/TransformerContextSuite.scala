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

package za.co.absa.pramen.framework.job

import com.typesafe.config.ConfigFactory
import org.scalatest.WordSpec
import za.co.absa.pramen.api.schedule.EveryDay

class TransformerContextSuite extends WordSpec {

  "fromConfig" should {
    "create context" in {
      val conf = ConfigFactory.parseString(
        """
          |my.job {
          |  job.name = table
          |  factory.class = MyClass
          |  schedule.type = "daily"
          |  input.tables = [ a, b, c]
          |  output.table = output_table
          |  output.info.date.expr = "beginOfWeek(@infoDate)"
          |  option {
          |    opt1 = a
          |  }
          |}
          |""".stripMargin)

      val ctx = TransformerContext.fromConfig(conf, "my.job")

      assert(ctx.jobName == "table")
      assert(ctx.factoryClass == "MyClass")
      assert(ctx.schedule.isInstanceOf[EveryDay])
      assert(ctx.inputTables == Seq("a", "b", "c"))
      assert(ctx.outputTable == "output_table")
      assert(ctx.outputInfoDateExpression.contains("beginOfWeek(@infoDate)"))
      assert(ctx.options.get("opt1").contains("a"))
    }
  }

}
