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

package za.co.absa.pramen.core.runner

import za.co.absa.pramen.core.expr.DateExprEvaluator

import java.time.LocalDate

object InfoDateCalculator {

  /**
    * Calculates the information date based on the date the job is scheduled to run.
    *
    * An expression is provided by the user to calculate the information date.
    *
    * Usually, the information date is the same as the run date.
    *
    * For example:
    * - for daily jobs, the expression can be
    *   - "@date" or "@date -1"
    *     - for weekly jobs, say the information date is the last Saturday:
    *   - "lastSaturday(@date)"
    *     - form monthly jobs it can be:
    *   - the first day of the month: "beginOfMonth(@date)"
    *   - the last day of the previous month: "beginOfMonth(@date) - 1"
    *
    *   Here '@date' and '@runDate' are synonyms.
    *
    * @param infoDateExpression the expression to calculate the information date from the run date
    * @param runDate            the run date
    * @return the calculated date
    */
  def calculateInformationDate(infoDateExpression: String, runDate: LocalDate): LocalDate = {
    val evaluator = new DateExprEvaluator
    evaluator.setValue("date", runDate)
    evaluator.setValue("runDate", runDate)
    evaluator.evalDate(infoDateExpression)
  }

}
