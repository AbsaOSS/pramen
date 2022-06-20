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

package za.co.absa.pramen.framework.mocks.state

import za.co.absa.pramen.framework.notify.{SchemaDifference, TaskCompleted}
import za.co.absa.pramen.framework.state.RunState

import scala.collection.mutable.ListBuffer

class RunStateSpy extends RunState{
  var setSuccessCalled = 0
  var setFailureCalled = 0
  var gotException: Option[Throwable] = None

  val completedTasks = new ListBuffer[TaskCompleted]

  val schemaDifferences = new ListBuffer[SchemaDifference]

  override def addCompletedTask(taskCompleted: TaskCompleted): Unit = completedTasks += taskCompleted

  override def addSchemaDifference(schemaDifference: SchemaDifference): Unit = schemaDifferences += schemaDifference

  override def setSuccess(): Unit = setSuccessCalled += 1

  override def setFailure(exception: Throwable): Unit = {
   setFailureCalled += 1
    gotException = Option(exception)
  }

  override def getExitCode: Int = 0
}
