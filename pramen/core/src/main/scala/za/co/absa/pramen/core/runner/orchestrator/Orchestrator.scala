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

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import za.co.absa.pramen.core.app.AppContext
import za.co.absa.pramen.core.pipeline.Job
import za.co.absa.pramen.core.runner.jobrunner.ConcurrentJobRunner
import za.co.absa.pramen.core.state.PipelineState

trait Orchestrator {
  def validateJobs(jobs: Seq[Job])(implicit appContext: AppContext,
                                   spark: SparkSession): Unit

  def runJobs(jobs: Seq[Job])(implicit conf: Config,
                              state: PipelineState,
                              appContext: AppContext,
                              jobRunner: ConcurrentJobRunner,
                              spark: SparkSession): Unit
}
