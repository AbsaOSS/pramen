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

package za.co.absa.pramen.api

import org.apache.spark.sql.DataFrame

import java.time.LocalDate

trait Transformer {
  /**
    * Validates if the transformation can run.
    *
    * If the transformation can run for the given information date, it should return Reason.Ready.
    *
    * If requirements are not met the validation routine should do one of teh following:
    * - return Reason.NotReady - this means some input data is missing, but the transformation can run in the future
    * when the requirements are met
    * - return Reason.Skip - this means the running of the transformation should be completely skipped.
    * - throw an exception - this will result in the same behavior as returning Reason.NotReady
    *
    * @param metastore The read only version of metastore. You can only query tables using it.
    * @param infoDate  The information date of the output of the transformation.
    * @param options   Extra options specified in the configuration for the transformation.
    * @return Reason as the validation result.
    */
  def validate(metastore: MetastoreReader,
               infoDate: LocalDate,
               options: Map[String, String]): Reason

  /**
    * A job has access to the metastore where it can query tables registered in the configuration.
    *
    * If task fails it should throw a runtime exception.
    *
    * Do not add information date column to the output dataframe. It will be added automatically.
    * If the information date column exists in the output dataframe, it will be deleted and replaced
    * by the framework.
    *
    * @param metastore The read only version of metastore. You can only query tables using it.
    * @param infoDate  The information date of the output of the transformation.
    * @param options   Extra options specified in the configuration for the transformation.
    * @return The output DataFrame
    */
  def run(metastore: MetastoreReader,
          infoDate: LocalDate,
          options: Map[String, String]): DataFrame

  /**
    * This method is called after the transformation is finished. You can query the output table form the output information
    * data and the data should be there.
    *
    * @param outputTableName The table name used as the output table of the transformer
    * @param metastore       The read only version of metastore. You can only query tables using it.
    * @param infoDate        The information date of the output of the transformation.
    * @param options         Extra options specified in the configuration for the transformation.
    */
  def postProcess(outputTableName: String,
                  metastore: MetastoreReader,
                  infoDate: LocalDate,
                  options: Map[String, String]): Unit = {}
}
