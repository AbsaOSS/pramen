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

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import za.co.absa.pramen.core.config.Keys._
import za.co.absa.pramen.core.pipeline.PipelineDef.PIPELINE_NAME_KEY
import za.co.absa.pramen.core.utils.ConfigUtils

object PipelineSparkSessionBuilder {
  private val log = LoggerFactory.getLogger(this.getClass)

  val SPARK_APP_NAME_KEY = "pramen.spark.app.name"

  /**
    * Builds a SparkSession for the pipeline from configuration.
    * The name of the Spark Application will be according to 'pramen.ingestion.name'
    *
    * Extra options can be passed as
    * {{{
    *   pramen.spark.conf {
    *     spark.config.option = "value"
    *   }
    * }}}
    *
    * Extra Hadoop options (AWS authentication for example), can be passed as
    * {{{
    *   hadoop.option {
    *     fs.s3a.aws.credentials.provider = "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    *   }
    * }}}
    */
  def buildSparkSession(conf: Config): SparkSession = {
    val isHiveEnabled = conf.getBoolean(ENABLE_HIVE_SUPPORT)
    log.info(s"Hive support enabled = $isHiveEnabled")

    val extraOptionsLegacy = ConfigUtils.getExtraOptions(conf, EXTRA_OPTIONS_PREFIX)
    val extraOptionsNew = ConfigUtils.getExtraOptions(conf, EXTRA_OPTIONS_PREFIX_V2)

    val extraOptions = if (extraOptionsLegacy.nonEmpty) {
      log.warn(s"Using legacy '$EXTRA_OPTIONS_PREFIX' option. Please, use the new option: '$EXTRA_OPTIONS_PREFIX_V2'")
      extraOptionsLegacy
    } else {
      extraOptionsNew
    }

    log.info("Extra Spark Config:")
    ConfigUtils.renderExtraOptions(extraOptions, KEYS_TO_REDACT)(s => log.info(s))

    val sparkSessionBuilder =
      SparkSession
        .builder()
        .appName(getSparkAppName(conf))

    val sparkSessionBuilderWithTimeZoneApplied = ConfigUtils.getOptionString(conf, TIMEZONE) match {
      case Some(tz) => sparkSessionBuilder.config("spark.sql.session.timeZone", tz)
      case None     => sparkSessionBuilder
    }

    val sparkSessionBuilderWithExtraOptApplied = extraOptions.foldLeft(sparkSessionBuilderWithTimeZoneApplied) {
      case (builder, (key, value)) => builder.config(key, value)
    }

    val spark = if (isHiveEnabled) {
      sparkSessionBuilderWithExtraOptApplied
        .enableHiveSupport()
        .getOrCreate()
    } else {
      sparkSessionBuilderWithExtraOptApplied
        .getOrCreate()
    }

    applyHadoopConfig(spark, conf)
  }

  def applyHadoopConfig(spark: SparkSession, conf: Config): SparkSession = {
    val redactTokens = ConfigUtils.getOptListStrings(conf, HADOOP_REDACT_TOKENS).toSet

    if (conf.hasPath(HADOOP_OPTION_PREFIX) || conf.hasPath(HADOOP_OPTION_PREFIX_V2)) {
      if (conf.hasPath(HADOOP_OPTION_PREFIX)) {
        log.warn(s"Using legacy '$HADOOP_OPTION_PREFIX' option. Please, use the new option: '$HADOOP_OPTION_PREFIX_V2'")
      }
      val sc = spark.sparkContext
      val hadoopOptions = ConfigUtils.getExtraOptions(conf, HADOOP_OPTION_PREFIX) ++ ConfigUtils.getExtraOptions(conf, HADOOP_OPTION_PREFIX_V2)

      hadoopOptions.foreach { case (key, value) =>
        val redactedValue = ConfigUtils.renderRedactedKeyValue(key, value, redactTokens).toString
        log.info(s"Hadoop config: $key = $redactedValue")

        sc.hadoopConfiguration.set(key, value)
      }
    }
    spark
  }

  private[core] def getSparkAppName(conf: Config): String = {
    if (conf.hasPath(SPARK_APP_NAME_KEY)) {
      conf.getString(SPARK_APP_NAME_KEY)
    } else {
      if (conf.hasPath(PIPELINE_NAME_KEY)) {
        s"Pramen - ${conf.getString(PIPELINE_NAME_KEY)}"
      } else {
        "Pramen"
      }
    }
  }
}
