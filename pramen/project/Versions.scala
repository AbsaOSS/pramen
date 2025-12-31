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

import sbt.*

object Versions {
  val defaultSparkVersionForScala211 = "2.4.8"
  val defaultSparkVersionForScala212 = "3.3.4"
  val defaultSparkVersionForScala213 = "3.4.4"

  val typesafeConfigVersion = "1.4.3"
  val postgreSqlDriverVersion = "42.7.7"
  val msSqlDriverVersion = "1.3.1"
  val mongoDbScalaDriverVersion = "2.7.0"
  val hsqlDbVersion = "2.7.1"
  val slickVersion = "3.3.3"
  val scoptVersion = "3.7.1"
  val channelVersion = "0.2.1"
  val requestsVersion = "0.8.0"
  val javaXMailVersion = "1.6.2"
  val embeddedMongoDbVersion = "4.22.0"
  val scalaCompatColsVersion = "2.12.0"
  val scalatestVersion = "3.2.14"
  val mockitoVersion = "2.28.2"
  val httpClientVersion = "4.5.14"

  def sparkFallbackVersion(scalaVersion: String): String = {
    if (scalaVersion.startsWith("2.11.")) {
      defaultSparkVersionForScala211
    } else if (scalaVersion.startsWith("2.12.")) {
      defaultSparkVersionForScala212
    } else if (scalaVersion.startsWith("2.13.")) {
      defaultSparkVersionForScala213
    } else {
      throw new IllegalArgumentException(s"Scala $scalaVersion not supported.")
    }
  }

  def sparkVersion(scalaVersion: String): String = sys.props.getOrElse("SPARK_VERSION", sparkFallbackVersion(scalaVersion))

  def sparkVersionShort(scalaVersion: String): String = {
    val fullVersion = sparkVersion(scalaVersion)

    fullVersion.split('.').take(2).mkString(".")
  }

  def getSparkVersionRelatedDeps(sparkVersion: String): Seq[ModuleID] = {
    if (sparkVersion.startsWith("2.")) {
      // Seq("com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7.3")
      Nil
    } else if (sparkVersion.startsWith("3.")) {
      Nil
    } else {
      throw new IllegalArgumentException(s"Spark $sparkVersion not supported.")
    }
  }

  def getDeltaDependency(sparkVersion: String, isCompile: Boolean, isTest: Boolean): ModuleID = {
    // According to this: https://docs.delta.io/latest/releases.html
    val (deltaArtifact, deltaVersion) = sparkVersion match {
      case version if version.startsWith("2.")   => ("delta-core", "0.6.1")
      case version if version.startsWith("3.0.") => ("delta-core", "0.8.0")
      case version if version.startsWith("3.1.") => ("delta-core", "1.0.1")
      case version if version.startsWith("3.2.") => ("delta-core", "2.0.2")
      case version if version.startsWith("3.3.") => ("delta-core", "2.2.0")
      case version if version.startsWith("3.4.") => ("delta-core", "2.4.0")
      case version if version.startsWith("3.5.") => ("delta-spark", "3.0.0")  // 'delta-core' was renamed to 'delta-spark' since 3.0.0.
      case _                                     => throw new IllegalArgumentException(s"Spark $sparkVersion not supported.")
    }
    if (isTest) {
      "io.delta" %% deltaArtifact % deltaVersion % Test
    } else {
      if (isCompile) {
        println(s"Using Delta version $deltaArtifact:$deltaVersion (compile)")
        "io.delta" %% deltaArtifact % deltaVersion % Compile
      } else {
        println(s"Using Delta version $deltaArtifact:$deltaVersion (provided)")
        "io.delta" %% deltaArtifact % deltaVersion % Provided
      }
    }
  }

  def getIcebergDependency(sparkVersion: String): ModuleID = {
    if (sparkVersion.startsWith("2.")) {
      // Spark runtime 2.4 does not include Scala version as part of the name of the artifact
      "org.apache.iceberg" % s"iceberg-spark-runtime-2.4" % "1.2.1" % Test
    } else {
      val (icebergVersion, sparkCompatVersion) = sparkVersion match {
        case version if version.startsWith("3.3.") => ("1.6.1", "3.3")
        case version if version.startsWith("3.4.") => ("1.6.1", "3.4")
        case version if version.startsWith("3.5.") => ("1.6.1", "3.5")
        case _ => throw new IllegalArgumentException(s"Spark $sparkVersion not supported.")
      }

      "org.apache.iceberg" %% s"iceberg-spark-runtime-${sparkCompatVersion}" % icebergVersion % Test
    }
  }

  def getKafkaClientsDependency(sparkVersion: String): ModuleID = {
    val kafkaClientsVersion = sparkVersion match {
      case version if version.startsWith("2.4.") => "2.5.1"
      case _ => "3.9.0"
    }

    println(s"Using 'kafla-clients' version $kafkaClientsVersion")

    "org.apache.kafka" % "kafka-clients" % kafkaClientsVersion
  }

  def getAbrisDependency(sparkVersion: String): ModuleID = {
    // According to this: https://github.com/AbsaOSS/ABRiS?tab=readme-ov-file#supported-versions
    val abrisVersion = sparkVersion match {
      case version if version.startsWith("2.4.") => "5.1.1"
      case version if version.startsWith("3.0.") => "5.1.1"
      case version if version.startsWith("3.1.") => "5.1.1"
      case version if version         == "3.2.0" => "6.1.1"
      case version if version.startsWith("3.")   => "6.4.1"
      case _                                     => throw new IllegalArgumentException(s"Spark $sparkVersion not supported for Abris dependency.")
    }

    println(s"Using Abris version $abrisVersion")

    "za.co.absa" %% "abris" % abrisVersion excludeAll(
      ExclusionRule(organization = "com.fasterxml.jackson.core"),
      ExclusionRule(organization = "org.apache.avro"),
      // Exclude Spark to avoid conflicts with different Spark versions
      // Please, include "org.apache.spark"` %% "spark-avro" % ${spark.version}}
      // explicitly when needed.
      ExclusionRule(organization = "org.apache.spark")
    )
  }

}
