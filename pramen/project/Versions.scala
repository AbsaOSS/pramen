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

import sbt._

object Versions {
  val defaultSparkVersionForScala211 = "2.4.8"
  val defaultSparkVersionForScala212 = "3.2.2"
  val defaultSparkVersionForScala213 = "3.2.2"

  val typesafeConfigVersion = "1.4.0"
  val postgreSqlDriverVersion = "42.3.3"
  val msSqlDriverVersion = "1.3.1"
  val mongoDbScalaDriverVersion = "2.7.0"
  val hsqlDbVersion = "2.5.1"
  val slickVersion = "3.3.3"
  val scoptVersion = "3.7.1"
  val channelsVersion = "0.1.4"
  val kafkaClientVersion = "2.5.1"
  val javaXMailVersion = "1.6.2"
  val embeddedMongoDbVersion = "2.2.0"
  val scalatestVersion = "3.0.9"
  val mockitoVersion = "2.28.2"

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

  def getDeltaDependency(sparkVersion: String): ModuleID = {
    // According to this: https://docs.delta.io/latest/releases.html
    val deltaVersion = sparkVersion match {
      case version if version.startsWith("2.")   => "0.6.1"
      case version if version.startsWith("3.0.") => "0.8.0"
      case version if version.startsWith("3.1.") => "1.0.1"
      case version if version.startsWith("3.2.") => "2.0.1"
      case version if version.startsWith("3.3.") => "2.2.0"
      case _                                     => throw new IllegalArgumentException(s"Spark $sparkVersion not supported.")
    }
    println(s"Using Delta version $deltaVersion")
    "io.delta" %% "delta-core" % deltaVersion
  }

  def getAbrisDependency(scalaVersion: String): ModuleID = {
    // According to this: https://docs.delta.io/latest/releases.html
    val abrisVersion = scalaVersion match {
      case version if version.startsWith("2.11.") => "5.1.1"
      case version if version.startsWith("2.12.") => "5.1.1"
      case version if version.startsWith("2.13.") => "6.0.0"
      case _                                      => throw new IllegalArgumentException(s"Scaka $scalaVersion not supported for Abris dependency.")
    }

    println(s"Using Abris version $abrisVersion")

    "za.co.absa" %% "abris" % abrisVersion excludeAll(
      ExclusionRule(organization = "com.fasterxml.jackson.core"),
      ExclusionRule(organization = "org.apache.avro")
    )
  }

}
