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
  val defaultSparkVersionForScala212 = "3.2.1"

  val typesafeConfigVersion = "1.4.0"
  val abrisVersion = "5.1.1"
  val postgreSqlDriverVersion = "42.3.3"
  val msSqlDriverVersion = "1.3.1"
  val mongoDbScalaDriverVersion = "2.7.0"
  val hsqlDbVersion = "2.5.1"
  val slickVersion = "3.3.3"
  val scoptVersion = "3.7.1"
  val channelsVersion = "0.1.3"
  val kafkaClientVersion = "2.5.1"
  val javaXMailVersion = "1.6.2"
  val embeddedMongoDbVersion = "2.2.0"
  val scalatestVersion = "3.0.3"
  val mockitoVersion = "2.28.2"

  def sparkFallbackVersion(scalaVersion: String): String = {
    if (scalaVersion.startsWith("2.11")) {
      defaultSparkVersionForScala211
    } else {
      defaultSparkVersionForScala212
    }
  }

  def sparkVersion(scalaVersion: String): String = sys.props.getOrElse("SPARK_VERSION", sparkFallbackVersion(scalaVersion))

  def getSparkVersionRelatedDeps(sparkVersion: String): Seq[ModuleID] = {
    if (sparkVersion.startsWith("2.")) {
      // Seq("com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7.3")
      Nil
    } else{
      Nil
    }
  }

  def getDeltaDependency(sparkVersion: String): ModuleID = {
    if (sparkVersion.startsWith("2.")) {
      "io.delta" %% "delta-core" % "0.6.1",
    } else {
      "io.delta" %% "delta-core" % "1.2.1",
    }
  }

}