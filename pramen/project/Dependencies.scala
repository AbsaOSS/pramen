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

object Dependencies {

  def sparkVersion: String = sys.props.getOrElse("SPARK_VERSION", "2.4.8")

  private val typesafeConfigVersion = "1.4.0"
  private val deltaCoreVersion = "0.6.1"
  private val scalatestVersion = "3.0.3"
  private val mockitoVersion = "2.28.2"

  private val defaultSparkVersionForScala211 = "2.4.8"
  private val defaultSparkVersionForScala212 = "3.2.1"

  def sparkFallbackVersion(scalaVersion: String): String = {
    if (scalaVersion.startsWith("2.11")) {
      defaultSparkVersionForScala211
    } else {
      defaultSparkVersionForScala212
    }
  }

  def sparkVersion(scalaVersion: String): String = sys.props.getOrElse("SPARK_VERSION", sparkFallbackVersion(scalaVersion))

  def getScalaDependency(scalaVersion: String): ModuleID = "org.scala-lang" % "scala-library" % scalaVersion % Provided

  def ApiDependencies(scalaVersion: String): Seq[ModuleID] = Seq(
    // compile
    "com.typesafe"       % "config"    % typesafeConfigVersion,

    // provided
    "org.apache.spark"   %% "spark-sql" % sparkVersion(scalaVersion) % Provided,

    // test
    "org.scalatest"      %% "scalatest" % scalatestVersion % Test
  )

  def FrameworkDependencies(scalaVersion: String): Seq[ModuleID] = Seq(
    // compile
    "io.delta"             %% "delta-core"                 % deltaCoreVersion,
    "org.mongodb.scala"    %% "mongo-scala-driver"         % "2.7.0",
    "com.typesafe.slick"   %% "slick"                      % "3.3.3",
    "com.typesafe.slick"   %% "slick-hikaricp"             % "3.3.3",
    "org.postgresql"       %  "postgresql"                 % "42.3.3",

    "org.json4s"           %% "json4s-native"              % "3.5.3",
    "com.github.yruslan"   %% "channel_scala"              % "0.1.3",
    "org.apache.kafka"     %  "kafka-clients"              % "2.5.1",
    "com.sun.mail"         %  "javax.mail"                 % "1.6.2",

    // provided
    "org.apache.spark"     %% "spark-sql"                  % sparkVersion(scalaVersion) % Provided,

    // test
    "org.scalatest"        %% "scalatest"                  % scalatestVersion % Test,
    "org.mockito"          %  "mockito-core"               % mockitoVersion % Test,
    "de.flapdoodle.embed"  %  "de.flapdoodle.embed.mongo"  % "2.2.0" % Test,
    "org.hsqldb"           %  "hsqldb"                     % "2.5.1" % Test
  )

  def BuildinJobsDependencies(scalaVersion: String): Seq[ModuleID] = Seq(
    // compile
    "za.co.absa"           %% "abris"                      % "4.2.0",
    "org.apache.spark  "   %% "spark-sql-kafka-0-10"       % sparkVersion(scalaVersion),
    "io.confluent"         %  "kafka-avro-serializer"      % "5.3.1",
    "net.sourceforge.jtds" %  "jtds"                       % "1.3.1",

    // provided
    "org.apache.spark"    %% "spark-sql"                  % sparkVersion(scalaVersion) % Provided,

    // test
    "org.scalatest"       %% "scalatest"                  % scalatestVersion % Test
  )

  def PipelineRunnerDependencied(scalaVersion: String): Seq[ModuleID] = Seq(
    // compile
    "com.github.scopt"    %% "scopt"                      % "3.7.1",

    // provided
    "org.apache.spark"    %% "spark-sql"                  % sparkVersion(scalaVersion) % Provided,

    // test
    "org.scalatest"       %% "scalatest"                  % scalatestVersion % Test
  )

}
