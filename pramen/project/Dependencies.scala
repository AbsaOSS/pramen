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
  import Versions._

  def getScalaDependency(scalaVersion: String): ModuleID = "org.scala-lang" % "scala-library" % scalaVersion % Provided

  def ApiDependencies(scalaVersion: String): Seq[ModuleID] = Seq(
    "org.apache.spark"     %% "spark-sql"                  % sparkVersion(scalaVersion) % Provided,
    "com.typesafe"         % "config"                      % typesafeConfigVersion,
    "org.scalatest"        %% "scalatest"                  % scalatestVersion           % Test
  )

  def FrameworkDependencies(scalaVersion: String): Seq[ModuleID] = Seq(
    "org.apache.spark"     %% "spark-sql"                  % sparkVersion(scalaVersion) % Provided,
    "org.mongodb.scala"    %% "mongo-scala-driver"         % "2.7.0",
    "com.typesafe.slick"   %% "slick"                      % "3.3.3",
    "com.typesafe.slick"   %% "slick-hikaricp"             % "3.3.3",
    "org.postgresql"       %  "postgresql"                 % "42.3.3",
    "com.github.yruslan"   %% "channel_scala"              % "0.1.3",
    "org.apache.kafka"     %  "kafka-clients"              % "2.5.1",
    "com.sun.mail"         %  "javax.mail"                 % "1.6.2",
    "org.scalatest"        %% "scalatest"                  % scalatestVersion           % Test,
    "org.mockito"          %  "mockito-core"               % mockitoVersion             % Test,
    "de.flapdoodle.embed"  %  "de.flapdoodle.embed.mongo"  % "2.2.0"                    % Test,
    "org.hsqldb"           %  "hsqldb"                     % "2.5.1"                    % Test
  ) :+ getDeltaDependency(scalaVersion)

  def BuildinJobsDependencies(scalaVersion: String): Seq[ModuleID] = Seq(
    "org.apache.spark"     %% "spark-sql"                  % sparkVersion(scalaVersion) % Provided,
    "za.co.absa"           %% "abris"                      % "5.1.1" excludeAll(
      ExclusionRule(organization = "com.fasterxml.jackson.core"),
      ExclusionRule(organization = "org.apache.avro")
    ),
    "net.sourceforge.jtds" %  "jtds"                       % "1.3.1",
    "org.scalatest"        %% "scalatest"                  % scalatestVersion           % Test
  )

  def PipelineRunnerDependencied(scalaVersion: String): Seq[ModuleID] = Seq(
    "org.apache.spark"     %% "spark-sql"                  % sparkVersion(scalaVersion) % Provided,
    "com.github.scopt"     %% "scopt"                      % "3.7.1",
    "org.scalatest"        %% "scalatest"                  % scalatestVersion           % Test
  )

}
