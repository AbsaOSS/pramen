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
    "com.typesafe"         %  "config"                     % typesafeConfigVersion,
    "org.scalatest"        %% "scalatest"                  % scalatestVersion           % Test
  )

  def CoreDependencies(scalaVersion: String, isDeltaCompile: Boolean): Seq[ModuleID] = Seq(
    "org.apache.spark"     %% "spark-sql"                  % sparkVersion(scalaVersion) % Provided,
    "org.mongodb.scala"    %% "mongo-scala-driver"         % mongoDbScalaDriverVersion,
    "com.typesafe.slick"   %% "slick"                      % slickVersion,
    "com.typesafe.slick"   %% "slick-hikaricp"             % slickVersion,
    "org.postgresql"       %  "postgresql"                 % postgreSqlDriverVersion,
    "com.github.scopt"     %% "scopt"                      % scoptVersion,
    "com.github.yruslan"   %% "channel_scala"              % channelVersion,
    "org.apache.kafka"     %  "kafka-clients"              % kafkaClientVersion,
    "com.sun.mail"         %  "javax.mail"                 % javaXMailVersion,
    "com.lihaoyi"          %% "requests"                   % requestsVersion,
    "org.scalatest"        %% "scalatest"                  % scalatestVersion           % Test,
    "org.mockito"          %  "mockito-core"               % mockitoVersion             % Test,
    "de.flapdoodle.embed"  %  "de.flapdoodle.embed.mongo"  % embeddedMongoDbVersion     % Test,
    "org.hsqldb"           %  "hsqldb"                     % hsqlDbVersion              % Test classifier "jdk8"
  ) :+ getDeltaDependency(sparkVersion(scalaVersion), isDeltaCompile, isTest = false)

  def ExtrasJobsDependencies(scalaVersion: String): Seq[ModuleID] = Seq(
    "org.apache.spark"          %% "spark-sql"                  % sparkVersion(scalaVersion) % Provided,
    "net.sourceforge.jtds"      %  "jtds"                       % msSqlDriverVersion,
    "org.apache.httpcomponents" %  "httpclient"                 % httpClientVersion,
    "org.scalatest"             %% "scalatest"                  % scalatestVersion           % Test,
    "org.mockito"               %  "mockito-core"               % mockitoVersion             % Test
  ) ++ Seq(
    getAbrisDependency(sparkVersion(scalaVersion)),
    getDeltaDependency(sparkVersion(scalaVersion), isCompile = false, isTest = true)
  )

}
