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

import Dependencies._
import Versions._

val scala211 = "2.11.12"
val scala212 = "2.12.16"

ThisBuild / organization := "za.co.absa"

ThisBuild / scalaVersion := scala211
ThisBuild / crossScalaVersions := Seq(scala211, scala212)

ThisBuild / scalacOptions := Seq("-unchecked", "-deprecation")

// Scala shouldn't be packaged so it is explicitly added as a provided dependency below
ThisBuild / autoScalaLibrary := false

lazy val printSparkVersion = taskKey[Unit]("Print Spark version pramen is building against.")

def itFilter(name: String): Boolean = name endsWith "LongSuite"
def unitFilter(name: String): Boolean = (name endsWith "Suite") && !itFilter(name)

lazy val IntegrationTest = config("integration") extend Test

lazy val pramen = (project in file("."))
  .disablePlugins(sbtassembly.AssemblyPlugin)
  .settings(
    name := "pramen",

    // No need to publish the aggregation [empty] artifact
    publishArtifact := false,
    publish := {},
    publishLocal := {}
  )
  .aggregate(api, framework, pipelineRunner, builtinJobs)

lazy val api = (project in file("api"))
  .disablePlugins(sbtassembly.AssemblyPlugin)
  .settings(
    name := "api",
    printSparkVersion := {
      val log = streams.value.log
      log.info(s"Building with Spark ${sparkVersion(scalaVersion.value)}, Scala ${scalaVersion.value}")
      sparkVersion(scalaVersion.value)
    },
    (Compile / compile) := ((Compile / compile) dependsOn printSparkVersion).value,
    libraryDependencies ++= ApiDependencies(scalaVersion.value) :+ getScalaDependency(scalaVersion.value),
    releasePublishArtifactsAction := PgpKeys.publishSigned.value
  )
  .enablePlugins(AutomateHeaderPlugin)

lazy val framework = (project in file("framework"))
  .disablePlugins(sbtassembly.AssemblyPlugin)
  .configs( IntegrationTest )
  .settings( inConfig(IntegrationTest)(Defaults.testTasks) : _*)
  .settings(
    name := "framework",
    printSparkVersion := {
      val log = streams.value.log
      log.info(s"Building with Spark ${sparkVersion(scalaVersion.value)}, Scala ${scalaVersion.value}")
      sparkVersion(scalaVersion.value)
    },
    (Compile / compile) := ((Compile / compile) dependsOn printSparkVersion).value,
    libraryDependencies ++= FrameworkDependencies(scalaVersion.value)  ++
      getSparkVersionRelatedDeps(sparkVersion(scalaVersion.value)) :+
      getScalaDependency(scalaVersion.value),
    (Test / testOptions) := Seq(Tests.Filter(unitFilter)),
    (IntegrationTest / testOptions) := Seq(Tests.Filter(itFilter)),
    Test / fork := true,
      //populateBuildInfoTemplate,
    releasePublishArtifactsAction := PgpKeys.publishSigned.value
  )
  .dependsOn(api)
  .enablePlugins(AutomateHeaderPlugin)

lazy val builtinJobs = (project in file("builtin-jobs"))
  .settings(
    name := "builtin-jobs",
    printSparkVersion := {
      val log = streams.value.log
      log.info(s"Building with Spark ${sparkVersion(scalaVersion.value)}, Scala ${scalaVersion.value}")
      sparkVersion(scalaVersion.value)
    },
    (Compile / compile) := ((Compile / compile) dependsOn printSparkVersion).value,
    libraryDependencies ++= BuildinJobsDependencies(scalaVersion.value) ++
      getSparkVersionRelatedDeps(sparkVersion(scalaVersion.value)) :+
      getScalaDependency(scalaVersion.value),
    resolvers += "confluent" at "https://packages.confluent.io/maven/",
    Test / fork := true,
    releasePublishArtifactsAction := PgpKeys.publishSigned.value,
    assemblySettingsBuiltInJobs
  )
  .dependsOn(framework)
  .enablePlugins(AutomateHeaderPlugin)

lazy val pipelineRunner = (project in file("pipeline-runner"))
  .settings(
    name := "pipeline-runner",
    printSparkVersion := {
      val log = streams.value.log
      log.info(s"Building with Spark ${sparkVersion(scalaVersion.value)}, Scala ${scalaVersion.value}")
      sparkVersion(scalaVersion.value)
    },
    (Compile / compile) := ((Compile / compile) dependsOn printSparkVersion).value,
    libraryDependencies ++= PipelineRunnerDependencied(scalaVersion.value)  ++
      getSparkVersionRelatedDeps(sparkVersion(scalaVersion.value)) :+
      getScalaDependency(scalaVersion.value),
    Test / fork := true,
    releasePublishArtifactsAction := PgpKeys.publishSigned.value,
    assemblySettingsPipelineRunner
  )
  .dependsOn(framework)
  .enablePlugins(AutomateHeaderPlugin)

// release settings
releaseCrossBuild := true

lazy val assemblySettingsCommon = Seq(
  // This merge strategy retains service entries for all services in manifest.
  assembly / assemblyMergeStrategy := {
    case "reference.conf"   => MergeStrategy.concat
    case "LICENSE"          => MergeStrategy.concat
    case "log4j.properties" => MergeStrategy.filterDistinctLines
    case PathList("META-INF", xs @ _*) =>
      xs map {_.toLowerCase} match {
        case ps @ (x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") => MergeStrategy.discard
        case "dependencies" :: Nil => MergeStrategy.discard
        case "notice" :: Nil       => MergeStrategy.discard
        case "license" :: Nil      => MergeStrategy.concat
        case "license.txt" :: Nil  => MergeStrategy.concat
        case "manifest.mf" :: Nil  => MergeStrategy.discard
        case "maven" :: x          => MergeStrategy.discard
        case "services" :: x       => MergeStrategy.filterDistinctLines
        case _                     => MergeStrategy.deduplicate
      }
    case _ => MergeStrategy.deduplicate
  },
  assembly / assemblyOption:= (assembly / assemblyOption).value.copy(includeScala = false),
  assembly / logLevel := Level.Info,
  assembly / test := {}
)

lazy val assemblySettingsBuiltInJobs = assemblySettingsCommon ++ Seq(assembly / assemblyShadeRules:= Seq(
  ShadeRule.zap("za.co.absa.pramen.**").inAll,
  ShadeRule.zap("com.typesafe.config.**").inAll,
  ShadeRule.zap("com.typesafe.slick.**").inAll,
  ShadeRule.zap("io.delta.**").inAll,
  ShadeRule.zap("org.antlr.**").inAll,
  ShadeRule.zap("org.glassfish.**").inAll,
  ShadeRule.zap("org.abego.**").inAll,
  ShadeRule.zap("org.checkerframework.**").inAll,
  ShadeRule.zap("org.reactivestreams.**").inAll,
  ShadeRule.zap("com.zaxxer.**").inAll,
  ShadeRule.zap("com.github.luben.**").inAll,
  ShadeRule.zap("org.lz4.**").inAll,
  ShadeRule.zap("org.xerial.snappy.**").inAll,
  ShadeRule.zap("org.json4s.**").inAll,
  ShadeRule.zap("com.sun.mail.**").inAll,
  ShadeRule.zap("javax.activation.**").inAll,
  ShadeRule.zap("com.github.yruslan.**").inAll,
  ShadeRule.zap("com.thoughtworks.paranamer.**").inAll,
  ShadeRule.zap("org.apache.zookeeper.**").inAll,
  ShadeRule.zap("log4j.**").inAll,
  ShadeRule.zap("io.netty.**").inAll,
  ShadeRule.zap("org.codehaus.jackson.**").inAll,
  ShadeRule.zap("com.fasterxml.**").inAll,
  ShadeRule.zap("org.apache.avro.**").inAll,
  ShadeRule.zap("org.apache.commons.**").inAll,
  ShadeRule.zap("org.tukaani.**").inAll,
  ShadeRule.zap("com.101tec.**").inAll,
  ShadeRule.zap("za.co.absa.commons.**").inAll,
  ShadeRule.zap("com.ibm.icu.**").inAll,
  ShadeRule.zap("org.mongodb.scala.**").inAll,
  ShadeRule.zap("org.postgresql.**").inAll,
  ShadeRule.zap("org.slf4j.**").inAll
))

lazy val assemblySettingsPipelineRunner = assemblySettingsCommon ++ Seq(assembly / assemblyShadeRules:= Seq(
  ShadeRule.zap("org.slf4j.**").inAll
))

addCommandAlias("releaseNow", ";set releaseVersionBump := sbtrelease.Version.Bump.Bugfix; release with-defaults")
addCommandAlias("itTest", "integration:test")
addCommandAlias("xcoverage", "clean;coverage;test;coverageReport")
