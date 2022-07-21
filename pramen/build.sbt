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

lazy val IntegrationTest = config("integration") extend(Test)

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
  .disablePlugins(sbtassembly.AssemblyPlugin)
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
    releasePublishArtifactsAction := PgpKeys.publishSigned.value
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
    assemblySettings
  )
  .dependsOn(framework)
  .enablePlugins(AutomateHeaderPlugin)

// release settings
releaseCrossBuild := true
addCommandAlias("releaseNow", ";set releaseVersionBump := sbtrelease.Version.Bump.Bugfix; release with-defaults")

lazy val assemblySettings = Seq(
  // This merge strategy retains service entries for all services in manifest.
  assembly / assemblyMergeStrategy := {
    case PathList("META-INF", xs @ _*) =>
      xs map {_.toLowerCase} match {
        case "manifest.mf" :: Nil =>
          MergeStrategy.discard
        case ps @ (x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
          MergeStrategy.discard
        case "maven" :: x =>
          MergeStrategy.discard
        case "services" :: x =>
          MergeStrategy.filterDistinctLines
        case _ => MergeStrategy.deduplicate
      }
    case _ => MergeStrategy.deduplicate
  },
  assembly / assemblyOption:= (assembly / assemblyOption).value.copy(includeScala = false),
  assembly / assemblyShadeRules:= Seq(
    // The SLF4j API and implementation are provided by Spark
    ShadeRule.zap("org.slf4j.**").inAll
  ),
  assembly / logLevel := Level.Info,
  assembly / test := {}
)
