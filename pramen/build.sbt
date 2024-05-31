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
import BuildInfoTemplateSettings._
import com.github.sbt.jacoco.report.JacocoReportSettings

val scala211 = "2.11.12"
val scala212 = "2.12.19"
val scala213 = "2.13.13"

ThisBuild / organization := "za.co.absa.pramen"

ThisBuild / scalaVersion := scala211
ThisBuild / crossScalaVersions := Seq(scala211, scala212, scala213)

ThisBuild / scalacOptions := Seq("-unchecked", "-deprecation")

ThisBuild / versionScheme := Some("early-semver")

// Scala shouldn't be packaged so it is explicitly added as a provided dependency below
ThisBuild / autoScalaLibrary := false

lazy val printSparkVersion = taskKey[Unit]("Print Spark version Pramen is building against.")

lazy val commonJacocoReportSettings: JacocoReportSettings = JacocoReportSettings()
  .withFormats(JacocoReportFormats.HTML, JacocoReportFormats.XML)
  .withThresholds(JacocoThresholds(line = 60))

lazy val commonJacocoExcludes: Seq[String] = Seq(
  "za.co.absa.pramen.api.*",
  "za.co.absa.pramen.buildinfo.*",
  "za.co.absa.pramen.core.exceptions.*",
  "za.co.absa.pramen.core.config.*"
)

val shadeBase = "za.co.absa.pramen.shaded"

def itFilter(name: String): Boolean = name endsWith "LongSuite"
def unitFilter(name: String): Boolean = (name endsWith "Suite") && !itFilter(name)
def shade(pkg: String): (String, String) = s"$pkg.**" -> s"$shadeBase.$pkg.@1"

/* This is so that Pramen uber jar has the name compatible to versions where 'pramen-runner' module existed */
def runnerAssemblyName(moduleName: String): String = {
  if (moduleName == "pramen-core")
    "pramen-runner"
  else moduleName
}

def runnerSparkVersionSuffix(moduleName: String, scalaVersion: String, includeDelta: Boolean): String = {
  if (includeDelta) {
    val minorSparkVersion = sparkVersionShort(scalaVersion)
    s"_$minorSparkVersion"
  } else ""
}

val assemblyFeatures = settingKey[Seq[String]]("Define assembly scope")

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
  .aggregate(api, core, extras)

lazy val api = (project in file("api"))
  .disablePlugins(sbtassembly.AssemblyPlugin)
  .settings(
    name := "pramen-api",
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

lazy val core = (project in file("core"))
  .configs( IntegrationTest )
  .settings( inConfig(IntegrationTest)(Defaults.testTasks) : _*)
  .settings(
    name := "pramen-core",
    printSparkVersion := {
      val log = streams.value.log
      log.info(s"Building with Spark ${sparkVersion(scalaVersion.value)}, Scala ${scalaVersion.value}")
      sparkVersion(scalaVersion.value)
    },
    (Compile / compile) := ((Compile / compile) dependsOn printSparkVersion).value,
    assemblyFeatures := sys.props.getOrElse("assembly.features", "").split(',').toSeq,
    libraryDependencies ++= CoreDependencies(scalaVersion.value, assemblyFeatures.value.contains("includeDelta"))  ++
      getSparkVersionRelatedDeps(sparkVersion(scalaVersion.value)) :+
      getScalaDependency(scalaVersion.value),
    (Test / testOptions) := Seq(Tests.Filter(unitFilter)),
    (IntegrationTest / testOptions) := Seq(Tests.Filter(itFilter)),
    Test / fork := true,
    populateBuildInfoTemplate,
    releasePublishArtifactsAction := PgpKeys.publishSigned.value,
    jacocoReportSettings := commonJacocoReportSettings.withTitle("pramen:core Jacoco Report"),
    jacocoExcludes := commonJacocoExcludes,
    assemblySettingsRunner
  )
  .dependsOn(api)
  .enablePlugins(AutomateHeaderPlugin)

lazy val extras = (project in file("extras"))
  .settings(
    name := "pramen-extras",
    printSparkVersion := {
      val log = streams.value.log
      log.info(s"Building with Spark ${sparkVersion(scalaVersion.value)}, Scala ${scalaVersion.value}")
      sparkVersion(scalaVersion.value)
    },
    (Compile / compile) := ((Compile / compile) dependsOn printSparkVersion).value,
    assemblyFeatures := sys.props.getOrElse("assembly.features", "").split(',').toSeq,
    libraryDependencies ++= ExtrasJobsDependencies(scalaVersion.value) ++
      getSparkVersionRelatedDeps(sparkVersion(scalaVersion.value)) :+
      getScalaDependency(scalaVersion.value),
    resolvers += "confluent" at "https://packages.confluent.io/maven/",
    Test / fork := true,
    releasePublishArtifactsAction := PgpKeys.publishSigned.value,
    jacocoReportSettings := commonJacocoReportSettings.withTitle("pramen-extras Jacoco Report"),
    jacocoExcludes := commonJacocoExcludes,
    assemblySettingsExtras
  )
  .dependsOn(core)
  .enablePlugins(AutomateHeaderPlugin)

// release settings
releaseCrossBuild := true

def isFiltered(fileName: String): Boolean = {
  val filteredExtensions = ".a" :: ".dll" :: ".dylib" :: ".py" :: ".so" :: ".st" :: ".stg" :: Nil
  filteredExtensions.exists(name => fileName.endsWith(name))
}

lazy val assemblySettingsCommon = Seq(
  // This merge strategy retains service entries for all services in manifest.
  assembly / assemblyMergeStrategy := {
    case "reference.conf"                          => MergeStrategy.concat
    case "LICENSE"                                 => MergeStrategy.concat
    case "log4j.properties"                        => MergeStrategy.filterDistinctLines
    case x if x.endsWith("module-info.class")      => MergeStrategy.discard
    case PathList("include", xs @ _*)              => MergeStrategy.discard
    case PathList("com", "ibm", "icu", xs @ _*)    => MergeStrategy.discard
    case PathList("common", "message", xs @ _*)    => MergeStrategy.discard
    case PathList("javax", "json", xs @ _*)        => MergeStrategy.discard
    case PathList(ps @ _*) if isFiltered(ps.last)  => MergeStrategy.discard
    case PathList("META-INF", xs @ _*) =>
      xs map {_.toLowerCase} match {
        case ps @ (x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") => MergeStrategy.discard
        case "dependencies" :: Nil => MergeStrategy.discard
        case "notice" :: Nil       => MergeStrategy.discard
        case "notice.txt" :: Nil   => MergeStrategy.discard
        case "notice.md" :: Nil    => MergeStrategy.discard
        case "license" :: Nil      => MergeStrategy.concat
        case "license.txt" :: Nil  => MergeStrategy.concat
        case "license.md" :: Nil   => MergeStrategy.concat
        case "manifest.mf" :: Nil  => MergeStrategy.discard
        case "maven" :: x          => MergeStrategy.discard
        case "services" :: x       => MergeStrategy.filterDistinctLines
        case _                     => MergeStrategy.deduplicate
      }
    case _ => MergeStrategy.deduplicate
  },
  assembly / assemblyOption:= (assembly / assemblyOption).value.copy(includeScala = false),
  assembly / assemblyJarName := s"${runnerAssemblyName(name.value)}_${scalaBinaryVersion.value}${runnerSparkVersionSuffix(name.value, scalaVersion.value, assemblyFeatures.value.contains("includeDelta"))}-${version.value}.jar",
  assembly / logLevel := Level.Info,
  assembly / test := {}
)

lazy val assemblySettingsExtras = assemblySettingsCommon ++ Seq(assembly / assemblyShadeRules:= Seq(
  ShadeRule.rename(shade("org.apache.http")).inAll,
  ShadeRule.zap("com.101tec.**").inAll,
  ShadeRule.zap("buildinfo.**").inAll,
  ShadeRule.zap("com.databricks.**").inAll,
  ShadeRule.zap("com.fasterxml.**").inAll,
  ShadeRule.zap("com.github.luben.**").inAll,
  ShadeRule.zap("com.github.yruslan.**").inAll,
  ShadeRule.zap("com.ibm.icu.**").inAll,
  ShadeRule.zap("com.mongodb.**").inAll,
  ShadeRule.zap("com.sun.**").inAll,
  ShadeRule.zap("com.sun.mail.**").inAll,
  ShadeRule.zap("com.thoughtworks.paranamer.**").inAll,
  ShadeRule.zap("com.typesafe.config.**").inAll,
  ShadeRule.zap("com.typesafe.slick.**").inAll,
  ShadeRule.zap("com.zaxxer.**").inAll,
  ShadeRule.zap("delta.**").inAll,
  ShadeRule.zap("edu.**").inAll,
  ShadeRule.zap("geny.**").inAll,
  ShadeRule.zap("io.delta.**").inAll,
  ShadeRule.zap("io.netty.**").inAll,
  ShadeRule.zap("javax.**").inAll,
  ShadeRule.zap("javax.activation.**").inAll,
  ShadeRule.zap("jline.**").inAll,
  ShadeRule.zap("log4j.**").inAll,
  ShadeRule.zap("net.jpountz.**").inAll,
  ShadeRule.zap("org.I0Itec.**").inAll,
  ShadeRule.zap("org.abego.**").inAll,
  ShadeRule.zap("org.antlr.**").inAll,
  ShadeRule.zap("org.apache.avro.**").inAll,
  ShadeRule.zap("org.apache.commons.**").inAll,
  ShadeRule.zap("org.apache.jute.**").inAll,
  ShadeRule.zap("org.apache.spark.annotation.**").inAll,
  ShadeRule.zap("org.apache.yetus.**").inAll,
  ShadeRule.zap("org.apache.zookeeper.**").inAll,
  ShadeRule.zap("org.bson.**").inAll,
  ShadeRule.zap("org.checkerframework.**").inAll,
  ShadeRule.zap("org.codehaus.jackson.**").inAll,
  ShadeRule.zap("org.glassfish.**").inAll,
  ShadeRule.zap("org.glassfish.**").inAll,
  ShadeRule.zap("org.jboss.**").inAll,
  ShadeRule.zap("org.json4s.**").inAll,
  ShadeRule.zap("org.lz4.**").inAll,
  ShadeRule.zap("org.mongodb.scala.**").inAll,
  ShadeRule.zap("org.postgresql.**").inAll,
  ShadeRule.zap("org.reactivestreams.**").inAll,
  ShadeRule.zap("org.slf4j.**").inAll,
  ShadeRule.zap("org.stringtemplate.**").inAll,
  ShadeRule.zap("org.tukaani.**").inAll,
  ShadeRule.zap("org.xerial.**").inAll,
  ShadeRule.zap("org.xerial.snappy.**").inAll,
  ShadeRule.zap("requests.**").inAll,
  ShadeRule.zap("scala.**").inAll,
  ShadeRule.zap("scopt.**").inAll,
  ShadeRule.zap("slick.**").inAll,
  ShadeRule.zap("za.co.absa.commons.**").inAll,
  ShadeRule.zap("za.co.absa.pramen.api.**").inAll,
  ShadeRule.zap("za.co.absa.pramen.core.**").inAll
))

lazy val assemblySettingsRunner = assemblySettingsCommon ++ Seq(assembly / assemblyShadeRules := Seq(
  ShadeRule.rename(shade("com.mongodb")).inAll,
  ShadeRule.rename(shade("geny")).inAll,
  ShadeRule.rename(shade("org.mongodb")).inAll,
  ShadeRule.rename(shade("requests")).inAll,
  ShadeRule.rename(shade("scopt")).inAll,
  ShadeRule.rename(shade("slick")).inAll,
  ShadeRule.zap("com.github.luben.**").inAll,
  ShadeRule.zap("com.ibm.icu.**").inAll,
  ShadeRule.zap("net.jpountz.**").inAll,
  ShadeRule.zap("org.abego.**").inAll,
  ShadeRule.zap("org.apache.kafka.**").inAll,
  ShadeRule.zap("org.glassfish.**").inAll,
  ShadeRule.zap("org.lz4.**").inAll,
  ShadeRule.zap("org.slf4j.**").inAll,
  ShadeRule.zap("org.slf4j.**").inAll,
  ShadeRule.zap("org.xerial.snappy.**").inAll
),
  assembly / mainClass := Some("za.co.absa.pramen.runner.PipelineRunner")
)

addCommandAlias("releaseNow", ";set releaseVersionBump := sbtrelease.Version.Bump.Bugfix; release with-defaults")
addCommandAlias("itTest", "integration:test")
addCommandAlias("xcoverage", "clean;coverage;test;coverageReport")
