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

package za.co.absa.pramen.core.mocks.dao

import de.flapdoodle.embed.mongo.config.{MongodConfigBuilder, Net, RuntimeConfigBuilder}
import de.flapdoodle.embed.mongo.distribution.Version
import de.flapdoodle.embed.mongo.{Command, MongodExecutable, MongodStarter}
import de.flapdoodle.embed.process.config.io.ProcessOutput
import de.flapdoodle.embed.process.runtime.Network
import org.slf4j.LoggerFactory

object MongoDbSingleton {
  private val log = LoggerFactory.getLogger(this.getClass)

  lazy val embeddedMongoDb: (Option[MongodExecutable], Int) = startEmbeddedMongoDb()

  /**
    * Create and run a MongoDb instance.
    *
    * How to configure embedded MongoDB: https://github.com/flapdoodle-oss/de.flapdoodle.embed.mongo
    *
    * @return A pair: a MongoDb executable object to be used to stop it and the port number the embedded MongoDB listens to.
    */
  private def startEmbeddedMongoDb(): (Option[MongodExecutable], Int) = {
    val mongoPort: Int = Network.getFreeServerPort()

    // Do not print Embedded MongoDB logs
    val runtimeConfig = new RuntimeConfigBuilder()
      .defaultsWithLogger(Command.MongoD, log)
      .processOutput(ProcessOutput.getDefaultInstanceSilent)
      .build()

    val starter = MongodStarter.getInstance(runtimeConfig)

    val mongodConfig = new MongodConfigBuilder()
      .version(Version.Main.V4_0)
      .net(new Net("localhost", mongoPort, Network.localhostIsIPv6()))
      .build()

    val executable = try {
      val exec = starter.prepare(mongodConfig)
      exec.start()
      Some(exec)
    } catch {
      case _: Throwable => None
    }

    (executable, mongoPort)
  }

}
