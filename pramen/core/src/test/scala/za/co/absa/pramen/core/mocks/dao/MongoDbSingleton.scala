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


import de.flapdoodle.embed.mongo.commands.ServerAddress
import de.flapdoodle.embed.mongo.distribution.Version
import de.flapdoodle.embed.mongo.transitions.{Mongod, RunningMongodProcess}
import de.flapdoodle.embed.process.io.{ProcessOutput, StreamProcessor}
import de.flapdoodle.reverse.transitions.Start
import de.flapdoodle.reverse.{StateID, TransitionWalker}
import org.slf4j.{Logger, LoggerFactory}

object MongoDbSingleton {
  private val log = LoggerFactory.getLogger(this.getClass)

  lazy val embeddedMongoDb: (Option[RunningMongodProcess], Int) = startEmbeddedMongoDb()

  final class Slf4jProcessor(logger: Logger, prefix: String) extends StreamProcessor {
    override def process(block: String): Unit = {
      if (block != null && block.nonEmpty) logger.info(s"$prefix$block")
    }

    override def onProcessed(): Unit = () // no-op
  }

  /**
    * Create and run a MongoDb instance.
    *
    * How to configure embedded MongoDB: https://github.com/flapdoodle-oss/de.flapdoodle.embed.mongo
    *
    * @return A pair: a MongoDb executable object to be used to stop it and the port number the embedded MongoDB listens to.
    */
  private def startEmbeddedMongoDb(): (Option[RunningMongodProcess], Int) = {
    try {
      val version: Version = Version.V8_2_2
      val mongod = Mongod.builder()
        .processOutput(
          Start.to(classOf[ProcessOutput]).initializedWith(
            ProcessOutput.builder()
              .output(new Slf4jProcessor(log, "[mongod-out] "))
              .error(new Slf4jProcessor(log, "[mongod-err] "))
              .commands(new Slf4jProcessor(log, "[mongod-cmd] "))
              .build()
          )
        )
        .build()

      val executable: TransitionWalker.ReachedState[RunningMongodProcess] =
        mongod.transitions(version)
          .walker()
          .initState(StateID.of(classOf[RunningMongodProcess]))

      val addr: ServerAddress = executable.current().getServerAddress
      val mongoPort: Int = addr.getPort

      (Option(executable.current()), mongoPort)
    } catch {
      case ex: Throwable =>
        log.warn("Couldn't start embedded Mongodb. MongoDB tests will be skipped", ex)
        (None, 0)
    }
  }

}
