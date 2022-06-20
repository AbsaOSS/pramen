package za.co.absa.pramen.framework.mocks.dao

import de.flapdoodle.embed.mongo.config.{MongodConfigBuilder, Net, RuntimeConfigBuilder}
import de.flapdoodle.embed.mongo.distribution.Version
import de.flapdoodle.embed.mongo.{Command, MongodExecutable, MongodStarter}
import de.flapdoodle.embed.process.config.io.ProcessOutput
import de.flapdoodle.embed.process.runtime.Network
import org.slf4j.LoggerFactory

object MongoDbSingleton {
  private val log = LoggerFactory.getLogger(this.getClass)

  lazy val embeddedMongoDb: (MongodExecutable, Int) = startEmbeddedMongoDb()

  /**
    * Create and run a MongoDb instance.
    *
    * How to configure embedded MongoDB: https://github.com/flapdoodle-oss/de.flapdoodle.embed.mongo
    *
    * @return A pair: a MongoDb executable object to be used to stop it and the port number the embedded MongoDB listens to.
    */
  private def startEmbeddedMongoDb(): (MongodExecutable, Int) = {
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

    val executable = starter.prepare(mongodConfig)
    executable.start()
    (executable, mongoPort)
  }

}
