package za.co.absa.pramen.framework

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.scalatest.{BeforeAndAfterAll, WordSpec}
import za.co.absa.pramen.framework.base.SparkTestBase
import za.co.absa.pramen.framework.bookkeeper.SyncBookKeeperNull
import za.co.absa.pramen.framework.journal.JournalNull
import za.co.absa.pramen.framework.lock.TokenLockFactoryAllow
import za.co.absa.pramen.framework.utils.ResourceUtils

class AppContextFactorySuite extends WordSpec with SparkTestBase {

  "AppContextFactory" should {
    val configStr = ResourceUtils.getResourceString("/test/config/app_context.conf")

    val fullConfig =
      s"""$configStr
         |pramen.metastore {
         |  tables = [
         |    {
         |      name = "table1"
         |      format = "parquet"
         |      path = "/dummy/path"
         |      records.per.partition = 1000000
         |    }
         |  ]
         |}
         |""".stripMargin
    val configBase = ConfigFactory.parseString(fullConfig)

    val conf = configBase
      .withFallback(ConfigFactory.load())
      .withValue("pramen.stop.spark.session", ConfigValueFactory.fromAnyRef(false))
      .resolve()

    "be able to create app context from config" in {
      val context = AppContextFactory.getOrCreate(conf)

      assert(context == AppContextFactory.get)

      assert(context.bookkeeper.isInstanceOf[SyncBookKeeperNull])
      assert(context.tokenLockFactory.isInstanceOf[TokenLockFactoryAllow])
      assert(context.journal.isInstanceOf[JournalNull])
      assert(context.metastore != null)

      context.close()
    }

    "be able to create mock app context" in {
      val bookkeeper = new SyncBookKeeperNull()
      val journal = new JournalNull()

      val context = AppContextFactory.createMockAppContext(conf, bookkeeper, journal)

      assert(context == AppContextFactory.get)

      assert(context.bookkeeper == bookkeeper)
      assert(context.journal == journal)
      assert(context.metastore != null)

      context.close()
    }
  }
}
