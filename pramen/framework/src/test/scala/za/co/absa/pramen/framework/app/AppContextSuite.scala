package za.co.absa.pramen.framework.app

import com.typesafe.config.ConfigFactory
import org.scalatest.WordSpec
import za.co.absa.pramen.framework.base.SparkTestBase
import za.co.absa.pramen.framework.bookkeeper.SyncBookKeeperNull
import za.co.absa.pramen.framework.journal.JournalNull
import za.co.absa.pramen.framework.lock.TokenLockFactoryAllow
import za.co.absa.pramen.framework.utils.ResourceUtils

class AppContextSuite extends WordSpec with SparkTestBase{
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
      .resolve()

    "be able to create app context from config" in {
      val context = AppContextImpl.apply(conf)

      assert(context.bookkeeper.isInstanceOf[SyncBookKeeperNull])
      assert(context.tokenLockFactory.isInstanceOf[TokenLockFactoryAllow])
      assert(context.journal.isInstanceOf[JournalNull])
      assert(context.metastore != null)
    }

    "be able to create mock app context" in {
      val bookkeeper = new SyncBookKeeperNull()
      val journal = new JournalNull()

      val context = AppContextImpl.getMock(conf, bookkeeper, journal)

      assert(context.bookkeeper == bookkeeper)
      assert(context.journal == journal)
      assert(context.metastore != null)
    }
  }
}
