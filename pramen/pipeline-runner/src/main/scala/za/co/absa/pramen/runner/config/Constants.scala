package za.co.absa.pramen.runner.config

object Constants {
  final val CONFIG_KEYS_TO_REDACT = Set(
    "java.class.path",
    "java.security.auth.login.config",
    "spark.driver.extraJavaOptions",
    "spark.yarn.dist.files",
    "spline.mongodb.url",
    "sun.boot.class.path",
    "sun.java.command"
  )

  final val CONFIG_WORDS_TO_REDACT = Set("password", "secret", "session.token", "auth.user.info")
}
