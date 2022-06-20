package za.co.absa.pramen.framework.mocks.notify

import za.co.absa.pramen.framework.notify.{NotificationBuilder, SchemaDifference, TaskCompleted}

import java.time.Instant

class NotificationBuilderSpy extends NotificationBuilder {
  var failureException: Option[Throwable] = None
  var appName = ""
  var environmentName = ""
  var appStarted: Instant = Instant.MIN
  var appFinished: Instant = Instant.MIN
  var isDryRun: Option[Boolean] = None
  var isUndercover: Option[Boolean] = None
  var minRps = 0
  var goodRps = 0

  var addCompletedTaskCalled = 0
  var addSchemaDifferencesCalled = 0

  override def addFailureException(ex: Throwable): Unit = failureException = Option(ex)

  override def addAppName(name: String): Unit = appName = name

  override def addEnvironmentName(env: String): Unit = environmentName = env

  override def addAppDuration(started: Instant, finished: Instant): Unit = {
    appStarted = started
    appFinished = finished
  }

  override def addDryRun(dryRun: Boolean): Unit = isDryRun = Some(dryRun)

  override def addUndercover(undercover: Boolean): Unit = isUndercover = Some(undercover)

  override def addRpsMetrics(min: Int, good: Int): Unit = {
    minRps = min
    goodRps = good
  }

  override def addCompletedTask(completedTask: TaskCompleted): Unit = addCompletedTaskCalled += 1

  override def addSchemaDifferences(schemaDifferences: SchemaDifference): Unit = addSchemaDifferencesCalled += 1
}
