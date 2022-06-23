package za.co.absa.pramen.framework.journal.model

case class TaskCompletedCsv(
                             jobName: String,
                             tableName: String,
                             periodBegin: String,
                             periodEnd: String,
                             informationDate: String,
                             inputRecordCount: Long,
                             inputRecordCountOld: Long,
                             outputRecordCount: Option[Long],
                             outputRecordCountOld: Option[Long],
                             outputSize: Option[Long],
                             startedAt: Long,
                             finishedAt: Long,
                             status: String,
                             failureReason: Option[String]
                           )
