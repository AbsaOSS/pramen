package za.co.absa.pramen.runner.migrations.model0

case class DataChunk0(tableName: String,
                      infoDate: String, /* Use String to workaround serialization issues */
                      inputRecordCount: Long,
                      outputRecordCount: Long,
                      jobStarted: Long,
                      jobFinished: Long)
