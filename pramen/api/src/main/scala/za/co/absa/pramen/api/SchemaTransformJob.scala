package za.co.absa.pramen.api

import org.apache.spark.sql.DataFrame

import java.time.LocalDate

/**
  * This is trait for jobs that support schema transformations
  * */
trait SchemaTransformJob {

  /**
    * Optional schema transformation during ingestion.
    *
    * @param inputTable An input table dataframe
    * @param infoDate   TableDataFrame
    * @return
    */
  def schemaTransformation(inputTable: TableDataFrame, infoDate: LocalDate): DataFrame
}
