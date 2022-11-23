package com.amazon.nova.dataservices.validation.periodOnPeriod
import com.amazon.nova.dataservices.utils.SparkSessionUtils.getSparkSession
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.Logger

object periodOnPeriodValidationTask {

  def main(args: Array[String]): Unit = {

    val sparkTask = new periodOnPeriodValidationTask()
    val params = periodOnPeriodValidationParams(args)

    /* Initialize spark session */
    val spark = getSparkSession(params.sparkMaster())

    /* Run the business logic and pass in the parameters */
    sparkTask.run(spark, params)
  }

}

class periodOnPeriodValidationTask{

  private def run(spark: SparkSession, params: periodOnPeriodValidationParams): Unit = {

    val dimension: String = params.dimensionList() //List of Dimensions required for Variance, to be passed in comma separated format
    val transformName: String = params.transformName() //Transform Name Passed As Input
    val metric: String = params.metricList() //List of Metric required for Variance, to be passed in comma separated format
    val metricType: String = params.metricTypeList() //List of MetricType for each Metric in same order
    val countryField: String = params.countryField() //Name of the Input Country Field Present in DataSet
    val pipelineName: String = params.pipelineName() //Rationale Name of the input Pipeline. It should match with the threshold configuration
    val moduleName: String = params.moduleName() //Rationale Name of the input Module. It should match with the threshold configuration
    val filterCondition: String = params.filterCondition() //If required to compare based on a subset of data from current & previous period
    val defaultThresholdValue: Int = params.defaultThresholdValue() //If any country is not present in configuration for the pipeline & module combination, this default threshold value will be considered
    val currentPeriodPath: String = params.currentPeriodPath() //Current Period S3 Absolute Path
    val previousPeriodPath: String = params.previousPeriodPath() //Previous Period S3 Absolute Path
    val currentPeriod: String = params.currentPeriod() //Current Period in YYYY-MM-DD
    val previousPeriod: String = params.previousPeriod() //Previous Period in YYYY-MM-DD
    val datasetSchema: String = params.datasetSchema() //The DataSet Object Name in String Format for Schema retrieval
    val s3InputPath: String = params.s3InputPath() //The S3 base path of the input datasets
    val fromEmail: String = params.fromEmail() //From Email Address
    val toEmail: String = params.toEmail() //To Email Address

    // Invoke the Object periodOnPeriodValidationFlow to invoke the method validationFlow

    val thresholdBreachedCount:DataFrame = periodOnPeriodValidationFlow.runRequestedTransform(spark,
      transformName,
      dimension,
      metric,
      metricType,
      countryField,
      pipelineName,
      moduleName,
      filterCondition,
      defaultThresholdValue,
      currentPeriodPath,
      previousPeriodPath,
      currentPeriod,
      previousPeriod,
      datasetSchema,
      s3InputPath,
      fromEmail,
      toEmail)

    val log = Logger.getLogger(getClass.getName)
    log.info("Final Resultant DataFrame Count "+thresholdBreachedCount.count.toString)

  }


}
