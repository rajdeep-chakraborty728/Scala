package com.amazon.nova.dataservices.validation.periodOnPeriod

import com.amazon.nova.dataservices.validation.utils.{Constants => localConstants}
import com.amazon.nova.dataservices.utils.{Constants => GlobalConstant}
import com.amazon.nova.dataservices.utils.SparkSessionUtils
import org.apache.spark.sql.DataFrame
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class periodOnPeriodValidationUtilsSpec extends FlatSpec with Matchers {

  val blankString = GlobalConstant.BlankString
  val defaultThreshold = 15
  val dataSetSchemaObject = "GamVcpuSchema.DataSetGamVcpuGcfCognosExtract"
  val dimension = "ROVarOpExLvl2,FC"
  val metric = "Amount"
  val metricType = "Cost"
  val country = "ROVarOpExLvl2"
  val pipeline = "GAM_VCPU"
  val module = "GCF_VCPU_Cognos_Extract"

  val currentPeriodPathThresholdCompliant = "tst/resources/validation/periodOnPeriod/threshold_compliant_current_period.tsv"
  val previousPeriodPathThresholdCompliant = "tst/resources/validation/periodOnPeriod/threshold_compliant_previous_period.tsv"

  val currentPeriodPathThresholdNonCompliant = "tst/resources/validation/periodOnPeriod/threshold_non_compliant_current_period.tsv"
  val previousPeriodPathThresholdNonCompliant = "tst/resources/validation/periodOnPeriod/threshold_non_compliant_previous_period.tsv"

  "runRequestedvalidationFlowInvalidPipelineModuleInputFilter" should "return a valid Long" in {

    // pipelineName is Modified to "GAM_VCP", which is not existing in Threshold Configuration

    val sparkMaster = "local[*]"
    val spark = SparkSessionUtils.getSparkSession(sparkMaster)
    val transformName = localConstants.`thresholdCalculationFlow`
    val dimensionList = dimension
    val metricList = metric
    val metricTypeList = metricType
    val countryField = country
    val pipelineName = "GAM_VCP"
    val moduleName = module
    val filterCondition = blankString
    val defaultThresholdValue = defaultThreshold
    val currentPeriodPath = currentPeriodPathThresholdCompliant
    val previousPeriodPath = previousPeriodPathThresholdCompliant
    val currentPeriod = blankString
    val previousPeriod = blankString
    val datasetSchema = dataSetSchemaObject
    val s3InputPath = blankString
    val fromEmail = blankString
    val toEmail = blankString

    val expected = 0

    val result: DataFrame = periodOnPeriodValidationFlow.runRequestedTransform(spark,transformName,
      dimensionList,
      metricList,
      metricTypeList,
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

    /* Assert */
    result.count() should be(expected)
  }

  "runRequestedvalidationFlowInvalidMetricTypeInputFilter" should "return a valid Long" in {

    // metricTypeList is Modified to "Costing", which is not a valid metric type in Threshold Configuration

    val sparkMaster = "local[*]"
    val spark = SparkSessionUtils.getSparkSession(sparkMaster)
    val transformName = localConstants.`thresholdCalculationFlow`
    val dimensionList = dimension
    val metricList = metric
    val metricTypeList = "Costing"
    val countryField = country
    val pipelineName = pipeline
    val moduleName = module
    val filterCondition = blankString
    val defaultThresholdValue = defaultThreshold
    val currentPeriodPath = currentPeriodPathThresholdCompliant
    val previousPeriodPath = previousPeriodPathThresholdCompliant
    val currentPeriod = blankString
    val previousPeriod = blankString
    val datasetSchema = dataSetSchemaObject
    val s3InputPath = blankString
    val fromEmail = blankString
    val toEmail = blankString

    val expected = 0

    val result: DataFrame = periodOnPeriodValidationFlow.runRequestedTransform(spark,transformName,
      dimensionList,
      metricList,
      metricTypeList,
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

    /* Assert */
    result.count() should be(expected)
  }

  "runRequestedvalidationFlowInvalidCountryFieldInputFilter" should "return a valid Long" in {

    // countryField is Modified to "country_id", which is not a valid dimension in dimensionList

    val sparkMaster = "local[*]"
    val spark = SparkSessionUtils.getSparkSession(sparkMaster)
    val transformName = localConstants.`thresholdCalculationFlow`
    val dimensionList = dimension
    val metricList = metric
    val metricTypeList = metricType
    val countryField = "country_id"
    val pipelineName = pipeline
    val moduleName = module
    val filterCondition = blankString
    val defaultThresholdValue = defaultThreshold
    val currentPeriodPath = currentPeriodPathThresholdCompliant
    val previousPeriodPath = previousPeriodPathThresholdCompliant
    val currentPeriod = blankString
    val previousPeriod = blankString
    val datasetSchema = dataSetSchemaObject
    val s3InputPath = blankString
    val fromEmail = blankString
    val toEmail = blankString

    val expected = 0

    val result: DataFrame = periodOnPeriodValidationFlow.runRequestedTransform(spark,transformName,
      dimensionList,
      metricList,
      metricTypeList,
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

    /* Assert */
    result.count() should be(expected)
  }

  "runRequestedvalidationFlowInvalidFilterCondition" should "return a valid Long" in {

    // filterCondition is Modified to "1=2", which will make the dataSet Empty

    val sparkMaster = "local[*]"
    val spark = SparkSessionUtils.getSparkSession(sparkMaster)
    val transformName = localConstants.`thresholdCalculationFlow`
    val dimensionList = dimension
    val metricList = metric
    val metricTypeList = metricType
    val countryField = country
    val pipelineName = pipeline
    val moduleName = module
    val filterCondition = "1=2"
    val defaultThresholdValue = defaultThreshold
    val currentPeriodPath = currentPeriodPathThresholdCompliant
    val previousPeriodPath = previousPeriodPathThresholdCompliant
    val currentPeriod = blankString
    val previousPeriod = blankString
    val datasetSchema = dataSetSchemaObject
    val s3InputPath = blankString
    val fromEmail = blankString
    val toEmail = blankString

    val expected = 0

    val result: DataFrame = periodOnPeriodValidationFlow.runRequestedTransform(spark, transformName,
      dimensionList,
      metricList,
      metricTypeList,
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

    /* Assert */
    result.count() should be(expected)
  }

  "runRequestedvalidationFlowNoThresholdBreachRecords" should "return a valid Long" in {

    // Current & Previous Period Test Data Has Been Configured To Not Breach Any Threshold
    // Current Period Path = tst/resources/validation/periodOnPeriod/threshold_compliant_current_period.tsv
    // Previous Period Path = tst/resources/validation/periodOnPeriod/threshold_compliant_previous_period.tsv

    val sparkMaster = "local[*]"
    val spark = SparkSessionUtils.getSparkSession(sparkMaster)
    val transformName = localConstants.`thresholdCalculationFlow`
    val dimensionList = dimension
    val metricList = metric
    val metricTypeList = metricType
    val countryField = country
    val pipelineName = pipeline
    val moduleName = module
    val filterCondition = blankString
    val defaultThresholdValue = defaultThreshold
    val currentPeriodPath = currentPeriodPathThresholdCompliant
    val previousPeriodPath = previousPeriodPathThresholdCompliant
    val currentPeriod = blankString
    val previousPeriod = blankString
    val datasetSchema = dataSetSchemaObject
    val s3InputPath = blankString
    val fromEmail = blankString
    val toEmail = blankString

    val expected = 0

    val result: DataFrame = periodOnPeriodValidationFlow.runRequestedTransform(spark, transformName,
      dimensionList,
      metricList,
      metricTypeList,
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

    /* Assert */
    result.count() should be(expected)
  }

  "runRequestedvalidationFlowThresholdBreachRecords" should "return a valid Long" in {

    // Current & Previous Period Test Data Has Been Configured To Breach Threshold for US and PL
    // Current Period Path = tst/resources/validation/periodOnPeriod/threshold_non_compliant_current_period.tsv
    // Previous Period Path = tst/resources/validation/periodOnPeriod/threshold_non_compliant_previous_period.tsv

    val sparkMaster = "local[*]"
    val spark = SparkSessionUtils.getSparkSession(sparkMaster)
    val transformName = localConstants.`thresholdCalculationFlow`
    val dimensionList = dimension
    val metricList = metric
    val metricTypeList = metricType
    val countryField = country
    val pipelineName = pipeline
    val moduleName = module
    val filterCondition = blankString
    val defaultThresholdValue = defaultThreshold
    val currentPeriodPath = currentPeriodPathThresholdNonCompliant
    val previousPeriodPath = previousPeriodPathThresholdNonCompliant
    val currentPeriod = blankString
    val previousPeriod = blankString
    val datasetSchema = dataSetSchemaObject
    val s3InputPath = blankString
    val fromEmail = blankString
    val toEmail = blankString

    val expected = 2

    val result: DataFrame = periodOnPeriodValidationFlow.runRequestedTransform(spark,transformName,
      dimensionList,
      metricList,
      metricTypeList,
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

    /* Assert */
    result.count() should be(expected)
  }

}
