package com.amazon.nova.dataservices.validation.periodOnPeriod

import com.amazon.nova.dataservices.utils.BaseParams
import com.amazon.nova.dataservices.validation.utils.{Constants =>  periodByPeriodConstants}
import com.amazon.nova.dataservices.utils.{Constants => globalConstants}

case class periodOnPeriodValidationParams(arguments: Seq[String]) extends BaseParams(arguments) {

  // Re Use SparkMaster,transformName from Global Constant
  val sparkMaster = stringParam(globalConstants.SparkMaster)
  val transformName = stringParam(globalConstants.transformName)

  // List of Dimension, Metric, Metric Type in comma separated
  val dimensionList = stringParam(periodByPeriodConstants.dimensionList)
  val metricList = stringParam(periodByPeriodConstants.metricList)
  val metricTypeList = stringParam(periodByPeriodConstants.metricTypeList)

  // Country ID field, available in s3 data header
  val countryField = stringParam(periodByPeriodConstants.countryField)

  // Pipeline & Module Name to read from Configuration to get threshold values pre defined
  val pipelineName = stringParam(periodByPeriodConstants.pipelineName)
  val moduleName = stringParam(periodByPeriodConstants.moduleName)

  // Filter Condition to be applied on both previous & current period dataset
  val filterCondition = stringParam(periodByPeriodConstants.filterCondition)

  // Default Threshold Value in case of missing in configuration
  val defaultThresholdValue = intParam(periodByPeriodConstants.defaultThresholdValue)

  // Data storage on S3 For Current & Previous Period
  val currentPeriodPath = stringParam(periodByPeriodConstants.currentPeriodPath)
  val previousPeriodPath = stringParam(periodByPeriodConstants.previousPeriodPath)

  // Current & Previous Period
  val currentPeriod = stringParam(periodByPeriodConstants.currentPeriod)
  val previousPeriod = stringParam(periodByPeriodConstants.previousPeriod)

  // Name of the DataSet Schema
  val datasetSchema = stringParam(periodByPeriodConstants.datasetSchema)

  val s3InputPath = stringParam(globalConstants.S3InputPath)

  // Email Sending & Recipient List
  val fromEmail = stringParam(globalConstants.FromEmail)
  val toEmail = stringParam(globalConstants.ToEmail)

  verify()

}
