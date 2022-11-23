package com.amazon.nova.dataservices.validation.utils

object Constants {

  val dimensionList: String = "DIMENSION_LIST"
  val metricList: String = "METRIC_LIST"
  val metricTypeList: String = "METRIC_TYPE_LIST"
  val countryField: String = "COUNTRY_FIELD"
  val pipelineName: String = "PIPELINE_NAME"
  val moduleName: String = "MODULE_NAME"
  val filterCondition: String = "FILTER_CONDITION"
  val defaultThresholdValue: String = "DEFAULT_THRESHOLD_VALUE"
  val currentPeriodPath: String = "CURRENT_PERIOD_PATH"
  val previousPeriodPath: String = "PREVIOUS_PERIOD_PATH"
  val currentPeriod: String = "CURRENT_PERIOD"
  val previousPeriod: String = "PREVIOUS_PERIOD"
  val datasetSchema: String = "DATASET_SCHEMA"

  val thresholdCalculationFlow: String = "THRESHOLD_CALCULATION_FLOW"

  // Constant to define prefixes for fields to denote current month metrix, previous month metrix, difference of metrix across two months & percentage of difference
  val currentPeriodPrefix = currentPeriod+"_"
  val previousPeriodPrefix = previousPeriod+"_"
  val diffCurrentPreviousPrefix = "DIFF_MONTHLY_"
  val diffPercentCurrentPreviousPrefix = "PERCENT_DIFF_MONTHLY_"
  val costThresholdField = "COST_THRESHOLD"
  val unitThresholdField = "UNIT_THRESHOLD"
  val hourThresholdField = "HOUR_THRESHOLD"

  // S3 Path for Validation Configuration
  val ThresholdConfigurationExtractDatasetType = "inbound/allocations/threshold-config"

  // Email Utility Subject Prefix, Header, Footer
  val mailSubject = "PeriodOnPeriod Threshold Validation"
  val mailHeader = """
 Hi Team,"""+"\n"+"\n"
  val mailFooter = "\n"+"\n"+"""
 Thanks,
 Nova Data Services Team
  """
  // Send Mail Body Type
  val emailBodyType = "text"

  // Messages - part of Email Communication
  val exceptionEncountered = "Exception Encountered"
  val successfulValidation = "Successful Validation - No Threshold Breached"
  val unSuccessfulValidation = "Unsuccessful Validation - Threshold Breached"
  val successfulValidationMessage= "No Threshold Breached Records have been found"
  val unhandledExceptionEncountered = "Unhandled Exception Encountered"

  val tabSeparator="|"
  val commaSeparator=","

  val processThresholdVariancePipelineModuleValidation="PipelineModuleValidation"
  val processThresholdVariancePipelineModuleMetricValidation="PipelineModuleMetricValidation"
  val processThresholdVarianceCountryNameInDimensionValidation="CountryNameInDimensionValidation"
  val processThresholdVarianceCurrentPeriodDataSufficiencyValidation="CurrentPeriodDataSufficiencyValidation"
  val processThresholdVariancePreviousPeriodDataSufficiencyValidation="PreviousPeriodDataSufficiencyValidation"
  val processThresholdVarianceThresholdNonBreachedDataSetValidation="ThresholdNonBreachedDataSetValidation"
  val processThresholdVarianceThresholdBreachedDataSetValidation="ThresholdBreachedDataSetValidation"
  val processUnhandledException="UnhandledException"

  val defaultFilterCondition="1=1"

}
