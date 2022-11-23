package com.amazon.nova.dataservices.validation.periodOnPeriod

import com.amazon.nova.dataservices.utils.DatasetObject
import com.amazon.nova.dataservices.validation.utils.Constants._
import org.apache.spark.sql.types.{DecimalType, StringType, StructField, StructType}

object periodOnPeriodValidationSchema {

  val DataSetThresholdConfiguration: DatasetObject =
    DatasetObject(
      schema = StructType(
        List(
          StructField("pipeline", StringType, true),
          StructField("module", StringType, true),
          StructField("country_code", StringType, true),
          StructField("metricType", StringType, true),
          StructField("metricThreshold", DecimalType(20, 0), true)
        )
      ),
      localDataFilePath = "tst/resources/validation/periodOnPeriod/threshold_configuration.tsv",
      datasetType = ThresholdConfigurationExtractDatasetType,
      s3Path = "extract-threshold-configuration",
      hiveViewName = "extract_threshold_configuration"
    )

}
