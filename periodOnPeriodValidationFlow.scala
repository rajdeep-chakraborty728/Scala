package com.amazon.nova.dataservices.validation.periodOnPeriod
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.amazon.nova.dataservices.validation.utils.{ValidationUtils, Constants => localConstants, DataSetStringToDataSetTypeMap => StringToDataSetMap}
import com.amazon.nova.dataservices.utils.{DatasetObject, Constants => globalConstants, FileUtils => globalFileUtils}
import com.amazon.nova.dataservices.utils.SparkSessionUtils.isLocaLSparkSession
import com.amazon.nova.dataservices.validation.periodOnPeriod.metricPercentThresholdVarianceModules._

object periodOnPeriodValidationFlow {

  def runRequestedTransform(spark: SparkSession,
                            transformName: String,
                            dimension: String,
                            metric: String,
                            metricType: String,
                            countryField: String,
                            pipelineName: String,
                            moduleName: String,
                            filterCondition: String,
                            defaultThresholdValue: Int,
                            currentPeriodPath: String,
                            previousPeriodPath: String,
                            currentPeriod: String,
                            previousPeriod: String,
                            datasetSchema: String,
                            s3InputPath: String,
                            fromEmail: String,
                            toEmail: String): DataFrame = {

    transformName match {

      case localConstants.`thresholdCalculationFlow` => metricPercentThresholdVarianceFlow(spark,
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

      case _ => throw new NoSuchElementException(s"Transform Segment $transformName does not exist in runRequestedTransform()")

    }

  }

  // This Method will be invoked From Class periodOnPeriodValidationTask Based On transformName
  // This Method contains the entire flow for Threshold Validation Flow
  // This Method will invoke Object metricPercentThresholdVarianceModules and it's methods time by time which will contain different pre validation Task
  // & Percent Calculation Logic to put modular logics in a separate scala object
  // At Any Point Of Time If a Pre validation Check or exception happens subsequently process will be skipped
  // The Flow for this method defined As Below

  // 1. If Input Pipeline + Module is a Valid Configuration Then Continue the process else Raise an Exception Mail & Exit
  // 2. If Input Pipeline + Module + Metric Type is a Valid Configuration Then Continue the process else Raise an Exception Mail & Exit
  // 3. If Input Country Name is  present in the Input Dimension Then Continue the process else Raise an Exception Mail & Exit
  // 4. If Both the Current & Previous DataSet after applying input filter are non empty Then Continue the process else Raise an Exception Mail & Exit
  // 5. Calculate the Difference of Metrics in Percentage
  // 6. Calculate If any Percent Difference of Metric is Breaching The Threshold Then Raise an Exception Mail & Exit Else Raise a Success Mail & Exit

  def metricPercentThresholdVarianceFlow(spark: SparkSession,
                         dimension: String,
                         metric: String,
                         metricType: String,
                         countryField: String,
                         pipelineName: String,
                         moduleName: String,
                         filterCondition: String,
                         defaultThresholdValue: Int,
                         currentPeriodPath: String,
                         previousPeriodPath: String,
                         currentPeriod: String,
                         previousPeriod: String,
                         datasetSchema: String,
                         s3InputPath: String,
                         fromEmail: String,
                         toEmail: String) : DataFrame = {

    //////////////////////////////////////////////////////////////////
    // Flow For Threshold Variance Method and it's business process
    //////////////////////////////////////////////////////////////////

    val thresholdVarianceModulesClass = new metricPercentThresholdVarianceModules(spark,
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
      toEmail
    )

    val sendMailSubject = localConstants.mailSubject + " For Module " + pipelineName + " - " + moduleName

    try {

      ////////////////////////////////////////////////////////////////////////////////////////
      // Read Threshold DataSet Into Dataframe For Further Validation & Calculation
      // Invoking the global utility readTSVToDataFrame
      ////////////////////////////////////////////////////////////////////////////////////////

      val thresholdTable: DataFrame = globalFileUtils.readTSVToDataFrame(
        spark,
        periodOnPeriodValidationSchema.DataSetThresholdConfiguration.schema,
        if (isLocaLSparkSession(spark)) {
          periodOnPeriodValidationSchema.DataSetThresholdConfiguration.localDataFilePath
        }
        else {
          s3InputPath + s"/" + periodOnPeriodValidationSchema.DataSetThresholdConfiguration.datasetType + s"/" + periodOnPeriodValidationSchema.DataSetThresholdConfiguration.s3Path + s"/"
        },
        globalConstants.IntDefaultHundred
      )

      //////////////////////////////////////////////////////////////////////////////////////////////////
      //Process - PipelineModuleValidation Starts
      //////////////////////////////////////////////////////////////////////////////////////////////////

          val validatePipelineModuleConfigurationOutput = generateDataSetPipelineModuleFilter(spark,
            thresholdTable,
            pipelineName,
            moduleName
          )

          val validatePipelineModuleConfigurationExistOutput= ValidationUtils.validateEmptyDataFrame(spark,validatePipelineModuleConfigurationOutput)

          //////////////////////////////////////////////////////////////////////////////////////////////////
          // If Input Pipeline, Module is not present in Configuration then exit the Process
          // determined by boolean variable validatePipelineModuleConfigurationExistOutput returned
          // from modular method generateDataSetPipelineModuleFilter and validateEmptyDataFrame
          // Generate a template Message and process will exit
          //////////////////////////////////////////////////////////////////////////////////////////////////
          if (validatePipelineModuleConfigurationExistOutput) {

            ValidationUtils.callSendEmail(
              spark,
              fromEmail,
              toEmail,
              sendMailSubject,
              thresholdVarianceModulesClass.mailBodyGeneration("PipelineModuleValidation"),
              localConstants.emailBodyType
            )

            // End The Process As Pipeline, Module Combination Validation Failed
            return spark.emptyDataFrame

          }

      //////////////////////////////////////////////////////////////////////////////////////////////////
      //Process - PipelineModuleValidation Ends
      //////////////////////////////////////////////////////////////////////////////////////////////////


      //////////////////////////////////////////////////////////////////////////////////////////////////
      //Process - PipelineModuleMetricValidation Starts
      //////////////////////////////////////////////////////////////////////////////////////////////////

          ////////////////////////////////////////////////////////////////////////////////////////////////////
          // Convert Input String to Scala List for Dimension & Metric using Utility stringToListConversion
          ////////////////////////////////////////////////////////////////////////////////////////////////////
          val dimensionList: List[String] = ValidationUtils.stringToListConversion(dimension, localConstants.commaSeparator)
          val metricList: List[String] = ValidationUtils.stringToListConversion(metric, localConstants.commaSeparator)
          val MetricTypeList = ValidationUtils.stringToListConversion(metricType, localConstants.commaSeparator)

          val MetricTypeFilter = MetricTypeList.mkString("('", "','", "')")

          val validatePipelineModuleMetricOutput = generateDataSetPipelineModuleMetricFilter(spark,
            thresholdTable,
            pipelineName,
            moduleName,
            MetricTypeFilter
          )

          val validatePipelineModuleMetricConfigurationExistOutput= ValidationUtils.validateEmptyDataFrame(spark,validatePipelineModuleMetricOutput)

          //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
          // If Input Pipeline, Module, Metric is not present in Configuration then exit the Process
          // determined by boolean variable validatePipelineModuleMetricConfigurationExistOutput returned
          // from modular method generateDataSetPipelineModuleMetricFilter and validateEmptyDataFrame
          // Generate a template Message and process will exit
          //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
          if (validatePipelineModuleMetricConfigurationExistOutput){

            ValidationUtils.callSendEmail(
              spark,
              fromEmail,
              toEmail,
              sendMailSubject,
              thresholdVarianceModulesClass.mailBodyGeneration("PipelineModuleMetricValidation"),
              localConstants.emailBodyType
            )

            // End The Process As Pipeline, Module, Metric Combination Validation Failed
            return spark.emptyDataFrame

          }

      //////////////////////////////////////////////////////////////////////////////////////////////////
      //Process - PipelineModuleMetricValidation Ends
      //////////////////////////////////////////////////////////////////////////////////////////////////

      ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
      // returned DataFrame thresholdTableDataDenormalised - Will be Used Later for calculation for threshold value
      ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
      val thresholdTableDataDenormalised=generateThresholdDenormalisedDataSet(spark,
        thresholdTable,
        countryField,
        pipelineName,
        moduleName,
        MetricTypeFilter
      )

      //////////////////////////////////////////////////////////////////////////////////////////////////
      //Process - CountryNameInDimensionValidation Starts
      //////////////////////////////////////////////////////////////////////////////////////////////////

          val validateExistCountryOutput = validateCountryFieldExistInDimension(spark,
            dimensionList,
            countryField
          )
          //////////////////////////////////////////////////////////////////////////////////////////////////
          // If Country Type is missing in Dimension List then progress the Process
          // determined by boolean variable validateExistCountryOutput returned
          // using the modular method validateCountryFieldExistInDimension
          // Generate a template Message and process will exit
          //////////////////////////////////////////////////////////////////////////////////////////////////
          if (!validateExistCountryOutput){

            ValidationUtils.callSendEmail(
              spark,
              fromEmail,
              toEmail,
              sendMailSubject,
              thresholdVarianceModulesClass.mailBodyGeneration("CountryNameInDimensionValidation"),
              localConstants.emailBodyType
            )

            // End The Process As Coutry Field Not in Dimension List Validation Failed
            return spark.emptyDataFrame

          }

      //////////////////////////////////////////////////////////////////////////////////////////////////
      //Process - CountryNameInDimensionValidation Ends
      //////////////////////////////////////////////////////////////////////////////////////////////////


      //////////////////////////////////////////////////////////////////////////////////////////////////
      // Read Raw Data From S3 Path for current period
      // using the global utility readTSVToDataFrame
      // utility map StringToDataSetMap is used to get the DataSet Schema
      //////////////////////////////////////////////////////////////////////////////////////////////////

      val readCurrentPeriodDataSet: DatasetObject = StringToDataSetMap.mapStringToDataSet(datasetSchema)
      val readCurrentPeriodRawData: DataFrame = globalFileUtils.readTSVToDataFrame(
        spark,
        readCurrentPeriodDataSet.schema,
        currentPeriodPath,
        globalConstants.IntDefaultHundred
      )

      /////////////////////////////////////////////////////////////////////////////////////////////////////
      // Read Raw Data From S3 Path for previous period
      // using the global utility readTSVToDataFrame
      // utility map StringToDataSetMap is used to get the DataSet Schema
      // Invoking the global utility readTSVToDataFrame
      /////////////////////////////////////////////////////////////////////////////////////////////////////

      val readPreviousPeriodDataSet: DatasetObject = StringToDataSetMap.mapStringToDataSet(datasetSchema)
      val readPreviousPeriodRawData: DataFrame = globalFileUtils.readTSVToDataFrame(
        spark,
        readPreviousPeriodDataSet.schema,
        previousPeriodPath,
        globalConstants.IntDefaultHundred
      )

      //////////////////////////////////////////////////////////////////////////////////////////////////
      //Process - CurrentPeriodDataSufficiencyValidation Starts
      //////////////////////////////////////////////////////////////////////////////////////////////////

          /////////////////////////////////////////////////////////////////////////////////////////////////////
          // Filter DataFrame From Raw Data for Current period
          // using the modular method filterDataFrame
          // Next Check filtered dataFrame is Empty or not
          // using the modular method validateEmptyDataFrame
          /////////////////////////////////////////////////////////////////////////////////////////////////////

          val readCurrentPeriodFilterData = ValidationUtils.filterDataFrame(spark, readCurrentPeriodRawData, changeEmptyFilterConditionToDefault(filterCondition))
          val validateCurrentPeriodDataFrameOutput = ValidationUtils.validateEmptyDataFrame(spark,
            readCurrentPeriodFilterData
          )

          //////////////////////////////////////////////////////////////////////////////////////////////////
          // If Current DataFrame Is Empty then Exit the Process
          // readCurrentPeriodFilterData - Will be Used Subsequently If Current DataFrame Post Filter is Non Empty
          // Generate a template Message and process will exit
          //////////////////////////////////////////////////////////////////////////////////////////////////
          if (validateCurrentPeriodDataFrameOutput){

            ValidationUtils.callSendEmail(
              spark,
              fromEmail,
              toEmail,
              sendMailSubject,
              thresholdVarianceModulesClass.mailBodyGeneration("CurrentPeriodDataSufficiencyValidation"),
              localConstants.emailBodyType
            )

            // End The Process As Current Period DataSet Empty Validation Failed
            return spark.emptyDataFrame

          }

      //////////////////////////////////////////////////////////////////////////////////////////////////
      //Process - CurrentPeriodDataSufficiencyValidation Ends
      //////////////////////////////////////////////////////////////////////////////////////////////////


      //////////////////////////////////////////////////////////////////////////////////////////////////
      //Process - PreviousPeriodDataSufficiencyValidation Starts
      //////////////////////////////////////////////////////////////////////////////////////////////////

          /////////////////////////////////////////////////////////////////////////////////////////////////////
          // Filter DataFrame From Raw Data for previous period
          // using the modular method filterDataFrame
          // Next Check filtered dataFrame is Empty or not
          // using the modular method validateEmptyDataFrame
          /////////////////////////////////////////////////////////////////////////////////////////////////////

          val readPreviousPeriodFilterData = ValidationUtils.filterDataFrame(spark, readPreviousPeriodRawData, changeEmptyFilterConditionToDefault(filterCondition))
          val validatePreviousPeriodDataFrameOutput = ValidationUtils.validateEmptyDataFrame(spark,
            readPreviousPeriodFilterData
          )
          //////////////////////////////////////////////////////////////////////////////////////////////////
          // If Previous DataFrame Is Empty then Exit the Process
          // readPreviousPeriodFilterData - Will be Used Subsequently If Current DataFrame Post Filter is Non Empty
          // Generate a template Message and process will exit
          //////////////////////////////////////////////////////////////////////////////////////////////////
          if (validatePreviousPeriodDataFrameOutput) {

            ValidationUtils.callSendEmail(
              spark,
              fromEmail,
              toEmail,
              sendMailSubject,
              thresholdVarianceModulesClass.mailBodyGeneration("PreviousPeriodDataSufficiencyValidation"),
              localConstants.emailBodyType
            )

            // End The Process As Previous Period DataSet Empty Validation Failed
            return spark.emptyDataFrame

          }

      //////////////////////////////////////////////////////////////////////////////////////////////////
      //Process - PreviousPeriodDataSufficiencyValidation Ends
      //////////////////////////////////////////////////////////////////////////////////////////////////

      ////////////////////////////////////////////////////////////////////////////////////////
      // Invoke method generateFinalThresholdBreachedData
      // To Get The Threshold Breached DataSet
      ////////////////////////////////////////////////////////////////////////////////////////
      val generateFinalThresholdBreachedData = calculateThresholdBreachedDataSet(
        spark,
        metricList,
        dimensionList,
        MetricTypeList,
        readCurrentPeriodFilterData,
        readPreviousPeriodFilterData,
        countryField,
        thresholdTableDataDenormalised,
        defaultThresholdValue
      )

      ////////////////////////////////////////////////////////////////////////////////////////
      // Invoke method validateEmptyDataFrame to verify if threshold breached
      // DataFrame is empty or not. If DataFrame is empty the drop a success message for Validation Saying No Threshold Breached
      // records have been found in a valid format, method will return False and Process will exit
      // If DataFrame is non empty return True
      ////////////////////////////////////////////////////////////////////////////////////////

      val validateFinalThresholdBreachedData = ValidationUtils.validateEmptyDataFrame(spark,
        generateFinalThresholdBreachedData
      )

      //////////////////////////////////////////////////////////////////////////////////////////////////
      //Process - ThresholdNonBreachedDataSetValidation Starts
      //////////////////////////////////////////////////////////////////////////////////////////////////

          ////////////////////////////////////////////////////////////////////////////////////////
          // If variable validateFinalThresholdBreachedData returns True
          // Means Threshold dataFrame generateFinalThresholdBreachedData is Empty
          // Generate a template Message and process will exit
          ////////////////////////////////////////////////////////////////////////////////////////

          if (validateFinalThresholdBreachedData){

            ValidationUtils.callSendEmail(
              spark,
              fromEmail,
              toEmail,
              sendMailSubject,
              thresholdVarianceModulesClass.mailBodyGeneration("ThresholdNonBreachedDataSetValidation"),
              localConstants.emailBodyType
            )

            // End The Process As No Threshold Breached Record Is Found
            spark.emptyDataFrame

          }

      //////////////////////////////////////////////////////////////////////////////////////////////////
      //Process - ThresholdNonBreachedDataSetValidation Ends
      //////////////////////////////////////////////////////////////////////////////////////////////////


      //////////////////////////////////////////////////////////////////////////////////////////////////
      //Process - ThresholdBreachedDataSetValidation Starts
      //////////////////////////////////////////////////////////////////////////////////////////////////

          ////////////////////////////////////////////////////////////////////////////////////////
          // if variable validateFinalThresholdBreachedData returns False
          // Means Threshold dataFrame generateFinalThresholdBreachedData is Non Empty
          // Convert DataFrame generateFinalThresholdBreachedData to a String using
          // validation utility convertDataFrameToString
          // Generate a template Message and process will exit
          ////////////////////////////////////////////////////////////////////////////////////////

          else{

            val resultHeader = generateFinalThresholdBreachedData.columns.mkString("|")
            val resultDetails = ValidationUtils.convertDataFrameToString(generateFinalThresholdBreachedData, separator = localConstants.tabSeparator)

            ValidationUtils.callSendEmail(
              spark,
              fromEmail,
              toEmail,
              sendMailSubject,
              thresholdVarianceModulesClass.mailBodyGeneration("ThresholdBreachedDataSetValidation",resultHeader,resultDetails),
              localConstants.emailBodyType
            )

            // End The Process As Threshold Breached Record Is Found and Converted
            generateFinalThresholdBreachedData

          }

      //////////////////////////////////////////////////////////////////////////////////////////////////
      //Process - ThresholdBreachedDataSetValidation Ends
      //////////////////////////////////////////////////////////////////////////////////////////////////

    } // Close Brace Of try block

    catch {

      case ex: Exception =>

        ////////////////////////////////////////////
        // Unhandled Exception report Via Mail
        ////////////////////////////////////////////

        ValidationUtils.callSendEmail(
          spark,
          fromEmail,
          toEmail,
          sendMailSubject,
          thresholdVarianceModulesClass.mailBodyGeneration("UnhandledException","",ex.toString),
          localConstants.emailBodyType
        )

        throw new Exception(ex.toString)

    }


  }

}
