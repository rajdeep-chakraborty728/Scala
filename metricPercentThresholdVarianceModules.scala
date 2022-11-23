package com.amazon.nova.dataservices.validation.periodOnPeriod
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.amazon.nova.dataservices.utils.{Constants => globalConstants, FileUtils => globalFileUtils}
import com.amazon.nova.dataservices.validation.utils.{ValidationUtils, Constants => localConstants}
import org.apache.spark.sql.functions._

object metricPercentThresholdVarianceModules {

  // Purpose of the methods defined in this object are to avoid repeated tasks of metricPercentThresholdVarianceFlow Task flow to make the code modular per process
  // These methods will be mostly scoped to the business flow of the task for metricPercentThresholdVarianceFlow
  // Can be reusable to other process also if similar requirement appears
  // Methods are invoked with limited parameters to make this reusable wherever possible in future

    /**
    * Generate Default Filter Condition If Nothing is passed as an Input, If Non Empty Filter Condition is passed, same will
    * be retained
    * @param inputFilterCondition  Input Filter Condition
    */
    def changeEmptyFilterConditionToDefault(
                                          inputFilterCondition: String
                                          ): String = {

      var outputFilterCondition = globalConstants.BlankString
      if (inputFilterCondition.trim() == globalConstants.BlankString){
        outputFilterCondition = localConstants.defaultFilterCondition
      }
      else{
        outputFilterCondition=inputFilterCondition
      }

      outputFilterCondition

    }
    /**
     * Generate DataFrame (Pipeline, Module) From Input DataSet and Apply Pipeline, Module Filter. Return The Resultant DataFrame
     *
     * @param spark          Spark Session
     * @param inputDataFrame Input Data Frame
     * @param pipelineName   Input Pipeline Name
     * @param moduleName     Module Name
     */

    def generateDataSetPipelineModuleFilter(
                                            spark: SparkSession,
                                            inputDataFrame: DataFrame,
                                            pipelineName: String,
                                            moduleName: String
                                          ): DataFrame = {

      inputDataFrame.createOrReplaceTempView("dataframeData")
      val inputDataFrameDataIsExist = spark.sql("select * from dataframeData where pipeline='" + pipelineName + "' and module='" + moduleName + "'")

      inputDataFrameDataIsExist

    }

    /**
     * Generate DataFrame (Pipeline, Module, Metric) From Input DataSet and Apply Pipeline, Module Filter. Return The Resultant DataFrame
     * Return the dataFrame caller function
     *
     * @param spark            Spark Session
     * @param inputDataFrame   Input Data Frame
     * @param pipelineName     Input Pipeline Name
     * @param moduleName       Module Name
     * @param MetricTypeFilter Metric Type List in String
     */

    def generateDataSetPipelineModuleMetricFilter(
                                                        spark: SparkSession,
                                                        inputDataFrame: DataFrame,
                                                        pipelineName: String,
                                                        moduleName: String,
                                                        MetricTypeFilter: String
                                                      ): DataFrame = {

      //////////////////////////////////////////////////////////////////////////////////////////
      // Filter On the Threshold Data
      // Apply the Filter passed from Map Reduce Task to reduce Threshold data for Configuration
      //////////////////////////////////////////////////////////////////////////////////////////

      inputDataFrame.createOrReplaceTempView("input_dataframe_data")
      val outputDataFrame = spark.sql("select * from input_dataframe_data where pipeline='" + pipelineName + "' and module='" + moduleName + "' and metricType IN " + MetricTypeFilter + "")

      outputDataFrame

    }

  /**
   * Generate Denormalised Threshold DataFrame for Threshold Breach Calculation
   * Return the dataFrame to caller function
   *
   * @param spark            Spark Session
   * @param inputDataFrame   Input Data Frame
   * @param countryField     Input Country Field
   * @param pipelineName     Input Pipeline Name
   * @param moduleName       Module Name
   * @param MetricTypeFilter Metric Type List in String
   */
    def generateThresholdDenormalisedDataSet(spark: SparkSession,
                                             inputDataFrame: DataFrame,
                                             countryField: String,
                                             pipelineName: String,
                                             moduleName: String,
                                             MetricTypeFilter: String
                                            ) : DataFrame = {

      inputDataFrame.createOrReplaceTempView("input_dataframe_data")
      val inputDataFrameFilter = spark.sql("select country_code AS " + countryField + ",metricType,metricThreshold from input_dataframe_data where pipeline='" + pipelineName + "' and module='" + moduleName + "' and metricType IN " + MetricTypeFilter + "")

      //////////////////////////////////////////////////////////////////////////////////////////
      // Calculate Denormalized Table from Configuration To Join Back with Aggregated Dataset
      //////////////////////////////////////////////////////////////////////////////////////////
      inputDataFrameFilter.createOrReplaceTempView("input_data_filtered")

      val inputDataFrameDenormalised = spark.sql(
        """
                SELECT
                  """ + countryField +"""
                  ,MAX(CASE WHEN metricType = 'Cost' THEN metricThreshold ELSE null END) AS """ + localConstants.costThresholdField +"""
                  ,MAX(CASE WHEN metricType = 'Unit' THEN metricThreshold ELSE null END) AS """ + localConstants.unitThresholdField +"""
                  ,MAX(CASE WHEN metricType = 'Hour' THEN metricThreshold ELSE null END) AS """ + localConstants.hourThresholdField +"""
                  from input_data_filtered
                GROUP BY
                  """ + countryField
      )

      inputDataFrameDenormalised

    }

    /**
     * Validate Country Field Present in Dimension Or Not. Return True/False to the caller function
     *
     * @param spark         Spark Session
     * @param dimensionList List Type Dimension
     * @param countryField  Country Name
     */

    def validateCountryFieldExistInDimension(
                                              spark: SparkSession,
                                              dimensionList: List[String],
                                              countryField: String
                                            ): (Boolean) = {

      if (!dimensionList.contains(countryField)) {

        false

      }
      else {

        true

      }


    }

    /**
     * Aggregate a Dataframe Based on Dimension & Metric and return a resultant dataframe with aggregated metric to caller function
     *
     * @param inputDataFrame Input DataFrame
     * @param fieldList      Field List
     * @param dimensionList  Dimension List
     * @param metricList     Metric List
     * @param prevFieldList  Previous Field List
     */

    def aggregateCurrentPreviousDataFrame(
                                           inputDataFrame: DataFrame,
                                           fieldList: List[String],
                                           dimensionList: List[String],
                                           metricList: List[String],
                                           renamedFieldList: List[String]
                                         ): (DataFrame) = {

      ////////////////////////////////////////////////////////////////////////////////////////
      // Dynamically Aggregate Filtered Data Based On Dimension & Metric
      ////////////////////////////////////////////////////////////////////////////////////////
      val readFinalData = inputDataFrame.select(fieldList.map(x => col(x)): _*)
      val readAggregateData = readFinalData
        .groupBy(
          dimensionList
            .map(x => col(x)): _*)
        .agg(metricList.map(y => y -> "sum")
          .toMap
        )
        .toDF(renamedFieldList: _*)

      readAggregateData

    }

    /**
     * Join Current & Previous Period Dataframe based on generated Join Condition Based on Common Dimensions from both DataFrame and return the
     * resultant dataframe to caller function
     *
     * @param dimensionList                   Dimension List
     * @param readCurrentPeriodAggregateData  DataFrame
     * @param readPreviousPeriodAggregateData DataFrame
     */

    def joinCurrentPreviousDataFrame(
                                      dimensionList: List[String],
                                      readCurrentPeriodAggregateData: DataFrame,
                                      readPreviousPeriodAggregateData: DataFrame
                                    ): (DataFrame) = {

      //////////////////////////////////////////////////////////////////////////////////////////////////////////
      // Generate the joining condition based on the dimension fields from both current and previous period data
      //////////////////////////////////////////////////////////////////////////////////////////////////////////
      val joinExpression = dimensionList
        .map {
          case (x) => readCurrentPeriodAggregateData(localConstants.currentPeriodPrefix + x) === readPreviousPeriodAggregateData(localConstants.previousPeriodPrefix + x)
        }.reduce(_ && _)

      val generateCurrentPreviousCombineAggregateData = readCurrentPeriodAggregateData.join(readPreviousPeriodAggregateData, joinExpression, globalConstants.JoinTypeLeft)

      generateCurrentPreviousCombineAggregateData

    }

    /**
     * Generate The Percent Difference For Each Metric in The Previous, Current Period Joined DataSet and return a resultant DataSet to
     * caller function
     *
     * @param spark                                         SparkSession
     * @param metricList                                    Metric List
     * @param CurrentPreviousCombineAggregateDifferenceData Aggregated DataFrame
     */

    def currentPreviousCombineAggregatePercentData(
                                                    spark: SparkSession,
                                                    metricList: List[String],
                                                    currentPreviousCombineAggregateDifferenceData: DataFrame
                                                  ): (DataFrame) = {

      ///////////////////////////////////////////////////////////////////////////////////////////////////
      // Calculate the percent difference for metrics for previous and current month
      // There are 4 scenarios have been considered
      // 1. If Both Current and Previous Period has value, calculate difference percent using formula (current - previous)/previous *100
      // 2. If Either of Previous or Current Period is 0 / empty then difference percent is 100
      // 3. If Both Previous and Current Period is 0 then difference percent is 0
      ///////////////////////////////////////////////////////////////////////////////////////////////////

      var loopFieldGeneratedStatement = ""
      var finalFieldGeneratedStatement = ""

      for (i <- metricList.indices) {

        loopFieldGeneratedStatement = "CASE " + "\n" +
          "WHEN COALESCE(" + localConstants.currentPeriodPrefix + metricList(i) + "," + globalConstants.IntDefaultValue + ") = " + globalConstants.IntDefaultValue + " AND COALESCE(" + localConstants.previousPeriodPrefix + metricList(i) + "," + globalConstants.IntDefaultValue + ") = " + globalConstants.IntDefaultValue + " THEN " + globalConstants.IntDefaultValue + " " + "\n" +
          "WHEN COALESCE(" + localConstants.currentPeriodPrefix + metricList(i) + "," + globalConstants.IntDefaultValue + ") = " + globalConstants.IntDefaultValue + " AND COALESCE(" + localConstants.previousPeriodPrefix + metricList(i) + "," + globalConstants.IntDefaultValue + ") > " + globalConstants.IntDefaultValue + " THEN " + globalConstants.IntDefaultHundred + " " + "\n" +
          "WHEN COALESCE(" + localConstants.previousPeriodPrefix + metricList(i) + "," + globalConstants.IntDefaultValue + ") = " + globalConstants.IntDefaultValue + " AND COALESCE(" + localConstants.currentPeriodPrefix + metricList(i) + "," + globalConstants.IntDefaultValue + ") > " + globalConstants.IntDefaultValue + " THEN " + globalConstants.IntDefaultHundred + " " + "\n" +
          "ELSE ROUND(((COALESCE(" + localConstants.currentPeriodPrefix + metricList(i) + "," + globalConstants.IntDefaultValue + ")-COALESCE(" + localConstants.previousPeriodPrefix + metricList(i) + ",0))/COALESCE(" + localConstants.previousPeriodPrefix + metricList(i) + "," + globalConstants.IntDefaultValue + "))*" + globalConstants.IntDefaultHundred + ",2) END AS " + localConstants.diffPercentCurrentPreviousPrefix + metricList(i)

        if (i == globalConstants.IntDefaultValue) {
          finalFieldGeneratedStatement = loopFieldGeneratedStatement;
        }
        else {
          finalFieldGeneratedStatement = finalFieldGeneratedStatement + "\n" + "," + "\n" + loopFieldGeneratedStatement;
        }

      }

      currentPreviousCombineAggregateDifferenceData.createOrReplaceTempView("current_previous_combine_aggregate_diff_data")
      val generateCurrentPreviousCombineAggregatePercentData = spark.sql("select *, " + finalFieldGeneratedStatement + " from current_previous_combine_aggregate_diff_data")

      generateCurrentPreviousCombineAggregatePercentData

    }

    /**
     * Take Input Current Month DataSet & Previous Month DataSet, Threshold DataSet and Generate Threshold Breached DataSet and return to
     * caller function
     *
     * @param spark                        SparkSession
     * @param metricList                   Metric List
     * @param dimensionList                dimensionList
     * @param MetricTypeList               MetricTypeList
     * @param readCurrentPeriodFilterData  Current Period Filtered Data
     * @param readPreviousPeriodFilterData Previous Period Filtered Data
     * @param countryField                 countryField
     * @param thresholdTableData           thresholdTableData
     * @param defaultThresholdValue        defaultThresholdValue
     */

    def calculateThresholdBreachedDataSet(
                                           spark: SparkSession,
                                           metricList: List[String],
                                           dimensionList: List[String],
                                           MetricTypeList: List[String],
                                           readCurrentPeriodFilterData: DataFrame,
                                           readPreviousPeriodFilterData: DataFrame,
                                           countryField: String,
                                           thresholdTableData: DataFrame,
                                           defaultThresholdValue: Int
                                         ): DataFrame = {

      // Generate a HashMap based on Metric Field Name vs Metric Field Type
      val mapMetricToMetricType = (metricList zip MetricTypeList).toMap

      // Concatenate List with List of Dimension + Metric Fields to be retrieved from Source data
      val fieldList = dimensionList ++ metricList

      ////////////////////////////////////////////////////////////////////////////////////////////////////////
      // Create Individual Lists having Field names generated with prefixes to use later for data manipulation
      // eg : Current_Period_<FieldName>, Previous_Period_<FieldName>
      ////////////////////////////////////////////////////////////////////////////////////////////////////////

      val currDimensionList = dimensionList.map(x => localConstants.currentPeriodPrefix + x)
      val currMetricList = metricList.map(x => localConstants.currentPeriodPrefix + x)
      val currFieldList = currDimensionList ++ currMetricList

      val prevDimensionList = dimensionList.map(x => localConstants.previousPeriodPrefix + x)
      val prevMetricList = metricList.map(x => localConstants.previousPeriodPrefix + x)
      val prevFieldList = prevDimensionList ++ prevMetricList

      ////////////////////////////////////////////////////////////////////////////////////////
      // Dynamically Aggregate Filtered Data Based On Dimension & Metric
      // Aggregation is done for both Current & Previous Periods
      // Using Common Method aggregateCurrentPreviousDataFrame in this object
      ////////////////////////////////////////////////////////////////////////////////////////

      val readCurrentPeriodAggregateData = metricPercentThresholdVarianceModules.aggregateCurrentPreviousDataFrame(readCurrentPeriodFilterData, fieldList, dimensionList, metricList, currFieldList)
      val readPreviousPeriodAggregateData = metricPercentThresholdVarianceModules.aggregateCurrentPreviousDataFrame(readPreviousPeriodFilterData, fieldList, dimensionList, metricList, prevFieldList)

      //////////////////////////////////////////////////////////////////////////////////////////////////////////
      // Generate the joining condition based on the dimension fields from both current and previous period data
      // Using Method joinCurrentPreviousDataFrame in this object to perform this operation
      //////////////////////////////////////////////////////////////////////////////////////////////////////////
      val generateCurrentPreviousCombineAggregateData = metricPercentThresholdVarianceModules.joinCurrentPreviousDataFrame(dimensionList, readCurrentPeriodAggregateData, readPreviousPeriodAggregateData)

      ////////////////////////////////////////////////////////////////////////////////////////
      // Calculate the difference for metrics for previous and current period
      ////////////////////////////////////////////////////////////////////////////////////////
      val selectDimensionList = dimensionList
        .map(
          x => coalesce(col(localConstants.currentPeriodPrefix + x), col(localConstants.previousPeriodPrefix + x)).as(x)
        )

      val selectMetricList = (currMetricList ++ prevMetricList)
        .map(
          y => coalesce(col(y), lit(globalConstants.IntDefaultValue)).as(y)
        )

      val calculatedDiffList = metricList
        .map(
          z => (
            coalesce(col(localConstants.currentPeriodPrefix + z), lit(globalConstants.IntDefaultValue))
              -
              coalesce(col(localConstants.previousPeriodPrefix + z), lit(globalConstants.IntDefaultValue))
            ).as(localConstants.diffCurrentPreviousPrefix + z)
        )

      val selectList = selectDimensionList ++ selectMetricList ++ calculatedDiffList
      val generateCurrentPreviousCombineAggregateDifferenceData = generateCurrentPreviousCombineAggregateData.select(selectList: _*)

      ///////////////////////////////////////////////////////////////////////////////////////////////////
      // Calculate the percent difference for metrics for previous and current month
      // There are 4 scenarios have been considered
      // 1. If Both Current and Previous Period has value, calculate difference percent using formula (current - previous)/previous *100
      // 2. If Either of Previous or Current Period is 0 / empty then difference percent is 100
      // 3. If Both Previous and Current Period is 0 then difference percent is 0
      // Using Method currentPreviousCombineAggregatePercentData in this object to perform this operation
      ///////////////////////////////////////////////////////////////////////////////////////////////////

      val generateCurrentPreviousCombineAggregatePercentData = metricPercentThresholdVarianceModules.currentPreviousCombineAggregatePercentData(spark, metricList, generateCurrentPreviousCombineAggregateDifferenceData)

      ///////////////////////////////////////////////////////////////////////////////////////////////////
      // Create a Dataframe with only Dimension List and Percent Difference Fields For Threshold Analysis
      ///////////////////////////////////////////////////////////////////////////////////////////////////
      val FinalDimensionList = dimensionList.map(z => col(z).as(z))
      val DiffPercentList = metricList.map(z => col(localConstants.diffPercentCurrentPreviousPrefix + z))
      val selectiveOutputList = FinalDimensionList ++ DiffPercentList

      val generateCurrentPreviousCombineAggregatePercentSelectiveData = generateCurrentPreviousCombineAggregatePercentData.select(selectiveOutputList: _*)

      ////////////////////////////////////////////////////////////////////////////////////////
      // Create A Dataframe to Combine Calculated Percent Difference and Threshold
      // Drop the Country Field from Configuration from Resultant dataset
      // Fill default threshold values which are not configured in configuration
      ////////////////////////////////////////////////////////////////////////////////////////

      val generateCurrentPreviousCombineAggregatePercentSelectiveWithThresholdData = generateCurrentPreviousCombineAggregatePercentSelectiveData
        .join(
          thresholdTableData,
          generateCurrentPreviousCombineAggregatePercentSelectiveData(countryField) === thresholdTableData(countryField),
          globalConstants.JoinTypeLeft
        )
        .drop(thresholdTableData(countryField))
        .na.fill(defaultThresholdValue, Array(localConstants.costThresholdField))
        .na.fill(defaultThresholdValue, Array(localConstants.unitThresholdField))
        .na.fill(defaultThresholdValue, Array(localConstants.hourThresholdField))

      ////////////////////////////////////////////////////////////////////////////////////////
      // Generate the Dynamic Filter For each Metric Column Based On it's Metric Type.
      // So that appropriate filter is generated to generate only threshold breached records
      ////////////////////////////////////////////////////////////////////////////////////////
      val percentMetricToMetricTypeFilter = metricList
        .map(
          p => abs(col(localConstants.diffPercentCurrentPreviousPrefix + p)) > col(mapMetricToMetricType(p) + "_THRESHOLD")
        )
        .reduce(_ || _)

      ///////////////////////////////////////////////////////////////////////////////////////////////
      // Create a dataframe with the aggregated records having percentage higher than threshold value
      ///////////////////////////////////////////////////////////////////////////////////////////////
      val generateFinalThresholdBreachedData = generateCurrentPreviousCombineAggregatePercentSelectiveWithThresholdData.filter(percentMetricToMetricTypeFilter)

      generateFinalThresholdBreachedData
        .orderBy(dimensionList
        .map(x => col(x)): _*)

    }

}

class metricPercentThresholdVarianceModules(spark: SparkSession,
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
                                            toEmail: String
                                           ){


  ///////////////////////////////////////////////////////////////////////////////////////////////////
  // The below Two Methods
  //
  // 1) mailBodyGeneration
  // 2) generateMessage
  //
  // Are very Specific and require maximum parameters of Threshold Flow, Thus encapsulated
  // under a class to avoid individual method invocation with identical parameters rather class
  // is instanced with input parameters and methods are invoked with required parameters
  ///////////////////////////////////////////////////////////////////////////////////////////////////


  ///////////////////////////////////////////////////////////////////////////////////////////////////
  // This Method Takes 3 inputs processName,resultHeader,resultDetails and generate Mail Body
  // For Different Processes With Appropriate Email Header, Email Message and Email Footer
  // Email Message Is a templated message based on a template defined in generateMessage
  // with all input parameters to generate a meaningful & user friendly message
  ///////////////////////////////////////////////////////////////////////////////////////////////////

  def mailBodyGeneration(processName: String,
                         resultHeader: String = "",
                         resultDetails: String = ""
                        ): String ={

    var mailBody:String=""

    processName match {

      case localConstants.`processThresholdVariancePipelineModuleValidation` => mailBody=ValidationUtils.generateMailContent(
        localConstants.mailHeader,
        generateMessage(
          localConstants.exceptionEncountered,
          "Input Pipeline " + pipelineName + " and Module " + moduleName + " are missing in Configuration"
        ),
        localConstants.mailFooter
      )

      case localConstants.`processThresholdVariancePipelineModuleMetricValidation` => mailBody=ValidationUtils.generateMailContent(
        localConstants.mailHeader,
        generateMessage(
          localConstants.exceptionEncountered,
          "Input Pipeline " + pipelineName + " and Module " + moduleName + " and MetricType " + metricType + " combination are missing in Configuration"),
        localConstants.mailFooter
      )

      case localConstants.`processThresholdVarianceCountryNameInDimensionValidation` => mailBody = ValidationUtils.generateMailContent(
        localConstants.mailHeader,
        generateMessage(
          localConstants.exceptionEncountered,
          "Input Country Field " + countryField + " is missing from " + dimension),
        localConstants.mailFooter
      )

      case localConstants.`processThresholdVarianceCurrentPeriodDataSufficiencyValidation` => mailBody = ValidationUtils.generateMailContent(
        localConstants.mailHeader,
        generateMessage(
          localConstants.exceptionEncountered,
          "Exception Message - No Data is returned for Current Period Post Applying Filter [" + filterCondition + "]"),
        localConstants.mailFooter
      )

      case localConstants.`processThresholdVariancePreviousPeriodDataSufficiencyValidation` => mailBody = ValidationUtils.generateMailContent(
        localConstants.mailHeader,
        generateMessage(
          localConstants.exceptionEncountered,
          "Exception Message - No Data is returned for Previous Period Post Applying Filter [" + filterCondition + "]"),
        localConstants.mailFooter
      )

      case localConstants.`processThresholdVarianceThresholdNonBreachedDataSetValidation` => mailBody = ValidationUtils.generateMailContent(
        localConstants.mailHeader,
        generateMessage(
          localConstants.successfulValidation,
          localConstants.successfulValidationMessage),
        localConstants.mailFooter
      )

      case localConstants.`processThresholdVarianceThresholdBreachedDataSetValidation` => mailBody = ValidationUtils.generateMailContent(
        localConstants.mailHeader,
        generateMessage(
          localConstants.unSuccessfulValidation,
          resultHeader+"\n"+resultDetails
          ),
        localConstants.mailFooter
      )

      case localConstants.`processUnhandledException` => mailBody = ValidationUtils.generateMailContent(
        localConstants.mailHeader,
        generateMessage(
          localConstants.unhandledExceptionEncountered,
          resultDetails
        ),
        localConstants.mailFooter
      )

    }

    mailBody

  }

  def generateMessage(messageHeader: String,
                      messageDetails: String): String = {

    var finalMessage: String = ""
    finalMessage = "\n" + " **** [" + messageHeader + "] **** : " + "\n" +
      "\n" + "--Input Pipeline **** [" + pipelineName + "]" +
      "\n" + "--Input Module **** [" + moduleName + "]" +
      "\n" + "--Input Dimension List **** [" + dimension + "]" +
      "\n" + "--Input Metric List **** [" + metric + "]" +
      "\n" + "--Input Metric Type List **** [" + metricType + "]" +
      "\n" + "--Input Country Field **** [" + countryField + "]" +
      "\n" + "--Input Filter Condition **** [" + filterCondition + "]" +
      "\n" + "--Input Default Threshold **** [" + defaultThresholdValue + "]" +
      "\n" + "--Input Current Period Path **** [" + currentPeriodPath + "]" +
      "\n" + "--Input Previous Period Path **** [" + previousPeriodPath + "]" +
      "\n" + "--Input Current Period **** [" + currentPeriod + "]" +
      "\n" + "--Input Previous Period **** [" + previousPeriod + "]" +
      "\n" + "--Input DataSet Schema **** [" + datasetSchema + "]" +
      "\n" + "\n" + messageDetails

    finalMessage

  }

}