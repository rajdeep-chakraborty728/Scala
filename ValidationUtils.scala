package com.amazon.nova.dataservices.validation.utils

import com.amazon.nova.dataservices.utils.EmailClientUtils.sendMail
import com.amazon.nova.dataservices.utils.SparkSessionUtils.isLocaLSparkSession
import org.apache.spark.sql.functions.{col, collect_list, concat_ws, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.amazon.nova.dataservices.utils.{Constants => globalConstants}
import com.amazon.nova.dataservices.validation.utils.{Constants => localConstants}

// Purpose of this utility is to make this functions reusable to any other business process

object ValidationUtils {

  /**
   * Validate If DataFrame Empty Or Not. If Empty Return True Else Return False Accordingly return true/false to the caller function
   *
   * @param spark          Spark Session
   * @param inputDataFrame Input DataFrame
   */

  def validateEmptyDataFrame(
                              spark: SparkSession,
                              inputDataFrame: DataFrame
                            ): (Boolean) = {

    if (inputDataFrame.count == globalConstants.IntDefaultValue) {

      true

    }
    else {

      false

    }

  }

  /**
   * convert a Input dataFrame to a string function and return to caller function
   *
   * @param inputDataFrame input dataFrame to be converted
   * @param separator      field separator
   */

  def convertDataFrameToString(inputDataFrame: DataFrame, separator: String): String = {

    val convertedDataFrameToString = inputDataFrame
      .withColumn("Id", lit(globalConstants.IntDefaulOnetValue))
      .withColumn(
        "Concatenated_Field",
        concat_ws(separator, inputDataFrame.columns.map(m => col(m)): _*)
      )
      .select(col("Id"), col("Concatenated_Field"))
      .groupBy(col("Id"))
      .agg(
        collect_list(col("Concatenated_Field"))
          .alias("Concatenated_Row_Column")
      )
      .withColumn("Concatenated_Output", concat_ws("\n", col("Concatenated_Row_Column")))
      .select("Concatenated_Output").collect.mkString(" ")
      .replaceAll("\\[", "")
      .replaceAll("\\]", "")

    convertedDataFrameToString

  }

  /**
   * Filter A DataFrame Based On a valid filter condition and return the filtered dataframe to the caller function
   *
   * @param spark          Spark Session
   * @param inputDataFrame input DataFrame to be filtered
   * @param separator      A valid filter condition
   */
  def filterDataFrame(spark: SparkSession, readRawData: DataFrame, filterCondition: String): DataFrame = {

    readRawData.createOrReplaceTempView("raw_data")
    val readFilterDataFrame = spark.sql("select * from raw_data where " + filterCondition)

    readFilterDataFrame

  }

  /**
   * Generate a Mail Body by passing Header, Body, Footer and return a concatenated string to the caller function
   *
   * @param mailheader session
   * @param mailbody   input DataFrame to be filtered
   * @param mailFooter a valid filter condition
   */

  def generateMailContent(
                        mailHeader: String,
                        mailBody: String,
                        mailFooter: String
                      ): String = {

    val emailBody = mailHeader + mailBody + mailFooter

    emailBody

  }

  /**
   * Invoke Existing sendEmail utility for emr sessions
   *
   * @param spark       Spark Session
   * @param fromEmail   From Email Address
   * @param toEmail     To Email Address
   * @param mailSubject Email Subject
   * @param mailBody    Email Body
   * @param mailType    Email Type
   */

  def callSendEmail(
                     spark: SparkSession,
                     fromEmail: String,
                     toEmail: String,
                     mailSubject: String,
                     mailBody: String,
                     mailType: String
                   ): Unit = {

    // Invoke Email Utility If Invoked from EMR
    if (!isLocaLSparkSession(spark)) sendMail(fromEmail, toEmail, mailSubject, mailBody, mailType)

  }

  /**
   * Convert a String To Scala List & Return to Caller Function
   *
   * @param inputString    Input String
   * @param inputSeparator Input Separator
   */

  def stringToListConversion(
                              inputString: String,
                              inputSeparator: String = localConstants.commaSeparator
                            ): List[String] = {

    val OutputList: List[String] = inputString.split(inputSeparator).map(_.trim).toList

    OutputList

  }

}
