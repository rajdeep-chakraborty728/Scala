package com.amazon.nova.dataservices.validation.utils

import com.amazon.nova.dataservices.allocations.cognos.transform.CognosSchema
import com.amazon.nova.dataservices.allocations.fam.famcognos.FamCognosSchema
import com.amazon.nova.dataservices.allocations.fam.famfyp.FamFypSchema
import com.amazon.nova.dataservices.allocations.fam.famfyp.FamFypSchemaIntermediate
import com.amazon.nova.dataservices.allocations.fam.famplanning.FamPlanningSchema
import com.amazon.nova.dataservices.allocations.gam.gamasin.GamAsinSchema
import com.amazon.nova.dataservices.allocations.gam.gamasin.GamAsinSchemaIntermediate
import com.amazon.nova.dataservices.allocations.gam.gamvcpu.GamVcpuSchema
import com.amazon.nova.dataservices.allocations.gam.gamvcpu.GamVcpuSchemaIntermediate
import com.amazon.nova.dataservices.utils.DatasetObject

// A Static Map To Define All Possible DataSets Required For PeriodOnPeriod Validation
// Key will be passed as String As Input to This Map and accordingly DataSet will be returned to Utils class For S3 File Read
// For Any Additional DataSet Onboard in validation, Key-Value Pair to be added in the Map and accordingly package to be imported

object DataSetStringToDataSetTypeMap {

  val mapStringToDataSet: Map[String, DatasetObject] = Map(

    // GAM VCPU DataSets
    "GamVcpuSchema.DataSetGamGsfFinalTransform" -> GamVcpuSchema.DataSetGamGsfFinalTransform,
    "GamVcpuSchema.DataSetGamVcpuGsfCognosExtract" -> GamVcpuSchema.DataSetGamVcpuGsfCognosExtract,
    "GamVcpuSchema.DataSetGsfVcpuFinalTransform" -> GamVcpuSchema.DataSetGsfVcpuFinalTransform,
    "GamVcpuSchema.DataSetGamVcpuGcfCognosExtract" -> GamVcpuSchema.DataSetGamVcpuGcfCognosExtract,
    "GamVcpuSchema.DatasetActualOfaRdsExtract" -> GamVcpuSchema.DatasetActualOfaRdsExtract,
    "GamVcpuSchema.DataSetGcfFinalConsolidatedCostTransform" -> GamVcpuSchema.DataSetGcfFinalConsolidatedCostTransform
  )

}
