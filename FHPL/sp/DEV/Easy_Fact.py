# Databricks notebook source
# MAGIC %run /Workspace/Users/basheerahmed.k@diggibyte.com/FHPL/sp/SP/sp_review

# COMMAND ----------

# DBTITLE 1,Module Import
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, IntegerType

# COMMAND ----------

# claims_df = spark.table("fhpl.claims").alias('c')
# claims_details_df = spark.table("fhpl.claimsdetails").alias('cd')
# member_policy_df = spark.table("fhpl.memberpolicy").alias('mp')
# provider_df = spark.table("fhpl.provider").alias('p')
# claim_request_type_df = spark.table("fhpl.claimrequesttype").alias('crt')
# claim_stage_df = spark.table("fhpl.claimstage").alias('cs')
# mst_facility_df = spark.table("fhpl.mst_facility").alias('mf')
# claim_action_items_df = spark.table("fhpl.claimactionitems").alias('ca')
# mst_property_values_df = spark.table("fhpl.mst_propertyvalues").alias('mpv')
# intimation_df = spark.table("fhpl.intimation").alias('i')
# claim_service_type_df = spark.table('fhpl.claimservicetype').alias("st")
# mst_issuingauthority_df = spark.table("fhpl.mst_issuingauthority").alias('IA')
# claimscoding_df = spark.table("fhpl.claimscoding").alias("CC")
# temprdnicdcode_df = spark.table("fhpl.temp_temprdnicdcode").alias('L')
# icd10_table_df = spark.table("fhpl.icd10")
# mst_state_df = spark.table("fhpl.mst_state")
# mst_gender_df = spark.table("fhpl.mst_gender")
# mst_relationship_df = spark.table("fhpl.mst_relationship")
# policy_df = spark.table("fhpl.policy").alias('pol')
# mst_corporate_df = spark.table("fhpl.mst_corporate").alias('mc')
# mst_payer_df = spark.table("fhpl.mst_payer")
# buffer_utilization_df = spark.table("fhpl.bufferutilization")
# tertiary_utilization_df = spark.table("fhpl.tertiaryutilization")
# member_contacts_df = spark.table("fhpl.membercontacts").alias("mc")
# provider_mou_df = spark.table("fhpl.providermou").alias("pmou")
# member_si_df = spark.table("fhpl.membersi").alias("msi")
# bpsuminsured_df = spark.table("fhpl.bpsuminsured").alias("bpi")
# claim_rejection_reasons_df = spark.table("fhpl.claimrejectionreasons").alias("crr")
# mst_rejection_reasons_df = spark.table("fhpl.mst_rejectionreasons").alias("mrr")
# claim_deduction_details_df = spark.table("fhpl.claimdeductiondetails")
# mst_deduction_reasons_df = spark.table("fhpl.mst_deductionreasons")

# bpsiconditions_df = spark.table("fhpl.bpsiconditions").fillna("").fillna(0).fillna(0.0)
# bp_service_config_details_df = spark.table("fhpl.bpserviceconfigdetails")
# claims_ir_reasons_df = spark.table("fhpl.claimsirreasons")
# mst_irdocuments_df = spark.table("fhpl.mst_irdocuments")
# claim_utilized_amount_df = spark.table("fhpl.claimutilizedamount")
# claims_ir_reasons_df = spark.table("fhpl.claimsirreasons")
# mst_services_df = spark.table("fhpl.mst_services")

# COMMAND ----------

# DBTITLE 1,load tables
claims_df = spark.table("fhpl.claims").alias('c')
claims_details_df = spark.table("fhpl.claimsdetails").alias('cd')
member_policy_df = spark.table("fhpl.memberpolicy").alias('mp')
provider_df = spark.table("fhpl.provider").alias('p')
claim_request_type_df = spark.table("fhpl.claimrequesttype").alias('crt')
claim_stage_df = spark.table("fhpl.claimstage").alias('cs')
mst_facility_df = spark.table("fhpl.mst_facility").alias('mf')
claim_action_items_df = spark.table("fhpl.claimactionitems").alias('ca')
mst_property_values_df = spark.table("fhpl.mst_propertyvalues").alias('mpv')
intimation_df = spark.table("fhpl.intimation").alias('i')
claim_service_type_df = spark.table('fhpl.claimservicetype').alias("st")
mst_issuingauthority_df = spark.table("fhpl.mst_issuingauthority").alias('IA')
claimscoding_df = spark.table("fhpl.claimscoding").alias("CC")
temprdnicdcode_df = spark.table("fhpl.temp_temprdnicdcode").alias('L')
icd10_table_df = spark.table("fhpl.icd10")
mst_state_df = spark.table("fhpl.mst_state")
mst_gender_df = spark.table("fhpl.mst_gender")
mst_relationship_df = spark.table("fhpl.mst_relationship")
policy_df = spark.table("fhpl.policy").alias('pol')
mst_corporate_df = spark.table("fhpl.mst_corporate").alias('mc')
mst_payer_df = spark.table("fhpl.mst_payer")
buffer_utilization_df = spark.table("fhpl.bufferutilization")
tertiary_utilization_df = spark.table("fhpl.tertiaryutilization")
member_contacts_df = spark.table("fhpl.membercontacts").alias("mc")
provider_mou_df = spark.table("fhpl.providermou").alias("pmou")
member_si_df = spark.table("fhpl.membersi").alias("msi")
bpsuminsured_df = spark.table("fhpl.bpsuminsured").alias("bpi")
claim_rejection_reasons_df = spark.table("fhpl.claimrejectionreasons").alias("crr")
mst_rejection_reasons_df = spark.table("fhpl.mst_rejectionreasons").alias("mrr")
claim_deduction_details_df = spark.table("fhpl.claimdeductiondetails")
mst_deduction_reasons_df = spark.table("fhpl.mst_deductionreasons")

bpsiconditions_df = spark.table("fhpl.bpsiconditions").fillna("").fillna(0).fillna(0.0)
bp_service_config_details_df = spark.table("fhpl.bpserviceconfigdetails")
claims_ir_reasons_df = spark.table("fhpl.claimsirreasons")
mst_irdocuments_df = spark.table("fhpl.mst_irdocuments")
claim_utilized_amount_df = spark.table("fhpl.claimutilizedamount")
claims_ir_reasons_df = spark.table("fhpl.claimsirreasons")
mst_services_df = spark.table("fhpl.mst_services")

# COMMAND ----------

# DBTITLE 1,intimation
initial_df = claims_df.join(claims_details_df.alias('cd1'), claims_df["Id"] == col("cd1.ClaimID"), "inner") \
    .join(member_policy_df, claims_df["MemberPolicyID"] == member_policy_df["ID"], "inner") \
    .join(provider_df, claims_df["ProviderID"] == provider_df["Id"], "inner") \
    .join(intimation_df, claims_df["Id"] == intimation_df["Claimid"], "leftouter") \
    .join(mst_issuingauthority_df.alias("IA"),claims_df["IssueID"] == col("IA.ID"),"inner")

claims_details_with_requesttype = claims_details_df.alias('cd2').join(claim_request_type_df, 
                                                                     col("cd2.Requesttypeid") == col("crt.id"), 
                                                                     "inner") \
                                                   .select(col("cd2.*"), 
                                                           col("crt.name").alias("Requesttype"))

claims_details_with_stage = claims_details_with_requesttype.join(claim_stage_df.alias('cs1'), 
                                                                 col("cd2.stageid") == col("cs1.id"), 
                                                                 "inner") \
                                                           .select(claims_details_with_requesttype['*'], 
                                                                   col("cs1.name").alias("CurrentClaimStatus"))

claims_details_with_facility = claims_details_with_stage.join(mst_facility_df.alias('mf1'), 
                                                              col("cd2.ApprovedFacilityID") == col("mf1.ID"), 
                                                              "inner") \
                                                        .select(claims_details_with_stage['*'], 
                                                                col("mf1.Level2").alias("ClassOfAccommodation"))

main_claim_df = claims_details_with_facility.alias('cd3').join(claim_action_items_df.alias('ca'), 
                                                              (col('cd3.claimid') == col('ca.claimid')) & 
                                                              (col('cd3.slno') == col('ca.slno')) & 
                                                              (col('ca.deleted') == 0), 
                                                              "inner") \
    .join(claim_stage_df.alias('cs2'), 
          col('ca.ClaimStageID') == col('cs2.id'), 
          "inner") \
    .join(claim_stage_df.alias('cs3'), 
          col('cd3.StageID') == col('cs3.id'), 
          "inner") \
    .join(mst_property_values_df.alias('mpv'), 
          (col('ca.ReasonIDs_P') == col('mpv.ID')) & (col('mpv.Deleted') == 0), 
          "inner") \
    .withColumn(
        'MainClaimStatus',
        when(col('cd3.StageID') == 27, lit('Settled'))
        .when((col('cd3.StageID') == 23) & col('ca.ID').isNotNull(), lit('For Repudiated'))
        .when(col('cd3.StageID') == 17, 
              concat(when(col('cs2.name').isNull(), col('cs3.name')).otherwise(col('cs2.name')), 
                     lit(' - '), 
                     col('mpv.Name')))
        .when(col('cd3.StageID') == 38, 
              when(col('cs2.name').isNull(), col('cs3.name')).otherwise(col('cs2.name')))
        .otherwise(when(col('cs2.name').isNull(), col('cs3.name')).otherwise(col('cs2.name')))
    ) \
    .withColumn(
        'MainClaimType',
        when(col('cd3.ClaimTypeID') == 1, lit('Cashless'))
        .when(col('cd3.ClaimTypeID') == 2, lit('Reimbursement'))
    )

joined_df = initial_df.join(main_claim_df, ["claimid", "slno"], "inner")

direct_selected = joined_df.select(
    col("cd1.claimid"),
    col("cd1.slno"),
    col("mp.MemberName").alias("Membername"),
    col("mp.DOB"),
    col("mp.Age").alias("Yrs"),
    col("mp.EmployeeID").alias("employeeid"),
    col("mp.MemberCommencingDate").alias("PolicyCommencingDate"),
    col("cd1.ReceivedDate").alias("Claimreceiveddate"),
    col("c.DateOfDischarge").alias("DisDate"),
    col("cd1.Diagnosis"),
    col("cd1.claimamount"),
    col("cd1.BillAmount").alias("billedamount"),
    col("cd1.SettledAmount").alias("settledamt"),
    col("cd1.TDSAmount"),
    col("cd1.NetAmount").alias("NetAmountPaid"),
    col("cd1.CoPayment"),
    col("cd1.BankTransactionNo").alias("Chequeno"),
    col("cd1.chequedate"),
    col("cd1.SettledDate"),
    col("c.roomdays"),
    col("c.icudays"),
    col("cd1.Notes"),
    col("cd1.CreatedDateTime").alias("claimcreateddatetime"),  # Use the renamed column
    col("cd1.InsurerClaimID"),
    col("mp.DOJ").alias("DateOfJoining"),
    col("p.isgipsappn").alias("GIPSAHospital"),
    col("mp.NIDB_RemovedDate"),
    col("Requesttype"),
    when(col("cd1.ClaimTypeID") == 1, "PP").when(col("cd1.ClaimTypeID") == 2, "MR").alias("ClaimType"),
    when(~col("cd1.Stageid").isin([4, 5, 23]), col("cd1.BillAmount") - col("cd1.PayableAmount")).alias("Disallowed_Amount"),
    when(col("cd1.MOUDiscount") > col("cd1.DiscountByHospital"), col("cd1.MOUDiscount")).otherwise(col("cd1.DiscountByHospital")).alias("Discountamount"),
    col("CurrentClaimStatus"),
    col("cd1.payeename").alias("Payee_name"),
    col("ClassOfAccommodation"),
    col("MainClaimStatus"),
    col("MainClaimType"),
    col("i.ID").alias("IntimationID"),
    col("i.IntimationDate").alias("IntimationDate"),
    col("c.ProbableDOA"), # need to drop at final stage/df
    col("c.DateOfAdmission"), #
    col("c.ServiceTypeID").alias("ServiceType"),
    col("c.ServiceSubTypeID").alias("ServiceSubType"),
    col("c.DateOfDischarge"), #
    col("c.IssueID"), #
    col("IA.Name").alias("insurancename"),
    col("c.memberpolicyid")
)

# COMMAND ----------

direct_selected_1 = direct_selected.withColumn("Admdate",coalesce(col("ProbableDOA"), col("DateOfAdmission"))).withColumn("TotalStay",datediff(col("DateOfDischarge"), col("DateOfAdmission")) + 1).drop("ProbableDOA", "DateOfAdmission", "DateOfDischarge", "IssueID")

# COMMAND ----------

# display(direct_selected_1.fillna("").fillna(0).filter(col("claimid")==24070402680).distinct())

# COMMAND ----------

# DBTITLE 1,ClaimsCoding
window_spec = Window.partitionBy("CC.ClaimID", "CC.Slno").orderBy(desc("CC.Createddatetime"))

joined_df = claims_details_df.alias('CD') \
    .join(claimscoding_df.alias('CC'), (col('CD.ClaimID') == col('CC.ClaimID')) & (col('CD.Slno') == col('CC.Slno')) & (col('CC.DELETED') == 0), 'left') \
    .join(temprdnicdcode_df.alias('L'), (col('CC.ClaimID') == col('L.ClaimID')) & (col('CC.Slno') == col('L.slno')), 'left')\
        .select("L.Level1DiseaseName","L.Level2DiseaseName","L.Level3DiseaseName","CC.ClaimID","CC.Slno","CC.Createddatetime","CC.TreatementTypeID_19", "CC.ICDcode","L.Level3DiseaseCode")


direct_col_with_claimscoding_df = direct_selected_1.join(joined_df,(direct_selected_1["claimid"] == "cc.claimid") & (direct_selected_1["slno"] == col("CC.Slno")),"left")\
    .withColumn('icdcodeFirstLevel', first(col('L.Level1DiseaseName')).over(window_spec))\
        .withColumn('icdcodeSecondLevel', first(col('L.Level2DiseaseName')).over(window_spec))\
            .withColumn('icdcodeThirdLevel', first(col('L.Level3DiseaseName')).over(window_spec))\
                .select(direct_selected_1['*'], "icdcodeFirstLevel", "icdcodeSecondLevel", "icdcodeThirdLevel","CC.TreatementTypeID_19", "CC.ICDcode", "L.Level3DiseaseCode")

# COMMAND ----------

# DBTITLE 1,ClaimsCoding
joined_with_treatmenttype_df = direct_col_with_claimscoding_df \
    .join(mst_property_values_df.alias('MPV'), col('CC.TreatementTypeID_19') == col('MPV.ID'), 'left') \
    .select(direct_col_with_claimscoding_df['*'],col("MPV.NAME").alias("TreatmentType"))

direct_claimscoding_df = joined_with_treatmenttype_df \
      .join(icd10_table_df.alias('ICD'), col('CC.ICDcode') == col('ICD.ID'), 'left') \
    .select(joined_with_treatmenttype_df["*"], col("L.Level3DiseaseCode").alias("icdThirdLevelCode")).drop("TreatementTypeID_19","ICDCode")

# COMMAND ----------

# DBTITLE 1,claimServiceDetails

claims_df = spark.table("fhpl.claims").alias('c')
claims_details_df = spark.table("fhpl.claimsdetails").alias('cd')
claims_service_details_df = spark.table("fhpl.claimsservicedetails").alias('csd')

claims_df1 = claims_df.withColumnRenamed("Id", "c_Id").withColumnRenamed("Slno", "c_Slno")
claims_details_df = claims_details_df.withColumnRenamed("Id", "cd_Id").withColumnRenamed("Slno", "cd_Slno")
claims_service_details_df = claims_service_details_df.withColumnRenamed("Id", "csd_Id").withColumnRenamed("Slno", "csd_Slno")

service_ids = [3]

joined_df = claims_service_details_df.join(
    claims_details_df,
    (claims_service_details_df["Claimid"] == claims_details_df["Claimid"]) &
    (claims_service_details_df["csd_Slno"] == claims_details_df["cd_Slno"]),
    how='inner'
).join(
    claims_df1,
    claims_df1["c_Id"] == claims_service_details_df["Claimid"],
    how='inner'
)

def calculate_sum(service_ids, amount_column, condition):
    return F.sum(
        F.when(
            condition & F.col("csd.serviceid").isin(service_ids), 
            F.coalesce(F.col(amount_column), F.lit(0))
        ).otherwise(F.lit(0))
    )

group_columns = ['c_Id']

csd_fact = joined_df.groupBy(group_columns).agg(
    calculate_sum(
        service_ids=[3], 
        amount_column="csd.BillAmount", 
        condition=(
            (F.col("c.servicetypeid") != 2) & 
            (~F.col("c.servicesubtypeid").isin([16, 3, 13])) &
            (F.coalesce(F.col("c.ismaternity"), F.lit(0)) != 1)
        )
    ).alias("RoomRentClaimed"),
    
    calculate_sum(
        service_ids=[2], 
        amount_column="csd.BillAmount", 
        condition=(
            (F.col("c.servicetypeid") != 2) & 
            (~F.col("c.servicesubtypeid").isin([16, 3, 13]))
        )
    ).alias("ICUClaimed"),
    
    calculate_sum(
        service_ids=[2], 
        amount_column="csd.SanctionedAmount", 
        condition=(
            (F.col("c.servicetypeid") != 2) & 
            (~F.col("c.servicesubtypeid").isin([16, 3, 13])) &
            (F.coalesce(F.col("c.ismaternity"), F.lit(0)) != 1)
        )
    ).alias("ICURelated"),
    
    calculate_sum(
        service_ids=[4], 
        amount_column="csd.BillAmount", 
        condition=(
            (F.col("c.servicetypeid") != 2) & 
            (~F.col("c.servicesubtypeid").isin([16, 3, 13])) &
            (F.coalesce(F.col("c.ismaternity"), F.lit(0)) != 1)
        )
    ).alias("NursingClaimed"),
    
    calculate_sum(
        service_ids=[4], 
        amount_column="csd.SanctionedAmount", 
        condition=(
            (F.col("c.servicetypeid") != 2) & 
            (~F.col("c.servicesubtypeid").isin([16, 3, 13])) &
            (F.coalesce(F.col("c.ismaternity"), F.lit(0)) != 1)
        )
    ).alias("NursingCharges"),
    
    calculate_sum(
        service_ids=[3], 
        amount_column="csd.SanctionedAmount", 
        condition=(
            (F.col("c.servicetypeid") != 2) & 
            (~F.col("c.servicesubtypeid").isin([16, 3, 13])) &
            (F.coalesce(F.col("c.ismaternity"), F.lit(0)) != 1) &
            ((F.col("MIG_ServiceID").isNull()) | (F.col("MIG_ServiceID").isin([2, 3])))
        )
    ).alias("RoomRentRelated"),
    
    calculate_sum(
        service_ids=[5, 7, 8, 9, 10, 11, 12], 
        amount_column="csd.SanctionedAmount", 
        condition=(
            (F.col("c.servicetypeid") != 2) & 
            (~F.col("c.servicesubtypeid").isin([16, 3, 13])) &
            ((F.col("MIG_ServiceID").isNull()) | (F.col("MIG_ServiceID").isin([8, 9, 10, 11, 12, 13, 14, 15])))
        )
    ).alias("ProfessionalCharges"),
    
    calculate_sum(
        service_ids=[13, 14, 15, 16, 17, 18, 19], 
        amount_column="csd.SanctionedAmount", 
        condition=(
            (F.col("c.servicetypeid") != 2) & 
            (~F.col("c.servicesubtypeid").isin([16, 3, 13])) &
            ((F.col("MIG_ServiceID").isNull()) | (F.col("MIG_ServiceID").isin([16, 17, 18, 19, 20, 21])))
        )
    ).alias("Drugs\Medication\Consumables\Investigationsetc"),
    
    calculate_sum(
        service_ids=[20, 21, 22, 23, 24], 
        amount_column="csd.SanctionedAmount", 
        condition=(
            (F.col("c.servicetypeid") != 2) & 
            (~F.col("c.servicesubtypeid").isin([16, 3, 13])) &
            ((F.col("MIG_ServiceID").isNull()) | (F.col("MIG_ServiceID").isin([22, 23, 24, 25, 26, 27, 28])))
        )
    ).alias("Investigations\Procedures(IP)"),
    
    calculate_sum(
        service_ids=[], 
        amount_column="csd.SanctionedAmount", 
        condition=(F.col("c.servicesubtypeid") == 16)
    ).alias("DomicillaryHospitalization"),
    
    calculate_sum(
        service_ids=[], 
        amount_column="csd.SanctionedAmount", 
        condition=(F.col("c.ismaternity") == 1)
    ).alias("Maternity"),
    
    calculate_sum(
        service_ids=[], 
        amount_column="csd.SanctionedAmount", 
        condition=(F.col("c.servicesubtypeid") == 3)
    ).alias("DayCare"),
    
    calculate_sum(
        service_ids=[33, 34, 35, 36, 37, 38, 39, 40, 41], 
        amount_column="csd.SanctionedAmount", 
        condition=(
            (F.col("c.servicetypeid") != 2) & 
            (~F.col("c.servicesubtypeid").isin([16, 3, 13])) &
            ((F.col("MIG_ServiceID").isNull()) | (F.col("MIG_ServiceID").isin([70, 71, 72, 73, 74])))
        )
    ).alias("OperationTheatre"),
    
    calculate_sum(
        service_ids=[49], 
        amount_column="csd.SanctionedAmount", 
        condition=(
            (F.col("c.servicetypeid") != 2) & 
            (~F.col("c.servicesubtypeid").isin([16, 3, 13])) &
            (F.coalesce(F.col("c.ismaternity"), F.lit(0)) != 1)
        )
    ).alias("OrganDonar"),
    
    calculate_sum(
        service_ids=[], 
        amount_column="csd.SanctionedAmount", 
        condition=(
            (F.col("c.servicetypeid") != 2) & 
            (~F.col("c.servicesubtypeid").isin([16, 3, 13])) &
            ((col("MIG_ServiceID").isNull()) | (col("MIG_ServiceID").isin([54, 55, 56, 57, 58, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69])))
        )
    ).alias("AncilliaryServices"),
    
    calculate_sum(
        service_ids=[], 
        amount_column="csd.SanctionedAmount", 
        condition=(F.col("c.servicetypeid") == 13)&
        ((col("MIG_ServiceID").isNull()) | (col("MIG_ServiceID").isin([110, 111, 112, 113,114, 115, 116, 117,118, 119, 120, 121,122, 123, 124, 49,50, 51, 52, 53])))
    ).alias("Dental"),
    
    calculate_sum(
        service_ids=[], 
        amount_column="csd.SanctionedAmount", 
        condition=(
            (F.col("c.servicetypeid") == 2) & 
            (~F.col("c.servicesubtypeid").isin([3, 13, 14])) &
        ((col("MIG_ServiceID").isNull()) | (col("MIG_ServiceID").isin([34, 35, 36, 37, 38, 75, 76, 77, 78, 79, 80, 81, 82])))
        )
    ).alias("OutPatientCoverage"),
    
    calculate_sum(
        service_ids=[87], 
        amount_column="csd.SanctionedAmount", 
        condition=(
            (F.col("c.servicetypeid") != 2) & 
            (~F.col("c.servicesubtypeid").isin([16, 3, 13])) &
            (F.coalesce(F.col("c.ismaternity"), F.lit(0)) != 1) &
            ((col("MIG_ServiceID").isNull()) | (col("MIG_ServiceID").isin([87])))
        )
    ).alias("PersonalAccident"),
    
    calculate_sum(
        service_ids=[88], 
        amount_column="csd.SanctionedAmount", 
        condition=(
            (F.col("c.servicetypeid") != 2) & 
            (~F.col("c.servicesubtypeid").isin([16, 3, 13])) &
            (F.coalesce(F.col("c.ismaternity"), F.lit(0)) != 1) &
            ((col("MIG_ServiceID").isNull()) | (col("MIG_ServiceID").isin([88])))
        )
    ).alias("CriticalIllness"),

    calculate_sum(
        service_ids=[28], 
        amount_column="csd.SanctionedAmount", 
        condition=(
            (F.col("c.servicetypeid") == 14)
        )
    ).alias("HealthCheckUp"),
    
    calculate_sum(
        service_ids=[91], 
        amount_column="csd.SanctionedAmount", 
        condition=(
            (F.col("c.servicetypeid") != 2) & 
            (~F.col("c.servicesubtypeid").isin([16, 3, 13])) &
            (F.coalesce(F.col("c.ismaternity"), F.lit(0)) != 1) &
            ((col("MIG_ServiceID").isNull()) | (col("MIG_ServiceID").isin([126])))
        )
    ).alias("Spectacles/ContactLenses/HearingAid"),
)

# COMMAND ----------

direct_claimscoding_csd_df = direct_claimscoding_df.join(
    csd_fact,
    direct_claimscoding_df.claimid == csd_fact.c_Id,
    "left"
).drop("c_Id")

# COMMAND ----------

# DBTITLE 1,ClaimActionItems
window_spec = Window.partitionBy('claimid', 'slno')
df = claim_action_items_df.withColumn(
    'ClaimPassedDate',
    F.max(F.when(F.col('ClaimStageID') == 26, F.col('OpenDate'))).over(window_spec)
)

df = df.withColumn(
    'Ir/Investigation',
    F.when(
        (F.col('ClaimStageID').isin(7, 8)) &
        (F.col('OpenDate').isin(F.col('OpenDate')) & ~F.col('CloseDate').isin(F.col('OpenDate'))) &
        F.col('ClaimStageID').isin(18),
        'IR and INV'
    ).when(
        F.col('ClaimStageID') == 18,
        'INV'
    ).when(
        (F.col('ClaimStageID').isin(7, 8)) &
        (F.col('OpenDate').isin(F.col('OpenDate')) & ~F.col('CloseDate').isin(F.col('OpenDate'))),
        'IR'
    )
)

df = df.withColumn(
    'Date_of_IR',
    F.min(F.when(
        (F.col('ClaimStageID').isin(7, 8)) &
        (F.col('OpenDate').isin(F.col('OpenDate')) & ~F.col('CloseDate').isin(F.col('OpenDate'))),
        F.col('OpenDate')
    )).over(window_spec)
)
df = df.withColumn(
    'FirstReminder',
    F.max(F.when(F.col('ClaimStageID').isin(30, 34), F.col('OpenDate'))).over(window_spec)
).withColumn(
    'SecondReminder',
    F.max(F.when(F.col('ClaimStageID').isin(31, 35), F.col('OpenDate'))).over(window_spec)
)

df = df.withColumn(
    'Date_of_IRretrievalDate',
    F.when(
        F.first(F.col('ClaimStageID')).over(window_spec).isin(7, 8),
        F.coalesce(
            F.min(F.when(
                (F.col('ClaimStageID').isin(12, 13)) & 
                (F.col('OpenDate') >= F.max(F.when(
                    F.col('ClaimStageID').isin(7, 8, 30, 31, 32, 33, 34, 35, 36, 37), 
                    F.col('OpenDate')
                )).over(window_spec)) & 
                (~F.expr("array_contains(collect_set(OpenDate) OVER (PARTITION BY claimid, slno), OpenDate)")),
                F.col('OpenDate')
            )).over(window_spec),
            
            F.min(F.when(
                (F.col('ClaimStageID') == 38) & 
                (F.col('OpenDate') >= F.max(F.when(
                    F.col('ClaimStageID').isin(7, 8, 30, 31, 32, 33, 34, 35, 36, 37),
                    F.col('OpenDate')
                )).over(window_spec)),
                F.col('OpenDate')
            )).over(window_spec),
            
            F.min(F.when(
                (F.col('ClaimStageID') == 24) & 
                (F.col('OpenDate') >= F.max(F.when(
                    F.col('ClaimStageID').isin(7, 8, 30, 31, 32, 33, 34, 35, 36, 37), 
                    F.col('OpenDate')
                )).over(window_spec)),
                F.col('OpenDate')
            )).over(window_spec),
            
            F.min(F.when(
                (F.col('ClaimStageID') == 5) & 
                (F.coalesce(F.col('remarks'), F.lit('For Processing')) == 'For Processing') & 
                (F.col('OpenDate') >= F.max(F.when(
                    F.col('ClaimStageID').isin(7, 8, 30, 31, 32, 33, 34, 35, 36, 37), 
                    F.col('OpenDate')
                )).over(window_spec)),
                F.col('OpenDate')
            )).over(window_spec),
            
            F.min(F.when(
                (F.col('ClaimStageID') == 29) & 
                (F.col('OpenDate') >= F.max(F.when(
                    F.col('ClaimStageID').isin(7, 8, 30, 31, 32, 33, 34, 35, 36, 37), 
                    F.col('OpenDate')
                )).over(window_spec)),
                F.col('OpenDate')
            )).over(window_spec)
        )
    )
)

df = df.withColumn(
    'InvestigationDate',
    F.max(F.when(F.col('ClaimStageID') == 18, F.col('OpenDate'))).over(window_spec)
)

df = df.withColumn(
    'Investigation_RetrievalDate',
    F.when(
        F.first(F.col('ClaimStageID')).over(window_spec) == 18,
        F.coalesce(
            F.min(F.when(
                (F.col('ClaimStageID') == 20) & (F.col('OpenDate') >= F.max(F.col('OpenDate')).over(window_spec)),
                F.col('OpenDate')
            )).over(window_spec),
            F.min(F.when(
                F.col('ClaimStageID') == 38,
                F.col('OpenDate')
            )).over(window_spec)
        )
    )
)

df = df.join(claims_df.select("*", claims_df["IssueID"]), df["ClaimID"] == claims_df["ID"], how="left")

df = df.withColumn(
    'ReopenedDate',
    F.when(
        claims_df["IssueID"] == 21,
        F.first(F.when(F.col('requesttypeid') == 6, F.col('OpenDate'))).over(window_spec)
    ).otherwise(F.lit(None))
)

df = df.withColumn(
    'RefertoInsurerDate',
    F.max(F.when(F.col('ClaimStageID') == 17, F.col('OpenDate'))).over(window_spec)
).withColumn(
    'RefertoInsurerReasons',
    F.first(F.when(F.col('ClaimStageID') == 17, F.col('Remarks'))).over(
        window_spec.orderBy(F.col('OpenDate')) # desc()
    )
)

window_spec_17 = Window.partitionBy('claimid', 'slno').orderBy(F.desc('OpenDate')).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

max_open_date_17 = F.max(F.when(F.col('ClaimStageID') == 17, F.col('OpenDate'))).over(window_spec_17)

df = df.withColumn(
    'ReceiveddatefromInsurer',
    F.coalesce(
        F.min(F.when(
            (F.col('ClaimStageID') == 14) & 
            (F.col('OpenDate') > max_open_date_17), 
            F.col('OpenDate')
        )).over(Window.partitionBy('claimid', 'slno')),

        F.min(F.when(
            (F.col('ClaimStageID') == 5) & 
            (F.col('Remarks').like('%For Processing%')) & 
            (F.col('OpenDate') > max_open_date_17),
            F.col('OpenDate')
        )).over(Window.partitionBy('claimid', 'slno')),

        F.min(F.when(
            (F.col('ClaimStageID') == 5) & 
            (~F.col('Remarks').rlike('%IR Pending%|%IR Docs Updated%|%Refer to Insurer – O%|%Refer to Insurer – R%|%Main Claim Pending%|%Rejection%|%Case Investigation%')) &
            (F.col('Remarks') != '') &
            (F.col('OpenDate') > max_open_date_17),
            F.col('OpenDate')
        )).over(Window.partitionBy('claimid', 'slno'))
    )
)
df = df.withColumn("IssueID", col("IssueID"))

# COMMAND ----------

# DBTITLE 1,ClaimActionItems
new_columns = [
    'ClaimPassedDate',
    'Ir/Investigation',
    'Date_of_IR',
    'FirstReminder',
    'SecondReminder',
    'Date_of_IRretrievalDate',
    'InvestigationDate',
    'Investigation_RetrievalDate',
    'ReopenedDate',
    'RefertoInsurerDate',
    'RefertoInsurerReasons',
    'ReceiveddatefromInsurer'
]

claim_action_items_selected = df.select(df['claimid'].alias('c_id'), *new_columns)

# COMMAND ----------

# DBTITLE 1,dropDuplicates
claim_action_items_selected = claim_action_items_selected.dropDuplicates()

# COMMAND ----------

# DBTITLE 1,dropDuplicates
direct_claimscoding_csd_df = direct_claimscoding_csd_df.dropDuplicates()

# COMMAND ----------

direct_claimscoding_csd_cai_df = direct_claimscoding_csd_df.join(
    claim_action_items_selected, 
    direct_claimscoding_csd_df['claimid'] == claim_action_items_selected['c_id'], 
    how='left'
).drop(claim_action_items_selected['c_id'])

# COMMAND ----------

direct_claimscoding_csd_cai_df.count()

# COMMAND ----------

# DBTITLE 1,provider
claims_provider_df = claims_df.join(provider_df, claims_df['Providerid'] == provider_df['id'], how='left')

claims_provider_df = claims_provider_df.withColumn('Providername', provider_df['ID'])

claims_provider_df = claims_provider_df.withColumn(
    'provideraddress', 
    F.concat_ws(' ', provider_df['Address1'], provider_df['Address2'])
)
claims_provider_df = claims_provider_df.withColumn('providerplace', provider_df['Location'])
claims_provider_df = claims_provider_df.withColumn('ProviderPincode', provider_df['PINCode'])
claims_provider_df = claims_provider_df.join(
    mst_state_df, 
    provider_df['StateID'] == mst_state_df['id'], 
    how='left'
).withColumn('providerstate', mst_state_df['name'])

claims_provider_df = claims_provider_df.join(
    mst_property_values_df, 
    provider_df['HospitalCategory_P68'] == mst_property_values_df['id'], 
    how='left'
).withColumn('ProviderIdentification', mst_property_values_df['name'])

fact_provider = claims_provider_df.select(
    claims_df['id'].alias('c_id'),
    'Providername',
    'provideraddress',
    'providerplace',
    'providerstate',
    'ProviderPincode',
    'ProviderIdentification'
)

# COMMAND ----------

direct_claimscoding_csd_cai_provider_df = direct_claimscoding_csd_cai_df.join(fact_provider,direct_claimscoding_csd_cai_df["claimid"]==fact_provider["c_id"], how='left').drop(fact_provider['c_id'])

# COMMAND ----------

# DBTITLE 1,memberpolicy
from pyspark.sql import functions as F

uhidno_df = member_policy_df.alias("m") \
    .join(mst_issuingauthority_df.alias("ia"), F.col("m.issueid") == F.col("ia.ID"), "inner") \
    .select(F.concat_ws('.', F.col("ia.code"), F.col("m.uhidno")).alias("UHIDNO"),F.col("m.ID").alias("memberpolicyID"))

mainmemuhidno_df = member_policy_df.alias("mp1") \
    .join(member_policy_df.alias("m"), F.col("mp1.MainMemberID") == F.col("m.ID"), "inner") \
    .join(mst_issuingauthority_df.alias("ia"), F.col("mp1.issueid") == F.col("ia.ID"), "inner") \
    .select(
        F.concat_ws('.', F.col("ia.code"), F.col("mp1.uhidno")).alias("MainMemuhidno"),
        F.col("mp1.MemberName").alias("MainMemname")
    )

gender_df = member_policy_df.alias("mp") \
    .join(mst_gender_df.alias("mg"), F.col("mp.GenderID") == F.col("mg.ID"), "inner") \
    .select(F.col("mp.ID").alias("PolicyID_Gender"), F.col("mg.name").alias("Gender"))

relationship_df = member_policy_df.alias("mp") \
    .join(mst_relationship_df.alias("mr"), F.col("mp.RelationshipID") == F.col("mr.ID"), "inner") \
    .select(F.col("mp.ID").alias("PolicyID_Relationship"), F.col("mr.name").alias("relationship"))

isvip_df = member_policy_df.alias("mp") \
    .select(
        F.col("mp.ID").alias("PolicyID_IsVIP"),
        F.when(F.col("mp.isVIP").isin([1, 183, 184]), "Yes")
        .otherwise("No").alias("IsVIP")
    )

fact_memberpolicy = uhidno_df \
    .join(mainmemuhidno_df, uhidno_df.UHIDNO == mainmemuhidno_df.MainMemuhidno, "left") \
    .join(gender_df, uhidno_df.memberpolicyID == gender_df.PolicyID_Gender, "left") \
    .join(relationship_df, uhidno_df.memberpolicyID == relationship_df.PolicyID_Relationship, "left") \
    .join(isvip_df, uhidno_df.memberpolicyID == isvip_df.PolicyID_IsVIP, "left") \
    .select(
        "UHIDNO",
        "MainMemuhidno",
        "MainMemname",
        "Gender",
        "relationship",
        "IsVIP",
        uhidno_df.memberpolicyID.alias("memberpolicyID")
    )

# COMMAND ----------

semi_final_df6 = direct_claimscoding_csd_cai_provider_df.join(fact_memberpolicy, direct_claimscoding_csd_cai_provider_df["memberpolicyid"] == fact_memberpolicy["memberpolicyid"],"left").drop(fact_memberpolicy["memberpolicyid"])

# COMMAND ----------

# DBTITLE 1,policy
policy_memberpolicy_df = policy_df.alias("p").join(
    member_policy_df.alias("m"),
    F.col("p.id") == F.col("m.PolicyID"),
    how="left"
).join(
    mst_payer_df.alias("mst_payer"),
    F.col("mst_payer.id") == F.col("p.payerid"),
    how="left"
).select(
    F.col("m.ID").alias("memberpolicyid"),
    F.col("p.PolicyNo"),
    F.col("p.StartDate").alias("PolicyStartDate"),
    F.col("p.EndDate").alias("PolicyExpiryDate"),
    F.col("p.CorpID"),
    F.col("p.payerid").alias("Roname")
)


fact_policy = policy_memberpolicy_df.alias("pm").join(
    mst_corporate_df.alias("mc"),
    (F.col("pm.CorpID") == F.col("mc.id")) & (F.col("pm.CorpID") != 0),
    how="left"
).select(
    F.col("pm.memberpolicyid"),
    F.col("pm.PolicyNo"),
    F.col("pm.PolicyStartDate"),
    F.col("pm.PolicyExpiryDate"),
    F.when(F.col("pm.CorpID") == 0, "Individual")
     .otherwise(F.col("mc.Name")).alias("Organisationname"),
    F.col("pm.Roname")
)


# COMMAND ----------

semi_final_df7 = semi_final_df6.join(fact_policy, semi_final_df6["memberpolicyid"] == fact_policy["memberpolicyid"],"left").drop(fact_policy["memberpolicyid"])

# COMMAND ----------

# DBTITLE 1,dropDuplicates
semi_final_df7 = semi_final_df7.dropDuplicates(["memberpolicyid"])

# COMMAND ----------

# DBTITLE 1,cd_cc_fact
joined_df = claims_details_df.join(
    claimscoding_df,
    claims_details_df["claimid"] == claimscoding_df["claimid"],
    "left"
)
fact_cd_cc = joined_df.withColumn(
    "Package",
    F.when(
        (F.col("cd.ispackage") == 1) |
        (F.col("cc.BillingType_P51").isin(201, 203)),
        "Package"
    ).otherwise("Non-Package")
).withColumn(
    "IsNIDB",
    F.when(
        (F.col("cd.ispackage") == 1) |
        (F.col("cc.BillingType_P51").isin(201, 203)),
        "Package"
    ).otherwise("Non-Package")
).select(
    col("cd.claimid").alias("claimid"),
    "Package",
    "IsNIDB"
)

# COMMAND ----------

semi_final_df8 = semi_final_df7.join(fact_cd_cc, semi_final_df7["claimid"] == fact_cd_cc["claimid"],"left").drop(fact_cd_cc["claimid"])

# COMMAND ----------

# DBTITLE 1,dropDuplicates
semi_final_df8 = semi_final_df8.distinct()

# COMMAND ----------

# DBTITLE 1,fact_utilization
buffer_joined_df = claims_details_df.join(
    buffer_utilization_df,
    (claims_details_df["ClaimID"] == buffer_utilization_df["ClaimID"]) &
    (claims_details_df["cd_Slno"] == buffer_utilization_df["Slno"]),
    "left"
)

final_joined_df = buffer_joined_df.join(
    tertiary_utilization_df,
    (buffer_joined_df["cd.ClaimID"] == tertiary_utilization_df["ClaimID"]) &
    (buffer_joined_df["Slno"] == tertiary_utilization_df["Slno"]),
    "left"
)

fact_utilization = final_joined_df.withColumn(
    "BufferAmount",
    F.coalesce(buffer_utilization_df["UtilizedAmount"], F.lit(0))
).withColumn(
    "tertiaryamount",
    F.coalesce(tertiary_utilization_df["UtilizedAmount"], F.lit(0))
)

fact_utilization = fact_utilization.select(
    col("cd.ClaimID").alias("claimid"),
    "BufferAmount",
    "tertiaryamount"
).distinct()

# COMMAND ----------

semi_final_df9 = semi_final_df8.join(fact_utilization, semi_final_df8["claimid"] == fact_utilization["claimid"],"left").drop(fact_utilization["claimid"])

# COMMAND ----------

# DBTITLE 1,dropDuplicates
semi_final_df9 = semi_final_df9.distinct()

# COMMAND ----------

# DBTITLE 1,MemberContacts
joined_df = member_contacts_df.join(
    member_policy_df,
    (member_contacts_df["EntityID"] == member_policy_df["id"]) &
    (member_contacts_df["IsBaseAddress"] == 1),
    "inner"
)

fact_member_contacts = joined_df.select(
    col("mp.ID").alias("memberpolicyid"),
    F.col("mc.MobileNo").alias("Mobile"),
    F.col("mc.EmailID").alias("Email")
).distinct()

# COMMAND ----------

semi_final_df10 = semi_final_df9.join(fact_member_contacts, semi_final_df9["memberpolicyid"] == fact_member_contacts["memberpolicyid"],"left").drop(fact_member_contacts["memberpolicyid"])

# COMMAND ----------

# DBTITLE 1,dropDuplicates
semi_final_df10 = semi_final_df10.distinct()

# COMMAND ----------

# DBTITLE 1,ProviderMOU, BPSuminsured, Mst_RejectionReasons
provider_mou_joined_df = provider_df.join(
    provider_mou_df,
    (provider_df["id"] == provider_mou_df["providerid"]) &
    (provider_mou_df["MOUTypeID_P44"] == 167),
    "left"
).distinct()

provider_with_type_df = provider_mou_joined_df.withColumn(
    "PROVIDERTYPE",
    F.when(
        (F.col("pmou.enddate") > current_date()) |
        (F.col("pmou.providerid").isNotNull()),
        "Network"
    ).otherwise("Non Network")
).select(col("p.id").alias("providerid"), "PROVIDERTYPE").distinct()

claims_with_policy_df = claims_df.join(
    member_policy_df,
    claims_df["memberpolicyid"] == member_policy_df["id"],
    "inner"
).distinct()

claims_with_policy_df = claims_df.join(
    member_policy_df,
    claims_df["memberpolicyid"] == member_policy_df["id"],
    "inner"
).distinct()

claims_with_membersi_df = claims_with_policy_df.join(
    member_si_df,
    claims_with_policy_df["memberpolicyid"] == member_si_df["memberpolicyid"],
    "inner"
).distinct()

coverage_df = claims_with_membersi_df.join(
    bpsuminsured_df,
    claims_with_membersi_df["bpsiid"] == bpsuminsured_df["ID"],
    "inner"
).filter(
    bpsuminsured_df["SICategoryID_P20"] == 69
).select(
    claims_with_membersi_df["c.id"].alias("claimid"),
    F.col("bpi.SumInsured").alias("coverageamount")
).distinct()

rejection_category_df = claim_rejection_reasons_df.join(
    mst_rejection_reasons_df,
    claim_rejection_reasons_df["RejectionReasonsID"] == mst_rejection_reasons_df["ID"],
    "inner"
).select(
    "claimid",
    F.col("mrr.ShortText").alias("Rejection_Category")
).distinct()

# COMMAND ----------

semi_final_df11 = semi_final_df10 \
    .join(provider_with_type_df, semi_final_df10["Providername"] == provider_with_type_df["providerid"], "left") \
    .join(coverage_df, "claimid", "left") \
    .join(rejection_category_df, "claimid", "left") \
    .select(
        semi_final_df10["*"],
        F.col("PROVIDERTYPE"),
        F.col("coverageamount"),
        F.col("Rejection_Category")
    ) \
    .drop(provider_with_type_df["providerid"]) \
    .drop(coverage_df["claimid"]) \
    .drop(rejection_category_df["claimid"])

# COMMAND ----------

# DBTITLE 1,dropDuplicates
semi_final_df11 = semi_final_df11.distinct()

# COMMAND ----------

# DBTITLE 1,claims_ir_reasons
claims_ir_reasons_df1 = claims_ir_reasons_df.select("claimid",
                                                    col("ismandatory").alias("isMandatory"),
                                                    col("isReceived").alias("isReceived")).distinct()

semi_final_df_12 = semi_final_df11.join(claims_ir_reasons_df1, "claimid", "left").drop(claims_ir_reasons_df1.claimid)

# COMMAND ----------

# DBTITLE 1,dropDuplicates
semi_final_df_12 = semi_final_df_12.distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC #Final Fact Table without SP columns

# COMMAND ----------

# semi_final_df_12.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### SP Columns

# COMMAND ----------

df1 = semi_final_df_12.withColumn(
    "DisAllowenceReason1",
    lit(udf_disallowence_reason(
        col("claimid"),
        col("slno")
    ))
).withColumn(
    "DisAllowenceReason2",
    lit("")
).withColumn(
    "Dis_Rej_reason",
    lit(udf_rejection_reason(
        col("claimid"),
        col("slno")
    ))
).withColumn(
    "BalanceSuminsured",
    lit(UFN_Spectra_BalanceSumInsured(col("claimid"))))

# COMMAND ----------

# def UFN_IRPendingReasons(claimid: int = None, slno: int = None, isMandatoryNew: int = None, IsReceived: int = None) -> str:

#     claims_ir_reasons_df1 = claims_ir_reasons_df.withColumnRenamed("isMandatory", "isMandatoryNew")

#     mst_services_df.alias("s")
#     claims_ir_reasons_df1.alias('cr')
#     claims_df.alias('c')
#     mst_irdocuments_df.alias('ir')

#     joined_df = claims_ir_reasons_df1.alias('cr') \
#         .join(mst_irdocuments_df.alias('ir'), col('cr.IRDocumentID') == col('ir.ID'), 'left') \
#         .join(claims_df.alias('c'), col('cr.ClaimID') == col('c.id'), 'left') \
#         .join(mst_issuingauthority_df.alias('i'), col('c.issueid') == col('i.id'), 'left')\
#         .join(mst_services_df.alias('s'), col('cr.ServiceID') == col('s.ID'), 'left')

#     joined_df = joined_df.filter((col('cr.ClaimID') == claimid) & (col('cr.Slno') == slno) & (col('cr.Deleted') == 0) &
#                 (claims_ir_reasons_df1["isMandatoryNew"] == (isMandatoryNew if isMandatoryNew is not None else claims_ir_reasons_df1["isMandatoryNew"])) &
#                 (claims_ir_reasons_df1["IsReceived"] == (IsReceived if IsReceived is not None else claims_ir_reasons_df1["IsReceived"])))

#     reason_df = joined_df.select(
#         when(claims_ir_reasons_df1["IRDocumentID"] == 0, claims_ir_reasons_df1["Remarks"])
#         .otherwise(concat_ws('', 
#                              when(col('ir.Name').contains('~INS~'), col('i.Name')).otherwise(col('ir.Name')),
#                              when(col('ir.Name').contains('|FT|'), col('cr.FreeText1')).otherwise(lit('')),
#                              when(col('ir.Name').contains('|FT1|'), col('cr.FreeText2')).otherwise(lit(''))
#                             )).alias('Reason')
#     ).distinct()

#     union_df = reason_df.union(
#         claims_ir_reasons_df1.filter((col('ClaimID') == claimid) & (col('slno') == slno) & (col('Deleted') == 0) &
#                 (claims_ir_reasons_df1["isMandatoryNew"] == (isMandatoryNew if isMandatoryNew is not None else claims_ir_reasons_df1["isMandatoryNew"])) &
#                                     (claims_ir_reasons_df1["IsReceived"] == (IsReceived if IsReceived is not None else claims_ir_reasons_df1["IsReceived"])) &
#                                     (claims_ir_reasons_df1["IRDocumentID"] == 0) & (claims_ir_reasons_df1["ServiceID"] == 0))
#         .select(claims_ir_reasons_df1["Remarks"].alias('Reason'))
#     )
    
#     union_df1 = union_df.union(
#         claims_ir_reasons_df1\
#         .filter((col('ClaimID') == claimid) & (col('slno') == slno) & (claims_ir_reasons_df["Deleted"] == 0) &
#                 (claims_ir_reasons_df1["isMandatoryNew"] == (isMandatoryNew if isMandatoryNew is not None else claims_ir_reasons_df1["isMandatoryNew"])) &
#                 (claims_ir_reasons_df1["IsReceived"] == (IsReceived if IsReceived is not None else claims_ir_reasons_df1["IsReceived"])) &
#                 (claims_ir_reasons_df1["IRDocumentID"] == 0))
#         .select(claims_ir_reasons_df1["Remarks"].alias('Reason'))
#     )

#     # display(union_df1)

#     result = union_df1.select(concat_ws(",", col("Reason")).alias("Reasons")).collect()
#     if result:
#         content = [row["Reasons"] for row in result]
#         output = ", ".join(content)
#         return output

#     return result

# COMMAND ----------

df2 = df1.withColumn(
    "PendReason",
    lit(UFN_IRPendingReasons(
        col("claimid"),
        col("slno"),
        col("isMandatory"),
        col("isReceived")
    )
))

# COMMAND ----------

# def UDF_GetIncurredAmount(claimid, slno):

#     claims_details_df.alias('cd')
#     claim_stage_df.alias('cs')
#     claims_df.alias('c')
#     member_policy_df.alias('m')
#     member_si_df.alias('ms')
#     bpsuminsured_df.alias('bs')
#     claims_details_df1 = claims_details_df.withColumnRenamed('cd_Slno', 'slno')

#     joined_df = claims_details_df1.alias('cd') \
#         .join(claims_df, (F.col('cd.ClaimID') == F.col('c.ID')) & (F.col('cd.deleted') == 0) & (F.col('c.deleted') == 0), 'inner') \
#         .join(claim_stage_df, F.col('cs.ID') == F.col('cd.StageID'), 'inner') \
#         .join(member_policy_df.alias('m'), F.col('m.ID') == F.col('c.memberpolicyid'), 'inner') \
#         .join(member_si_df.alias('ms'), F.col('ms.memberpolicyid') == F.col('m.id'), 'inner') \
#         .join(bpsuminsured_df.alias('bs'), 
#               (F.col('bs.id') == F.col('ms.BPSIID')) & (F.col('bs.SICategoryID_P20') == 69), 'inner') \
#         .filter((claims_details_df1["ClaimID"] == claimid) & (col("Slno") == slno))

#     values = joined_df.select(
#         F.col('cd.ClaimTypeID').alias('Claimtype'),
#         F.col('cd.RequestTypeID').alias('Requesttype'),
#         F.when(F.col('cd.RequestTypeID').isin(1, 2, 3), F.col('cs.Name'))
#         .when(F.col('cd.StageID') == 27, F.lit('Settled'))
#         .when(F.col('cd.StageID').isin(21, 23, 25), F.col('cs.Name'))
#         .when(F.col('cs.Name') == 'Received Requests', F.lit('Received Requests'))  # Handle 'Received Requests'
#         .otherwise(
#             F.coalesce(
#                 F.lit(spark.sql("""
#                     SELECT cs1.Name
#                     FROM fhpl.claimsdetails cd 
#                     JOIN fhpl.claimactionitems ca1 ON cd.ClaimID = ca1.ClaimID
#                     JOIN fhpl.claimstage cs1 ON ca1.ClaimStageID = cs1.ID
#                     WHERE ca1.claimid = cd.ClaimID AND ca1.Slno = cd.Slno
#                     ORDER BY ca1.ID DESC
#                     LIMIT 1
#                 """).first()['Name']),
#                 F.col('cs.Name')
#             )
#         ).alias('Currentclaimstatus'),
#         F.col('cd.PayableAmount').alias('Incurred'),
#         F.col('cd.SettledAmount').alias('Settled'),
#         F.col('cd.ClaimAmount').alias('Claimed'),
#         F.when(F.col('cd.StageID') == 23, F.lit(0)).otherwise(F.col('cd.SanctionedAmount')).alias('Sanctioned'),
#         F.col('bs.SumInsured').alias('coverageamount'),
#         F.col('c.Deleted').alias('Claim_Deleted'),
#         F.col('cd.Deleted').alias('Claimdetails_Deleted')
#     ).first()
# ###################
#     result = claims_details_df1.filter(
#         (col("ClaimID") == claimid) &
#         (col("deleted") == 0) &
#         col("RequestTypeID").isin([1, 2, 3]) &
#         ~col("stageid").isin([23])
#     ).withColumn("rowno", row_number().over(Window.partitionBy("ClaimID").orderBy("ClaimID", col("Slno").desc()))) \
#         .select(
#             col("ClaimID"),
#             col("Slno"),
#             col("claimtypeid"),
#             col("sanctionedamount"),
#             col("rowno")
#         ) \
#         .filter(col("rowno") == 1) \
#         .select(
#             coalesce(sum(when(col("claimtypeid") == 1, col("sanctionedamount")).otherwise(0)), lit(0)).alias("PASanctionedAmount")
#         ) \
#         .first()
#     pasanctioned_amount = result["PASanctionedAmount"]
#     # return result["PASanctionedAmount"]
#  ##################   
#     # Main Member Status SQL query
#     # mainmemberstatus_query = f"""
#     #     SELECT 1 AS mainmemberstatus
#     #     FROM fhpl.ClaimsDetails cd
#     #     JOIN fhpl.Claims c ON cd.ClaimID = c.ID
#     #     JOIN fhpl.MemberPolicy mp ON c.MemberPolicyID = mp.Id
#     #     JOIN fhpl.MemberPolicy mp2 ON mp.MainmemberID = mp2.MainmemberID
#     #     JOIN fhpl.Claims c2 ON mp2.Id = c2.MemberPolicyID
#     #     WHERE cd.ClaimTypeID = 1 AND cd.ClaimID = c2.ID AND cd.deleted = 0 
#     #           AND c.deleted = 0 AND mp.deleted = 0 AND mp2.deleted = 0 AND c2.deleted = 0
#     #           AND NOT EXISTS (
#     #               SELECT 1
#     #               FROM fhpl.Claimsdetails cd2
#     #               WHERE cd2.ClaimID = c2.ID AND cd2.RequestTypeID < 4 AND cd2.StageID = 29
#     #           )
#     #           AND cd.ClaimID = {claimid} AND cd.Slno = {slno}
#     # """
#     # mainmemberstatus_result = spark.sql(mainmemberstatus_query).first()
#     result = claims_details_df.alias("cd") \
#         .join(claims_df.alias("c"), col("cd.ClaimID") == col("c.ID")) \
#         .join(member_policy_df.alias("mp"), col("c.MemberPolicyID") == col("mp.Id")) \
#         .join(member_policy_df.alias("mp2"), col("mp.MainmemberID") == col("mp2.MainmemberID")) \
#         .join(claims_df.alias("c2"), col("mp2.Id") == col("c2.MemberPolicyID")) \
#         .filter((col("cd.ClaimTypeID") == 1) & 
#                 (col("cd.ClaimID") == col("c2.ID")) & 
#                 (col("cd.deleted") == 0) & 
#                 (col("c.deleted") == 0) & 
#                 (col("mp.deleted") == 0) & 
#                 (col("mp2.deleted") == 0) & 
#                 (col("c2.deleted") == 0)) \
#         .withColumn("has_non_main_member_claims",  # New column to check subquery results
#                     count("*").over(Window.partitionBy("c2.ID"))
#                     .filter(col("has_non_main_member_claims") > 0)  # Filter based on count
#                     .filter((col("cd.ClaimID") == claimid) & 
#                             (col("cd.Slno") == slno)) \
#         .select(lit(1).alias("mainmemberstatus")) \
#         .limit(1) \
#         .collect())

#     # Assuming you have defined UDF_GetIncurredAmount elsewhere
#     main_member_status = result[0]["mainmemberstatus"] if result else 0

#     # if mainmemberstatus_result is not None:
#     #     mainmemberstatus = mainmemberstatus_result['mainmemberstatus']
#     # else:
#     #     mainmemberstatus = 0

#     # if values is not None:
#     #     incurred = values['Incurred']
#     # else:
#     #     incurred = 0
#     # if values is not None:
#     #     if values['Claim_Deleted'] == 0 and values['Claimdetails_Deleted'] == 0:
#     #         if values['Currentclaimstatus'] in (
#     #             'Registration', 'For Adjudication', 'Refer to CS', 'Refer to Enrollment', 'Query Response (H)', 'Query Response (M)',
#     #             'Insurer Response', 'CS Response', 'Enrollment Response', 'Refer to Insurer', 'Insurer Authorization', 'Investigation Done',
#     #             'Query to Hospital', 'Query to Member', 'First Reminder-Hospital', 'Second Reminder-Hospital', 'Third Reminder-Hospital',
#     #             'Fourth Reminder-Hospital', 'First Reminder-Member', 'Second Reminder-Member', 'Third Reminder-Member', 'Fourth Reminder-Member',
#     #             'From CRM', 'For Investigation', 'For Bill Entry', 'For Audit', 'Refer To CRM', 'Audit Return', "Received Requests"
#     #         ) and values['Requesttype'] not in (1, 2, 3):
#     #             incurred = min(values['Claimed'], values['coverageamount']) if values['Claimtype'] == 2 else min(values['Claimed'], pasanctioned_amount or values['Sanctioned'])

#     #         if values['Requesttype'] in (1, 2, 3):
#     #             incurred = min(values['Claimed'], values['Sanctioned'])

#     #         if values['Claimtype'] == 1 and mainmemberstatus == 1 and values['Requesttype'] in (1, 2, 3):
#     #             balance_sum_insured = balance_sum_insured_without_present_claim(claimid, 3)
#     #             incurred = min(values['Claimed'], balance_sum_insured or values['Sanctioned'])

#     #         if values['Claimtype'] == 2 and values['Currentclaimstatus'] in (
#     #             'For Bill Entry', 'For Adjudication', 'Query to Hospital', 'Query to Member', 'Refer to CS', 'Refer to CRM', 'Refer to Enrollment',
#     #             'Insurer Response', 'CS Response', 'Enrollment Response', 'Refer to Insurer', 'For Investigation', 'Investigation Done', 'Sent for Audit',
#     #             'For Audit', 'Audit Return', 'First Reminder-Hospital', 'Second Reminder-Hospital', 'Third Reminder-Hospital', 'Fourth Reminder-Hospital',
#     #             'First Reminder-Member', 'Second Reminder-Member', 'Third Reminder-Member', 'Fourth Reminder-Member', 'From CRM', "Received Requests"
#     #         ):
#     #             incurred_amount = claims_df.filter(col("ID") == claimid).selectExpr("balance_sum_insured_without_present_claim(ID, 1) AS IncurredAmount").first()['IncurredAmount']
#     #             bill_amount = values['Claimed']
#     #             incurred = min(incurred_amount, bill_amount)

#     #         if values['Currentclaimstatus'] == 'Settled':
#     #             incurred = values['Settled']

#     #         if values['Currentclaimstatus'] in ('Closed', 'Repudiated', 'Cancelled'):
#     #             incurred = 0

#     #         if values['Currentclaimstatus'] in ('For Payment', 'For Settlement') and values['Claimtype'] == 2:
#     #             incurred = values['Sanctioned']

#     #         if values['Claimtype'] == 2 and values['Currentclaimstatus'] in (
#     #             'For Bill Entry', 'For Adjudication', 'Query to Hospital', 'Query to Member', 'Refer to CS', 'Refer to CRM', 'Refer to Enrollment',
#     #             'Insurer Response', 'CS Response', 'Enrollment Response', 'Refer to Insurer', 'For Investigation', 'Investigation Done', 'Sent for Audit',
#     #             'For Audit', 'Audit Return', 'First Reminder-Hospital', 'Second Reminder-Hospital', 'Third Reminder-Hospital', 'Fourth Reminder-Hospital',
#     #             'First Reminder-Member', 'Second Reminder-Member', 'Third Reminder-Member', 'Fourth Reminder-Member', 'From CRM',"Received Requests"
#     #         ):
#     #             incurred_amount = claims_df.filter(col("ID") == claimid).selectExpr("balance_sum_insured_without_present_claim(ID, 1) AS IncurredAmount").first()['IncurredAmount']
#     #             bill_amount = values['Claimed']
#     #             incurred = min(incurred_amount, bill_amount)

#     #         if incurred < 0 or (
#     #             values['Currentclaimstatus'] in (
#     #                 'For Bill Entry', 'For Adjudication', 'Refer to CS', 'Refer to CRM', 'Refer to Enrollment', 'Insurer Response', 'CS Response',
#     #                 'Enrollment Response', 'Refer to Insurer', 'For Investigation', 'Investigation Done', 'Sent for Audit', 'For Audit', 'For Payment',
#     #                 'For Settlement', 'Audit Return', 'From CRM',"Received Requests"
#     #             ) and claims_action_items_df.filter(
#     #                 (col('ClaimID') == claimid) & (col('Slno') == slno)
#     #             ).orderBy(col('OpenDate').desc()).first()['Remarks'].contains('Refer to Insurer – R')
#     #         ):
#     #             incurred = 0

#     #         if values['Requesttype'] in (1, 2, 3) and values['Claimtype'] == 1 and values['Currentclaimstatus'] == 'Cashless Approved':
#     #             incurred = values['Sanctioned']

#     #     else:
#     #         incurred = 0
#     # else:
#     #     incurred = 0
#     # return float(incurred)

# COMMAND ----------

from pyspark.sql import functions as F
def UDF_GetIncurredAmount(claimid, slno):
    df_claims = spark.table("fhpl.claims")
    df_claims_details = spark.table("fhpl.claimsdetails")
    df_memberpolicy = spark.table("fhpl.memberpolicy")
    df_membersi = spark.table("fhpl.membersi")
    df_bpsuminsured = spark.table("fhpl.bpsuminsured")
    df_claimstage = spark.table("fhpl.claimstage")
    df_claims_action_items = spark.table("fhpl.claimactionitems")

    df_cd = df_claims_details.alias("cd")
    df_c = df_claims.alias("c")
    df_cs = df_claimstage.alias("cs")
    df_mp = df_memberpolicy.alias("mp")
    df_msi = df_membersi.alias("msi")
    df_bs = df_bpsuminsured.alias("bs")
    df_ca = df_claims_action_items.alias("ca")

    joined_df = (
        df_c
        .join(df_cd, df_cd["ClaimID"] == df_c["ID"], "inner")
        .join(df_cs, df_cd["StageID"] == df_cs["ID"], "inner")
        .join(df_mp, df_c["MemberPolicyID"] == df_mp["Id"], "inner")
        .join(df_msi, df_mp["Id"] == df_msi["memberpolicyid"], "inner")
        .join(df_bs, df_msi["BPSIID"] == df_bs["ID"], "inner")
        .filter((df_cd["ClaimID"] == claimid) & (df_cd["Slno"] == slno))
    )

    # To get the Currentclaimstatus from the claimactionitems table
    df_claims_details = spark.table("fhpl.claimsdetails").alias("cd")
    df_claims_action_items = df_claims_action_items.alias("df_ca")
    df_claimstage = df_claimstage.alias("cs")

    window_spec = Window.partitionBy("df_ca.ClaimID", "df_ca.Slno").orderBy(F.desc("df_ca.ID"))

    subquery_df = df_claims_action_items \
        .join(df_cs, col("df_ca.ClaimStageID") == col("cs.ID")) \
        .where(df_claims_action_items["df_ca.CloseDate"].isNotNull()) \
        .withColumn("rank", F.row_number().over(window_spec)) \
        .filter(F.col("rank") == 1) \
        .select(F.col("df_ca.ClaimID"), F.col("df_ca.Slno"), F.col("cs.Name").alias("subquery_name"))

    currentclaimstatus_df = df_cd \
        .join(subquery_df, ["ClaimID", "Slno"], "left_outer")\
        .join(df_cs, col("cd.StageID") == col("cs.ID")) 
    
    currentclaimstatus = currentclaimstatus_df.select("subquery_name").collect()
    currentclaimstatus_value = currentclaimstatus[0][0] if currentclaimstatus else None

    initial_vars_df = (
        joined_df
        .select(
            df_cd["ClaimTypeID"].alias("Claimtype"),
            df_cd["RequestTypeID"].alias("Requesttype"),
            F.when(df_cd["RequestTypeID"].isin([1, 2, 3]), df_cs["Name"])
            .when(df_cd["StageID"] == 27, F.lit('Settled'))
            .when(df_cd["StageID"].isin([21, 23, 25]), df_cs["Name"])
            .otherwise(F.coalesce(F.lit(currentclaimstatus_value), df_cs["Name"])).alias("Currentclaimstatus"),
            F.coalesce(df_cd["payableamount"], F.lit(0)).alias("Incurred"),
            F.coalesce(df_cd["settledamount"], F.lit(0)).alias("Settled"),
            F.coalesce(df_cd["claimamount"], F.lit(0)).alias("Claimed"),
            F.when(df_cd["StageID"] == 23, F.lit(0))
            .otherwise(F.coalesce(df_cd["SanctionedAmount"], F.lit(0))).alias("Sanctioned"),
            F.coalesce(df_bs["SumInsured"], F.lit(0)).alias("coverageamount"),
            df_c["deleted"].alias("Claim_Deleted"),
            df_cd["deleted"].alias("Claimdetails_Deleted")
        )
    ).first()

    if not initial_vars_df:
        return 0.0

    Claimtype = initial_vars_df["Claimtype"]
    Requesttype = initial_vars_df["Requesttype"]
    Currentclaimstatus = initial_vars_df["Currentclaimstatus"]
    Incurred = initial_vars_df["Incurred"]
    Settled = initial_vars_df["Settled"]
    Claimed = initial_vars_df["Claimed"]
    Sanctioned = initial_vars_df["Sanctioned"]
    coverageamount = initial_vars_df["coverageamount"]
    Claim_Deleted = initial_vars_df["Claim_Deleted"]
    Claimdetails_Deleted = initial_vars_df["Claimdetails_Deleted"]

    if Claim_Deleted == 0 and Claimdetails_Deleted == 0:
        if Currentclaimstatus in ['Registration', 'For Adjudication', 'Refer to CS', 'Refer to Enrollment',
                                  'Query Response (H)', 'Query Response (M)', 'Insurer Response', 'CS Response',
                                  'Enrollment Response', 'Refer to Insurer', 'Insurer Authorization',
                                  'Investigation Done', 'Query to Hospital', 'Query to Member', 'First Reminder-Hospital',
                                  'Second Reminder-Hospital', 'Third Reminder-Hospital', 'Fourth Reminder-Hospital',
                                  'First Reminder-Member', 'Second Reminder-Member', 'Third Reminder-Member',
                                  'Fourth Reminder-Member', 'From CRM', 'For Investigation', 'For Bill Entry',
                                  'For Audit', 'Refer To CRM', 'Audit Return'] and Requesttype not in [1, 2, 3]:
            if Claimtype == 2:
                Incurred = min(coverageamount, Claimed)
            elif Claimtype == 1:
                Incurred = min(Sanctioned, Claimed)

        elif Requesttype in [1, 2, 3]:
            Incurred = min(Sanctioned, Claimed)

        elif Claimtype == 1:
            balance_sum_insured = balance_sum_insured_without_present_claim(claimid, slno)
            Incurred = min(balance_sum_insured if Sanctioned == 0 else Sanctioned, Claimed)
            

        elif Currentclaimstatus == 'Settled':
            Incurred = Settled

        elif Currentclaimstatus in ['Closed', 'Repudiated', 'Cancelled']:
            Incurred = 0

        elif Currentclaimstatus in ['For Payment', 'For Settlement'] and Claimtype == 2:
            Incurred = Sanctioned

    statuses = [
        'For Bill Entry', 'For Adjudication', 'Query to Hospital', 'Query to Member', 'Refer to CS', 'Refer to CRM',
        'Refer to Enrollment', 'Insurer Response', 'CS Response', 'Enrollment Response', 'Refer to Insurer',
        'For Investigation', 'Investigation Done', 'Sent for Audit', 'For Audit', 'Audit Return',
        'First Reminder-Hospital', 'Second Reminder-Hospital', 'Third Reminder-Hospital', 'Fourth Reminder-Hospital',
        'First Reminder-Member', 'Second Reminder-Member', 'Third Reminder-Member', 'Fourth Reminder-Member', 'From CRM'
    ]

    if Claimtype == 2 and Currentclaimstatus in statuses:
        B = balance_sum_insured_df.filter(F.col("ID") == claimid).select("IncurredAmount").first()

        if B:
            B_IncurredAmount = B["IncurredAmount"]

            CD = uvw_claimsdetails_df.filter((F.col("claimid") == claimid) & (F.col("slno") == slno)).first()

            if CD:
                BillAmount = F.coalesce(F.col("BillAmount"), F.lit(0))
                DeductionAmount = F.coalesce(F.col("DeductionAmount"), F.lit(0))
                ClaimedAmount = F.coalesce(F.col("ClaimedAmount"), F.lit(0))

                Incurred = F.when(
                    B_IncurredAmount > F.when(BillAmount > 0, 
                        F.when(BillAmount - F.when(CD.StageID.isin([26, 27]), DeductionAmount, F.lit(0)) > Claimed, Claimed)
                        .otherwise(BillAmount - F.when(CD.StageID.isin([26, 27]), DeductionAmount, F.lit(0))))
                    .otherwise(Claimed),
                    F.when(BillAmount > 0, 
                        F.when(BillAmount - F.when(CD.StageID.isin([26, 27]), DeductionAmount, F.lit(0)) > Claimed, Claimed)
                        .otherwise(BillAmount - F.when(CD.StageID.isin([26, 27]), DeductionAmount, F.lit(0))))
                    .otherwise(Claimed)
                ).otherwise(B_IncurredAmount)

    remarks_row = (
        df_claims_action_items
        .filter((F.col("claimid") == claimid) & (F.col("slno") == slno))
        .orderBy(F.desc("OpenDate"))
        .select("Remarks")
        .first()
    )
    
    remarks = remarks_row["Remarks"] if remarks_row else None

    if Incurred < 0 or (Currentclaimstatus in statuses and remarks and "Refer to Insurer – R" in remarks):
        Incurred = 0

    if Requesttype in [1, 2, 3] and Claimtype == 1 and Currentclaimstatus == 'Cashless Approved':
        Incurred = Sanctioned

    return Incurred

# COMMAND ----------

df3 = df1.withColumn(
    "IncurredAmount",
    lit(UDF_GetIncurredAmount(
        col("claimid"),
        col("slno")
    )))