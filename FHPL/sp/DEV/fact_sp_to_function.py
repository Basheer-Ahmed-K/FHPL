# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,incremental_claims_ALLInsurer
from pyspark.sql.functions import col, lit, row_number, concat_ws, when
from pyspark.sql.window import Window

spark.sql('use database fhpl')

claims_df = spark.table("Claims")
intimation_df = spark.table("Intimation")
memberpolicy_df = spark.table("Memberpolicy")
mst_issuingauthority_df = spark.table("mst_issuingauthority")
mst_relationship_df = spark.table("Mst_RelationShip")
policy_df = spark.table("Policy")
benefitplan_df = spark.table("BenefitPlan")
mst_corporate_df = spark.table("Mst_Corporate")
membersi_df = spark.table("MemberSi")
membercontacts_df = spark.table("MemberContacts")
mst_state_df = spark.table("mst_state")
bpsuminsured_df = spark.table("BPSuminsured")
claimsdetails_df = spark.table("Claimsdetails")
claimservicetype_df = spark.table("ClaimServiceType")
claimstage_df = spark.table("ClaimStage")
claimrequesttype_df = spark.table("claimrequesttype")
provider_df = spark.table("Provider")
providermou_df = spark.table("providerMOU")
mst_agent_df = spark.table("MST_Agent")
bpsiconditions_df = spark.table("BPSIConditions")
claimactionitems_df = spark.table("temp_ClaimActionItems")
insurerdumpretrievaltimestamp_df = spark.table("fhpl.dba_reports_insurerdumpretrievaltimestamp_allinsurers")

window_spec = Window.partitionBy("claimid").orderBy(col("claimid"), col("slno").desc())

claimsdetails_subquery = claimsdetails_df.filter(
    (col("deleted") == 0) & 
    (col("RequestTypeID").isin(1, 2, 3)) & 
    (col("stageid").isNotNull()) & 
    (~col("stageid").isin(23))
).withColumn(
    "rowno", row_number().over(window_spec)
).select(
    "Claimid", "slno", "sanctionedamount", "Remarks", "Diagnosis", "rowno"
)

incremental_claims_df = claims_df.alias("C").join(
    intimation_df.alias("I"), col("I.ClaimID") == col("C.ID"), "left_outer"
).join(
    memberpolicy_df.alias("M"), col("C.MemberPolicyID") == col("M.ID"), "inner"
).join(
    memberpolicy_df.alias("mp1"), col("mp1.id") == col("M.mainmemberid"), "inner"
).join(
    mst_issuingauthority_df.alias("IA"), col("IA.id") == col("C.IssueID"), "inner"
).join(
    mst_relationship_df.alias("Rs"), col("Rs.id") == col("M.RelationshipID"), "inner"
).join(
    policy_df.alias("P"), (col("P.ID") == col("M.PolicyID")) & (col("P.deleted") == 0), "inner"
).join(
    benefitplan_df.alias("bp"), (col("bp.ID") == col("P.BenefitPlanID")) & (col("bp.deleted") == 0), "inner"
).join(
    mst_corporate_df.alias("CP"), col("CP.id") == col("P.CorpID"), "left_outer"
).join(
    membersi_df.alias("MS"), (col("MS.memberpolicyid") == col("M.ID")) & (col("MS.deleted") == 0), "inner"
).join(
    membersi_df.alias("MS2"), (col("MS2.memberpolicyid") == col("mp1.ID")) & (col("MS2.deleted") == 0), "inner"
).join(
    membercontacts_df.alias("MA"), (col("MA.EntityID") == col("mp1.ID")) & (col("IsBaseAddress") == 1) & (col("ma.deleted") == 0), "left"
).join(
    mst_state_df.alias("S"), col("S.id") == col("MA.stateid"), "left"
).join(
    bpsuminsured_df.alias("BS"), (col("BS.id") == col("MS.BPSIID")) & (col("BS.deleted") == 0) & (col("BS.SICategoryID_P20") == 69), "inner"
).join(
    bpsuminsured_df.alias("BS2"), (col("BS2.id") == col("MS2.BPSIID")) & (col("BS2.deleted") == 0) & (col("BS2.SICategoryID_P20") == 69), "left"
).join(
    claimsdetails_df.alias("Cd"), (col("C.id") == col("cd.ClaimID")) & (col("cd.deleted") == 0), "inner"
).join(
    claimsdetails_subquery.alias("a"), (col("a.claimid") == col("cd.claimid")) & (col("a.rowno") == 1), "left_outer"
).join(
    claimservicetype_df.alias("st"), col("st.id") == col("c.ServiceTypeID"), "inner"
).join(
    claimstage_df.alias("CS"), col("cs.id") == col("cd.StageID"), "inner"
).join(
    claimrequesttype_df.alias("crt"), col("crt.id") == col("cd.requesttypeid"), "inner"
).join(
    provider_df.alias("Pr"), col("Pr.ID") == col("C.ProviderID"), "inner"
# ).join(
#     providermou_df.alias("PM"), (col("PM.ProviderID") == col("Pr.ID")) & (col("PM.MOUTypeID_P44") == 166) & (col("PM.ID").isin(
#         providermou_df.filter(
#             (col("providerid") == col("ProviderID")) &
            # (claims_df["DateofAdmission"].between(col("StartDate"), col("EndDate"))) &
#             (col("Deleted") == 0) &
#             (col("MOUTypeID_P44") == providermou_df["MOUTypeID_P44"])
#         ).selectExpr("max(id)").collect()
#     )),
#     "left_outer"
# ).join(
#     providermou_df.alias("PM1"), (col("PM1.ProviderID") == col("Pr.ID")) & (col("PM1.MOUTypeID_P44") == 167) & (col("M.IssueID") == col("PM1.MOUEntityID")) & (col("PM1.ID").isin(
#         providermou_df.filter(
#             (col("providerid") == col("PM1.providerid")) & 
#             (claims_df["DateofAdmission"].between(col("StartDate"), col("EndDate"))) & 
#             (col("Deleted") == 0) & 
#             (col("MOUEntityID") == col("M.IssueID"))
#         ).selectExpr("max(id)").collect()
#     )), "left_outer"
).join(
    mst_agent_df.alias("AGT"), col("P.AgentID") == col("AGT.ID"), "left"
).join(
    bpsiconditions_df.alias("BPSIC"), (col("BPSIC.BPSIID") == col("BS.ID")) & (col("BS.PolicyID") == col("BPSIC.PolicyID")) & (col("BPSIC.Deleted") == 0) & (col("BPSIC.BPConditionID") == 30) & (
        (concat_ws(",", lit(","), col("BPSIC.RelationshipID"), lit(","))).contains(concat_ws(",", lit("%,"), col("M.RelationshipID"), lit(",%"))) | col("BPSIC.RelationshipID").isNull()
    ), "left"
).join(
    claimactionitems_df.alias("CACT"), col("CACT.ClaimID") == col("CD.ClaimID"), "left"
).join(
    insurerdumpretrievaltimestamp_df.alias("IDR"), col("M.ISSUEID") == col("IDR.ISSUEID"), "left"
).filter(
    (col("C.deleted") == 0) & 
    (col("Cd.deleted") == 0) & 
    (col("IDR.IsActive") == 1) & 
    (
        (col("C.CreatedDatetime").isNull() | (col("C.CreatedDatetime") > col("IDR.LastDumpRetrievalDate"))) |
        (col("C.ModifiedDatetime").isNull() | (col("C.ModifiedDatetime") > col("IDR.LastDumpRetrievalDate"))) |
        (col("M.CreatedDatetime").isNull() | (col("M.CreatedDatetime") > col("IDR.LastDumpRetrievalDate"))) |
        (col("M.ModifiedDatetime").isNull() | (col("M.ModifiedDatetime") > col("IDR.LastDumpRetrievalDate"))) |
        (col("mp1.CreatedDatetime").isNull() | (col("mp1.CreatedDatetime") > col("IDR.LastDumpRetrievalDate"))) |
        (col("mp1.ModifiedDatetime").isNull() | (col("mp1.ModifiedDatetime") > col("IDR.LastDumpRetrievalDate"))) |
        (col("P.CreatedDatetime").isNull() | (col("P.CreatedDatetime") > col("IDR.LastDumpRetrievalDate"))) |
        (col("P.LastModifiedDate").isNull() | (col("P.LastModifiedDate") > col("IDR.LastDumpRetrievalDate"))) |
        (col("MA.CreatedDate").isNull() | (col("MA.CreatedDate") > col("IDR.LastDumpRetrievalDate"))) |
        (col("MA.ModifiedDate").isNull() | (col("MA.ModifiedDate") > col("IDR.LastDumpRetrievalDate"))) |
        (col("CD.CreatedDatetime").isNull() | (col("CD.CreatedDatetime") > col("IDR.LastDumpRetrievalDate"))) |
        (col("CD.ModifiedDatetime").isNull() | (col("CD.ModifiedDatetime") > col("IDR.LastDumpRetrievalDate"))) |
        (col("CACT.OpenDate").isNull() | (col("CACT.OpenDate") > col("IDR.LastDumpRetrievalDate"))) |
        (col("CACT.CloseDate").isNull() | (col("CACT.CloseDate") > col("IDR.LastDumpRetrievalDate"))) |
        (col("BS.CreatedDate").isNull() | (col("BS.CreatedDate") > col("IDR.LastDumpRetrievalDate"))) |
        (col("BS.LastModifiedDate").isNull() | (col("BS.LastModifiedDate") > col("IDR.LastDumpRetrievalDate"))) |
        (col("MS.ModifiedDateTime").isNull() | (col("MS.ModifiedDateTime") > col("IDR.LastDumpRetrievalDate")))
    )
).select(
    col("CD.ClaimID").alias("ClaimID"), lit(0).alias("IsProcessed")
).distinct()

display(incremental_claims_df)

# COMMAND ----------

# DBTITLE 1,temp_ICD_Levels
from pyspark.sql.functions import col

icd10_df = spark.table("fhpl.icd10")
root_nodes_df = icd10_df.filter(col("ParentID") == 0)

temp_icd_levels = root_nodes_df.alias("L1") \
    .join(icd10_df.alias("L2"), col("L2.ParentID") == col("L1.ID"), "left") \
    .join(icd10_df.alias("L3"), col("L3.ParentID") == col("L2.ID"), "left") \
    .join(icd10_df.alias("L4"), col("L4.ParentID") == col("L3.ID"), "left") \
    .join(icd10_df.alias("L5"), col("L5.ParentID") == col("L4.ID"), "left") \
    .join(icd10_df.alias("L6"), col("L6.ParentID") == col("L5.ID"), "left") \
    .join(icd10_df.alias("L7"), col("L7.ParentID") == col("L6.ID"), "left") \
    .select(
        col("L1.ID").alias("Level1ICDCode"), col("L1.DiseaseCode").alias("Level1DiseaseCode"), col("L1.Name").alias("Level1DiseaseName"),
        col("L2.ID").alias("Level2ICDCode"), col("L2.DiseaseCode").alias("Level2DiseaseCode"), col("L2.Name").alias("Level2DiseaseName"),
        col("L3.ID").alias("Level3ICDCode"), col("L3.DiseaseCode").alias("Level3DiseaseCode"), col("L3.Name").alias("Level3DiseaseName"),
        col("L4.ID").alias("Level4ICDCode"), col("L4.DiseaseCode").alias("Level4DiseaseCode"), col("L4.Name").alias("Level4DiseaseName"),
        col("L5.ID").alias("Level5ICDCode"), col("L5.DiseaseCode").alias("Level5DiseaseCode"), col("L5.Name").alias("Level5DiseaseName"),
        col("L6.ID").alias("Level6ICDCode"), col("L6.DiseaseCode").alias("Level6DiseaseCode"), col("L6.Name").alias("Level6DiseaseName"),
        col("L7.ID").alias("Level7ICDCode"), col("L7.DiseaseCode").alias("Level7DiseaseCode"), col("L7.Name").alias("Level7DiseaseName")
    )


# COMMAND ----------

temp_icd_levels.display()

# COMMAND ----------

# DBTITLE 1,temp_CliamActionItems
claim_action_items_df = spark.table("fhpl.claimactionitems")
claims_df = spark.table("fhpl.claims")
insurer_dump_retrieval_df = spark.table("fhpl.dba_reports_insurerdumpretrievaltimestamp_allinsurers")

joined_df = claim_action_items_df.join(claims_df, claim_action_items_df.ClaimID == claims_df.ID) \
                                 .join(insurer_dump_retrieval_df, claims_df.IssueID == insurer_dump_retrieval_df.ISSUEID) \
                                 .filter(insurer_dump_retrieval_df.IsActive == 1)

temp_ClaimActionItems = joined_df.groupBy("ClaimId") \
                     .agg({"OpenDate": "max", "CloseDate": "max"}) \
                     .withColumnRenamed("max(OpenDate)", "OpenDate") \
                     .withColumnRenamed("max(CloseDate)", "CloseDate")

temp_ClaimActionItems.createOrReplaceTempView("ClaimActionItems")

# COMMAND ----------

# IncrementalClaims_AllInsurers
# dba_reports..ReprocessClaimList
# DBA_Reports..IncrementalClaims_AllInsurers
# dba_reports..AllInsurers_db_claims_temp_tbd

# COMMAND ----------

# DBTITLE 1,temp_TEMPRDNICDCODE
incremental_claims_df = spark.table("fhpl.dba_reports_incrementalclaims_allinsurers")
claims_coding_df = spark.table("fhpl.claimscoding")
icd_levels_df = spark.table("fhpl.temp_icd_levels")
claims_coding_filtered_df = claims_coding_df.filter(col("Deleted") == 0)

temp_TEMPRDNICDCODE = claims_coding_filtered_df.alias("cd") \
    .join(incremental_claims_df.alias("ir"), col("ir.ClaimID") == col("cd.ClaimID"), "inner") \
    .join(icd_levels_df.alias("icd"), col("cd.ICDCode") == col("icd.Level7ICDCode"), "left") \
    .select(
        col("cd.ID"), col("cd.ClaimID"), col("cd.slno"),
        col("icd.Level1DiseaseName"), col("icd.Level2DiseaseName"), col("icd.Level3DiseaseName"),
        col("icd.Level1DiseaseCode"), col("icd.Level2DiseaseCode"), col("icd.Level3DiseaseCode")
    ) \
    .union(
        claims_coding_filtered_df.alias("cd")
        .join(incremental_claims_df.alias("ir"), col("ir.ClaimID") == col("cd.ClaimID"), "inner")
        .join(icd_levels_df.alias("icd"), col("cd.ICDCode") == col("icd.Level6ICDCode"), "left")
        .select(
            col("cd.ID"), col("cd.ClaimID"), col("cd.slno"),
            col("icd.Level1DiseaseName"), col("icd.Level2DiseaseName"), col("icd.Level3DiseaseName"),
            col("icd.Level1DiseaseCode"), col("icd.Level2DiseaseCode"), col("icd.Level3DiseaseCode")
        )
    ) \
    .union(
        claims_coding_filtered_df.alias("cd")
        .join(incremental_claims_df.alias("ir"), col("ir.ClaimID") == col("cd.ClaimID"), "inner")
        .join(icd_levels_df.alias("icd"), col("cd.ICDCode") == col("icd.Level5ICDCode"), "left")
        .select(
            col("cd.ID"), col("cd.ClaimID"), col("cd.slno"),
            col("icd.Level1DiseaseName"), col("icd.Level2DiseaseName"), col("icd.Level3DiseaseName"),
            col("icd.Level1DiseaseCode"), col("icd.Level2DiseaseCode"), col("icd.Level3DiseaseCode")
        )
    ) \
    .union(
        claims_coding_filtered_df.alias("cd")
        .join(incremental_claims_df.alias("ir"), col("ir.ClaimID") == col("cd.ClaimID"), "inner")
        .join(icd_levels_df.alias("icd"), col("cd.ICDCode") == col("icd.Level4ICDCode"), "left")
        .select(
            col("cd.ID"), col("cd.ClaimID"), col("cd.slno"),
            col("icd.Level1DiseaseName"), col("icd.Level2DiseaseName"), col("icd.Level3DiseaseName"),
            col("icd.Level1DiseaseCode"), col("icd.Level2DiseaseCode"), col("icd.Level3DiseaseCode")
        )
    ) \
    .union(
        claims_coding_filtered_df.alias("cd")
        .join(incremental_claims_df.alias("ir"), col("ir.ClaimID") == col("cd.ClaimID"), "inner")
        .join(icd_levels_df.alias("icd"), col("cd.ICDCode") == col("icd.Level3ICDCode"), "left")
        .select(
            col("cd.ID"), col("cd.ClaimID"), col("cd.slno"),
            col("icd.Level1DiseaseName"), col("icd.Level2DiseaseName"), col("icd.Level3DiseaseName"),
            col("icd.Level1DiseaseCode"), col("icd.Level2DiseaseCode"), col("icd.Level3DiseaseCode")
        )
    ) \
    .union(
        claims_coding_filtered_df.alias("cd")
        .join(incremental_claims_df.alias("ir"), col("ir.ClaimID") == col("cd.ClaimID"), "inner")
        .join(icd_levels_df.alias("icd"), col("cd.ICDCode") == col("icd.Level2ICDCode"), "left")
        .select(
            col("cd.ID"), col("cd.ClaimID"), col("cd.slno"),
            col("icd.Level1DiseaseName"), col("icd.Level2DiseaseName"), col("icd.Level3DiseaseName"),
            col("icd.Level1DiseaseCode"), col("icd.Level2DiseaseCode"), col("icd.Level3DiseaseCode")
        )
    ) \
    .union(
        claims_coding_filtered_df.alias("cd")
        .join(incremental_claims_df.alias("ir"), col("ir.ClaimID") == col("cd.ClaimID"), "inner")
        .join(icd_levels_df.alias("icd"), col("cd.ICDCode") == col("icd.Level1ICDCode"), "left")
        .select(
            col("cd.ID"), col("cd.ClaimID"), col("cd.slno"),
            col("icd.Level1DiseaseName"), col("icd.Level2DiseaseName"), col("icd.Level3DiseaseName"),
            col("icd.Level1DiseaseCode"), col("icd.Level2DiseaseCode"), col("icd.Level3DiseaseCode")
        )
    ) \
    .orderBy(desc("ID"))

temp_TEMPRDNICDCODE.createOrReplaceTempView("TEMPRDNICDCODE")
display(temp_TEMPRDNICDCODE)

# COMMAND ----------

# temp_icd_levels.write.mode("overwrite").saveAsTable("fhpl.temp_icd_levels")

# COMMAND ----------

# DBTITLE 1,136 - 202
from pyspark.sql.functions import col, concat, lit, expr, coalesce, when
from pyspark.sql.types import DateType

# Load necessary tables
intimation_df = spark.table("intimation")
claims_df = spark.table("claims")
claims_details_df = spark.table("claimsdetails")
member_df = spark.table("memberpolicy")
policy_df = spark.table("policy")
mst_gender_df = spark.table("mst_gender")
# mst_agetype_df = spark.table("mst_agetype")
mst_relationship_df = spark.table("mst_relationship")
# member_address_df = spark.table("member_address")
# mst_district_df = spark.table("mst_district")
mst_state_df = spark.table("mst_state")
claims_coding_df = spark.table("claimscoding")
tpa_procedures_df = spark.table("tpaprocedures")
mst_pcs_df = spark.table("mst_pcs")
provider_df = spark.table("provider")
mst_issuingauthority_df = spark.table("fhpl.mst_issuingauthority").alias("IA")
membercontacts_df = spark.table('fhpl.membercontacts').alias('MA')
mst_corporate_df = spark.table('fhpl.mst_corporate').alias('cp')

# Convert specific SQL joins and selections to PySpark

result_df = intimation_df.alias("I") \
    .join(claims_df.alias("C"), col("C.ID") == col("I.ClaimID"), "left") \
    .join(claims_details_df.alias("CD"), col("C.ID") == col("CD.ClaimID"), "left") \
    .join(member_df.alias("M"), col("C.MemberPolicyID") == col("M.ID"), "left") \
    .join(policy_df.alias("P"), col("M.PolicyID") == col("P.ID"), "left") \
    .join(mst_gender_df.alias("G"), col("M.GenderID") == col("G.ID"), "left") \
    .join(mst_relationship_df.alias("Rs"), col("M.RelationshipID") == col("Rs.ID"), "left") \
    .join(provider_df.alias("Pr"), col("C.ProviderID") == col("Pr.ID"), "left")\
    # .join(member_address_df.alias("MA"), col("M.ID") == col("MA.MemberID"), "left") \
    # .join(mst_district_df.alias("D"), col("MA.DistrictID") == col("D.ID"), "left") \
    # .join(mst_state_df.alias("S"), col("MA.StateID") == col("S.ID"), "left") \
    # .join(mst_agetype_df.alias("A"), col("M.AgeTypeID") == col("A.ID"), "left") \

# Apply the necessary transformations
result_df = result_df.select(
    col("I.ID").alias("IntimationID"),
    col("I.IntimationDate"),
    col("C.ID").cast("bigint").alias("claimid"),
    col("CD.slno").alias("Slno"),
    concat(mst_issuingauthority_df["code"], lit("."), col("M.uhidno")).alias("UHIDNO"),
    col("M.InsUhidno").alias("InsUHIDNo"),
    col("M.MemberName").alias("Membername"),
    concat(mst_issuingauthority_df["code"], lit("."), col("M.uhidno")).alias("MainMemuhidno"),
    col("M.MemberName").alias("MainMemname"),
    col("G.name").alias("Gender"),
    when(col("M.DOB") == '', None).otherwise(col("M.DOB").cast(DateType())).alias("DOB"),
    # concat(col("M.age").cast("varchar"), lit(" "), col("A.NAME")).alias("age"),
    col("Rs.name").alias("relationship"),
    col("M.employeeid"),
    concat(coalesce(membercontacts_df["STDCode"]), lit('')), lit(' '), coalesce(membercontacts_df["PhoneNo"]), lit(''))).alias("Phone1"),
    lit('').alias("Phone2"),
    coalesce(membercontacts_df["MobileNo"], col("C.Mobile"), lit('')).alias("Mobile"),
    col(membercontacts_df["EmailID"]).alias("Email"),
    col("M.PolicyNo"),
    col("P.StartDate").alias("PolicyStartDate"),
    col("M.MemberCommencingDate").alias("PolicyCommencingDate"),
    col("P.EndDate").alias("PolicyExpiryDate"),
    col("M.DOL").alias("MemberDiscontinuedDate"),
    col("Ms.netpremium").alias("premium"),
    expr("CASE WHEN coalesce(P.RenewedCount, 0) = 0 THEN 'Fresh' ELSE 'Renewal' END").alias("polStatus"),
    expr("CASE WHEN p.CorpID = 0 THEN 'Individual' ELSE cp.Name END").alias("Organisationname"),
    col("BS.SumInsured").alias("coverageamount"),
    expr("CASE WHEN p.PolicyTypeID_P2 = 4 THEN coalesce(ms.cb_amount, (SELECT cb_amount FROM membersi ms1 WHERE ms1.memberpolicyid = m.MainmemberID AND ms1.deleted = 0 AND ms.cb_amount IS NULL)) ELSE ms.cb_amount END").alias("CumulativeBonus"),
    expr("CASE WHEN M.IssueID = 20 THEN COALESCE(m.Partycode, m.agentcode) ELSE COALESCE(AGT.Code, M.AgentCode) END").alias("agentcode"),
    expr("IF(CD.RequestTypeID >= 4, CAST(Cd.ReceivedDate AS DATE), Cd.ReceivedDate)").alias("Claimreceiveddate"),
    expr("IF(CD.RequestTypeID >= 4, C.DateOfAdmission, coalesce(C.ProbableDOA, C.DateOfAdmission))").alias("Admdate"),
    col("C.DateOfDischarge").alias("DisDate"),
    col("cd.Diagnosis"),
    col("ST.name").alias("ServiceType"),
    lit('').alias("DisAllowenceReason1"),
    lit('').alias("DisAllowenceReason2"),
    lit('').alias("DisAllowenceReason3"),
    lit('').alias("DisAllowenceReason4"),
    lit('').alias("DisAllowenceReason5"),
    lit('').alias("DisAllowenceReason6"),
    lit('').alias("DisAllowenceReason7"),
    lit('').alias("DisAllowenceReason8"),
    lit('').alias("DisAllowenceReason9"),
    lit('').alias("DisAllowenceReason10"),
    expr("(SELECT tp.FHPLCode FROM ClaimsCoding cc, TPAProcedures tp WHERE cc.TPAProcedureID = tp.ID AND cc.ClaimID = cd.ClaimID AND cc.Slno = cd.Slno ORDER BY cc.Createddatetime DESC LIMIT 1)").alias("Fhpcode"),
    expr("(SELECT tp.Level3 FROM ClaimsCoding cc, TPAProcedures tp WHERE cc.TPAProcedureID = tp.ID AND cc.ClaimID = cd.ClaimID AND cc.Slno = cd.Slno ORDER BY cc.Createddatetime DESC LIMIT 1)").alias("FhpDisease"),
    )
result_df = result_df.withColumn(
      "Roname",
      F.when(F.col(m_col + ".IssueID") == 20, "Kotak Mahindra General Insurance Company Ltd")
      .when(F.col(m_col + ".IssueID") == 21, "TATA AIG General Insurance Co. Ltd.")
      .otherwise(
          F.coalesce(
              mst_payer_df.where(F.col("ID") == F.concat(F.col(p_col + ".payerid"), F.lit("0000"))).select("name").alias("name"),
              mst_payer_df.where(F.col("ID") == F.col(p_col + ".payerid")).select("name").alias("name"),
              F.lit("")
          )
      )
  ).withColumn(
      "icdcodeFirstLevel",
      F.first(F.col(l_col + ".Level1DiseaseName")).over(window_spec)
  ).withColumn(
      "icdcodeSecondLevel",
      F.first(F.col(l_col + ".Level2DiseaseName")).over(window_spec)
  ).withColumn(
      "icdcodeThirdLevel",
      F.first(F.col(l_col + ".Level3DiseaseName")).over(window_spec)
  ).join(
      p_col.where(F.col(p_col + ".id") == F.cast(F.col(cc_col + ".pcscode"), "string")),
      on=F.col(cc_col + ".pcscode") == F.col(p_col + ".id"), how="left"
  ).withColumn(
      "final_procedure_code",
      F.first(F.col(p_col + ".Code")).over(window_spec)
  ).withColumn(
      "ClaimType",
      F.when(F.col(cd_col + ".ClaimTypeID") == 1, "PP")
      .when(F.col(cd_col + ".claimtypeid") == 2, "MR")
      .otherwise("")
  ).withColumn(
      "Providername", F.col(pr_col + ".Name")
  ).withColumn(
      "provideraddress",
      F.left(
          F.concat(
              F.coalesce(F.replace(F.col(pr_col + ".Address1"), '"', " "), lit("")),
              lit(" "),
              F.coalesce(F.replace(F.col(pr_col + ".Address2"), '"', " "), lit(""))
          ),
          4000
      )
  ).withColumn(
      "providerplace", F.col(pr_col + ".Location")
  ).withColumn(
      "providerstate",
      F.first(F.col(ms_col + ".Name")).over(F.windowSpec.partitionBy(F.col(pr_col + ".StateID")).orderBy(lit(1)))
  ).withColumn(
      "ProviderPincode", F.col(pr_col + ".PINCode")
  ).withColumn(
      "ProviderSTDCode", F.col(pr_col + ".STDCode")
  ).withColumn(
      "ProviderPhoneNo1", F.col(pr_col + ".RegLandlineNo")
  ).withColumn(
      "claimamount", F.col(cd_col + ".claimamount")
  ).withColumn(
      "billedamount", F.coalesce(F.col(cd_col + ".BillAmount"), lit(0))
  ).withColumn(
      "SanctionedAmount",
      F.when(F.col(cd_col + ".stageid").isin(23), 0)
      .otherwise(F.col(cd_col + ".SanctionedAmount"))
  ).withColumn(
      "ProviderPhoneNo2", lit("")
  ).withColumn(
      "ProviderPhoneNo3", lit("")
  ).withColumn(
      "ProviderPhoneNo4", lit("")
  ).withColumn(
      "ProviderFaxNo", F.col(pr_col + ".RegFaxNo")
  ).withColumn(
      "ProviderMobileNo", F.col(pr_col + ".RegMobileNo")
  ).withColumn(
      "ProviderEmail", F.col(pr_col + ".RegEmailID")
  ).withColumn("Organisationname", F.coalesce(F.col("Organisationname"), lit("Unknown"))).withColumn(
      "Address",
      F.concat(
          F.col("Address1"),
          lit(" "),
          F.col("Address2"),
          F.lit(" ("),
          F.col("pincode"),
          F.lit(") ")
      )
  ).withColumn("memplace", F.col("Location"))\
   .withColumn("Distict", F.col("D.NAME"))\
   .withColumn("State", F.coalesce(F.col("S.name"), lit("")))


# COMMAND ----------

from pyspark.sql.functions import col, concat, lit, expr, coalesce, when
from pyspark.sql.types import DateType

# Load necessary tables
intimation_df = spark.table("intimation")
claims_df = spark.table("claims")
claims_details_df = spark.table("claimsdetails")
member_df = spark.table("memberpolicy")
policy_df = spark.table("policy")
mst_gender_df = spark.table("mst_gender")
mst_relationship_df = spark.table("mst_relationship")
mst_state_df = spark.table("mst_state")
claims_coding_df = spark.table("claimscoding")
tpa_procedures_df = spark.table("tpaprocedures")
mst_pcs_df = spark.table("mst_pcs")
provider_df = spark.table("provider")
mst_issuingauthority_df = spark.table("fhpl.mst_issuingauthority").alias("IA")
membercontacts_df = spark.table('fhpl.membercontacts').alias('MA')
membersi_df = spark.table("fhpl.membersi").alias('Ms')
mst_corporate_df = spark.table('fhpl.mst_corporate').alias('cp')

# Convert specific SQL joins and selections to PySpark

result_df = intimation_df.alias("I") \
    .join(claims_df.alias("C"), col("C.ID") == col("I.ClaimID"), "left") \
    .join(claims_details_df.alias("CD"), col("C.ID") == col("CD.ClaimID"), "left") \
    .join(member_df.alias("M"), col("C.MemberPolicyID") == col("M.ID"), "left") \
    .join(policy_df.alias("P"), col("M.PolicyID") == col("P.ID"), "left") \
    .join(mst_gender_df.alias("G"), col("M.GenderID") == col("G.ID"), "left") \
    .join(mst_relationship_df.alias("Rs"), col("M.RelationshipID") == col("Rs.ID"), "left") \
    .join(provider_df.alias("Pr"), col("C.ProviderID") == col("Pr.ID"), "left")

# Apply the necessary transformations
result_df = result_df.select(
    col("I.ID").alias("IntimationID"),
    col("I.IntimationDate"),
    col("C.ID").cast("bigint").alias("claimid"),
    col("CD.slno").alias("Slno"),
    concat(mst_issuingauthority_df["code"], lit("."), col("M.uhidno")).alias("UHIDNO"),
    col("M.InsUhidno").alias("InsUHIDNo"),
    col("M.MemberName").alias("Membername"),
    concat(mst_issuingauthority_df["code"], lit("."), col("M.uhidno")).alias("MainMemuhidno"),
    col("M.MemberName").alias("MainMemname"),
    col("G.name").alias("Gender"),
    when(col("M.DOB") == '', None).otherwise(col("M.DOB").cast(DateType())).alias("DOB"),
    col("Rs.name").alias("relationship"),
    col("M.employeeid"),
    concat(
        coalesce(membercontacts_df["STDCode"], lit('')),
        lit(' '),
        coalesce(membercontacts_df["PhoneNo"], lit(''))
    ).alias("Phone1"),
    lit('').alias("Phone2"),
    coalesce(membercontacts_df["MobileNo"], col("C.Mobile"), lit('')).alias("Mobile"),
    membercontacts_df["EmailID"].alias("Email"),
    col("M.PolicyNo"),
    col("P.StartDate").alias("PolicyStartDate"),
    col("M.MemberCommencingDate").alias("PolicyCommencingDate"),
    col("P.EndDate").alias("PolicyExpiryDate"),
    col("M.DOL").alias("MemberDiscontinuedDate"),
    membersi_df["netpremium"].alias("premium"),
    expr("CASE WHEN coalesce(P.RenewedCount, 0) = 0 THEN 'Fresh' ELSE 'Renewal' END").alias("polStatus"),
    expr("CASE WHEN p.CorpID = 0 THEN 'Individual' ELSE cp.Name END").alias("Organisationname"),
    col("BS.SumInsured").alias("coverageamount"),
    expr("CASE WHEN p.PolicyTypeID_P2 = 4 THEN coalesce(ms.cb_amount, (SELECT cb_amount FROM membersi ms1 WHERE ms1.memberpolicyid = m.MainmemberID AND ms1.deleted = 0 AND ms.cb_amount IS NULL)) ELSE ms.cb_amount END").alias("CumulativeBonus"),
    expr("CASE WHEN M.IssueID = 20 THEN COALESCE(m.Partycode, m.agentcode) ELSE COALESCE(AGT.Code, M.AgentCode) END").alias("agentcode"),
    expr("IF(CD.RequestTypeID >= 4, CAST(Cd.ReceivedDate AS DATE), Cd.ReceivedDate)").alias("Claimreceiveddate"),
    expr("IF(CD.RequestTypeID >= 4, C.DateOfAdmission, coalesce(C.ProbableDOA, C.DateOfAdmission))").alias("Admdate"),
    col("C.DateOfDischarge").alias("DisDate"),
    col("cd.Diagnosis"),
    col("ST.name").alias("ServiceType"),
    lit('').alias("DisAllowenceReason1"),
    lit('').alias("DisAllowenceReason2"),
    lit('').alias("DisAllowenceReason3"),
    lit('').alias("DisAllowenceReason4"),
    lit('').alias("DisAllowenceReason5"),
    lit('').alias("DisAllowenceReason6"),
    lit('').alias("DisAllowenceReason7"),
    lit('').alias("DisAllowenceReason8"),
    lit('').alias("DisAllowenceReason9"),
    lit('').alias("DisAllowenceReason10"),
    expr("(SELECT tp.FHPLCode FROM ClaimsCoding cc, TPAProcedures tp WHERE cc.TPAProcedureID = tp.ID AND cc.ClaimID = cd.ClaimID AND cc.Slno = cd.Slno ORDER BY cc.Createddatetime DESC LIMIT 1)").alias("Fhpcode"),
    expr("(SELECT tp.Level3 FROM ClaimsCoding cc, TPAProcedures tp WHERE cc.TPAProcedureID = tp.ID AND cc.ClaimID = cd.ClaimID AND cc.Slno = cd.Slno ORDER BY cc.Createddatetime DESC LIMIT 1)").alias("FhpDisease")
)

result_df = result_df.withColumn(
    "Roname",
    when(col("M.IssueID") == 20, "Kotak Mahindra General Insurance Company Ltd")
    .when(col("M.IssueID") == 21, "TATA AIG General Insurance Co. Ltd.")
    .otherwise(
        coalesce(
            mst_payer_df.where(col("ID") == concat(col("P.payerid"), lit("0000"))).select("name").alias("name"),
            mst_payer_df.where(col("ID") == col("P.payerid")).select("name").alias("name"),
            lit("")
        )
    )
).withColumn(
    "icdcodeFirstLevel",
    first(col("L.Level1DiseaseName")).over(window_spec)
).withColumn(
    "icdcodeSecondLevel",
    first(col("L.Level2DiseaseName")).over(window_spec)
).withColumn(
    "icdcodeThirdLevel",
    first(col("L.Level3DiseaseName")).over(window_spec)
).join(
    p_col.where(col("P.id") == cast(col("CC.pcscode"), "string")),
    on=col("CC.pcscode") == col("P.id"), how="left"
).withColumn(
    "final_procedure_code",
    first(col("P.Code")).over(window_spec)
).withColumn(
    "ClaimType",
    when(col("CD.ClaimTypeID") == 1, "PP")
    .when(col("CD.claimtypeid") == 2, "MR")
    .otherwise("")
).withColumn(
    "Providername", col("Pr.Name")
).withColumn(
    "provideraddress",
    expr("LEFT(CONCAT(COALESCE(REPLACE(Pr.Address1, '\"', ' '), ''), ' ', COALESCE(REPLACE(Pr.Address2, '\"', ' '), '')), 4000)")
).withColumn(
    "providerplace", col("Pr.Location")
).withColumn(
    "providerstate",
    first(col("MS.Name")).over(window_spec.partitionBy(col("Pr.StateID")).orderBy(lit(1)))
).withColumn(
    "ProviderPincode", col("Pr.PINCode")
).withColumn(
    "ProviderSTDCode", col("Pr.STDCode")
).withColumn(
    "ProviderPhoneNo1", col("Pr.RegLandlineNo")
).withColumn(
    "claimamount", col("CD.claimamount")
).withColumn(
    "billedamount", coalesce(col("CD.BillAmount"), lit(0))
).withColumn(
    "SanctionedAmount",
    when(col("CD.stageid").isin(23), 0)
    .otherwise(col("CD.SanctionedAmount"))
).withColumn(
    "ProviderPhoneNo2", lit("")
).withColumn(
    "ProviderPhoneNo3", lit("")
).withColumn(
    "ProviderPhoneNo4", lit("")
).withColumn(
    "ProviderFaxNo", col("Pr.RegFaxNo")
).withColumn(
    "ProviderMobileNo", col("Pr.RegMobileNo")
).withColumn(
    "ProviderEmail", col("Pr.RegEmailID")
).withColumn(
    "Organisationname", coalesce(col("Organisationname"), lit("Unknown"))
).withColumn(
    "Address",
    concat(
        col("Address1"),
        lit(" "),
        col("Address2"),
        lit(" ("),
        col("pincode"),
        lit(") ")
    )
).withColumn(
    "memplace", col("Location")
).withColumn(
    "Distict", col("D.NAME")
).withColumn(
    "State", coalesce(col("S.name"), lit(""))
)

display(result_df)