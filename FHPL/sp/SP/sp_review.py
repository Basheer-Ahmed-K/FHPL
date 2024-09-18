# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.functions import sum as _sum

# COMMAND ----------

# MAGIC %md
# MAGIC #### Dis_Rejection_Reason

# COMMAND ----------

# DBTITLE 1,Dis_Rej_reason
def udf_rejection_reason(claimid, slno):
    part1 = claim_rejection_reasons_df.alias("CRR").join(
        mst_rejection_reasons_df.alias("RR"),
        (col("CRR.RejectionReasonsID") == col("RR.ID")) & (col("RR.Deleted") == 0)
    ).join(
        claims_df.alias("C"),
        col("C.ID") == col("CRR.ClaimID")
    ).join(
        member_policy_df.alias("MD"),
        col("C.MemberPolicyID") == col("MD.ID")
    ).filter(
        (col("CRR.Deleted") == 0) &
        (col("C.Deleted") == 0) &
        (col("MD.Deleted") == 0) &
        (col("CRR.claimid") == lit(claimid)) &
        (col("CRR.slno") == lit(slno))
    ).selectExpr(
        "replace(replace(replace(replace(replace(replace(RR.Name, '|FT|', coalesce(CRR.FreeText1, '')), '|FT1|', coalesce(CRR.FreeText1, '')), '~DOA~', date_format(C.DateOfAdmission, 'dd-MM-yyyy')), '~DOD~', coalesce(date_format(C.DateOfDischarge, 'dd-MM-yyyy'), '')), '~PSD~', date_format(MD.MemberCommencingDate, 'dd-MM-yyyy')), '~PID~', coalesce(date_format(MD.MemberInceptionDate, 'dd-MM-yyyy'), '')) as Reason1"
    )
# Comment By Anand
# This select expression to be checked later and if the result is not correct.

    part2 = claim_rejection_reasons_df.alias("CRR").join(
        claims_df.alias("C"),
        col("C.ID") == col("CRR.ClaimID")
    ).join(
        member_policy_df.alias("MD"),
        col("C.MemberPolicyID") == col("MD.ID")
    ).filter(
        (col("CRR.Deleted") == 0) &
        (col("C.Deleted") == 0) &
        (col("MD.Deleted") == 0) &
        (col("CRR.RejectionReasonsID") == 0) &
        (col("CRR.Remarks").isNotNull()) &
        (col("CRR.claimid") == lit(claimid)) &
        (col("CRR.slno") == lit(slno))
    ).select(col("CRR.Remarks").alias("Reason1"))

    part3 = claims_details_df.filter(
        (col("claimid") == lit(claimid)) &
        (col("slno") == lit(slno)) &
        (col("reason").isNotNull()) &
        (col("createddatetime") < lit('2016-05-12')) &
        (col("deleted") == 0)
    ).select(col("reason").alias("Reason1"))

    part4 = claims_details_df.filter(
        (col("claimid") == lit(claimid)) &
        (col("slno") == lit(slno)) &
        (col("remarks").isNotNull()) &
        (col("createddatetime") < lit('2016-05-12')) &
        (col("requesttypeid").isin(1, 2, 3)) &
        (col("stageid") == 23)
    ).select(col("remarks").alias("Reason1"))

    combined = part1.union(part2).union(part3).union(part4)

    result = combined.select(concat_ws(",", col("Reason1")).alias("Reasons")).collect()

    if result:
        content = [row["Reasons"] for row in result]
        output = ", ".join(content)
        return output
    else:
        return ""

# COMMAND ----------

# MAGIC %md
# MAGIC #### DisAllowenceReason1

# COMMAND ----------


def udf_disallowence_reason(claimid, slno):
    # Filter and join DataFrames
    filtered_df = claim_deduction_details_df.filter(
        (col("deleted") == 0) & 
        (col("claimid") == claimid) & 
        (col("slno") == slno)
    ).join(
        mst_deduction_reasons_df, claim_deduction_details_df["DeductionReasonID"] == mst_deduction_reasons_df["id"]
    )
    
    # Replace unwanted characters in FreeTextValue
    unwanted_chars = [
        '\u0000', '\u0001', '\u0002', '\u0003', '\u0004', '\u0005', '\u0006', '\u0007', 
        '\u0008', '\u000B', '\u000C', '\u000E', '\u000F', '\u0010', '\u0011', '\u0012', 
        '\u0013', '\u0014', '\u0015', '\u0016', '\u0017', '\u0018', '\u0019', '\u001A', 
        '\u001B', '\u001C', '\u001D', '\u001E', '\u001F', '|FT|'
    ]
    cleaned_df = filtered_df.withColumn(
        "cleaned_text", 
        col("FreeTextValue")
    )
    for char in unwanted_chars:
        cleaned_df = cleaned_df.withColumn(
            "cleaned_text", 
            when(col("cleaned_text").contains(char), regexp_replace(col("cleaned_text"), char, '')).otherwise(col("cleaned_text"))
        )
            
    # Concatenate the desired string
    result_df = cleaned_df.withColumn(
        "reason", 
        concat_ws(", ", lit("Rs."), col("deductionamount"), col("name"), col("cleaned_text"))
    ).orderBy("serviceid", "deductionslno")
    
    # Collect and return as a single string
    reason = result_df.select("reason").rdd.flatMap(lambda x: x).collect()
    return ", ".join(reason)

# COMMAND ----------

# MAGIC %md
# MAGIC #### UFN_IRPendingReasons

# COMMAND ----------

# DBTITLE 1,UFN_IRPendingReasons
# def UFN_IRPendingReasons(claimid: int = None, slno: int = None, IsMandatory: int = None, IsReceived: int = None) -> str:
#     joined_df = claims_ir_reasons_df.alias('cr') \
#         .join(mst_irdocuments_df.alias('ir'), col('cr.IRDocumentID') == col('ir.ID'), 'left') \
#         .join(claims_df.alias('c'), col('cr.ClaimID') == col('c.id'), 'left') \
#         .join(mst_issuingauthority_df.alias('i'), col('c.issueid') == col('i.id'), 'left')

#     joined_df = joined_df.filter((col('cr.ClaimID') == claimid) & (col('cr.Slno') == slno) & (col('cr.Deleted') == 0) &
#                 (claims_ir_reasons_df["isMandatory"] == (IsMandatory if IsMandatory is not None else claims_ir_reasons_df["isMandatory"])) &
#                 (claims_ir_reasons_df["IsReceived"] == (IsReceived if IsReceived is not None else claims_ir_reasons_df["IsReceived"])))

#     reason_df = joined_df.select(
#         when(claims_ir_reasons_df["IRDocumentID"] == 0, claims_ir_reasons_df["Remarks"])
#         .otherwise(concat_ws('', 
#                              when(col('ir.Name').contains('~INS~'), col('i.Name')).otherwise(col('ir.Name')),
#                              when(col('ir.Name').contains('|FT|'), col('cr.FreeText1')).otherwise(lit('')),
#                              when(col('ir.Name').contains('|FT1|'), col('cr.FreeText2')).otherwise(lit(''))
#                             )).alias('Reason')
#     ).distinct()

#     union_df = reason_df.union(
#         claims_ir_reasons_df.filter((col('ClaimID') == claimid) & (col('slno') == slno) & (col('Deleted') == 0) &
#                                     (claims_ir_reasons_df["isMandatory"] == (IsMandatory if IsMandatory is not None else claims_ir_reasons_df["isMandatory"])) &
#                                     (claims_ir_reasons_df["IsReceived"] == (IsReceived if IsReceived is not None else claims_ir_reasons_df["IsReceived"])) &
#                                     (claims_ir_reasons_df["IRDocumentID"] == 0) & (claims_ir_reasons_df["serviceid"] == 0))
#         .select(claims_ir_reasons_df["Remarks"].alias('Reason'))
#     ).union(
#         claims_ir_reasons_df.alias('cr')
#         .join(mst_services_df.alias('s'), col('cr.serviceid') == col('s.ID'), 'left')
#         .filter((col('cr.ClaimID') == claimid) & (col('slno') == slno) & (col('cr.Deleted') == 0) &
#                 (claims_ir_reasons_df["isMandatory"] == (IsMandatory if IsMandatory is not None else claims_ir_reasons_df["isMandatory"])) &
#                 (claims_ir_reasons_df["IsReceived"] == (IsReceived if IsReceived is not None else claims_ir_reasons_df["IsReceived"])) &
#                 (claims_ir_reasons_df["IRDocumentID"] == 0))
#         .select(claims_ir_reasons_df["Remarks"].alias('Reason'))
#     )

#     result_df = union_df.select(concat_ws('.', col('Reason')).alias('Reasons'))

#     result = result_df.collect()[0]['Reasons'] if result_df.count() > 0 else None

#     result = union_df.select(concat_ws(",", col("Reason")).alias("Reasons")).collect()
#     if result:
#         content = [row["Reasons"] for row in result]
#         output = ", ".join(content)
#         return output

#     return result

# COMMAND ----------

# DBTITLE 1,UFN_IRPendingReasons
def UFN_IRPendingReasons(claimid: int = None, slno: int = None, isMandatoryNew: int = None, IsReceived: int = None) -> str:

    claims_ir_reasons_df1 = claims_ir_reasons_df.withColumnRenamed("isMandatory", "isMandatoryNew")

    mst_services_df.alias("s")
    claims_ir_reasons_df1.alias('cr')
    claims_df.alias('c')
    mst_irdocuments_df.alias('ir')

    joined_df = claims_ir_reasons_df1.alias('cr') \
        .join(mst_irdocuments_df.alias('ir'), col('cr.IRDocumentID') == col('ir.ID'), 'left') \
        .join(claims_df.alias('c'), col('cr.ClaimID') == col('c.id'), 'left') \
        .join(mst_issuingauthority_df.alias('i'), col('c.issueid') == col('i.id'), 'left')\
        .join(mst_services_df.alias('s'), col('cr.ServiceID') == col('s.ID'), 'left')

    joined_df = joined_df.filter((col('cr.ClaimID') == claimid) & (col('cr.Slno') == slno) & (col('cr.Deleted') == 0) &
                (claims_ir_reasons_df1["isMandatoryNew"] == (isMandatoryNew if isMandatoryNew is not None else claims_ir_reasons_df1["isMandatoryNew"])) &
                (claims_ir_reasons_df1["IsReceived"] == (IsReceived if IsReceived is not None else claims_ir_reasons_df1["IsReceived"])))

    reason_df = joined_df.select(
        when(claims_ir_reasons_df1["IRDocumentID"] == 0, claims_ir_reasons_df1["Remarks"])
        .otherwise(concat_ws('', 
                             when(col('ir.Name').contains('~INS~'), col('i.Name')).otherwise(col('ir.Name')),
                             when(col('ir.Name').contains('|FT|'), col('cr.FreeText1')).otherwise(lit('')),
                             when(col('ir.Name').contains('|FT1|'), col('cr.FreeText2')).otherwise(lit(''))
                            )).alias('Reason')
    ).distinct()

    union_df = reason_df.union(
        claims_ir_reasons_df1.filter((col('ClaimID') == claimid) & (col('slno') == slno) & (col('Deleted') == 0) &
                (claims_ir_reasons_df1["isMandatoryNew"] == (isMandatoryNew if isMandatoryNew is not None else claims_ir_reasons_df1["isMandatoryNew"])) &
                                    (claims_ir_reasons_df1["IsReceived"] == (IsReceived if IsReceived is not None else claims_ir_reasons_df1["IsReceived"])) &
                                    (claims_ir_reasons_df1["IRDocumentID"] == 0) & (claims_ir_reasons_df1["ServiceID"] == 0))
        .select(claims_ir_reasons_df1["Remarks"].alias('Reason'))
    )
    
    union_df1 = union_df.union(
        claims_ir_reasons_df1\
        .filter((col('ClaimID') == claimid) & (col('slno') == slno) & (claims_ir_reasons_df["Deleted"] == 0) &
                (claims_ir_reasons_df1["isMandatoryNew"] == (isMandatoryNew if isMandatoryNew is not None else claims_ir_reasons_df1["isMandatoryNew"])) &
                (claims_ir_reasons_df1["IsReceived"] == (IsReceived if IsReceived is not None else claims_ir_reasons_df1["IsReceived"])) &
                (claims_ir_reasons_df1["IRDocumentID"] == 0))
        .select(claims_ir_reasons_df1["Remarks"].alias('Reason'))
    )

    # display(union_df1)

    result = union_df1.select(concat_ws(",", col("Reason")).alias("Reasons")).collect()
    if result:
        content = [row["Reasons"] for row in result]
        output = ", ".join(content)
        return output

    return result

# COMMAND ----------

# MAGIC %md
# MAGIC #### UFN_Spectra_BalanceSumInsured

# COMMAND ----------

# DBTITLE 1,UFN_Spectra_BalanceSumInsured
# SP Name: "BalanceSumInsured Function On 2024-07-18"
def UFN_Spectra_BalanceSumInsured(claimid):
    SITypeID = 0
    MainMemberID = 0
    MemberPolicyID = 0
    RelationshipIds = 0
    SumInsured = 0
    CB_Amount = 0
    joined_df = claims_df.alias("C") \
        .join(member_policy_df.alias("MP"), col("C.MemberPolicyID") == col("MP.id")) \
        .join(member_si_df.alias("S"), col("MP.id") == col("S.memberpolicyid")) \
        .join(bpsuminsured_df.alias("BP"), (col("BP.id") == col("S.BPSIID")) & (col("BP.SICategoryID_P20") == 69)) \
        .join(bpsiconditions_df.alias("BPCON"), (col("BPCON.bpsiid") == col("BP.id")) & (col("BPCON.bpconditionid") == 30) &
            (col("BP.policyid") == coalesce(col("BPCON.policyid"), lit(0))), "left") \
        .join(bp_service_config_details_df.alias("BPSCON_IND"), (col("BPSCON_IND.benefitplansiid") == col("BP.id")) &
            (col("C.servicetypeid") == col("BPSCON_IND.servicetypeid")) &
            (col("BPSCON_IND.LimitCatg_P29") == 108), "left") \
        .join(bp_service_config_details_df.alias("BPSCON_FAM"), (col("BPSCON_FAM.benefitplansiid") == col("BP.id")) &
            (col("C.servicetypeid") == col("BPSCON_FAM.servicetypeid")) &
            (col("BPSCON_FAM.LimitCatg_P29") == 107), "left") \
        .filter(col("C.ID") == claimid)

    result = joined_df.select(
        col("MP.MainmemberID").alias("MainMemberID"),
        col("C.MemberPolicyID").alias("MemberPolicyID"),
        col("BP.SITypeID").alias("SITypeID"),
        coalesce(col("S.CB_Amount"), lit(0)).alias("CB_Amount"),
        col("BP.SumInsured").alias("SumInsured"),
        col("BPCON.Relationshipid").alias("RelationshipIds"),
        col("BPCON.FamilyLimit").alias("SubLimit"),
        col("MP.RelationshipID").alias("ClaimRelationshipID"),
        when(col("BPSCON_FAM.InternalValueAbs") < col("BPSCON_IND.InternalValueAbs"), col("BPSCON_FAM.InternalValueAbs"))
            .otherwise(col("BPSCON_IND.InternalValueAbs")).alias("ServiceLimit")
    ).limit(1).collect()

    if result:
        MainMemberID = result[0]["MainMemberID"]
        MemberPolicyID = result[0]["MemberPolicyID"]
        SITypeID = result[0]["SITypeID"]
        CB_Amount = result[0]["CB_Amount"]
        SumInsured = result[0]["SumInsured"]
        RelationshipIds = result[0]["RelationshipIds"]
        SubLimit = result[0]["SubLimit"]
        ClaimRelationshipID = result[0]["ClaimRelationshipID"]
        ServiceLimit = result[0]["ServiceLimit"]

    # Additional logic for SITypeID
    if SITypeID == 5:
        MemberPolicyID = MainMemberID
        if CB_Amount == 0:
            cb_amount_df = member_si_df.alias("MS") \
                .join(member_policy_df.alias("MP1"), col("MS.memberpolicyid") == col("MP1.id")) \
                .filter(col("MP1.id") == MainMemberID) \
                .select(coalesce(col("MS.CB_Amount"), lit(0)).alias("CB_Amount")).limit(1).collect()
            if cb_amount_df:
                CB_Amount = cb_amount_df[0]["CB_Amount"]

    # Calculate sums
    settled_cases_sum = claim_utilized_amount_df.alias("CU") \
        .join(claims_details_df.alias("CD"), (col("CU.Claimid") == col("CD.ClaimID")) & (col("CU.Slno") == col("CD.cd_Slno"))) \
        .join(claims_df.alias("C"), col("CD.ClaimID") == col("C.ID")) \
        .join(member_policy_df.alias("MP"), (col("MP.ID") == col("C.MemberPolicyId")) & (col("MP.MainmemberID") == MainMemberID) &
              (col("MP.ID") == when(SITypeID == lit(5), col("MP.ID")).otherwise(MemberPolicyID))) \
        .filter((col("CD.RequestTypeID") >= 4) & (col("CD.StageID") == 27) & (col("CD.RequestTypeID") != 9)) \
        .agg(_sum(coalesce(col("CU.SanctionedAmount"), lit(0)))).collect()[0][0]

    mr_cases_sum = claims_df.alias("C") \
    .join(claims_details_df.alias("CD"), col("CD.ClaimID") == col("C.ID"), "inner") \
    .join(member_policy_df.alias("MP"), col("MP.ID") == col("C.MemberPolicyId"), "inner") \
    .filter((col("CD.RequestTypeID") >= 4) & (col("CD.ClaimTypeID") == 2) & (col("C.Deleted") == 0) & (col("CD.Deleted") == 0) & 
            (col("MP.MainmemberID") == MainMemberID) & (col("MP.ID") == (when(SITypeID == lit(5), col("MP.ID")).otherwise(MemberPolicyID))) & 
            (expr(f"(',' + cast('{RelationshipIds}' as string) + ',') like '%,' + cast(MP.relationshipid as string) + ',%'")) & 
            (col("CD.ClaimID") != col("C.ID")) & (col("CD.RequestTypeID") < 4) & (~col("CD.StageID").isin([27, 23, 21, 25]))) \
    .agg(_sum(when(col("CD.SanctionedAmount").isNotNull(), col("CD.SanctionedAmount")).otherwise(col("CD.ClaimAmount") - when(col("CD.StageID") == 26, col("CD.DeductionAmount")).otherwise(lit(0)))).alias("sum")) \
    .collect()[0]["sum"]

    # Create a DataFrame for the subquery to filter out unwanted rows
    excluded_ids_df = claims_df.alias('C') \
    .join(claims_details_df.alias("CD3"), col("CD3.ClaimID") == col("C.ID")) \
    .filter((col("CD3.RequestTypeid") >= 4) & (col("CD3.StageID").isin([27, 23, 21, 25]))) \
    .select("CD3.cd_Id")

    # Calculate pp_cases_not_settled_sum
    pp_cases_not_settled_sum = claim_utilized_amount_df.alias("CU") \
    .join(claims_details_df.alias("CD"), (col("CU.Claimid") == col("CD.ClaimID")) & (col("CU.Slno") == col("CD.cd_Slno"))) \
    .join(claims_df.alias("C"), col("CD.ClaimID") == col("C.ID")) \
    .join(member_policy_df.alias("MP"), (col("MP.ID") == col("C.MemberPolicyId")) & (col("MP.MainMemberID") == MainMemberID) &
          (col("MP.ID") == when(col("SITypeID") == lit(5), col("MP.ID")).otherwise(MemberPolicyID))) \
    .join(excluded_ids_df, col("CD.cd_Id") == col("CD3.cd_Id"), "left_anti") \
    .filter((col("CD.RequestTypeID") < 4) & (~col("CD.StageID").isin([21, 23, 25]))) \
    .agg(_sum(coalesce(col("CU.SanctionedAmount"), lit(0)))).collect()[0][0]

    SubLimitbalance =  SumInsured + CB_Amount

    # Calculate Totalbalance
    total_settled_cases_sum = claim_utilized_amount_df.alias("CU") \
        .join(claims_details_df.alias("CD"), (col("CU.Claimid") == col("CD.ClaimID")) & (col("CU.Slno") == col("CD.cd_Slno"))) \
        .join(claims_df.alias("C"), col("CD.ClaimID") == col("C.ID")) \
        .join(member_policy_df.alias("MP"), (col("MP.ID") == col("C.MemberPolicyId")) & (col("MP.MainMemberID") == MainMemberID) &
              (col("MP.ID") == when(SITypeID == lit(5), col("MP.ID")).otherwise(MemberPolicyID))) \
        .filter((col("CD.RequestTypeID") >= 4) & (col("CD.StageID") == 27) & (col("CD.RequestTypeID") != 9)) \
        .agg(_sum(coalesce(col("CU.SanctionedAmount"), lit(0)))).collect()[0][0]

    total_mr_cases_not_settled_sum = claims_df.alias("C") \
    .join(claims_details_df.alias("CD"), col("CD.ClaimID") == col("C.ID")) \
    .join(member_policy_df.alias("MP"), (col("MP.ID") == col("C.MemberPolicyId")) & (col("MP.MainMemberID") == MainMemberID) &
          (col("MP.ID") == when(SITypeID == lit(5), col("MP.ID")).otherwise(MemberPolicyID))) \
    .join(excluded_ids_df.alias("EX"), col("CD.cd_Id") == col("EX.cd_Id"), "left_anti") \
    .filter((col("CD.RequestTypeID") >= 4) & (col("CD.ClaimTypeID") == 2)) \
    .agg(_sum(
        when(coalesce(col("CD.SanctionedAmount"), lit(0)) > lit(0), coalesce(col("CD.SanctionedAmount"), lit(0)))
        .otherwise(col("CD.ClaimAmount") - when(col("CD.StageID") == 26, coalesce(col("CD.DeductionAmount"), lit(0))).otherwise(lit(0)))
    )).collect()[0][0]

    total_pp_cases_not_settled_sum = claim_utilized_amount_df.alias("CU") \
    .join(claims_details_df.alias("CD"), (col("CU.Claimid") == col("CD.ClaimID")) & (col("CU.Slno") == col("CD.cd_Slno"))) \
    .join(claims_df.alias("C"), col("CD.ClaimID") == col("C.ID")) \
    .join(member_policy_df.alias("MP"), (col("MP.ID") == col("C.MemberPolicyId")) & (col("MP.MainMemberID") == MainMemberID) &
          (col("MP.ID") == when(SITypeID == lit(5), col("MP.ID")).otherwise(MemberPolicyID))) \
    .join(excluded_ids_df.alias("EX"), col("CD.cd_Id") == col("EX.cd_Id"), "left_anti") \
    .filter((col("CD.RequestTypeID") < 4) & ~col("CD.StageID").isin([21, 23, 25])) \
    .agg(_sum(coalesce(col("CU.SanctionedAmount"), lit(0)))).collect()[0][0]
    total_pp_cases_not_settled_sum = total_pp_cases_not_settled_sum if total_pp_cases_not_settled_sum is not None else 0

    Totalbalance = SumInsured + CB_Amount - total_pp_cases_not_settled_sum
    if Totalbalance > SubLimitbalance:
        return Totalbalance
    else:
        return SubLimitbalance

# COMMAND ----------

# MAGIC %md
# MAGIC #### balance_sum_insured_without_present_claim

# COMMAND ----------

# DBTITLE 1,balance_sum_insured_without_present_claim
## SP Name: "IncurredAmount Function On 2024-07-18"

# Check the commented logic that has issue and try to see whether we are applying it correct

def balance_sum_insured_without_present_claim(claimID, slno):
    
    initial_data = claims_df.alias("C").join(
            member_policy_df.alias("MP"),
            col("C.MemberPolicyID") == col("MP.ID")
        ).join(
            member_si_df.alias("S"),
            col("MP.ID") == col("S.MemberPolicyID")
        ).join(
            bpsuminsured_df.alias("BP"),
            (col("BP.ID") == col("S.BPSIID")) & (col("BP.SICategoryID_P20") == 69)
        ).join(
            bpsiconditions_df.alias("BPCON"),
            (col("BPCON.BPSIID") == col("BP.ID")) & (col("BPCON.BPConditionID") == 30) &
            (col("BP.PolicyID") == col("BPCON.PolicyID")) &
            (concat(lit(","), col("BPCON.RelationshipID"), lit(","))).contains(concat(lit("%,"), col("MP.RelationshipID").cast("string"), lit(",%"))) |
            col("BPCON.RelationshipID").isNull(),
            "left"
        ).join(
            bp_service_config_details_df.alias("BPSCON_IND"),
            (col("BPSCON_IND.BenefitPlanSIID") == col("BP.ID")) & 
            (col("C.ServiceTypeID") == col("BPSCON_IND.ServiceTypeID")) & 
            (concat(lit(","), col("BPSCON_IND.AllowedRelationIDs"), lit(","))).contains(concat(lit("%,"), col("MP.RelationshipID").cast("string"), lit(",%"))) & 
            (col("BPSCON_IND.LimitCatg_P29") == 108),
            "left"
        ).join(
            bp_service_config_details_df.alias("BPSCON_FAM"),
            (col("BPSCON_FAM.BenefitPlanSIID") == col("BP.ID")) & 
            (col("C.ServiceTypeID") == col("BPSCON_FAM.ServiceTypeID")) & 
            (concat(lit(","), col("BPSCON_FAM.AllowedRelationIDs"), lit(","))).contains(concat(lit("%,"), col("MP.RelationshipID").cast("string"), lit(",%"))) & 
            (col("BPSCON_FAM.LimitCatg_P29") == 107),
            "left"
        ).filter(
            (col("C.ID") == lit(claimID)) & (col("C.Deleted") == 0) & (col("S.Deleted") == 0)
        ).select(
            col("MP.MainMemberID").alias("MainMemberID"),
            col("C.MemberPolicyID").alias("MemberPolicyID"),
            col("BP.SITypeID").alias("SITypeID"),
            coalesce(col("S.CB_Amount"), lit(0)).alias("CB_Amount"),
            col("BP.SumInsured").alias("SumInsured"),
            col("BPCON.RelationshipID").alias("RelationshipIDs"),
            col("BPCON.FamilyLimit").alias("SubLimit"),
            col("MP.RelationshipID").alias("ClaimRelationshipID"),
            col("C.ReceivedDate").alias("ClaimReceivedDate"),
            least(col("BPSCON_FAM.InternalValueAbs"), col("BPSCON_IND.InternalValueAbs")).alias("ServiceLimit")
        ).limit(1)
    
    if initial_data.count() == 0:
        print("No initial data found for the given ClaimID.")

    initial_row = initial_data.first()

    C = claims_df.alias("C")
    MP = member_policy_df.alias("MP")
    S = member_si_df.alias("S")
    BP = bpsuminsured_df.alias("BP")
    BPCON = bpsiconditions_df.alias("BPCON")
    BPSCON_IND = bp_service_config_details_df.alias("BPSCON_IND")
    BPSCON_FAM = bp_service_config_details_df.alias("BPSCON_FAM")
    CU = claim_utilized_amount_df.alias("CU")
    CD = claims_details_df.alias("CD")
    sub_limit_balance = (
        initial_data.withColumn(
            "SubLimitBalance",
            coalesce(col("ServiceLimit"), col("SumInsured")) + col("CB_Amount") - 
            coalesce(
                lit(CU.join(
                    C,
                    col("CU.ClaimID") == col("C.ID")
                ).join(
                    CD,
                    (col("CD.ClaimID") == col("C.ID")) & (col("CD.cd_Slno") == col("CU.SlNo"))
                ).filter(
                    (col("C.Deleted") == 0) & (col("CU.Deleted") == 0) & (col("CD.Deleted") == 0) &
                    (col("CD.RequestTypeID") >= 4) & (col("CD.StageID") == 27) & (col("CD.RequestTypeID") != 9) &
                    (col("CU.ClaimID") != lit(claimID).cast("string")) & (col("C.ReceivedDate") <= initial_row["ClaimReceivedDate"])
                ).select(sum(coalesce(col("CU.SanctionedAmount"), lit(0))).alias("SettledCases")).first()["SettledCases"]),
                lit(0)
            ) 
            - 
            coalesce(
                lit(CD.join(
                    C,
                    col("CD.ClaimID") == col("C.ID")
                ).filter(
                    (col("C.Deleted") == 0) & (col("CD.Deleted") == 0) &
                    (col("CD.RequestTypeID") >= 4) & (col("CD.ClaimTypeID") == 2) &
                    (col("CD.ClaimID") != col("C.ID")) & (col("CD.RequestTypeID") < 4) & 
                    (~col("CD.StageID").isin(27, 23, 21, 25)) &
                    (col("CD.ClaimID") != lit(claimID).cast("string")) & (col("C.ReceivedDate") <= initial_row["ClaimReceivedDate"])
                ).select(
                    sum(
                        when(col("CD.SanctionedAmount").isNotNull(), col("CD.SanctionedAmount"))
                        .otherwise(col("CD.ClaimAmount") - when(col("CD.StageID") == 26, col("CD.DeductionAmount")).otherwise(lit(0)))
                    ).alias("MRNotSettled")
                ).first()["MRNotSettled"]),
                lit(0)
            ) 
            - 
            coalesce(
                lit(CU.join(
                    C,
                    col("CU.ClaimID") == col("C.ID")
                ).join(
                    CD,
                    (col("CD.ClaimID") == col("C.ID")) & (col("CD.cd_Slno") == col("CU.SlNo"))
                ).filter(
                    (col("C.Deleted") == 0) & (col("CU.Deleted") == 0) & (col("CD.Deleted") == 0) &
                    (col("CD.RequestTypeID") < 4) & (~col("CD.StageID").isin(21, 23, 25)) &
                    (col("CD.ClaimID") != col("C.ID")) & (col("CD.RequestTypeID") < 4) & 
                    (~col("CD.StageID").isin(27, 23, 21, 25)) &
                    (col("CU.ClaimID") != lit(claimID).cast("string")) & (col("C.ReceivedDate") <= initial_row["ClaimReceivedDate"])
                ).select(sum(coalesce(col("CU.SanctionedAmount"), lit(0))).alias("PPNotSettled")).first()["PPNotSettled"]),
                lit(0)
            ) -
            coalesce(
                lit(CU.join(
                    C,
                    col("CU.ClaimID") == col("C.ID")
                ).join(
                    CD,
                    (col("CD.ClaimID") == col("C.ID")) & (col("CD.cd_Slno") == col("CU.SlNo"))
                ).filter(
                    (col("C.Deleted") == 0) & (col("CU.Deleted") == 0) & (col("CD.Deleted") == 0) &
                    (col("CD.RequestTypeID") < 4) & (col("CD.StageID") == 29) &
                    (col("CD.ClaimID") != col("C.ID")) & (col("CD.RequestTypeID") < 4) & 
                    (~col("CD.StageID").isin(26, 27, 23, 21, 25)) &
                    (col("CU.ClaimID") != lit(claimID).cast("string")) & (col("C.ReceivedDate") > initial_row["ClaimReceivedDate"])
                ).select(sum(coalesce(col("CU.SanctionedAmount"), lit(0))).alias("PPSettled")).first()["PPSettled"]),
                lit(0)
            )
        )
    )
    
    return sub_limit_balance.select("SubLimitBalance").first()["SubLimitBalance"]

# COMMAND ----------

# def UDF_GetIncurredAmount(claimid,slno):

#     joined_df = claims_details_df.alias('cd') \
#         .join(claims_df.alias('c'), (col('cd.ClaimID') == col('c.ID')) & (col('cd.deleted') == 0) & (col('c.deleted') == 0), 'inner') \
#         .join(claim_stage_df.alias('cs'), col('cs.ID') == col('cd.StageID'), 'inner') \
#         .join(member_policy_df.alias('m'), col('m.ID') == col('c.memberpolicyid'), 'inner') \
#         .join(member_si_df.alias('ms'), col('ms.memberpolicyid') == col('m.id'), 'inner') \
#         .join(bpsuminsured_df.alias('bs'), (col('bs.id') == col('ms.BPSIID')) & (col('bs.SICategoryID_P20') == 69), 'inner') \
#         .filter((claims_details_df["ClaimID"] == claimid) & (claims_details_df["cd_Slno"] == slno))

#     values = joined_df.select(
#         col('cd.ClaimTypeID').alias('Claimtype'),
#         col('cd.RequestTypeID').alias('Requesttype'),
#         when(col('cd.RequestTypeID').isin(1, 2, 3), col('cs.Name'))
#         .when(col('cd.StageID') == 27, lit('Settled'))
#         .when(col('cd.StageID').isin(21, 23, 25), col('cs.Name'))
#         .otherwise(
#             coalesce(
#                 spark.sql("""
#                     SELECT cs1.Name
#                     FROM fhpl.claimsdetails cd 
#                     JOIN fhpl.claimactionitems ca1 on cd.ClaimID = ca1.ClaimID
#                     JOIN fhpl.claimstage cs1 ON ca1.ClaimStageID = cs1.ID
#                     WHERE ca1.claimid = cd.ClaimID AND ca1.Slno = cd.Slno --AND ca1.CloseDate IS NULL
#                     ORDER BY ca1.ID DESC
#                     LIMIT 1
#                 """).first()['Name'],
#                 col('cs.Name')
#             )
#         ).alias('Currentclaimstatus'),
#         col('cd.PayableAmount').alias('Incurred'),
#         col('cd.SettledAmount').alias('Settled'),
#         col('cd.ClaimAmount').alias('Claimed'),
#         when(col('cd.StageID') == 23, lit(0)).otherwise(col('cd.SanctionedAmount')).alias('Sanctioned'),
#         col('bs.SumInsured').alias('coverageamount'),
#         col('c.Deleted').alias('Claim_Deleted'),
#         col('cd.Deleted').alias('Claimdetails_Deleted')
#     ).first()
#     pasanctioned_amount = spark.sql(f"""
#         SELECT CASE WHEN claimtypeid = 1 THEN isnull(a.SanctionedAmount, 0) END AS PASanctionedAmount
#         FROM (
#             SELECT ClaimID, Slno, claimtypeid, sanctionedamount, row_number() OVER (PARTITION BY claimid ORDER BY claimid, Slno DESC) AS rowno
#             FROM fhpl.Claimsdetails
#             WHERE ClaimID = {df["claimid"]} AND deleted = 0 AND RequestTypeID IN (1, 2, 3) AND stageid NOT IN (23)
#         ) a
#         WHERE a.rowno = 1
#     """).first()['PASanctionedAmount']

#     mainmemberstatus = spark.sql(f"""
#         SELECT 1 AS mainmemberstatus
#         FROM fhpl.ClaimsDetails cd
#         JOIN fhpl.Claims c ON cd.ClaimID = c.ID
#         JOIN fhpl.MemberPolicy mp ON c.MemberPolicyID = mp.Id
#         JOIN fhpl.MemberPolicy mp2 ON mp.MainmemberID = mp2.MainmemberID
#         JOIN fhpl.Claims c2 ON mp2.Id = c2.MemberPolicyID
#         WHERE cd.ClaimTypeID = 1 AND cd.ClaimID = c2.ID AND cd.deleted = 0 AND c.deleted = 0 AND mp.deleted = 0 AND mp2.deleted = 0 AND c2.deleted = 0
#         AND NOT EXISTS (
#             SELECT 1
#             FROM fhpl.Claimsdetails cd2
#             WHERE cd2.ClaimID = c2.ID AND cd2.RequestTypeID < 4 AND cd2.StageID = 29
#         )
#         AND cd.ClaimID = {claimid} AND cd.Slno = {slno}
#     """).first()['mainmemberstatus']

#     incurred = values['Incurred']
#     if values['Claim_Deleted'] == 0 and values['Claimdetails_Deleted'] == 0:
#         if values['Currentclaimstatus'] in (
#             'Registration', 'For Adjudication', 'Refer to CS', 'Refer to Enrollment', 'Query Response (H)', 'Query Response (M)',
#             'Insurer Response', 'CS Response', 'Enrollment Response', 'Refer to Insurer', 'Insurer Authorization', 'Investigation Done',
#             'Query to Hospital', 'Query to Member', 'First Reminder-Hospital', 'Second Reminder-Hospital', 'Third Reminder-Hospital',
#             'Fourth Reminder-Hospital', 'First Reminder-Member', 'Second Reminder-Member', 'Third Reminder-Member', 'Fourth Reminder-Member',
#             'From CRM', 'For Investigation', 'For Bill Entry', 'For Audit', 'Refer To CRM', 'Audit Return'
#         ) and values['Requesttype'] not in (1, 2, 3):
#             incurred = min(values['Claimed'], values['coverageamount']) if values['Claimtype'] == 2 else min(values['Claimed'], pasanctioned_amount or values['Sanctioned'])

#         if values['Requesttype'] in (1, 2, 3):
#             incurred = min(values['Claimed'], values['Sanctioned'])

#         if values['Claimtype'] == 1 and mainmemberstatus == 1 and values['Requesttype'] in (1, 2, 3):
#             balance_sum_insured = spark.sql(f"""
#                 SELECT balance_sum_insured_without_present_claim(ID, 3) AS BalanceSumInsured
#                 FROM fhpl.Claims
#                 WHERE ID = {df["claimid"]}
#             """).first()['BalanceSumInsured']
#             incurred = min(values['Claimed'], balance_sum_insured or values['Sanctioned'])

#         if values['Claimtype'] == 2 and values['Currentclaimstatus'] in (
#             'For Bill Entry', 'For Adjudication', 'Query to Hospital', 'Query to Member', 'Refer to CS', 'Refer to CRM', 'Refer to Enrollment',
#             'Insurer Response', 'CS Response', 'Enrollment Response', 'Refer to Insurer', 'For Investigation', 'Investigation Done', 'Sent for Audit',
#             'For Audit', 'Audit Return', 'First Reminder-Hospital', 'Second Reminder-Hospital', 'Third Reminder-Hospital', 'Fourth Reminder-Hospital',
#             'First Reminder-Member', 'Second Reminder-Member', 'Third Reminder-Member', 'Fourth Reminder-Member', 'From CRM'
#         ):
#             incurred_amount = spark.sql(f"""
#                 SELECT UFN_Spectra_BalanceSumInsured_Without_PresentClaim(ID, 1) AS IncurredAmount
#                 FROM fhpl.Claims
#                 WHERE ID = {df["claimid"]}
#             """).first()['IncurredAmount']
#             bill_amount = values['Claimed']
#             incurred = min(incurred_amount, bill_amount)

#         if values['Currentclaimstatus'] == 'Settled':
#             incurred = values['Settled']

#         if values['Currentclaimstatus'] in ('Closed', 'Repudiated', 'Cancelled'):
#             incurred = 0

#         if values['Currentclaimstatus'] in ('For Payment', 'For Settlement') and values['Claimtype'] == 2:
#             incurred = values['Sanctioned']

#         if values['Claimtype'] == 2 and values['Currentclaimstatus'] in (
#             'For Bill Entry', 'For Adjudication', 'Query to Hospital', 'Query to Member', 'Refer to CS', 'Refer to CRM', 'Refer to Enrollment',
#             'Insurer Response', 'CS Response', 'Enrollment Response', 'Refer to Insurer', 'For Investigation', 'Investigation Done', 'Sent for Audit',
#             'For Audit', 'Audit Return', 'First Reminder-Hospital', 'Second Reminder-Hospital', 'Third Reminder-Hospital', 'Fourth Reminder-Hospital',
#             'First Reminder-Member', 'Second Reminder-Member', 'Third Reminder-Member', 'Fourth Reminder-Member', 'From CRM'
#         ):
#             incurred_amount = spark.sql(f"""
#                 SELECT UFN_Spectra_BalanceSumInsured_Without_PresentClaim(ID, 1) AS IncurredAmount
#                 FROM fhpl.Claims
#                 WHERE ID = {df["claimid"]}
#             """).first()['IncurredAmount']
#             bill_amount = values['Claimed']
#             incurred = min(incurred_amount, bill_amount)

#         if incurred < 0 or (
#             values['Currentclaimstatus'] in (
#                 'For Bill Entry', 'For Adjudication', 'Refer to CS', 'Refer to CRM', 'Refer to Enrollment', 'Insurer Response', 'CS Response',
#                 'Enrollment Response', 'Refer to Insurer', 'For Investigation', 'Investigation Done', 'Sent for Audit', 'For Audit', 'For Payment',
#                 'For Settlement', 'Audit Return', 'From CRM'
#             ) and claims_action_items_df.filter(
#                 (col('ClaimID') == claimid) & (col('Slno') == slno)
#             ).orderBy(col('OpenDate').desc()).first()['Remarks'].contains('Refer to Insurer – R')
#         ):
#             incurred = 0

#         if values['Requesttype'] in (1, 2, 3) and values['Claimtype'] == 1 and values['Currentclaimstatus'] == 'Cashless Approved':
#             incurred = values['Sanctioned']

#     else:
#         incurred = 0

#     return float(incurred)

# COMMAND ----------

# def UDF_GetIncurredAmount(claimid, slno):
#     joined_df = claims_details_df.alias('cd') \
#         .join(claims_df.alias('c'), 
#               (F.col('cd.ClaimID') == F.col('c.ID')) & (F.col('cd.deleted') == 0) & (F.col('c.deleted') == 0), 'inner') \
#         .join(claim_stage_df.alias('cs'), F.col('cs.ID') == F.col('cd.StageID'), 'inner') \
#         .join(member_policy_df.alias('m'), F.col('m.ID') == F.col('c.memberpolicyid'), 'inner') \
#         .join(member_si_df.alias('ms'), F.col('ms.memberpolicyid') == F.col('m.id'), 'inner') \
#         .join(bpsuminsured_df.alias('bs'), 
#               (F.col('bs.id') == F.col('ms.BPSIID')) & (F.col('bs.SICategoryID_P20') == 69), 'inner') \
#         .filter((claims_details_df["ClaimID"] == claimid) & (claims_details_df["cd_Slno"] == slno))

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
#     pasanctioned_amount_query = f"""
#         SELECT COALESCE(SUM(CASE WHEN claimtypeid = 1 THEN a.sanctionedamount ELSE 0 END), 0) AS PASanctionedAmount
#         FROM (
#             SELECT ClaimID, Slno, claimtypeid, sanctionedamount, 
#                    row_number() OVER (PARTITION BY claimid ORDER BY claimid, Slno DESC) AS rowno
#             FROM fhpl.Claimsdetails
#             WHERE ClaimID = {claimid} AND deleted = 0 
#                   AND RequestTypeID IN (1, 2, 3) AND stageid NOT IN (23)
#         ) a
#         WHERE a.rowno = 1
#     """
#     pasanctioned_amount = spark.sql(pasanctioned_amount_query).first()['PASanctionedAmount']
    
#     # Main Member Status SQL query
#     mainmemberstatus_query = f"""
#         SELECT 1 AS mainmemberstatus
#         FROM fhpl.ClaimsDetails cd
#         JOIN fhpl.Claims c ON cd.ClaimID = c.ID
#         JOIN fhpl.MemberPolicy mp ON c.MemberPolicyID = mp.Id
#         JOIN fhpl.MemberPolicy mp2 ON mp.MainmemberID = mp2.MainmemberID
#         JOIN fhpl.Claims c2 ON mp2.Id = c2.MemberPolicyID
#         WHERE cd.ClaimTypeID = 1 AND cd.ClaimID = c2.ID AND cd.deleted = 0 
#               AND c.deleted = 0 AND mp.deleted = 0 AND mp2.deleted = 0 AND c2.deleted = 0
#               AND NOT EXISTS (
#                   SELECT 1
#                   FROM fhpl.Claimsdetails cd2
#                   WHERE cd2.ClaimID = c2.ID AND cd2.RequestTypeID < 4 AND cd2.StageID = 29
#               )
#               AND cd.ClaimID = {claimid} AND cd.Slno = {slno}
#     """
#     mainmemberstatus_result = spark.sql(mainmemberstatus_query).first()
#     if mainmemberstatus_result is not None:
#         mainmemberstatus = mainmemberstatus_result['mainmemberstatus']
#     else:
#         mainmemberstatus = 0

#     if values is not None:
#         incurred = values['Incurred']
#     else:
#         incurred = 0
#     if values is not None:
#         if values['Claim_Deleted'] == 0 and values['Claimdetails_Deleted'] == 0:
#             if values['Currentclaimstatus'] in (
#                 'Registration', 'For Adjudication', 'Refer to CS', 'Refer to Enrollment', 'Query Response (H)', 'Query Response (M)',
#                 'Insurer Response', 'CS Response', 'Enrollment Response', 'Refer to Insurer', 'Insurer Authorization', 'Investigation Done',
#                 'Query to Hospital', 'Query to Member', 'First Reminder-Hospital', 'Second Reminder-Hospital', 'Third Reminder-Hospital',
#                 'Fourth Reminder-Hospital', 'First Reminder-Member', 'Second Reminder-Member', 'Third Reminder-Member', 'Fourth Reminder-Member',
#                 'From CRM', 'For Investigation', 'For Bill Entry', 'For Audit', 'Refer To CRM', 'Audit Return', "Received Requests"
#             ) and values['Requesttype'] not in (1, 2, 3):
#                 incurred = min(values['Claimed'], values['coverageamount']) if values['Claimtype'] == 2 else min(values['Claimed'], pasanctioned_amount or values['Sanctioned'])

#             if values['Requesttype'] in (1, 2, 3):
#                 incurred = min(values['Claimed'], values['Sanctioned'])

#             if values['Claimtype'] == 1 and mainmemberstatus == 1 and values['Requesttype'] in (1, 2, 3):
#                 balance_sum_insured = balance_sum_insured_without_present_claim(claimid, 3)
#                 incurred = min(values['Claimed'], balance_sum_insured or values['Sanctioned'])

#             if values['Claimtype'] == 2 and values['Currentclaimstatus'] in (
#                 'For Bill Entry', 'For Adjudication', 'Query to Hospital', 'Query to Member', 'Refer to CS', 'Refer to CRM', 'Refer to Enrollment',
#                 'Insurer Response', 'CS Response', 'Enrollment Response', 'Refer to Insurer', 'For Investigation', 'Investigation Done', 'Sent for Audit',
#                 'For Audit', 'Audit Return', 'First Reminder-Hospital', 'Second Reminder-Hospital', 'Third Reminder-Hospital', 'Fourth Reminder-Hospital',
#                 'First Reminder-Member', 'Second Reminder-Member', 'Third Reminder-Member', 'Fourth Reminder-Member', 'From CRM', "Received Requests"
#             ):
#                 incurred_amount = claims_df.filter(col("ID") == claimid).selectExpr("balance_sum_insured_without_present_claim(ID, 1) AS IncurredAmount").first()['IncurredAmount']
#                 bill_amount = values['Claimed']
#                 incurred = min(incurred_amount, bill_amount)

#             if values['Currentclaimstatus'] == 'Settled':
#                 incurred = values['Settled']

#             if values['Currentclaimstatus'] in ('Closed', 'Repudiated', 'Cancelled'):
#                 incurred = 0

#             if values['Currentclaimstatus'] in ('For Payment', 'For Settlement') and values['Claimtype'] == 2:
#                 incurred = values['Sanctioned']

#             if values['Claimtype'] == 2 and values['Currentclaimstatus'] in (
#                 'For Bill Entry', 'For Adjudication', 'Query to Hospital', 'Query to Member', 'Refer to CS', 'Refer to CRM', 'Refer to Enrollment',
#                 'Insurer Response', 'CS Response', 'Enrollment Response', 'Refer to Insurer', 'For Investigation', 'Investigation Done', 'Sent for Audit',
#                 'For Audit', 'Audit Return', 'First Reminder-Hospital', 'Second Reminder-Hospital', 'Third Reminder-Hospital', 'Fourth Reminder-Hospital',
#                 'First Reminder-Member', 'Second Reminder-Member', 'Third Reminder-Member', 'Fourth Reminder-Member', 'From CRM',"Received Requests"
#             ):
#                 incurred_amount = claims_df.filter(col("ID") == claimid).selectExpr("balance_sum_insured_without_present_claim(ID, 1) AS IncurredAmount").first()['IncurredAmount']
#                 bill_amount = values['Claimed']
#                 incurred = min(incurred_amount, bill_amount)

#             if incurred < 0 or (
#                 values['Currentclaimstatus'] in (
#                     'For Bill Entry', 'For Adjudication', 'Refer to CS', 'Refer to CRM', 'Refer to Enrollment', 'Insurer Response', 'CS Response',
#                     'Enrollment Response', 'Refer to Insurer', 'For Investigation', 'Investigation Done', 'Sent for Audit', 'For Audit', 'For Payment',
#                     'For Settlement', 'Audit Return', 'From CRM',"Received Requests"
#                 ) and claims_action_items_df.filter(
#                     (col('ClaimID') == claimid) & (col('Slno') == slno)
#                 ).orderBy(col('OpenDate').desc()).first()['Remarks'].contains('Refer to Insurer – R')
#             ):
#                 incurred = 0

#             if values['Requesttype'] in (1, 2, 3) and values['Claimtype'] == 1 and values['Currentclaimstatus'] == 'Cashless Approved':
#                 incurred = values['Sanctioned']

#         else:
#             incurred = 0
#     else:
#         incurred = 0
#     return float(incurred)

# COMMAND ----------

# DBTITLE 1,GetIncurredAmount
# updated code by Basheer
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
        df_cd
        .join(df_c, df_cd["ClaimID"] == df_c["ID"], "inner")
        .join(df_cs, df_cd["StageID"] == df_cs["ID"], "inner")
        .join(df_mp, df_c["MemberPolicyID"] == df_mp["Id"], "inner")
        .join(df_msi, df_mp["Id"] == df_msi["memberpolicyid"], "inner")
        .join(df_bs, df_msi["BPSIID"] == df_bs["ID"], "inner")
        .filter((df_cd["ClaimID"] == claimid) & (df_cd["Slno"] == slno))
    )

    currentclaimstatus_df = (
        df_ca
        .join(df_cs, df_ca["ClaimStageID"] == df_cs["ID"], "inner")
        .filter(
            (df_ca["ClaimID"] == claimid) &
            (df_ca["Slno"] == slno) &
            (df_ca["CloseDate"].isNull())
        )
        .orderBy(F.desc(df_ca["ID"]))
        .select(df_cs["Name"])
        .limit(1)
    )

    currentclaimstatus = currentclaimstatus_df.collect()
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
                        F.when(BillAmount - F.when(CD.StageID.isin([26, 27]) , DeductionAmount, F.lit(0)) > Claimed, Claimed)
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

# MAGIC %md
# MAGIC # To be Reviewed

# COMMAND ----------

# DBTITLE 1,balance_sum_insured_without_present_claim
from pyspark.sql import functions as F
from pyspark.sql import Window

claims_df = spark.table("fhpl.Claims")
memberpolicy_df = spark.table("fhpl.MemberPolicy")
membersi_df = spark.table("fhpl.Membersi")
bpsuminsured_df = spark.table("fhpl.bpsuminsured")
bpsiconditions_df = spark.table("fhpl.bpsiconditions")
bpserviceconfigdetails_df = spark.table("fhpl.BPServiceConfigDetails")
claim_utilized_df = spark.table("fhpl.ClaimUtilizedAmount")
claims_details_df = spark.table("fhpl.ClaimsDetails")
claim_utilized_amount_df = spark.table("fhpl.claimutilizedamount")

def balance_sum_insured_without_present_claim(claimID, slno):
    
    BPSIIDs = ""
    MainMemberID = None
    MemberSIID = ""
    MemberPolicyID = None
    SITypeID = None
    CB_Amount = 0
    SumInsured = 0
    Sublimitbalance = 0
    Totalbalance = 0
    RelationshipIds = ""
    SubLimit = 0
    ServiceLimit = 0
    ClaimRelationshipID = None
    ClaimReceivedDate = None
    
    # Filter Claims and join with required tables
    selected_df = claims_df.alias('C')\
        .join(memberpolicy_df.alias('MP'), F.col('C.MemberPolicyID') == F.col('MP.id'), 'inner')\
        .join(membersi_df.alias('S'), F.col('MP.id') == F.col('S.memberpolicyid'), 'inner')\
        .join(bpsuminsured_df.alias('BP'), (F.col('BP.id') == F.col('S.BPSIID')) & (F.col('BP.SICategoryID_P20') == 69), 'inner')\
        .join(bpsiconditions_df.alias('BPCON'), 
              (F.col('BPCON.bpsiid') == F.col('BP.id')) & 
              (F.col('BPCON.bpconditionid') == 30) &
              (F.expr("',' + BPCON.Relationshipid + ',' like '%,' + CAST(MP.relationshipid AS STRING) + ',%'")) &
              (F.col('BP.policyid') == F.col('BPCON.policyid')), 'left')\
        .join(bpserviceconfigdetails_df.alias('BPSCON_IND'),
              (F.col('BPSCON_IND.benefitplansiid') == F.col('BP.id')) & 
              (F.col('C.servicetypeid') == F.col('BPSCON_IND.servicetypeid')) &
              (F.expr("',' + BPSCON_IND.AllowedRelationids + ',' like '%,' + CAST(MP.relationshipid AS STRING) + ',%'")) &
              (F.col('BPSCON_IND.LimitCatg_P29') == 108), 'left')\
        .join(bpserviceconfigdetails_df.alias('BPSCON_FAM'),
              (F.col('BPSCON_FAM.benefitplansiid') == F.col('BP.id')) & 
              (F.col('C.servicetypeid') == F.col('BPSCON_FAM.servicetypeid')) &
              (F.expr("',' + BPSCON_FAM.AllowedRelationids + ',' like '%,' + CAST(MP.relationshipid AS STRING) + ',%'")) &
              (F.col('BPSCON_FAM.LimitCatg_P29') == 107), 'left')\
        .filter((F.col('C.ID') == claimID) & (F.col('C.Deleted') == 0) & (F.col('S.deleted') == 0))\
        .select(F.col('MP.MainmemberID').alias('MainMemberID'),
                F.col('C.MemberPolicyID').alias('MemberPolicyID'),
                F.col('BP.SITypeID').alias('SITypeID'),
                F.coalesce(F.col('S.CB_Amount'), F.lit(0)).alias('CB_Amount'),
                F.col('BP.SumInsured').alias('SumInsured'),
                F.col('BPCON.Relationshipid').alias('RelationshipIds'),
                F.coalesce(F.col('BPCON.FamilyLimit'), F.lit(0)).alias('SubLimit'),
                F.col('MP.RelationshipID').alias('ClaimRelationshipID'),
                F.col('C.ReceivedDate').alias('ClaimReceivedDate'),
                F.least(F.col('BPSCON_FAM.InternalValueAbs'), F.col('BPSCON_IND.InternalValueAbs')).alias('ServiceLimit')
               ).first()

    if selected_df:
        MainMemberID = selected_df['MainMemberID']
        MemberPolicyID = selected_df['MemberPolicyID']
        SITypeID = selected_df['SITypeID']
        CB_Amount = selected_df['CB_Amount']
        SumInsured = selected_df['SumInsured']
        RelationshipIds = selected_df['RelationshipIds']
        SubLimit = selected_df['SubLimit']
        ClaimRelationshipID = selected_df['ClaimRelationshipID']
        ClaimReceivedDate = selected_df['ClaimReceivedDate']
        ServiceLimit = selected_df['ServiceLimit']

    if SITypeID == 5:
        MemberPolicyID = MainMemberID
        
        if CB_Amount == 0:
            CB_Amount_row = (
                membersi_df.join(memberpolicy_df, membersi_df["memberpolicyid"] == memberpolicy_df["id"])
                .filter(memberpolicy_df["id"] == MainMemberID)
                .select("cb_amount")
                .first()
            )
            if CB_Amount_row:
                CB_Amount = CB_Amount_row["cb_amount"]

    if CB_Amount: 
        CB_Amount = CB_Amount
    else:
        CB_Amount = 0
    if ServiceLimit:
        ServiceLimit = ServiceLimit
    else:
        ServiceLimit = 0

    # added the coalesce logic as per requirement
    # Compute SubLimitbalance
    if ClaimRelationshipID == 2:
        SubLimitbalance = F.coalesce(F.lit(ServiceLimit), F.lit(SumInsured)) + CB_Amount
    else:
        SubLimitbalance = F.coalesce(F.lit(ServiceLimit), F.lit(SubLimit), F.lit(SumInsured)) + CB_Amount
    
    # Define the common filter condition
    common_filter = (
        (claims_df["Deleted"] == 0) &
        (claims_details_df["Deleted"] == 0) &
        (claims_details_df["ClaimID"] == claims_df["ID"]) &
        (claims_details_df["ClaimID"] != claimID) &
        (claims_df["ReceivedDate"] <= F.lit(ClaimReceivedDate)) &
        (memberpolicy_df["MainMemberID"] == F.lit(MainMemberID)) &
        (memberpolicy_df["id"] == F.when(F.lit(SITypeID) == 5, memberpolicy_df["id"]).otherwise(F.lit(MemberPolicyID))) #&
        # F.expr(f"',{RelationshipIds},' like '%,{memberpolicy_df.RelationshipID},%'") 
        #ToDO:: need to Implement
    )

    # Settled Cases
    settled_cases_amount = (
        claim_utilized_df.join(claims_details_df, 
                            (claim_utilized_df["ClaimId"] == claims_details_df["ClaimId"]) &
                            (claim_utilized_df["SlNo"] == claims_details_df["SlNo"]))
        .join(claims_df, claim_utilized_df["ClaimId"] == claims_df["ID"])
        .join(memberpolicy_df, claims_df["MemberPolicyId"] == memberpolicy_df["ID"])
        .filter(common_filter & 
                (claims_details_df["RequestTypeID"] >= 4) &
                (claims_details_df["StageID"] == 27) &
                (claims_details_df["RequestTypeID"] != 9))
        .agg(F.sum(F.coalesce(claim_utilized_df["SanctionedAmount"], F.lit(0))).alias("settled_cases_amount"))
        .first()["settled_cases_amount"] or 0
    )

    Sublimitbalance -= settled_cases_amount

    # MR Cases not yet settled
    mr_cases_amount = (
        claims_details_df.join(claims_df, claims_details_df["ClaimID"] == claims_df["ID"])
        .join(memberpolicy_df, claims_df["MemberPolicyId"] == memberpolicy_df["ID"])
        .filter(common_filter & 
                (claims_details_df["RequestTypeID"] >= 4) &
                (claims_details_df["ClaimTypeID"] == 2) &
                ~claims_details_df["ClaimID"].isin(claimID))
        .agg(F.sum(F.when(F.coalesce(claims_details_df["SanctionedAmount"], F.lit(0)) > 0, 
                        F.coalesce(claims_details_df["SanctionedAmount"], F.lit(0)))
                .otherwise(F.coalesce(claims_details_df["ClaimAmount"], F.lit(0)) - 
                F.when(claims_details_df["StageID"] == 26, F.coalesce(claims_details_df["DeductionAmount"], F.lit(0))).otherwise(F.lit(0)))).alias("mr_cases_amount"))
        .first()["mr_cases_amount"] or 0
    )

    Sublimitbalance -= mr_cases_amount

    # # PP Cases not yet settled
    pp_cases_amount = (
        claim_utilized_df.join(claims_details_df, 
                            (claim_utilized_df["ClaimId"] == claims_details_df["ClaimId"]) &
                            (claim_utilized_df["SlNo"] == claims_details_df["SlNo"]))
        .join(claims_df, claim_utilized_df["ClaimId"] == claims_df["ID"])
        .join(memberpolicy_df, claims_df["MemberPolicyId"] == memberpolicy_df["ID"])
        .filter(common_filter & 
                (claims_details_df["RequestTypeID"] < 4) &
                (~claims_details_df["StageID"].isin(21, 23, 25)))
        .agg(F.sum(F.coalesce(claim_utilized_df["SanctionedAmount"], F.lit(0))).alias("pp_cases_amount"))
        .first()["pp_cases_amount"] or 0
    )

    Sublimitbalance -= pp_cases_amount

    ###### Reviewed
    # Add the exclusion logic using a left anti-join
    pp_sanctioned_after_present_claim_first = (
        claim_utilized_df
        .join(claims_details_df,
            (claim_utilized_df["ClaimId"] == claims_details_df["ClaimId"]) & 
            (claim_utilized_df["SlNo"] == claims_details_df["SlNo"]))
        # .join(claims_df, claim_utilized_df["ClaimId"] == claims_df["ID"])
        .join(memberpolicy_df, claims_df["MemberPolicyId"] == memberpolicy_df["ID"])
        .filter(
            common_filter &
            (claims_details_df["RequestTypeID"] < 4) &
            (claims_details_df["StageID"] == 29) &
            (claim_utilized_df["ClaimId"] != claimID) & 
            (claims_df["ReceivedDate"] > F.lit(ClaimReceivedDate)) & 
            (claims_details_df["RequestTypeID"] >= 4) &
            (claims_details_df["StageID"].isin(26, 27, 23, 21, 25))
        )
        .agg(F.sum(F.coalesce(claim_utilized_df["SanctionedAmount"], F.lit(0))).alias("pp_sanctioned_after_present_claim_first"))
        .first()["pp_sanctioned_after_present_claim_first"] or 0
    )

    pp_sanctioned_after_present_claim_sec = (
        claim_utilized_df
        .join(claims_details_df,
            (claim_utilized_df["ClaimId"] == claims_details_df["ClaimId"]) & 
            (claim_utilized_df["SlNo"] == claims_details_df["SlNo"]))
        # .join(claims_df, claim_utilized_df["ClaimId"] == claimID) #TODO::
        .join(memberpolicy_df, claims_df["MemberPolicyId"] == memberpolicy_df["ID"])
        .filter(
            common_filter &
            (claims_details_df["RequestTypeID"] >= 4) &
            (claim_utilized_df["ClaimId"] != claimID) & 
            (claims_df["ReceivedDate"] > F.lit(ClaimReceivedDate)) & 
            (claims_details_df["RequestTypeID"] != 9) &
            (claims_details_df["StageID"].isin(26, 27))
        )
        .agg(F.sum(F.coalesce(claim_utilized_df["SanctionedAmount"], F.lit(0))).alias("pp_sanctioned_after_present_claim_sec"))
        .first()["pp_sanctioned_after_present_claim_sec"] or 0
    )
    pp_sanctioned_after_present_claim = pp_sanctioned_after_present_claim_first - pp_sanctioned_after_present_claim_sec
    Sublimitbalance -= pp_sanctioned_after_present_claim
    ###### Reviewed

    # MR Claims with Sanctioned Amount after the present claim
    mr_sanctioned_after_present_claim = (
        claims_details_df.join(claims_df, claims_details_df["ClaimID"] == claims_df["ID"])
        .join(memberpolicy_df, claims_df["MemberPolicyId"] == memberpolicy_df["ID"])
        .filter(common_filter & 
                (claims_details_df["RequestTypeID"] >= 4) &
                (claims_details_df["ClaimTypeID"] == 2) &
                claims_details_df["StageID"].isin(26, 27))
        .agg(F.sum(F.when(F.coalesce(claims_details_df["SanctionedAmount"], F.lit(0)) > 0, 
                        F.coalesce(claims_details_df["SanctionedAmount"], F.lit(0)))
                .otherwise(F.coalesce(claims_details_df["ClaimAmount"], F.lit(0)) - 
                            F.when(claims_details_df["StageID"] == 26, F.coalesce(claims_details_df["DeductionAmount"], F.lit(0))).otherwise(F.lit(0)))).alias("mr_sanctioned_after_present_claim"))
        .first()["mr_sanctioned_after_present_claim"] or 0
    )

    Sublimitbalance -= mr_sanctioned_after_present_claim

    
    filtered_claims_df = claims_df.filter(
        (F.col('Deleted') == 0) &
        (F.col('ReceivedDate') <= ClaimReceivedDate) &
        (F.col('ID') != claimID)
    )

    filtered_claims_details_df = claims_details_df.filter(
        (F.col('Deleted') == 0) &
        (F.col('ClaimID').isin(filtered_claims_df.select('ID').rdd.flatMap(lambda x: x).collect()))
    )

    filtered_claim_utilized_amount_df = claim_utilized_amount_df.filter(
        (F.col('Deleted') == 0) &
        (F.col('ClaimId').isin(filtered_claims_details_df.select('ClaimID').rdd.flatMap(lambda x: x).collect()))
    )

    filtered_member_policy_df = memberpolicy_df.filter(
        (F.col('ID').isin(filtered_claims_df.select('MemberPolicyId').rdd.flatMap(lambda x: x).collect())) &
        (F.col('MainmemberID') == MainMemberID) &
        (F.col('ID') == F.when(F.lit(SITypeID) == 5, F.col('ID')).otherwise(MemberPolicyID))
    )

    # SETTLED CASES
    settled_cases = filtered_claim_utilized_amount_df.join(
        filtered_claims_details_df,
        (filtered_claim_utilized_amount_df.Claimid == filtered_claims_details_df.ClaimID) &
        (filtered_claim_utilized_amount_df.Slno == filtered_claims_details_df.Slno)
    ).filter(
        (filtered_claims_details_df.RequestTypeID >= 4) &
        (filtered_claims_details_df.StageID == 27) &
        (filtered_claims_details_df.RequestTypeID != 9)
    ).agg(F.sum(F.coalesce(filtered_claim_utilized_amount_df.SanctionedAmount, F.lit(0))).alias('SettledAmount'))

    settled_amount = settled_cases.first()['SettledAmount']

    # MR CASES WHICH ARE NOT YET SETTLED
    settled_claim_ids = claims_details_df.filter(
    (claims_details_df.StageID.isin([27, 23, 21, 25])) &
    (claims_details_df.RequestTypeID >= 4)
    ).select('ClaimID').rdd.flatMap(lambda x: x).collect()

    # Now, filter the filtered_claims_details_df for MR cases that are not yet settled
    mr_cases_not_settled = filtered_claims_details_df.filter(
        (filtered_claims_details_df.RequestTypeID >= 4) &
        (filtered_claims_details_df.ClaimTypeID == 2) &
        (~filtered_claims_details_df["ClaimID"].isin(settled_claim_ids))
    ).agg(F.sum(
        F.when(F.col('SanctionedAmount') > 0, F.col('SanctionedAmount')).otherwise(
            F.col('ClaimAmount') - F.when(F.col('StageID') == 26, F.col('DeductionAmount')).otherwise(F.lit(0))
        )
    ).alias('MRAmount'))

    mr_amount = mr_cases_not_settled.first()['MRAmount'] if mr_cases_not_settled.first() else 0

    # PP CASES WHICH ARE NOT YET SETTLED
    settled_claim_ids = claims_details_df.filter(
    (claims_details_df.StageID.isin([27, 23, 21, 25])) &
    (claims_details_df.RequestTypeID >= 4)
    ).select('ClaimID').rdd.flatMap(lambda x: x).collect()

    # Now, join and filter the DataFrames for PP cases that are not yet settled
    pp_cases_not_settled = filtered_claim_utilized_amount_df.join(
        filtered_claims_details_df,
        (filtered_claim_utilized_amount_df.Claimid == filtered_claims_details_df.ClaimID) &
        (filtered_claim_utilized_amount_df.Slno == filtered_claims_details_df.Slno)
    ).filter(
        (filtered_claims_details_df.RequestTypeID < 4) &
        (~filtered_claims_details_df.StageID.isin([21, 23, 25])) &
        (~filtered_claims_details_df["ClaimID"].isin(settled_claim_ids))
    ).agg(F.sum(F.coalesce(filtered_claim_utilized_amount_df.SanctionedAmount, F.lit(0))).alias('PPAmount'))

    # Retrieve the PPAmount result
    pp_amount = pp_cases_not_settled.first()['PPAmount'] if pp_cases_not_settled.first() else 0

    pp_amount_after = pp_sanctioned_after_present_claim_first - pp_sanctioned_after_present_claim_sec

    if pp_amount_after:
        pp_amount_after = pp_amount_after
    else:
        pp_amount_after = 0

    #  MR CLAIMS WHICH HAVE SANCTIONED AMOUNT AFTER THE PRESENT CLAIM
    mr_sanctioned_after = (
        claims_details_df.join(claims_df, claims_details_df["ClaimID"] == claims_df["ID"])
        .join(memberpolicy_df, claims_df["MemberPolicyId"] == memberpolicy_df["ID"])
        .filter(common_filter & 
                (claims_details_df["RequestTypeID"] >= 4) &
                (claims_details_df["ClaimTypeID"] == 2) &
                claims_details_df["StageID"].isin(26, 27))
        .agg(F.sum(F.when(F.coalesce(claims_details_df["SanctionedAmount"], F.lit(0)) > 0, 
                        F.coalesce(claims_details_df["SanctionedAmount"], F.lit(0)))
                .otherwise(F.coalesce(claims_details_df["ClaimAmount"], F.lit(0)) - 
                            F.when(claims_details_df["StageID"] == 26, F.coalesce(claims_details_df["DeductionAmount"], F.lit(0))).otherwise(F.lit(0)))).alias("mr_sanctioned_after"))
        .first()["mr_sanctioned_after"] or 0
    )
        
    # Calculate Total Balance
    total_balance = SumInsured + CB_Amount - settled_amount - mr_amount - pp_amount - pp_amount_after - mr_sanctioned_after


    if total_balance < Sublimitbalance:
        if total_balance < 0:
            return 0
        else:
            return total_balance
    else:
        if Sublimitbalance < 0:
            return 0
        else:
            return Sublimitbalance
print(balance_sum_insured_without_present_claim(24070402719, 2))

# COMMAND ----------

# DBTITLE 1,UFN_Spectra_BalanceSumInsured
from pyspark.sql.functions import *
from pyspark.sql.functions import sum as _sum

member_policy_df = spark.table("fhpl.memberpolicy")
member_si_df = spark.table("fhpl.membersi")
bpsuminsured_df = spark.table("fhpl.bpsuminsured")
bpsiconditions_df = spark.table("fhpl.bpsiconditions")
bp_service_config_details_df = spark.table("fhpl.bpserviceconfigdetails")
claims_df = spark.table("fhpl.claims")
claim_utilized_amount_df = spark.table("fhpl.claimutilizedamount")
claims_details_df = spark.table("fhpl.claimsdetails")


# SP Name: "BalanceSumInsured Function On 2024-07-18"
def UFN_Spectra_BalanceSumInsured(claimid, slno=None):
    SITypeID = 0
    MainMemberID = 0
    MemberPolicyID = 0
    RelationshipIds = 0
    SumInsured = 0
    CB_Amount = 0
    joined_df = claims_df.alias("C") \
        .join(member_policy_df.alias("MP"), col("C.MemberPolicyID") == col("MP.id")) \
        .join(member_si_df.alias("S"), col("MP.id") == col("S.memberpolicyid")) \
        .join(bpsuminsured_df.alias("BP"), (col("BP.id") == col("S.BPSIID")) & (col("BP.SICategoryID_P20") == 69)) \
        .join(bpsiconditions_df.alias("BPCON"), (col("BPCON.bpsiid") == col("BP.id")) & (col("BPCON.bpconditionid") == 30) &
            (col("BP.policyid") == coalesce(col("BPCON.policyid"), lit(0))), "left") \
        .join(bp_service_config_details_df.alias("BPSCON_IND"), (col("BPSCON_IND.benefitplansiid") == col("BP.id")) &
            (col("C.servicetypeid") == col("BPSCON_IND.servicetypeid")) &
            (col("BPSCON_IND.LimitCatg_P29") == 108), "left") \
        .join(bp_service_config_details_df.alias("BPSCON_FAM"), (col("BPSCON_FAM.benefitplansiid") == col("BP.id")) &
            (col("C.servicetypeid") == col("BPSCON_FAM.servicetypeid")) &
            (col("BPSCON_FAM.LimitCatg_P29") == 107), "left") \
        .filter(col("C.ID") == claimid)   

    result = joined_df.select(
        col("MP.MainmemberID").alias("MainMemberID"),
        col("C.MemberPolicyID").alias("MemberPolicyID"),
        col("BP.SITypeID").alias("SITypeID"),
        coalesce(col("S.CB_Amount"), lit(0)).alias("CB_Amount"),
        col("BP.SumInsured").alias("SumInsured"),
        col("BPCON.Relationshipid").alias("RelationshipIds"),
        col("BPCON.FamilyLimit").alias("SubLimit"),
        col("MP.RelationshipID").alias("ClaimRelationshipID"),
        when(col("BPSCON_FAM.InternalValueAbs") < col("BPSCON_IND.InternalValueAbs"), col("BPSCON_FAM.InternalValueAbs"))
            .otherwise(col("BPSCON_IND.InternalValueAbs")).alias("ServiceLimit")
    ).limit(1).collect()

    if result:
        MainMemberID = result[0]["MainMemberID"]
        MemberPolicyID = result[0]["MemberPolicyID"]
        SITypeID = result[0]["SITypeID"]
        CB_Amount = result[0]["CB_Amount"]
        SumInsured = result[0]["SumInsured"]
        RelationshipIds = result[0]["RelationshipIds"]
        SubLimit = result[0]["SubLimit"]
        ClaimRelationshipID = result[0]["ClaimRelationshipID"]
        ServiceLimit = result[0]["ServiceLimit"]

    # Additional logic for SITypeID
    if SITypeID == 5:
        MemberPolicyID = MainMemberID
        if CB_Amount == 0:
            cb_amount_df = member_si_df.alias("MS") \
                .join(member_policy_df.alias("MP1"), col("MS.memberpolicyid") == col("MP1.id")) \
                .filter(col("MP1.id") == MainMemberID) \
                .select(coalesce(col("MS.CB_Amount"), lit(0)).alias("CB_Amount")).limit(1).collect()
            if cb_amount_df:
                CB_Amount = cb_amount_df[0]["CB_Amount"]

    # Calculate sums
    settled_cases_sum = claim_utilized_amount_df.alias("CU") \
        .join(claims_details_df.alias("CD"), (col("CU.Claimid") == col("CD.ClaimID")) & (col("CU.Slno") == col("CD.Slno"))) \
        .join(claims_df.alias("C"), col("CD.ClaimID") == col("C.ID")) \
        .join(member_policy_df.alias("MP"), (col("MP.ID") == col("C.MemberPolicyId")) & (col("MP.MainmemberID") == MainMemberID) &
              (col("MP.ID") == when(SITypeID == lit(5), col("MP.ID")).otherwise(MemberPolicyID))) \
        .filter((col("CD.RequestTypeID") >= 4) & (col("CD.StageID") == 27) & (col("CD.RequestTypeID") != 9)) \
        .agg(_sum(coalesce(col("CU.SanctionedAmount"), lit(0)))).collect()[0][0]

    mr_cases_sum = claims_df.alias("C") \
    .join(claims_details_df.alias("CD"), col("CD.ClaimID") == col("C.ID"), "inner") \
    .join(member_policy_df.alias("MP"), col("MP.ID") == col("C.MemberPolicyId"), "inner") \
    .filter((col("CD.RequestTypeID") >= 4) & (col("CD.ClaimTypeID") == 2) & (col("C.Deleted") == 0) & (col("CD.Deleted") == 0) & 
            (col("MP.MainmemberID") == MainMemberID) & (col("MP.ID") == (when(SITypeID == lit(5), col("MP.ID")).otherwise(MemberPolicyID))) & 
            (expr(f"(',' + cast('{RelationshipIds}' as string) + ',') like '%,' + cast(MP.relationshipid as string) + ',%'")) & 
            (col("CD.ClaimID") != col("C.ID")) & (col("CD.RequestTypeID") < 4) & (~col("CD.StageID").isin([27, 23, 21, 25]))) \
    .agg(_sum(when(col("CD.SanctionedAmount").isNotNull(), col("CD.SanctionedAmount")).otherwise(col("CD.ClaimAmount") - when(col("CD.StageID") == 26, col("CD.DeductionAmount")).otherwise(lit(0)))).alias("sum")) \
    .collect()[0]["sum"]

    # Create a DataFrame for the subquery to filter out unwanted rows
    excluded_ids_df = claims_df.alias('C') \
    .join(claims_details_df.alias("CD3"), col("CD3.ClaimID") == col("C.ID")) \
    .filter((col("CD3.RequestTypeid") >= 4) & (col("CD3.StageID").isin([27, 23, 21, 25]))) \
    .select("CD3.Id")

    # Calculate pp_cases_not_settled_sum
    pp_cases_not_settled_sum = claim_utilized_amount_df.alias("CU") \
    .join(claims_details_df.alias("CD"), (col("CU.Claimid") == col("CD.ClaimID")) & (col("CU.Slno") == col("CD.Slno"))) \
    .join(claims_df.alias("C"), col("CD.ClaimID") == col("C.ID")) \
    .join(member_policy_df.alias("MP"), (col("MP.ID") == col("C.MemberPolicyId")) & (col("MP.MainMemberID") == MainMemberID) &
          (col("MP.ID") == when(col("SITypeID") == lit(5), col("MP.ID")).otherwise(MemberPolicyID))) \
    .join(excluded_ids_df, col("CD.Id") == col("CD3.Id"), "left_anti") \
    .filter((col("CD.RequestTypeID") < 4) & (~col("CD.StageID").isin([21, 23, 25]))) \
    .agg(_sum(coalesce(col("CU.SanctionedAmount"), lit(0)))).collect()[0][0]

    SubLimitbalance =  SumInsured - settled_cases_sum - mr_cases_sum - pp_cases_not_settled_sum + CB_Amount #TODO: add the subtraction here # update done

    # Calculate Totalbalance
    total_settled_cases_sum = claim_utilized_amount_df.alias("CU") \
        .join(claims_details_df.alias("CD"), (col("CU.Claimid") == col("CD.ClaimID")) & (col("CU.Slno") == col("CD.Slno"))) \
        .join(claims_df.alias("C"), col("CD.ClaimID") == col("C.ID")) \
        .join(member_policy_df.alias("MP"), (col("MP.ID") == col("C.MemberPolicyId")) & (col("MP.MainMemberID") == MainMemberID) &
              (col("MP.ID") == when(SITypeID == lit(5), col("MP.ID")).otherwise(MemberPolicyID))) \
        .filter((col("CD.RequestTypeID") >= 4) & (col("CD.StageID") == 27) & (col("CD.RequestTypeID") != 9)) \
        .agg(_sum(coalesce(col("CU.SanctionedAmount"), lit(0)))).collect()[0][0]

    total_mr_cases_not_settled_sum = claims_df.alias("C") \
    .join(claims_details_df.alias("CD"), col("CD.ClaimID") == col("C.ID")) \
    .join(member_policy_df.alias("MP"), (col("MP.ID") == col("C.MemberPolicyId")) & (col("MP.MainMemberID") == MainMemberID) &
          (col("MP.ID") == when(SITypeID == lit(5), col("MP.ID")).otherwise(MemberPolicyID))) \
    .join(excluded_ids_df.alias("EX"), col("CD.Id") == col("EX.Id"), "left_anti") \
    .filter((col("CD.RequestTypeID") >= 4) & (col("CD.ClaimTypeID") == 2)) \
    .agg(_sum(
        when(coalesce(col("CD.SanctionedAmount"), lit(0)) > lit(0), coalesce(col("CD.SanctionedAmount"), lit(0)))
        .otherwise(col("CD.ClaimAmount") - when(col("CD.StageID") == 26, coalesce(col("CD.DeductionAmount"), lit(0))).otherwise(lit(0)))
    )).collect()[0][0]

    total_pp_cases_not_settled_sum = claim_utilized_amount_df.alias("CU") \
    .join(claims_details_df.alias("CD"), (col("CU.Claimid") == col("CD.ClaimID")) & (col("CU.Slno") == col("CD.Slno"))) \
    .join(claims_df.alias("C"), col("CD.ClaimID") == col("C.ID")) \
    .join(member_policy_df.alias("MP"), (col("MP.ID") == col("C.MemberPolicyId")) & (col("MP.MainMemberID") == MainMemberID) &
          (col("MP.ID") == when(SITypeID == lit(5), col("MP.ID")).otherwise(MemberPolicyID))) \
    .join(excluded_ids_df.alias("EX"), col("CD.Id") == col("EX.Id"), "left_anti") \
    .filter((col("CD.RequestTypeID") < 4) & ~col("CD.StageID").isin([21, 23, 25])) \
    .agg(_sum(coalesce(col("CU.SanctionedAmount"), lit(0)))).collect()[0][0]
    total_pp_cases_not_settled_sum = total_pp_cases_not_settled_sum if total_pp_cases_not_settled_sum is not None else 0

    Totalbalance = SumInsured - total_settled_cases_sum - total_mr_cases_not_settled_sum - total_pp_cases_not_settled_sum + CB_Amount 
    # TODO: add the subtraction here # added the subtraction 
    if Totalbalance > SubLimitbalance:
        return Totalbalance
    else:
        return SubLimitbalance
    
print(UFN_Spectra_BalanceSumInsured(24070402719))

# COMMAND ----------

