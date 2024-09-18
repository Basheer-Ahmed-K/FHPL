# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,sp_RejectionReason
# %sql
# USE [McarePlus]
# GO
# /****** Object:  UserDefinedFunction [dbo].[UDF_RejectionReason]    Script Date: 22-07-2024 11:52:25 ******/
# SET ANSI_NULLS ON
# GO
# SET QUOTED_IDENTIFIER ON
# GO
# --select dbo.UDF_RejectionReason(16052600021 ,1)    
    
# ALTER function [dbo].[UDF_RejectionReason](@claimid bigint,@slno tinyint)            
# returns varchar(max)            
# as            
# Begin            
# Declare @Reason varchar(max),@serviceid int,@deductionslno tinyint            
# declare @i int,@j int            
# declare @reason1 varchar(max)            
# declare cursor_Dis_Reason cursor for            
# select Replace(Replace(Replace(RePlace(Replace(Replace(RR.Name,'|FT|',  IsNull(CRR.FreeText1,'')),'|FT1|',  IsNull(CRR.FreeText1,'')), '~DOA~', Convert(Varchar, C.DateOfAdmission,106)),               
#  '~DOD~', isnull(Convert(Varchar, C.DateOfDischarge,106),'')), '~PSD~', Convert(Varchar, MD.MemberCommencingDate,106)), '~PID~', isnull(Convert(Varchar, MD.MemberInceptionDate,106),''))        
# FROM ClaimRejectionReasons CRR with inner join Mst_RejectionReasons RR with on CRR.RejectionReasonsID=RR.ID and RR.Deleted=0 ,     
# Claims C with, MemberPolicy MD with       
# where CRR.Deleted=0 and C.Deleted=0 and MD.Deleted=0 and         
# C.ID=CRR.ClaimID and C.MemberPolicyID=MD.ID       
# and crr.claimid=@claimid and crr.slno=@slno    
# --union 
# --select isnull(RuleName,'') from ClaimRules where isOverride=1 and claimid=@claimid and SlNo=@slno and Deleted=0    
# union    
# select CRR.Remarks      
# FROM ClaimRejectionReasons CRR with,     
# Claims C with, MemberPolicy MD with        
# where CRR.Deleted=0 and C.Deleted=0 and MD.Deleted=0 and         
# C.ID=CRR.ClaimID and C.MemberPolicyID=MD.ID   and crr.RejectionReasonsID=0 and crr.Remarks is not null    
# and crr.claimid=@claimid and crr.slno=@slno    
#  union  
#  select reason from claimsdetails with where claimid=@Claimid and slno=@Slno and ltrim(rtrim(isnull(reason,'')))<>'' and createddatetime<'2016-05-12' and deleted=0  
#  union  
# select remarks from claimsdetails with where claimid=@Claimid and slno=@Slno and ltrim(rtrim(isnull(remarks,'')))<>'' and createddatetime<'2016-05-12' 
# and requesttypeid in(1,2,3) and stageid=23
   
# open cursor_Dis_Reason     
# fetch next from cursor_Dis_Reason into @Reason1            
# set @reason=''            
# while(@@fetch_status=0)            
# Begin            
# set @reason=@reason1+','+@reason            
            
# fetch next from cursor_Dis_Reason into @Reason1            
            
# end            
# close cursor_Dis_Reason            
# deallocate cursor_Dis_Reason            
# Return @reason            
            
# End 

# COMMAND ----------

def udf_rejection_reason(claimid, slno, claims_df, claim_rejection_reasons_df, mst_rejection_reasons_df, member_policy_df, claims_details_df):
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

# DBTITLE 1,call sp_RejectionReason
claim_rejection_reasons_df = spark.sql("SELECT * FROM fhpl.ClaimRejectionReasons")
mst_rejection_reasons_df = spark.sql("SELECT * FROM fhpl.Mst_RejectionReasons")
claims_df = spark.sql("SELECT * FROM fhpl.Claims")
member_policy_df = spark.sql("SELECT * FROM fhpl.MemberPolicy")
claims_details_df = spark.sql("SELECT * FROM fhpl.ClaimsDetails")
claimid = 24070402926
slno = 1
reason = udf_rejection_reason(claimid, slno, claims_df, claim_rejection_reasons_df, mst_rejection_reasons_df, member_policy_df, claims_details_df)
print(reason)

# COMMAND ----------

# DBTITLE 1,[UFN_Spectra_BalanceSumInsured_Without_PresentClaim]
## Corrected Code by ANAND
# Check the commented logic that has issue and try to see whether we are applying it correct

def balance_sum_insured_without_present_claim(claimID, slno, claims_df, member_policy_df, membersi_df, bpsuminsured_df, bpsiconditions_df, bpserviceconfigdetails_df, claim_utilized_amount_df, claims_details_df):
    claims_df = claims_df.alias("C")
    member_policy_df = member_policy_df.alias("MP")
    membersi_df = membersi_df.alias("S")
    bpsuminsured_df = bpsuminsured_df.alias("BP")
    bpsiconditions_df = bpsiconditions_df.alias("BPCON")
    bpserviceconfigdetails_df = bpserviceconfigdetails_df.alias("BPSCON")
    claim_utilized_amount_df = claim_utilized_amount_df.alias("CU")
    claims_details_df = claims_details_df.alias("CD")
    
    initial_data= claims_df.join(
            member_policy_df,
            col("C.MemberPolicyID") == col("MP.ID")
        ).join(
            membersi_df,
            col("MP.ID") == col("S.MemberPolicyID")
        ).join(
            bpsuminsured_df,
            (col("BP.ID") == col("S.BPSIID")) & (col("BP.SICategoryID_P20") == 69)
        ).join(
            bpsiconditions_df,
            (col("BPCON.BPSIID") == col("BP.ID")) & (col("BPCON.BPConditionID") == 30) &
            (col("BP.PolicyID") == col("BPCON.PolicyID")) &
            (concat(lit(","), col("BPCON.RelationshipID"), lit(","))).contains(concat(lit("%,"), col("MP.RelationshipID").cast("string"), lit(",%"))) |
            col("BPCON.RelationshipID").isNull(),
            "left"
        ).join(
            bpserviceconfigdetails_df.alias("BPSCON_IND"),
            (col("BPSCON_IND.BenefitPlanSIID") == col("BP.ID")) & 
            (col("C.ServiceTypeID") == col("BPSCON_IND.ServiceTypeID")) & 
            (concat(lit(","), col("BPSCON_IND.AllowedRelationIDs"), lit(","))).contains(concat(lit("%,"), col("MP.RelationshipID").cast("string"), lit(",%"))) & 
            (col("BPSCON_IND.LimitCatg_P29") == 108),
            "left"
        ).join(
            bpserviceconfigdetails_df.alias("BPSCON_FAM"),
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

    sub_limit_balance = (
        initial_data.withColumn(
            "SubLimitBalance",
            coalesce(col("ServiceLimit"), col("SumInsured")) + col("CB_Amount") - 
            coalesce(
                lit(claim_utilized_amount_df.join(
                    claims_df,
                    col("CU.ClaimID") == col("C.ID")
                ).join(
                    claims_details_df,
                    (col("CD.ClaimID") == col("C.ID")) & (col("CD.SlNo") == col("CU.SlNo"))
                ).filter(
                    (col("C.Deleted") == 0) & (col("CU.Deleted") == 0) & (col("CD.Deleted") == 0) &
                    (col("CD.RequestTypeID") >= 4) & (col("CD.StageID") == 27) & (col("CD.RequestTypeID") != 9) &
                    (col("CU.ClaimID") != lit(claimID).cast("string")) & (col("C.ReceivedDate") <= initial_row["ClaimReceivedDate"])
                ).select(sum(coalesce(col("CU.SanctionedAmount"), lit(0))).alias("SettledCases")).first()["SettledCases"]),
                lit(0)
            ) 
            - 
            coalesce(
                lit(claims_details_df.join(
                    claims_df,
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
                lit(claim_utilized_amount_df.join(
                    claims_df,
                    col("CU.ClaimID") == col("C.ID")
                ).join(
                    claims_details_df,
                    (col("CD.ClaimID") == col("C.ID")) & (col("CD.SlNo") == col("CU.SlNo"))
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
                lit(claim_utilized_amount_df.join(
                    claims_df,
                    col("CU.ClaimID") == col("C.ID")
                ).join(
                    claims_details_df,
                    (col("CD.ClaimID") == col("C.ID")) & (col("CD.SlNo") == col("CU.SlNo"))
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

claimID = 24070402456
slno = 3
claims_df = spark.sql("SELECT * FROM fhpl.Claims")
member_policy_df = spark.sql("SELECT * FROM fhpl.MemberPolicy")
claims_details_df = spark.sql("SELECT * FROM fhpl.ClaimsDetails")
membersi_df = spark.sql("SELECT * FROM fhpl.MemberSI")
bpsuminsured_df = spark.sql("SELECT * FROM fhpl.BPSumInsured")
bpsiconditions_df = spark.sql("SELECT * FROM fhpl.BPSIConditions")
bpserviceconfigdetails_df = spark.sql("SELECT * FROM fhpl.BPServiceConfigDetails")
claim_utilized_amount_df = spark.sql("SELECT * FROM fhpl.ClaimUtilizedAmount")

balance = balance_sum_insured_without_present_claim(
    claimID, slno, claims_df, member_policy_df, membersi_df, bpsuminsured_df, 
    bpsiconditions_df, bpserviceconfigdetails_df, claim_utilized_amount_df, claims_details_df
)
print(balance)

# COMMAND ----------

# DBTITLE 1,UFN_IRPendingReasons
def UFN_IRPendingReasons(claimid: int, slno: int, IsMandatory: int = None, IsReceived: int = None) -> str:
    claims_ir_reasons_df = spark.sql("SELECT * FROM fhpl.ClaimsIRReasons")
    mst_irdocuments_df = spark.sql("SELECT * FROM fhpl.Mst_IRDocuments")
    mst_issuingauthority_df = spark.sql("SELECT * FROM fhpl.Mst_IssuingAuthority")
    claims_df = spark.sql("SELECT * FROM fhpl.Claims")
    mst_services_df = spark.sql("SELECT * FROM fhpl.Mst_Services")

    joined_df = claims_ir_reasons_df.alias('cr') \
        .join(mst_irdocuments_df.alias('ir'), col('cr.IRDocumentID') == col('ir.ID'), 'left') \
        .join(claims_df.alias('c'), col('cr.ClaimID') == col('c.id'), 'left') \
        .join(mst_issuingauthority_df.alias('i'), col('c.issueid') == col('i.id'), 'left') \
        .filter((col('cr.ClaimID') == claimid) & (col('slno') == slno) & (col('cr.Deleted') == 0) &
                (col('cr.ismandatory') == (IsMandatory if IsMandatory is not None else col('cr.ismandatory'))) &
                (col('cr.IsReceived') == (IsReceived if IsReceived is not None else col('cr.IsReceived'))))
    # joined_df.display()


    reason_df = joined_df.select(
        when(col('cr.IRDocumentID') == 0, col('cr.Remarks'))
        .otherwise(concat_ws('', 
                             when(col('ir.Name').contains('~INS~'), col('i.Name')).otherwise(col('ir.Name')),
                             when(col('ir.Name').contains('|FT|'), col('cr.FreeText1')).otherwise(lit('')),
                             when(col('ir.Name').contains('|FT1|'), col('cr.FreeText2')).otherwise(lit(''))
                            )).alias('Reason')
    ).distinct()
    # reason_df.display()

    union_df = reason_df.union(
        claims_ir_reasons_df.filter((col('ClaimID') == claimid) & (col('slno') == slno) & (col('Deleted') == 0) &
                                    (col('ismandatory') == (IsMandatory if IsMandatory is not None else col('ismandatory'))) &
                                    (col('IsReceived') == (IsReceived if IsReceived is not None else col('IsReceived'))) &
                                    (col('IRDocumentID') == 0) & (col('serviceid') == 0))
        .select(col('Remarks').alias('Reason'))
    ).union(
        claims_ir_reasons_df.alias('cr')
        .join(mst_services_df.alias('s'), col('cr.serviceid') == col('s.ID'), 'left')
        .filter((col('cr.ClaimID') == claimid) & (col('slno') == slno) & (col('cr.Deleted') == 0) &
                (col('cr.ismandatory') == (IsMandatory if IsMandatory is not None else col('cr.ismandatory'))) &
                (col('cr.IsReceived') == (IsReceived if IsReceived is not None else col('cr.IsReceived'))) &
                (col('cr.IRDocumentID') == 0))
        .select(col('cr.Remarks').alias('Reason'))
    )
    # union_df.display()

    result_df = union_df.select(concat_ws('.', col('Reason')).alias('Reasons'))
    # result_df.display()

    result = result_df.collect()[0]['Reasons'] if result_df.count() > 0 else None

    # result = union_df.select(concat_ws(",", col("Reason1")).alias("Reasons")).collect()
    # if result:
    #     content = [row["Reasons"] for row in result]
    #     output = ", ".join(content)
    #     return output

    return result

# COMMAND ----------

claimID = 24070402002
slno = 3
isMandatory = 1
isReceived = 0

pending_reasons = UFN_IRPendingReasons(claimID, slno, isMandatory, isReceived)
print(pending_reasons)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM fhpl.Claims

# COMMAND ----------

# DBTITLE 1,UDF_GetIncurredAmount
def UDF_GetIncurredAmount(claimid, slno):
    claims_df = spark.sql("SELECT * FROM fhpl.Claims")
    member_policy_df = spark.sql("SELECT * FROM fhpl.MemberPolicy").filter(col('deleted') == False)
    claims_details_df = spark.sql("SELECT * FROM fhpl.ClaimsDetails")
    member_si_df = spark.sql("SELECT * FROM fhpl.MemberSI").filter(col('deleted') == False)
    bp_suminsured_df = spark.sql("SELECT * FROM fhpl.BPSumInsured").filter(col('deleted') == False)
    claim_stage_df = spark.sql("SELECT * FROM fhpl.claimstage")
    claims_action_items_df = spark.sql("SELECT * FROM fhpl.claimActionItems")

    joined_df = claims_details_df.alias('cd') \
        .join(claims_df.alias('c'), (col('cd.Claimid') == col('c.id')) & (col('cd.deleted') == 0) & (col('c.deleted') == 0), 'inner') \
        .join(claim_stage_df.alias('cs'), col('cs.id') == col('cd.StageID'), 'inner') \
        .join(member_policy_df.alias('m'), col('m.id') == col('c.memberpolicyid'), 'inner') \
        .join(member_si_df.alias('ms'), col('ms.memberpolicyid') == col('m.id'), 'inner') \
        .join(bp_suminsured_df.alias('bs'), (col('bs.id') == col('ms.BPSIID')) & (col('bs.SICategoryID_P20') == 69), 'inner') \
        .filter((col('cd.claimid') == claimid) & (col('cd.slno') == slno))

    values = joined_df.select(
        col('cd.Claimtypeid').alias('Claimtype'),
        col('cd.Requesttypeid').alias('Requesttype'),
        when(col('cd.requesttypeid').isin(1, 2, 3), col('cs.name'))
        .when(col('cd.stageid') == 27, lit('Settled'))
        .when(col('cd.StageID').isin(21, 23, 25), col('cs.name'))
        .otherwise(
            coalesce(
                spark.sql("""
                    SELECT cs1.name
                    FROM fhpl.claimsdetails cd 
                    JOIN fhpl.claimactionitems ca1 on cd.ClaimID = ca1.ClaimID
                    JOIN fhpl.claimstage cs1 ON ca1.ClaimStageID = cs1.id
                    WHERE ca1.claimid = cd.claimid AND ca1.slno = cd.slno --AND ca1.CloseDate IS NULL
                    ORDER BY ca1.ID DESC
                    LIMIT 1
                """).first()['name'],
                col('cs.Name')
            )
        ).alias('Currentclaimstatus'),
        col('cd.payableamount').alias('Incurred'),
        col('cd.settledamount').alias('Settled'),
        col('cd.claimamount').alias('Claimed'),
        when(col('cd.stageid') == 23, lit(0)).otherwise(col('cd.SanctionedAmount')).alias('Sanctioned'),
        col('bs.SumInsured').alias('coverageamount'),
        col('c.deleted').alias('Claim_Deleted'),
        col('cd.deleted').alias('Claimdetails_Deleted')
    ).first()
    display(values)
    pasanctioned_amount = spark.sql(f"""
        SELECT CASE WHEN claimtypeid = 1 THEN isnull(a.SanctionedAmount, 0) END AS PASanctionedAmount
        FROM (
            SELECT Claimid, slno, claimtypeid, sanctionedamount, row_number() OVER (PARTITION BY claimid ORDER BY claimid, slno DESC) AS rowno
            FROM fhpl.Claimsdetails
            WHERE claimid = {claimid} AND deleted = 0 AND RequestTypeID IN (1, 2, 3) AND stageid NOT IN (23)
        ) a
        WHERE a.rowno = 1
    """).first()['PASanctionedAmount']

    mainmemberstatus = spark.sql(f"""
        SELECT 1 AS mainmemberstatus
        FROM fhpl.ClaimsDetails cd
        JOIN fhpl.Claims c ON cd.claimid = c.id
        JOIN fhpl.MemberPolicy mp ON c.MemberPolicyID = mp.id
        JOIN fhpl.MemberPolicy mp2 ON mp.MainmemberID = mp2.MainmemberID
        JOIN fhpl.Claims c2 ON mp2.id = c2.MemberPolicyID
        WHERE cd.CLAIMTYPEID = 1 AND cd.claimid = c2.id AND cd.deleted = 0 AND c.deleted = 0 AND mp.deleted = 0 AND mp2.deleted = 0 AND c2.deleted = 0
        AND NOT EXISTS (
            SELECT 1
            FROM fhpl.Claimsdetails cd2
            WHERE cd2.ClaimID = c2.id AND cd2.RequestTypeID < 4 AND cd2.StageID = 29
        )
        AND cd.claimid = {claimid} AND cd.slno = {slno}
    """).first()['mainmemberstatus']

    incurred = values['Incurred']
    if values['Claim_Deleted'] == 0 and values['Claimdetails_Deleted'] == 0:
        if values['Currentclaimstatus'] in (
            'Registration', 'For Adjudication', 'Refer to CS', 'Refer to Enrollment', 'Query Response (H)', 'Query Response (M)',
            'Insurer Response', 'CS Response', 'Enrollment Response', 'Refer to Insurer', 'Insurer Authorization', 'Investigation Done',
            'Query to Hospital', 'Query to Member', 'First Reminder-Hospital', 'Second Reminder-Hospital', 'Third Reminder-Hospital',
            'Fourth Reminder-Hospital', 'First Reminder-Member', 'Second Reminder-Member', 'Third Reminder-Member', 'Fourth Reminder-Member',
            'From CRM', 'For Investigation', 'For Bill Entry', 'For Audit', 'Refer To CRM', 'Audit Return'
        ) and values['Requesttype'] not in (1, 2, 3):
            incurred = min(values['Claimed'], values['coverageamount']) if values['Claimtype'] == 2 else min(values['Claimed'], pasanctioned_amount or values['Sanctioned'])

        if values['Requesttype'] in (1, 2, 3):
            incurred = min(values['Claimed'], values['Sanctioned'])

        if values['Claimtype'] == 1 and mainmemberstatus == 1 and values['Requesttype'] in (1, 2, 3):
            balance_sum_insured = spark.sql(f"""
                SELECT balance_sum_insured_without_present_claim(claimid,3) AS BalanceSumInsured
                FROM fhpl.Claims
                WHERE ID = {claimid}
            """).first()['BalanceSumInsured']
            incurred = min(values['Claimed'], balance_sum_insured or values['Sanctioned'])

        if values['Claimtype'] == 2 and values['Currentclaimstatus'] in (
            'For Bill Entry', 'For Adjudication', 'Query to Hospital', 'Query to Member', 'Refer to CS', 'Refer to CRM', 'Refer to Enrollment',
            'Insurer Response', 'CS Response', 'Enrollment Response', 'Refer to Insurer', 'For Investigation', 'Investigation Done', 'Sent for Audit',
            'For Audit', 'Audit Return', 'First Reminder-Hospital', 'Second Reminder-Hospital', 'Third Reminder-Hospital', 'Fourth Reminder-Hospital',
            'First Reminder-Member', 'Second Reminder-Member', 'Third Reminder-Member', 'Fourth Reminder-Member', 'From CRM'
        ):
            incurred_amount = spark.sql(f"""
                SELECT fhpl.UFN_Spectra_BalanceSumInsured_Without_PresentClaim(ID, 1) AS IncurredAmount
                FROM fhpl.Claims
                WHERE ID = {claimid}
            """).first()['IncurredAmount']
            bill_amount = values['Claimed']
            incurred = min(incurred_amount, bill_amount)

        if values['Currentclaimstatus'] == 'Settled':
            incurred = values['Settled']

        if values['Currentclaimstatus'] in ('Closed', 'Repudiated', 'Cancelled'):
            incurred = 0

        if values['Currentclaimstatus'] in ('For Payment', 'For Settlement') and values['Claimtype'] == 2:
            incurred = values['Sanctioned']

        if values['Claimtype'] == 2 and values['Currentclaimstatus'] in (
            'For Bill Entry', 'For Adjudication', 'Query to Hospital', 'Query to Member', 'Refer to CS', 'Refer to CRM', 'Refer to Enrollment',
            'Insurer Response', 'CS Response', 'Enrollment Response', 'Refer to Insurer', 'For Investigation', 'Investigation Done', 'Sent for Audit',
            'For Audit', 'Audit Return', 'First Reminder-Hospital', 'Second Reminder-Hospital', 'Third Reminder-Hospital', 'Fourth Reminder-Hospital',
            'First Reminder-Member', 'Second Reminder-Member', 'Third Reminder-Member', 'Fourth Reminder-Member', 'From CRM'
        ):
            incurred_amount = spark.sql(f"""
                SELECT fhpl.UFN_Spectra_BalanceSumInsured_Without_PresentClaim(ID, 1) AS IncurredAmount
                FROM fhpl.Claims
                WHERE ID = {claimid}
            """).first()['IncurredAmount']
            bill_amount = values['Claimed'] 
            incurred = min(incurred_amount, bill_amount)

        if incurred < 0 or (
            values['Currentclaimstatus'] in (
                'For Bill Entry', 'For Adjudication', 'Refer to CS', 'Refer to CRM', 'Refer to Enrollment', 'Insurer Response', 'CS Response',
                'Enrollment Response', 'Refer to Insurer', 'For Investigation', 'Investigation Done', 'Sent for Audit', 'For Audit', 'For Payment',
                'For Settlement', 'Audit Return', 'From CRM'
            ) and claims_action_items_df.filter(
                (col('claimid') == claimid) & (col('slno') == slno)
            ).orderBy(col('OpenDate').desc()).first()['Remarks'].contains('Refer to Insurer – R')
        ):
            incurred = 0

        if values['Requesttype'] in (1, 2, 3) and values['Claimtype'] == 1 and values['Currentclaimstatus'] == 'Cashless Approved':
            incurred = values['Sanctioned']

    else:
        incurred = 0

    return float(incurred)

# COMMAND ----------

from pyspark.sql.functions import col , when, lit, coalesce
claimID = 24070402405
slno = 2
UDF_GetIncurredAmount(claimID, slno)

# COMMAND ----------

    claims_df = spark.sql("SELECT * FROM fhpl.Claims").alias('c')
    claims_df.display()
    member_policy_df = spark.sql("SELECT * FROM fhpl.MemberPolicy").filter(col('deleted') == False)
    claims_details_df = spark.sql("SELECT * FROM fhpl.ClaimsDetails").alias('cd')
    claims_details_df.display()
    member_si_df = spark.sql("SELECT * FROM fhpl.MemberSI").filter(col('deleted') == False)
    bp_suminsured_df = spark.sql("SELECT * FROM fhpl.BPSumInsured").filter(col('deleted') == False)
    claim_stage_df = spark.sql("SELECT * FROM fhpl.claimstage")
    claims_action_items_df = spark.sql("SELECT * FROM fhpl.claimActionItems")

# COMMAND ----------

def UDF_GetIncurredAmount(claimid, slno):
    claims_df = spark.sql("SELECT * FROM fhpl.Claims").alias('c')
    member_policy_df = spark.sql("SELECT * FROM fhpl.MemberPolicy").filter(col('deleted') == False)
    claims_details_df = spark.sql("SELECT * FROM fhpl.ClaimsDetails").alias('cd')
    member_si_df = spark.sql("SELECT * FROM fhpl.MemberSI").filter(col('deleted') == False)
    bp_suminsured_df = spark.sql("SELECT * FROM fhpl.BPSumInsured").filter(col('deleted') == False)
    claim_stage_df = spark.sql("SELECT * FROM fhpl.claimstage")
    claims_action_items_df = spark.sql("SELECT * FROM fhpl.claimActionItems")

    joined_df = claims_details_df \
    .join(claims_df, (col('cd.Claimid') == col('c.id')) & (col('cd.deleted') == 0) & (col('c.deleted') == 0), 'inner') \
    .join(claim_stage_df.alias('cs'), col('cs.id') == col('cd.StageID'), 'inner') \
    .join(member_policy_df.alias('m'), col('m.id') == col('c.memberpolicyid'), 'inner') \
    .join(member_si_df.alias('ms'), col('ms.memberpolicyid') == col('m.id'), 'inner') \
    .join(bp_suminsured_df.alias('bs'), (col('bs.id') == col('ms.BPSIID')) & (col('bs.SICategoryID_P20') == 69), 'inner') \
    .filter((col('cd.claimid') == claimid) & (col('cd.slno') == slno))

    #  claimID = 24070402138
    #  slno = 2
    # display(joined_df)

    claim_status_subquery = claims_action_items_df.alias('ca1') \
        .join(claims_details_df.alias('cd1'), (col('cd1.claimid') == col('ca1.claimid')) & (col('cd1.Slno') == col('ca1.Slno'))) \
        .join(claim_stage_df.alias('cs1'), col('ca1.ClaimStageID') == col('cs1.id')) \
        .filter((col('ca1.ClaimID') == col('cd1.ClaimID')) & (col('ca1.slno') == col('cd1.Slno'))) \
        .orderBy(col('ca1.ID').desc()) \
        .select(col('cs1.name')).limit(1)
    # display(claim_status_subquery)

    values = joined_df.select(
        col('cd.Claimtypeid').alias('Claimtype'),
        col('cd.Requesttypeid').alias('Requesttype'),
        when(col('cd.requesttypeid').isin(1, 2, 3), col('cs.name'))
        .when(col('cd.stageid') == 27, lit('Settled'))
        .when(col('cd.StageID').isin(21, 23, 25), col('cs.name'))
        .otherwise(coalesce(lit(claim_status_subquery.first()['name']), col('cs.Name'))).alias('Currentclaimstatus'),
        col('cd.payableamount').alias('Incurred'),
        col('cd.settledamount').alias('Settled'),
        col('cd.claimamount').alias('Claimed'),
        when(col('cd.stageid') == 23, lit(0)).otherwise(col('cd.SanctionedAmount')).alias('Sanctioned'),
        col('bs.SumInsured').alias('coverageamount'),
        col('c.deleted').alias('Claim_Deleted'),
        col('cd.deleted').alias('Claimdetails_Deleted')).limit(1)

    # display(values)

    pasanctioned_amount = spark.sql(f"""
        SELECT CASE WHEN claimtypeid = 1 THEN isnull(SanctionedAmount) END AS PASanctionedAmount
        FROM (
            SELECT Claimid, slno, claimtypeid, sanctionedamount, row_number() OVER (PARTITION BY claimid ORDER BY claimid, slno DESC) AS rowno
            FROM fhpl.Claimsdetails
            WHERE claimid = {claimid} AND deleted = False AND RequestTypeID IN (1, 2, 3) AND stageid NOT IN (23)
        ) a
         WHERE a.rowno = 1
    """).first()['PASanctionedAmount']
    # 24070402146
    # print(pasanctioned_amount)

    mainmemberstatus = spark.sql(f"""
        SELECT 1 AS mainmemberstatus
        FROM fhpl.ClaimsDetails cd
        JOIN fhpl.Claims c ON cd.claimid = c.id
        JOIN fhpl.MemberPolicy mp ON c.MemberPolicyID = mp.id
        JOIN fhpl.MemberPolicy mp2 ON mp.MainmemberID = mp2.MainmemberID
        JOIN fhpl.Claims c2 ON mp2.id = c2.MemberPolicyID
        WHERE cd.CLAIMTYPEID = 1 AND cd.claimid = c2.id AND cd.deleted = 0 AND c.deleted = 0 AND mp.deleted = 0 AND mp2.deleted = 0 AND c2.deleted = 0
        AND NOT EXISTS (
            SELECT 1
            FROM fhpl.Claimsdetails cd2
            WHERE cd2.ClaimID = c2.id AND cd2.RequestTypeID < 4 AND cd2.StageID = 29
        )
        AND cd.claimid = {claimid} AND cd.slno = {slno}
    """) 
    # claimid = 24070402330
    # slno = 4
    # print(mainmemberstatus)

    incurred = values['Incurred']
    if values['Claim_Deleted'] == 0 & values['Claimdetails_Deleted'] == 0:
        if values['Currentclaimstatus'] in (
            'Registration', 'For Adjudication', 'Refer to CS', 'Refer to Enrollment', 'Query Response (H)', 'Query Response (M)',
            'Insurer Response', 'CS Response', 'Enrollment Response', 'Refer to Insurer', 'Insurer Authorization', 'Investigation Done',
            'Query to Hospital', 'Query to Member', 'First Reminder-Hospital', 'Second Reminder-Hospital', 'Third Reminder-Hospital',
            'Fourth Reminder-Hospital', 'First Reminder-Member', 'Second Reminder-Member', 'Third Reminder-Member', 'Fourth Reminder-Member',
            'From CRM', 'For Investigation', 'For Bill Entry', 'For Audit', 'Refer To CRM', 'Audit Return'
        ) and values['Requesttype'] not in (1, 2, 3):
            incurred = min(values['Claimed'], values['coverageamount']) if values['Claimtype'] == 2 else min(values['Claimed'], pasanctioned_amount or values['Sanctioned'])

    #     if values['Requesttype'] in (1, 2, 3):
    #         incurred = min(values['Claimed'], values['Sanctioned'])

    #     if values['Claimtype'] == 1 and mainmemberstatus == 1 and values['Requesttype'] in (1, 2, 3):
    #         balance_sum_insured = spark.sql(f"""
    #             SELECT balance_sum_insured_without_present_claim(claimid,3) AS BalanceSumInsured
    #             FROM fhpl.Claims
    #             WHERE ID = {claimid}
    #         """).first()['BalanceSumInsured']
    #         incurred = min(values['Claimed'], balance_sum_insured or values['Sanctioned'])

    #     if values['Claimtype'] == 2 and values['Currentclaimstatus'] in (
    #         'For Bill Entry', 'For Adjudication', 'Query to Hospital', 'Query to Member', 'Refer to CS', 'Refer to CRM', 'Refer to Enrollment',
    #         'Insurer Response', 'CS Response', 'Enrollment Response', 'Refer to Insurer', 'For Investigation', 'Investigation Done', 'Sent for Audit',
    #         'For Audit', 'Audit Return', 'First Reminder-Hospital', 'Second Reminder-Hospital', 'Third Reminder-Hospital', 'Fourth Reminder-Hospital',
    #         'First Reminder-Member', 'Second Reminder-Member', 'Third Reminder-Member', 'Fourth Reminder-Member', 'From CRM'
    #     ):
    #         incurred_amount = spark.sql(f"""
    #             SELECT fhpl.UFN_Spectra_BalanceSumInsured_Without_PresentClaim(ID, 1) AS IncurredAmount
    #             FROM fhpl.Claims
    #             WHERE ID = {claimid}
    #         """).first()['IncurredAmount']
    #         bill_amount = values['Claimed']
    #         incurred = min(incurred_amount, bill_amount)

    #     if values['Currentclaimstatus'] == 'Settled':
    #         incurred = values['Settled']

    #     if values['Currentclaimstatus'] in ('Closed', 'Repudiated', 'Cancelled'):
    #         incurred = 0

    #     if values['Currentclaimstatus'] in ('For Payment', 'For Settlement') and values['Claimtype'] == 2:
    #         incurred = values['Sanctioned']

    #     if values['Claimtype'] == 2 and values['Currentclaimstatus'] in (
    #         'For Bill Entry', 'For Adjudication', 'Query to Hospital', 'Query to Member', 'Refer to CS', 'Refer to CRM', 'Refer to Enrollment',
    #         'Insurer Response', 'CS Response', 'Enrollment Response', 'Refer to Insurer', 'For Investigation', 'Investigation Done', 'Sent for Audit',
    #         'For Audit', 'Audit Return', 'First Reminder-Hospital', 'Second Reminder-Hospital', 'Third Reminder-Hospital', 'Fourth Reminder-Hospital',
    #         'First Reminder-Member', 'Second Reminder-Member', 'Third Reminder-Member', 'Fourth Reminder-Member', 'From CRM'
    #     ):
    #         incurred_amount = spark.sql(f"""
    #             SELECT fhpl.UFN_Spectra_BalanceSumInsured_Without_PresentClaim(ID, 1) AS IncurredAmount
    #             FROM fhpl.Claims
    #             WHERE ID = {claimid}
    #         """).first()['IncurredAmount']
    #         bill_amount = values['Claimed'] 
    #         incurred = min(incurred_amount, bill_amount)

    #     if incurred < 0 or (
    #         values['Currentclaimstatus'] in (
    #             'For Bill Entry', 'For Adjudication', 'Refer to CS', 'Refer to CRM', 'Refer to Enrollment', 'Insurer Response', 'CS Response',
    #             'Enrollment Response', 'Refer to Insurer', 'For Investigation', 'Investigation Done', 'Sent for Audit', 'For Audit', 'For Payment',
    #             'For Settlement', 'Audit Return', 'From CRM'
    #         ) and claims_action_items_df.filter(
    #             (col('claimid') == claimid) & (col('slno') == slno)
    #         ).orderBy(col('OpenDate').desc()).first()['Remarks'].contains('Refer to Insurer – R')
    #     ):
    #         incurred = 0

    #     if values['Requesttype'] in (1, 2, 3) and values['Claimtype'] == 1 and values['Currentclaimstatus'] == 'Cashless Approved':
    #         incurred = values['Sanctioned']

    # else:
    #     incurred = 0

    # return float(incurred)

claimID = 24070402138
slno = 2
UDF_GetIncurredAmount(claimID, slno)

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  *
# MAGIC         FROM fhpl.ClaimsDetails cd
# MAGIC         JOIN fhpl.Claims c ON cd.claimid = c.id
# MAGIC         JOIN fhpl.MemberPolicy mp ON c.MemberPolicyID = mp.id
# MAGIC         JOIN fhpl.MemberPolicy mp2 ON mp.MainmemberID = mp2.MainmemberID
# MAGIC         JOIN fhpl.Claims c2 ON mp2.id = c2.MemberPolicyID
# MAGIC         WHERE cd.CLAIMTYPEID = 1 AND cd.claimid = c2.id AND cd.deleted = 0 AND c.deleted = 0 AND mp.deleted = 0 AND mp2.deleted = 0 AND c2.deleted = 0
# MAGIC         AND NOT EXISTS (
# MAGIC             SELECT 1
# MAGIC             FROM fhpl.Claimsdetails cd2
# MAGIC             WHERE cd2.ClaimID = c2.id AND cd2.RequestTypeID < 4 AND cd2.StageID = 29)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM fhpl.ClaimsDetails WHERE  deleted = False AND RequestTypeID IN (1, 2, 3) AND stageid NOT IN (23) AND ClaimTypeID = 1
# MAGIC
# MAGIC -- WHERE claimID = 24070402146 and slno = 3 
# MAGIC
# MAGIC --24070402445 , Slno -3
# MAGIC --24070402869 , Slno -4

# COMMAND ----------

# MAGIC %sql
# MAGIC         SELECT  *
# MAGIC         FROM fhpl.ClaimsDetails cd
# MAGIC         JOIN fhpl.Claims c ON cd.claimid = c.id
# MAGIC         JOIN fhpl.MemberPolicy mp ON c.MemberPolicyID = mp.id
# MAGIC         JOIN fhpl.MemberPolicy mp2 ON mp.MainmemberID = mp2.MainmemberID
# MAGIC         JOIN fhpl.Claims c2 ON mp2.id = c2.MemberPolicyID  WHERE cd.CLAIMTYPEID = 1 AND cd.claimid = c2.id AND cd.deleted = 0 AND c.deleted = 0 AND mp.deleted = 0 AND mp2.deleted = 0 AND c2.deleted = 0 AND NOT EXISTS (
# MAGIC             SELECT 1
# MAGIC             FROM fhpl.Claimsdetails cd2
# MAGIC             --WHERE cd2.ClaimID = c2.id AND cd2.RequestTypeID < 4 AND cd2.StageID = 29
# MAGIC         ) AND cd.claimid = 24070402146 

# COMMAND ----------

# MAGIC %sql
# MAGIC         SELECT CASE WHEN claimtypeid = 1 THEN isnull(SanctionedAmount) END AS PASanctionedAmount
# MAGIC         FROM (
# MAGIC             SELECT Claimid, slno, claimtypeid, sanctionedamount, row_number() OVER (PARTITION BY claimid ORDER BY claimid, slno DESC) AS rowno
# MAGIC             FROM fhpl.Claimsdetails
# MAGIC             WHERE claimid = 24070402146  AND deleted = False AND RequestTypeID IN (1, 2, 3) AND stageid NOT IN (23)
# MAGIC         ) a
# MAGIC          WHERE a.rowno = 1