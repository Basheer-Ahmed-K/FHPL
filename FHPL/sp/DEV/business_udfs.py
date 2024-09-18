# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.functions import sum as _sum
from pyspark.sql.types import StringType

# COMMAND ----------

# MAGIC %md
# MAGIC # DisAllowenceReason1

# COMMAND ----------

# SP
# USE [McarePlus]
# GO
# /****** Object:  UserDefinedFunction [dbo].[UDF_DisallowenceReason]    Script Date: 18-07-2024 13:45:51 ******/
# SET ANSI_NULLS ON
# GO
# SET QUOTED_IDENTIFIER ON
# GO
    
# --select dbo.UDF_DisallowenceReason(18100900072,1)    
      
# ALTER function [dbo].[UDF_DisallowenceReason](@claimid bigint,@slno tinyint)              
# returns string(max)              
# as              
# Begin              
# Declare @Reason string(max)    
# set @Reason=STUFF((SELECT ',' +'Rs.'+convert(string,deductionamount)+' '+replace(dr.name,'|FT|'  
# ,REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
# REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
# REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
# REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
# REPLACE( REPLACE( REPLACE( REPLACE( REPLACE( 
# REPLACE( REPLACE( REPLACE( REPLACE( 
#     cd.[FreeTextValue]
# ,char(0x0000),'') ,char(0x0001),'') ,char(0x0002),'') ,char(0x0003),'') ,char(0x0004),'') 
# ,char(0x0005),'') ,char(0x0006),'') ,char(0x0007),'') ,char(0x0008),'') ,char(0x000B),'') 
# ,char(0x000C),'') ,char(0x000E),'') ,char(0x000F),'') ,char(0x0010),'') ,char(0x0011),'') 
# ,char(0x0012),'') ,char(0x0013),'') ,char(0x0014),'') ,char(0x0015),'') ,char(0x0016),'') 
# ,char(0x0017),'') ,char(0x0018),'') ,char(0x0019),'') ,char(0x001A),'') ,char(0x001B),'') 
# ,char(0x001C),'') ,char(0x001D),'') ,char(0x001E),'') ,char(0x001F),'')
# )     
# from ClaimDeductionDetails cd,Mst_DeductionReasons dr               
# where cd.deleted=0 and cd.DeductionReasonID=dr.id     
# and claimid=@claimid and slno=@slno order by serviceid,deductionslno FOR XML PATH(''), TYPE).value('.', 'string(max)'),1,1,'')              
    
# Return @reason              
              
# End


# COMMAND ----------

def udf_disallowence_reason(claim_deduction_details, mst_deduction_reasons, claimid, slno):
    # Filter and join DataFrames
    filtered_df = claim_deduction_details.filter(
        (col("deleted") == 0) & 
        (col("claimid") == claimid) & 
        (col("slno") == slno)
    ).join(
        mst_deduction_reasons, claim_deduction_details["DeductionReasonID"] == mst_deduction_reasons["id"]
    )
    
    # Replace unwanted characters in FreeTextValue
    unwanted_chars = [
        '\u0000', '\u0001', '\u0002', '\u0003', '\u0004', '\u0005', '\u0006', '\u0007', 
        '\u0008', '\u000B', '\u000C', '\u000E', '\u000F', '\u0010', '\u0011', '\u0012', 
        '\u0013', '\u0014', '\u0015', '\u0016', '\u0017', '\u0018', '\u0019', '\u001A', 
        '\u001B', '\u001C', '\u001D', '\u001E', '\u001F'
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

claim_deduction_details = spark.sql("SELECT * FROM fhpl.claimdeductiondetails")
mst_deduction_reasons = spark.sql("SELECT * FROM fhpl.mst_deductionreasons")

claimid = 24070402633
slno = 4
reason = udf_disallowence_reason(claim_deduction_details, mst_deduction_reasons, claimid, slno)
print(reason)

# COMMAND ----------

# MAGIC %md
# MAGIC # BalanceSuminsured

# COMMAND ----------

# DBTITLE 1,UFN_Spectra_BalanceSumInsured

# USE [McarePlus]
# GO
# /****** Object:  UserDefinedFunction [dbo].[UFN_Spectra_BalanceSumInsured]    Script Date: 18-07-2024 13:47:23 ******/
# SET ANSI_NULLS ON
# GO
# SET QUOTED_IDENTIFIER ON
# GO
# ALTER FUNCTION [dbo].[UFN_Spectra_BalanceSumInsured] (@claimID bigint=NULL,@slno int=NULL)
# returns money
# as
# begin

# declare @BPSIIDs string(100)='',@MainMemberID bigint,@MemberSIID string(100)='',@MemberPolicyID bigint,@SITypeID int,@CB_Amount money=0, @SumInsured money = 0

# Declare @SubLimitbalance money, @Totalbalance money,  @RelationshipIds string(50), @SubLimit money = 0, @ServiceLimit money = 0, @ClaimRelationshipID int

# Select TOP 1 @MainMemberID=MainmemberID,@MemberPolicyID=C.MemberPolicyID,@SITypeID=bp.SITypeID,@CB_Amount=isnull(s.CB_Amount,0),
# @SumInsured = bp.SumInsured, @RelationshipIds = bpcon.Relationshipid, @SubLimit = bpcon.FamilyLimit, @ClaimRelationshipID = MP.RelationshipID,
# @ServiceLimit = IIF(bpscon_fam.[InternalValueAbs] < bpscon_ind.[InternalValueAbs], bpscon_fam.[InternalValueAbs], bpscon_ind.[InternalValueAbs])
# from Claims C (NOLOCK)
# inner join MemberPolicy Mp (NOLOCK) on c.MemberPolicyID = Mp.id
# inner join Membersi s (NOLOCK) on mp.id = s.memberpolicyid
# inner join bpsuminsured bp (NOLOCK) on bp.id=s.BPSIID and bp.SICategoryID_P20=69
# left join bpsiconditions bpcon (NOLOCK) on bpcon.bpsiid = bp.id and bpcon.bpconditionid = 30
# and (',' + bpcon.Relationshipid + ',' like '%,' + CONVERT(string,mp.relationshipid) + ',%' OR bpcon.Relationshipid is null)
# and bp.policyid = isnull(bpcon.policyid,0)
# left join [BPServiceConfigDetails] bpscon_ind (NOLOCK) on bpscon_ind.benefitplansiid = bp.id and c.servicetypeid = bpscon_ind.servicetypeid
# and ',' + bpscon_ind.AllowedRelationids + ',' like '%,' + CONVERT(string,mp.relationshipid) + ',%' and bpscon_ind.[LimitCatg_P29] = 108
# left join [BPServiceConfigDetails] bpscon_fam (NOLOCK) on bpscon_fam.benefitplansiid = bp.id and c.servicetypeid = bpscon_fam.servicetypeid
# and ',' + bpscon_fam.AllowedRelationids + ',' like '%,' + CONVERT(string,mp.relationshipid) + ',%' and bpscon_fam.[LimitCatg_P29] = 107
# where C.ID=@ClaimID and C.Deleted=0 and s.deleted=0


# if(@SITypeID=5)
# Begin
# set @MemberPolicyID=@MainMemberID
# if(@CB_Amount=0)
# select @CB_Amount=cb_amount from membersi ms (NOLOCK),memberpolicy mp1 (NOLOCK) where ms.memberpolicyid=mp1.id and mp1.id=@mainmemberid
# End;

# SELECT @SubLimitbalance = IIF(@ClaimRelationshipID = 2, ISNULL(@ServiceLimit, @SumInsured), COALESCE(@ServiceLimit, @SubLimit, @SumInsured)) + isnull(@CB_Amount,0) -
# -- SETTLED CASES
# ISNULL((
# Select SUM(isnull(CU.SanctionedAmount,0))
# FROM ClaimUtilizedAmount CU (NOLOCK), Claims C (NOLOCK) , ClaimsDetails CD (NOLOCK) Where 
# CD.ClaimID=C.ID AND C.Deleted=0 AND CU.Deleted=0 AND CU.ClaimId = CD.ClaimId AND CU.SlNo = CD.SlNo
# AND CD.ClaimId = C.ID AND CD.Deleted = 0
# AND EXISTS (SELECT 1 FROM MemberPolicy MP (NOLOCK) WHERE MP.ID = C.MemberPolicyId AND MP.MainmemberID = @MainMemberID AND MP.ID = IIF(@SITypeID=5, MP.ID,@MemberPolicyID) AND ',' + ISNULL(@RelationshipIds,'0') + ',' like '%,' + IIF(@RelationshipIds IS NULL, '0', CONVERT(string,MP.relationshipid)) + ',%')
# AND CD.RequestTypeID >= 4 AND CD.StageID = 27 AND CD.RequestTypeID <> 9
# ),0)
# -
# -- MR CASES WHICH ARE NOT YET SETTLED
# ISNULL((
# Select SUM(IIF(isnull(CD.SanctionedAmount,0) > 0, isnull(CD.SanctionedAmount,0), isnull(CD.ClaimAmount,0)- CASE WHEN CD.StageID = 26 THEN isnull(CD.DeductionAmount,0) ELSE 0 END))
# FROM Claims C (NOLOCK), ClaimsDetails CD (NOLOCK) Where 
# CD.ClaimID=C.ID AND C.Deleted=0 AND CD.Deleted = 0 
# AND EXISTS (SELECT 1 FROM MemberPolicy MP (NOLOCK) WHERE MP.ID = C.MemberPolicyId AND MP.MainmemberID = @MainMemberID AND MP.ID = IIF(@SITypeID=5, MP.ID,@MemberPolicyID) AND ',' + ISNULL(@RelationshipIds,'0') + ',' like '%,' + IIF(@RelationshipIds IS NULL, '0', CONVERT(string,MP.relationshipid)) + ',%')
# AND CD.RequestTypeID >= 4 AND CD.ClaimTypeID = 2
# AND NOT EXISTS (SELECT 1 FROM ClaimsDetails CD3 (NOLOCK) WHERE CD3.ClaimId = C.ID AND RequestTypeid >= 4 AND CD3.StageID IN (27,23,21,25))
# ),0)
# -
# -- PP CASES WHICH ARE NOT YET SETTLED
# ISNULL((
# Select SUM(isnull(CU.SanctionedAmount,0))
# FROM ClaimUtilizedAmount CU (NOLOCK), Claims C (NOLOCK), ClaimsDetails CD (NOLOCK) Where 
# CD.ClaimID=C.ID AND C.Deleted=0 AND CU.Deleted=0 AND CU.ClaimId = CD.ClaimId AND CU.SlNo = CD.SlNo
# AND CD.ClaimId = C.ID AND CD.Deleted = 0
# AND EXISTS (SELECT 1 FROM MemberPolicy MP (NOLOCK) WHERE MP.ID = C.MemberPolicyId AND MP.MainmemberID = @MainMemberID AND MP.ID = IIF(@SITypeID=5, MP.ID,@MemberPolicyID) AND ',' + ISNULL(@RelationshipIds,'0') + ',' like '%,' + IIF(@RelationshipIds IS NULL, '0', CONVERT(string,MP.relationshipid)) + ',%')
# AND CD.RequestTypeID < 4 AND CD.StageID NOT IN (21,23,25)
# AND NOT EXISTS (SELECT 1 FROM ClaimsDetails CD3 (NOLOCK) WHERE CD3.ClaimId = C.ID AND RequestTypeid >= 4 AND CD3.StageID IN (27,23,21,25))
# ),0)

# -- Calculate with total sum insured

# SELECT @Totalbalance = isnull(@SumInsured,0) + isnull(@CB_Amount,0) -
# -- SETTLED CASES
# ISNULL((
# Select SUM(isnull(CU.SanctionedAmount,0))
# FROM ClaimUtilizedAmount CU (NOLOCK), Claims C (NOLOCK), ClaimsDetails CD (NOLOCK) Where 
# CD.ClaimID=C.ID AND C.Deleted=0 AND CU.Deleted=0 AND CU.ClaimId = CD.ClaimId AND CU.SlNo = CD.SlNo
# AND CD.ClaimId = C.ID AND CD.Deleted = 0
# AND EXISTS (SELECT 1 FROM MemberPolicy MP (NOLOCK) WHERE MP.ID = C.MemberPolicyId AND MP.MainmemberID = @MainMemberID AND MP.ID = IIF(@SITypeID=5, MP.ID,@MemberPolicyID))
# AND CD.RequestTypeID >= 4 AND CD.StageID = 27 AND CD.RequestTypeID <> 9
# ),0)
# -
# -- MR CASES WHICH ARE NOT YET SETTLED
# ISNULL((
# Select SUM(IIF(isnull(CD.SanctionedAmount,0) > 0, isnull(CD.SanctionedAmount,0), isnull(CD.ClaimAmount,0)- CASE WHEN CD.StageID = 26 THEN isnull(CD.DeductionAmount,0) ELSE 0 END))
# FROM Claims C (NOLOCK), ClaimsDetails CD (NOLOCK) Where 
# CD.ClaimID=C.ID AND C.Deleted=0 AND CD.Deleted = 0 
# AND EXISTS (SELECT 1 FROM MemberPolicy MP (NOLOCK) WHERE MP.ID = C.MemberPolicyId AND MP.MainmemberID = @MainMemberID AND MP.ID = IIF(@SITypeID=5, MP.ID,@MemberPolicyID))
# AND CD.RequestTypeID >= 4 AND CD.ClaimTypeID = 2
# AND NOT EXISTS (SELECT 1 FROM ClaimsDetails CD3 (NOLOCK) WHERE CD3.ClaimId = C.ID AND RequestTypeid >= 4 AND CD3.StageID IN (27,23,21,25))
# ),0)
# -
# -- PP CASES WHICH ARE NOT YET SETTLED
# ISNULL((
# Select SUM(isnull(CU.SanctionedAmount,0))
# FROM ClaimUtilizedAmount CU (NOLOCK), Claims C (NOLOCK), ClaimsDetails CD (NOLOCK) Where 
# CD.ClaimID=C.ID AND C.Deleted=0 AND CU.Deleted=0 AND CU.ClaimId = CD.ClaimId AND CU.SlNo = CD.SlNo
# AND CD.ClaimId = C.ID AND CD.Deleted = 0
# AND EXISTS (SELECT 1 FROM MemberPolicy MP (NOLOCK) WHERE MP.ID = C.MemberPolicyId AND MP.MainmemberID = @MainMemberID AND MP.ID = IIF(@SITypeID=5, MP.ID,@MemberPolicyID))
# AND CD.RequestTypeID < 4 AND CD.StageID NOT IN (21,23,25)
# AND NOT EXISTS (SELECT 1 FROM ClaimsDetails CD3 (NOLOCK) WHERE CD3.ClaimId = C.ID AND RequestTypeid >= 4 AND CD3.StageID IN (27,23,21,25))
# ),0)

# RETURN IIF(@Totalbalance < @Sublimitbalance, IIF(@Totalbalance < 0,0,@Totalbalance), IIF(@Sublimitbalance < 0, 0,@Sublimitbalance))

# End

# COMMAND ----------

from pyspark.sql.functions import col, when, sum as _sum, coalesce

def UFN_Spectra_BalanceSumInsured(claimID=None, slno=None):
    # Initialize variables
    BPSIIDs = ""
    MainMemberID = None
    MemberSIID = ""
    MemberPolicyID = None
    SITypeID = None
    CB_Amount = 0
    SumInsured = 0
    SubLimitbalance = 0
    Totalbalance = 0
    RelationshipIds = ""
    SubLimit = 0
    ServiceLimit = 0
    ClaimRelationshipID = None

    # Read data from tables
    claims_df = spark.sql("SELECT * FROM fhpl.Claims")
    member_policy_df = spark.sql("SELECT * FROM fhpl.MemberPolicy")
    claims_details_df = spark.sql("SELECT * FROM fhpl.ClaimsDetails").filter("Deleted = 0")
    membersi_df = spark.sql("SELECT * FROM fhpl.MemberSI")
    bpsuminsured_df = spark.sql("SELECT * FROM fhpl.BPSumInsured")
    bpsiconditions_df = spark.sql("SELECT * FROM fhpl.BPSIConditions")
    bp_service_config_details_df = spark.sql("SELECT * FROM fhpl.BPServiceConfigDetails")
    claim_utilized_amount_df = spark.sql("SELECT * FROM fhpl.ClaimUtilizedAmount").filter("Deleted = 0")

    # Join the dataframes to get necessary columns
    joined_df = claims_df.alias("C") \
        .join(member_policy_df.alias("MP"), col("C.MemberPolicyID") == col("MP.id")) \
        .join(membersi_df.alias("S"), col("MP.id") == col("S.memberpolicyid")) \
        .join(bpsuminsured_df.alias("BP"), (col("BP.id") == col("S.BPSIID")) & (col("BP.SICategoryID_P20") == 69)) \
        .join(bpsiconditions_df.alias("BPCON"), (col("BPCON.bpsiid") == col("BP.id")) & (col("BPCON.bpconditionid") == 30) &
              (col("BP.policyid") == coalesce(col("BPCON.policyid"), lit(0))), "left") \
        .join(bp_service_config_details_df.alias("BPSCON_IND"), (col("BPSCON_IND.benefitplansiid") == col("BP.id")) &
              (col("C.servicetypeid") == col("BPSCON_IND.servicetypeid")) &
              (col("BPSCON_IND.LimitCatg_P29") == 108), "left") \
        .join(bp_service_config_details_df.alias("BPSCON_FAM"), (col("BPSCON_FAM.benefitplansiid") == col("BP.id")) &
              (col("C.servicetypeid") == col("BPSCON_FAM.servicetypeid")) &
              (col("BPSCON_FAM.LimitCatg_P29") == 107), "left") \
        .filter((col("C.ID") == claimID))

    # Select the top row
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
            cb_amount_df = membersi_df.alias("MS") \
                .join(member_policy_df.alias("MP1"), col("MS.memberpolicyid") == col("MP1.id")) \
                .filter(col("MP1.id") == MainMemberID) \
                .select(coalesce(col("MS.CB_Amount"), 0).alias("CB_Amount")).limit(1).collect()
            if cb_amount_df:
                CB_Amount = cb_amount_df[0]["CB_Amount"]

    settled_cases_sum = claim_utilized_amount_df.alias("CU") \
        .join(claims_details_df.alias("CD"), (col("CU.ClaimId") == col("CD.ClaimId")) & (col("CU.SlNo") == col("CD.SlNo"))) \
        .join(claims_df.alias("C"), col("CD.ClaimId") == col("C.ID")) \
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
            (col("CD.ClaimId") != col("C.ID")) & (col("CD.RequestTypeID") < 4) & (~col("CD.StageID").isin([27, 23, 21, 25]))) \
    .agg(_sum(when(col("CD.SanctionedAmount").isNotNull(), col("CD.SanctionedAmount")).otherwise(col("CD.ClaimAmount") - when(col("CD.StageID") == 26, col("CD.DeductionAmount")).otherwise(lit(0)))).alias("sum")) \
    .collect()[0]["sum"]

    # Create a DataFrame for the subquery to filter out unwanted rows
    # Define the excluded IDs DataFrame correctly
    excluded_ids_df = claims_df.alias('C') \
    .join(claims_details_df.alias("CD3"), col("CD3.ClaimId") == col("C.ID")) \
    .filter((col("CD3.RequestTypeid") >= 4) & (col("CD3.StageID").isin([27, 23, 21, 25]))) \
    .select("CD3.ID")


    # Join the excluded_ids_df with the main DataFrame to filter out the unwanted rows
    pp_cases_not_settled_sum = claim_utilized_amount_df.alias("CU") \
    .join(claims_details_df.alias("CD"), (col("CU.ClaimId") == col("CD.ClaimId")) & (col("CU.SlNo") == col("CD.SlNo"))) \
    .join(claims_df.alias("C"), col("CD.ClaimId") == col("C.ID")) \
    .join(member_policy_df.alias("MP"), (col("MP.ID") == col("C.MemberPolicyId")) & (col("MP.MainmemberID") == MainMemberID) &
          (col("MP.ID") == when(col("SITypeID") == lit(5), col("MP.ID")).otherwise(MemberPolicyID))) \
    .join(excluded_ids_df, col("CD.ID") == col("CD3.ID"), "left_anti") \
    .filter((col("CD.RequestTypeID") < 4) & (~col("CD.StageID").isin([21, 23, 25]))) \
    .agg(_sum(coalesce(col("CU.SanctionedAmount"), lit(0)))).collect()[0][0]

    # SubLimitbalance = (
    # (ServiceLimit if ClaimRelationshipID == 2 else coalesce(col("ServiceLimit"), col("SubLimit"), col("SumInsured")))
    # + col("CB_Amount")
    # - (col("settled_cases_sum"))
    # - (col("mr_cases_not_settled_sum"))
    # - (col("pp_cases_not_settled_sum"))
    # )
    SubLimitbalance = (
            ServiceLimit if ClaimRelationshipID == 2 else min(ServiceLimit, SubLimit, SumInsured)
        ) + CB_Amount - settled_cases_sum - mr_cases_sum - pp_cases_not_settled_sum

    # Calculate Totalbalance
    total_settled_cases_sum = claim_utilized_amount_df.alias("CU") \
        .join(claims_details_df.alias("CD"), (col("CU.ClaimId") == col("CD.ClaimId")) & (col("CU.SlNo") == col("CD.SlNo"))) \
        .join(claims_df.alias("C"), col("CD.ClaimId") == col("C.ID")) \
        .join(member_policy_df.alias("MP"), (col("MP.ID") == col("C.MemberPolicyId")) & (col("MP.MainmemberID") == MainMemberID) &
              (col("MP.ID") == when(SITypeID == lit(5), col("MP.ID")).otherwise(MemberPolicyID))) \
        .filter((col("CD.RequestTypeID") >= 4) & (col("CD.StageID") == 27) & (col("CD.RequestTypeID") != 9)) \
        .agg(_sum(coalesce(col("CU.SanctionedAmount"), lit(0)))).collect()[0][0]

    excluded_ids_df = claims_df.alias('C') \
    .join(claims_details_df.alias("CD3"), col("CD3.ClaimId") == col("C.ID")) \
    .filter((col("CD3.RequestTypeid") >= 4) & (col("CD3.StageID").isin([27, 23, 21, 25]))) \
    .select("CD3.ID")

# Calculate total_mr_cases_not_settled_sum
    total_mr_cases_not_settled_sum = claims_df.alias("C") \
    .join(claims_details_df.alias("CD"), col("CD.ClaimID") == col("C.ID")) \
    .join(member_policy_df.alias("MP"), (col("MP.ID") == col("C.MemberPolicyId")) & (col("MP.MainmemberID") == MainMemberID) &
          (col("MP.ID") == when(SITypeID == lit(5), col("MP.ID")).otherwise(MemberPolicyID))) \
    .join(excluded_ids_df.alias("EX"), col("CD.ID") == col("EX.ID"), "left_anti") \
    .filter((col("CD.RequestTypeID") >= 4) & (col("CD.ClaimTypeID") == 2)) \
    .agg(_sum(
        when(coalesce(col("CD.SanctionedAmount"), lit(0)) > lit(0), coalesce(col("CD.SanctionedAmount"), lit(0)))
        .otherwise(col("CD.ClaimAmount") - when(col("CD.StageID") == 26, coalesce(col("CD.DeductionAmount"), lit(0))).otherwise(lit(0)))
    )).collect()[0][0]

    excluded_ids_df = claims_df.alias('C') \
    .join(claims_details_df.alias("CD3"), col("CD3.ClaimId") == col("C.ID")) \
    .filter((col("CD3.RequestTypeid") >= 4) & (col("CD3.StageID").isin([27, 23, 21, 25]))) \
    .select("CD3.ID")

# Calculate total_pp_cases_not_settled_sum
    total_pp_cases_not_settled_sum = claim_utilized_amount_df.alias("CU") \
    .join(claims_details_df.alias("CD"), (col("CU.ClaimId") == col("CD.ClaimId")) & (col("CU.SlNo") == col("CD.SlNo"))) \
    .join(claims_df.alias("C"), col("CD.ClaimId") == col("C.ID")) \
    .join(member_policy_df.alias("MP"), (col("MP.ID") == col("C.MemberPolicyId")) & (col("MP.MainmemberID") == MainMemberID) &
          (col("MP.ID") == when(SITypeID == lit(5), col("MP.ID")).otherwise(MemberPolicyID))) \
    .join(excluded_ids_df.alias("EX"), col("CD.ID") == col("EX.ID"), "left_anti") \
    .filter((col("CD.RequestTypeID") < 4) & ~col("CD.StageID").isin([21, 23, 25])) \
    .agg(_sum(coalesce(col("CU.SanctionedAmount"), lit(0)))).collect()[0][0]


    total_settled_cases_sum = total_settled_cases_sum if total_settled_cases_sum is not None else 0
    total_mr_cases_not_settled_sum = total_mr_cases_not_settled_sum if total_mr_cases_not_settled_sum is not None else 0
    total_pp_cases_not_settled_sum = total_pp_cases_not_settled_sum if total_pp_cases_not_settled_sum is not None else 0


    Totalbalance = SumInsured + CB_Amount - total_settled_cases_sum - total_pp_cases_not_settled_sum
    print(Totalbalance)
    print(SubLimitbalance)

    return result


claimID = 24070402597
result = UFN_Spectra_BalanceSumInsured(claimID)
print(result)


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as _sum, coalesce, lit, least

# Initialize Spark session
spark = SparkSession.builder.appName("Example").getOrCreate()

def UFN_Spectra_BalanceSumInsured(claimID=None, slno=None):
    # Read data from tables
    claims_df = spark.sql("SELECT * FROM fhpl.Claims")
    member_policy_df = spark.sql("SELECT * FROM fhpl.MemberPolicy")
    claims_details_df = spark.sql("SELECT * FROM fhpl.ClaimsDetails").filter("Deleted = 0")
    membersi_df = spark.sql("SELECT * FROM fhpl.MemberSI")
    bpsuminsured_df = spark.sql("SELECT * FROM fhpl.BPSumInsured")
    bpsiconditions_df = spark.sql("SELECT * FROM fhpl.BPSIConditions")
    bp_service_config_details_df = spark.sql("SELECT * FROM fhpl.BPServiceConfigDetails")
    claim_utilized_amount_df = spark.sql("SELECT * FROM fhpl.ClaimUtilizedAmount").filter("Deleted = 0")

    # Join the dataframes to get necessary columns
    joined_df = claims_df.alias("C") \
        .join(member_policy_df.alias("MP"), col("C.MemberPolicyID") == col("MP.id")) \
        .join(membersi_df.alias("S"), col("MP.id") == col("S.memberpolicyid")) \
        .join(bpsuminsured_df.alias("BP"), (col("BP.id") == col("S.BPSIID")) & (col("BP.SICategoryID_P20") == 69)) \
        .join(bpsiconditions_df.alias("BPCON"), (col("BPCON.bpsiid") == col("BP.id")) & (col("BPCON.bpconditionid") == 30) &
              (col("BP.policyid") == coalesce(col("BPCON.policyid"), lit(0))), "left") \
        .join(bp_service_config_details_df.alias("BPSCON_IND"), (col("BPSCON_IND.benefitplansiid") == col("BP.id")) &
              (col("C.servicetypeid") == col("BPSCON_IND.servicetypeid")) &
              (col("BPSCON_IND.LimitCatg_P29") == 108), "left") \
        .join(bp_service_config_details_df.alias("BPSCON_FAM"), (col("BPSCON_FAM.benefitplansiid") == col("BP.id")) &
              (col("C.servicetypeid") == col("BPSCON_FAM.servicetypeid")) &
              (col("BPSCON_FAM.LimitCatg_P29") == 107), "left") \
        .filter((col("C.ID") == claimID))

    # Select the top row
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
            cb_amount_df = membersi_df.alias("MS") \
                .join(member_policy_df.alias("MP1"), col("MS.memberpolicyid") == col("MP1.id")) \
                .filter(col("MP1.id") == MainMemberID) \
                .select(coalesce(col("MS.CB_Amount"), lit(0)).alias("CB_Amount")).limit(1).collect()
            if cb_amount_df:
                CB_Amount = cb_amount_df[0]["CB_Amount"]

    # Calculate sums
    settled_cases_sum = claim_utilized_amount_df.alias("CU") \
        .join(claims_details_df.alias("CD"), (col("CU.ClaimId") == col("CD.ClaimId")) & (col("CU.SlNo") == col("CD.SlNo"))) \
        .join(claims_df.alias("C"), col("CD.ClaimId") == col("C.ID")) \
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
            (col("CD.ClaimId") != col("C.ID")) & (col("CD.RequestTypeID") < 4) & (~col("CD.StageID").isin([27, 23, 21, 25]))) \
    .agg(_sum(when(col("CD.SanctionedAmount").isNotNull(), col("CD.SanctionedAmount")).otherwise(col("CD.ClaimAmount") - when(col("CD.StageID") == 26, col("CD.DeductionAmount")).otherwise(lit(0)))).alias("sum")) \
    .collect()[0]["sum"]

    # Create a DataFrame for the subquery to filter out unwanted rows
    excluded_ids_df = claims_df.alias('C') \
    .join(claims_details_df.alias("CD3"), col("CD3.ClaimId") == col("C.ID")) \
    .filter((col("CD3.RequestTypeid") >= 4) & (col("CD3.StageID").isin([27, 23, 21, 25]))) \
    .select("CD3.ID")

    # Calculate pp_cases_not_settled_sum
    pp_cases_not_settled_sum = claim_utilized_amount_df.alias("CU") \
    .join(claims_details_df.alias("CD"), (col("CU.ClaimId") == col("CD.ClaimId")) & (col("CU.SlNo") == col("CD.SlNo"))) \
    .join(claims_df.alias("C"), col("CD.ClaimId") == col("C.ID")) \
    .join(member_policy_df.alias("MP"), (col("MP.ID") == col("C.MemberPolicyId")) & (col("MP.MainMemberID") == MainMemberID) &
          (col("MP.ID") == when(col("SITypeID") == lit(5), col("MP.ID")).otherwise(MemberPolicyID))) \
    .join(excluded_ids_df, col("CD.ID") == col("CD3.ID"), "left_anti") \
    .filter((col("CD.RequestTypeID") < 4) & (~col("CD.StageID").isin([21, 23, 25]))) \
    .agg(_sum(coalesce(col("CU.SanctionedAmount"), lit(0)))).collect()[0][0]

    SubLimitbalance =  SumInsured + CB_Amount

    # Calculate Totalbalance
    total_settled_cases_sum = claim_utilized_amount_df.alias("CU") \
        .join(claims_details_df.alias("CD"), (col("CU.ClaimId") == col("CD.ClaimId")) & (col("CU.SlNo") == col("CD.SlNo"))) \
        .join(claims_df.alias("C"), col("CD.ClaimId") == col("C.ID")) \
        .join(member_policy_df.alias("MP"), (col("MP.ID") == col("C.MemberPolicyId")) & (col("MP.MainMemberID") == MainMemberID) &
              (col("MP.ID") == when(SITypeID == lit(5), col("MP.ID")).otherwise(MemberPolicyID))) \
        .filter((col("CD.RequestTypeID") >= 4) & (col("CD.StageID") == 27) & (col("CD.RequestTypeID") != 9)) \
        .agg(_sum(coalesce(col("CU.SanctionedAmount"), lit(0)))).collect()[0][0]

    total_mr_cases_not_settled_sum = claims_df.alias("C") \
    .join(claims_details_df.alias("CD"), col("CD.ClaimID") == col("C.ID")) \
    .join(member_policy_df.alias("MP"), (col("MP.ID") == col("C.MemberPolicyId")) & (col("MP.MainMemberID") == MainMemberID) &
          (col("MP.ID") == when(SITypeID == lit(5), col("MP.ID")).otherwise(MemberPolicyID))) \
    .join(excluded_ids_df.alias("EX"), col("CD.ID") == col("EX.ID"), "left_anti") \
    .filter((col("CD.RequestTypeID") >= 4) & (col("CD.ClaimTypeID") == 2)) \
    .agg(_sum(
        when(coalesce(col("CD.SanctionedAmount"), lit(0)) > lit(0), coalesce(col("CD.SanctionedAmount"), lit(0)))
        .otherwise(col("CD.ClaimAmount") - when(col("CD.StageID") == 26, coalesce(col("CD.DeductionAmount"), lit(0))).otherwise(lit(0)))
    )).collect()[0][0]

    total_pp_cases_not_settled_sum = claim_utilized_amount_df.alias("CU") \
    .join(claims_details_df.alias("CD"), (col("CU.ClaimId") == col("CD.ClaimId")) & (col("CU.SlNo") == col("CD.SlNo"))) \
    .join(claims_df.alias("C"), col("CD.ClaimId") == col("C.ID")) \
    .join(member_policy_df.alias("MP"), (col("MP.ID") == col("C.MemberPolicyId")) & (col("MP.MainMemberID") == MainMemberID) &
          (col("MP.ID") == when(SITypeID == lit(5), col("MP.ID")).otherwise(MemberPolicyID))) \
    .join(excluded_ids_df.alias("EX"), col("CD.ID") == col("EX.ID"), "left_anti") \
    .filter((col("CD.RequestTypeID") < 4) & ~col("CD.StageID").isin([21, 23, 25])) \
    .agg(_sum(coalesce(col("CU.SanctionedAmount"), lit(0)))).collect()[0][0]

    Totalbalance = SumInsured + CB_Amount - total_pp_cases_not_settled_sum
    if Totalbalance > SubLimitbalance:
        return Totalbalance
    else:
        return SubLimitbalance

# Test the function
claimID = 24070402597
result = UFN_Spectra_BalanceSumInsured(claimID)
result