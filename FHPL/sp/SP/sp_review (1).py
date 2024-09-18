# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.functions import sum as _sum

# COMMAND ----------

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
bpsiconditions_df = spark.table("fhpl.bpsiconditions")
bp_service_config_details_df = spark.table("fhpl.bpserviceconfigdetails")
claims_ir_reasons_df = spark.table("fhpl.claimsirreasons")
mst_irdocuments_df = spark.table("fhpl.mst_irdocuments")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Dis_Rejection_Reason

# COMMAND ----------

# DBTITLE 1,Dis_Rejection_Reason
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
# FROM ClaimRejectionReasons CRR with(nolock) inner join Mst_RejectionReasons RR with(nolock) on CRR.RejectionReasonsID=RR.ID and RR.Deleted=0 ,     
# Claims C with(nolock), MemberPolicy MD with(nolock)       
# where CRR.Deleted=0 and C.Deleted=0 and MD.Deleted=0 and         
# C.ID=CRR.ClaimID and C.MemberPolicyID=MD.ID       
# and crr.claimid=@claimid and crr.slno=@slno    
# --union 
# --select isnull(RuleName,'') from ClaimRules where isOverride=1 and claimid=@claimid and SlNo=@slno and Deleted=0    
# union    
# select CRR.Remarks      
# FROM ClaimRejectionReasons CRR with(nolock),     
# Claims C with(nolock), MemberPolicy MD with(nolock)        
# where CRR.Deleted=0 and C.Deleted=0 and MD.Deleted=0 and         
# C.ID=CRR.ClaimID and C.MemberPolicyID=MD.ID   and crr.RejectionReasonsID=0 and crr.Remarks is not null    
# and crr.claimid=@claimid and crr.slno=@slno    
#  union  
#  select reason from claimsdetails with(nolock) where claimid=@Claimid and slno=@Slno and ltrim(rtrim(isnull(reason,'')))<>'' and createddatetime<'2016-05-12' and deleted=0  
#  union  
# select remarks from claimsdetails with(nolock) where claimid=@Claimid and slno=@Slno and ltrim(rtrim(isnull(remarks,'')))<>'' and createddatetime<'2016-05-12' 
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

# DBTITLE 1,Dis_Rej_reason
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

# MAGIC %md
# MAGIC #### DisAllowenceReason1

# COMMAND ----------

# DBTITLE 1,UDF_DisallowenceReason
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
# returns varchar(max)              
# as              
# Begin              
# Declare @Reason varchar(max)    
# set @Reason=STUFF((SELECT ',' +'Rs.'+convert(varchar,deductionamount)+' '+replace(dr.name,'|FT|'  
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
# and claimid=@claimid and slno=@slno order by serviceid,deductionslno FOR XML PATH(''), TYPE).value('.', 'varchar(max)'),1,1,'')              
    
# Return @reason              
              
# End

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
reason = udf_disallowence_reason(claimid, slno)
print(reason)

# COMMAND ----------

# MAGIC %md
# MAGIC #### UFN_IRPendingReasons

# COMMAND ----------

# DBTITLE 1,UFN_IRPendingReasons
# %sql
# USE [McarePlus]
# GO
# /****** Object:  UserDefinedFunction [dbo].[UFN_IRPendingReasons]    Script Date: 18-07-2024 13:29:44 ******/
# SET ANSI_NULLS ON
# GO
# SET QUOTED_IDENTIFIER ON
# GO
      
# --select dbo.[UFN_IRPendingReasons](895370,1,1,1)    
 
        
# ALTER function [dbo].[UFN_IRPendingReasons] (@claimid bigint,@slno int,@IsMandatory bit=NULL,@IsReceived bit=NULL) returns varchar(8000)      
# as          
# Begin          
    
# declare @string varchar(8000)    
          
# --if(@IsMandatory=1 and @IsReceived=1)      
      
# select @string=stuff((select distinct '.'+Reason  as 'data()' from (         
            
# select Replace(Replace(Replace(IR.Name,'~INS~',i.Name),'|FT|',IsNull(CR.FreeText1,'')),'|FT1|',IsNull(CR.FreeText2,''))  Reason        
# from ClaimsIRReasons cr with(nolock),Mst_IRDocuments ir with(nolock),mst_issuingauthority I with(nolock),claims c with(nolock)           
# where cr.IRDocumentID=ir.ID and cr.ClaimID=@claimid and slno=@slno and cr.ismandatory=isnull(@IsMandatory,cr.ismandatory) 
# and cr.IsReceived=isnull(@IsReceived,cr.IsReceived)           
# and cr.Deleted=0          
# and I.id=c.issueid and c.id=cr.claimid          
# union          
# select isnull(cr.Remarks,'')          
# from ClaimsIRReasons cr with(nolock)          
# where  cr.ClaimID=@claimid and slno=@slno and Deleted=0 and cr.ismandatory=isnull(@IsMandatory,cr.ismandatory) 
# and cr.IsReceived=isnull(@IsReceived,cr.IsReceived) and cr.IRDocumentID=0 and serviceid=0
# union         
# select isnull(cr.Remarks,'')          
# from ClaimsIRReasons cr with(nolock),Mst_Services S with(nolock)        
# where  cr.ClaimID=@claimid and slno=@slno and cr.Deleted=0 and cr.ismandatory=isnull(@IsMandatory,cr.ismandatory) 
# and cr.IsReceived=isnull(@IsReceived,cr.IsReceived) and cr.IRDocumentID=0 and serviceid=S.ID        
# )a      
# FOR XML PATH('')),1,1,'')      
      
# Return @string      
# End 

# COMMAND ----------

# DBTITLE 1,UFN_IRPendingReasons
def UFN_IRPendingReasons(claimid: int, slno: int, IsMandatory: int = None, IsReceived: int = None) -> str:
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

    # result = union_df.select(concat_ws(",", col("Reason")).alias("Reasons")).collect()
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

claims_ir_reasons_df = spark.sql("SELECT * FROM fhpl.ClaimsIRReasons")
mst_irdocuments_df = spark.sql("SELECT * FROM fhpl.Mst_IRDocuments")
mst_issuingauthority_df = spark.sql("SELECT * FROM fhpl.Mst_IssuingAuthority")
claims_df = spark.sql("SELECT * FROM fhpl.Claims")
mst_services_df = spark.sql("SELECT * FROM fhpl.Mst_Services")

pending_reasons = UFN_IRPendingReasons(claimID, slno, isMandatory, isReceived)
print(pending_reasons)

# COMMAND ----------

# MAGIC %md
# MAGIC #### IncurredAmount

# COMMAND ----------

# %sql
# USE [McarePlus]
# GO
# /****** Object:  UserDefinedFunction [dbo].[UFN_Spectra_BalanceSumInsured_Without_PresentClaim]    Script Date: 18-07-2024 13:49:37 ******/
# SET ANSI_NULLS ON
# GO
# SET QUOTED_IDENTIFIER ON
# GO

# --SELECT [dbo].[UFN_Spectra_BalanceSumInsured_Without_PresentClaim](19030800104,1)

# ALTER FUNCTION [dbo].[UFN_Spectra_BalanceSumInsured_Without_PresentClaim] (@claimID bigint=NULL,@slno int=NULL)
# returns money
# as
# begin

# declare @BPSIIDs varchar(100)='',@MainMemberID bigint,@MemberSIID varchar(100)='',@MemberPolicyID bigint,@SITypeID int,@CB_Amount money=0, @SumInsured money = 0

# Declare @SubLimitbalance money, @Totalbalance money,  @RelationshipIds varchar(50), @SubLimit money = 0, @ServiceLimit money = 0, @ClaimRelationshipID int, @ClaimReceivedDate datetime

# Select TOP 1 @MainMemberID=MainmemberID,@MemberPolicyID=C.MemberPolicyID,@SITypeID=bp.SITypeID,@CB_Amount=isnull(s.CB_Amount,0),
# @SumInsured = bp.SumInsured, @RelationshipIds = bpcon.Relationshipid, @SubLimit = bpcon.FamilyLimit, @ClaimRelationshipID = MP.RelationshipID, @ClaimReceivedDate = C.ReceivedDate,
# @ServiceLimit = IIF(bpscon_fam.[InternalValueAbs] < bpscon_ind.[InternalValueAbs], bpscon_fam.[InternalValueAbs], bpscon_ind.[InternalValueAbs])
# from Claims C
# inner join MemberPolicy Mp (NOLOCK) on c.MemberPolicyID = Mp.id
# inner join Membersi s (NOLOCK) on mp.id = s.memberpolicyid
# inner join bpsuminsured bp on bp.id=s.BPSIID and bp.SICategoryID_P20=69
# left join bpsiconditions bpcon on bpcon.bpsiid = bp.id and bpcon.bpconditionid = 30
# and (',' + bpcon.Relationshipid + ',' like '%,' + CONVERT(VARCHAR,mp.relationshipid) + ',%' OR bpcon.Relationshipid is null)
# and bp.policyid = bpcon.policyid
# left join [BPServiceConfigDetails] bpscon_ind on bpscon_ind.benefitplansiid = bp.id and c.servicetypeid = bpscon_ind.servicetypeid
# and ',' + bpscon_ind.AllowedRelationids + ',' like '%,' + CONVERT(VARCHAR,mp.relationshipid) + ',%' and bpscon_ind.[LimitCatg_P29] = 108
# left join [BPServiceConfigDetails] bpscon_fam on bpscon_fam.benefitplansiid = bp.id and c.servicetypeid = bpscon_fam.servicetypeid
# and ',' + bpscon_fam.AllowedRelationids + ',' like '%,' + CONVERT(VARCHAR,mp.relationshipid) + ',%' and bpscon_fam.[LimitCatg_P29] = 107
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
# FROM ClaimUtilizedAmount CU, Claims C, ClaimsDetails CD Where 
# CD.ClaimID=C.ID AND C.Deleted=0 AND CU.Deleted=0 AND CU.ClaimId = CD.ClaimId AND CU.SlNo = CD.SlNo
# AND CD.ClaimId = C.ID AND CD.Deleted = 0
# AND EXISTS (SELECT 1 FROM MemberPolicy MP (NOLOCK) WHERE MP.ID = C.MemberPolicyId AND MP.MainmemberID = @MainMemberID AND MP.ID = IIF(@SITypeID=5, MP.ID,@MemberPolicyID) AND ',' + ISNULL(@RelationshipIds,'0') + ',' like '%,' + IIF(@RelationshipIds IS NULL, '0', CONVERT(VARCHAR,MP.relationshipid)) + ',%')
# AND CD.RequestTypeID >= 4 AND CD.StageID = 27 AND CD.RequestTypeID <> 9
# AND CU.ClaimID <> @ClaimId AND C.ReceivedDate <= @ClaimReceivedDate
# ),0)
# -
# -- MR CASES WHICH ARE NOT YET SETTLED
# ISNULL((
# Select SUM(IIF(isnull(CD.SanctionedAmount,0) > 0, isnull(CD.SanctionedAmount,0), isnull(CD.ClaimAmount,0)- CASE WHEN CD.StageID = 26 THEN isnull(CD.DeductionAmount,0) ELSE 0 END))
# FROM Claims C, ClaimsDetails CD Where 
# CD.ClaimID=C.ID AND C.Deleted=0 AND CD.Deleted = 0 
# AND EXISTS (SELECT 1 FROM MemberPolicy MP (NOLOCK) WHERE MP.ID = C.MemberPolicyId AND MP.MainmemberID = @MainMemberID AND MP.ID = IIF(@SITypeID=5, MP.ID,@MemberPolicyID) AND ',' + ISNULL(@RelationshipIds,'0') + ',' like '%,' + IIF(@RelationshipIds IS NULL, '0', CONVERT(VARCHAR,MP.relationshipid)) + ',%')
# AND CD.RequestTypeID >= 4 AND CD.ClaimTypeID = 2
# AND NOT EXISTS (SELECT 1 FROM ClaimsDetails CD3 WHERE CD3.ClaimId = C.ID AND RequestTypeid >= 4 AND CD3.StageID IN (27,23,21,25))
# AND CD.ClaimID <> @ClaimId AND C.ReceivedDate <= @ClaimReceivedDate
# ),0)
# -
# -- PP CASES WHICH ARE NOT YET SETTLED
# ISNULL((
# Select SUM(isnull(CU.SanctionedAmount,0))
# FROM ClaimUtilizedAmount CU, Claims C, ClaimsDetails CD Where 
# CD.ClaimID=C.ID AND C.Deleted=0 AND CU.Deleted=0 AND CU.ClaimId = CD.ClaimId AND CU.SlNo = CD.SlNo
# AND CD.ClaimId = C.ID AND CD.Deleted = 0
# AND EXISTS (SELECT 1 FROM MemberPolicy MP (NOLOCK) WHERE MP.ID = C.MemberPolicyId AND MP.MainmemberID = @MainMemberID AND MP.ID = IIF(@SITypeID=5, MP.ID,@MemberPolicyID) AND ',' + ISNULL(@RelationshipIds,'0') + ',' like '%,' + IIF(@RelationshipIds IS NULL, '0', CONVERT(VARCHAR,MP.relationshipid)) + ',%')
# AND CD.RequestTypeID < 4 AND CD.StageID NOT IN (21,23,25)
# AND NOT EXISTS (SELECT 1 FROM ClaimsDetails CD3 WHERE CD3.ClaimId = C.ID AND RequestTypeid >= 4 AND CD3.StageID IN (27,23,21,25))
# AND CU.ClaimID <> @ClaimId AND C.ReceivedDate <= @ClaimReceivedDate
# ),0)
# -
# -- PP CLAIMS WHICH HAVE SANCTIONED AMOUNT AFTER THE PRESENT CLAIM
# ISNULL((
# Select SUM(isnull(CU.SanctionedAmount,0))
# FROM ClaimUtilizedAmount CU, Claims C, ClaimsDetails CD Where 
# CD.ClaimID=C.ID AND C.Deleted=0 AND CU.Deleted=0 AND CU.ClaimId = CD.ClaimId AND CU.SlNo = CD.SlNo
# AND CD.ClaimId = C.ID AND CD.Deleted = 0
# AND EXISTS (SELECT 1 FROM MemberPolicy MP (NOLOCK) WHERE MP.ID = C.MemberPolicyId AND MP.MainmemberID = @MainMemberID AND MP.ID = IIF(@SITypeID=5, MP.ID,@MemberPolicyID) AND ',' + ISNULL(@RelationshipIds,'0') + ',' like '%,' + IIF(@RelationshipIds IS NULL, '0', CONVERT(VARCHAR,MP.relationshipid)) + ',%')
# AND CD.RequestTypeID < 4 AND CD.StageID = 29
# AND NOT EXISTS (SELECT 1 FROM ClaimsDetails CD3 WHERE CD3.ClaimId = C.ID AND RequestTypeid >= 4 AND CD3.StageID IN (26,27,23,21,25))
# AND CU.ClaimID <> @ClaimId AND C.ReceivedDate > @ClaimReceivedDate
# ),0)
# -
# ISNULL((
# Select SUM(isnull(CU.SanctionedAmount,0))
# FROM ClaimUtilizedAmount CU, Claims C, ClaimsDetails CD Where 
# CD.ClaimID=C.ID AND C.Deleted=0 AND CU.Deleted=0 AND CU.ClaimId = CD.ClaimId AND CU.SlNo = CD.SlNo
# AND CD.ClaimId = C.ID AND CD.Deleted = 0
# AND EXISTS (SELECT 1 FROM MemberPolicy MP (NOLOCK) WHERE MP.ID = C.MemberPolicyId AND MP.MainmemberID = @MainMemberID AND MP.ID = IIF(@SITypeID=5, MP.ID,@MemberPolicyID) AND ',' + ISNULL(@RelationshipIds,'0') + ',' like '%,' + IIF(@RelationshipIds IS NULL, '0', CONVERT(VARCHAR,MP.relationshipid)) + ',%')
# AND CD.RequestTypeID >= 4 AND CD.StageID IN (26,27) AND CD.RequestTypeID <> 9
# AND CU.ClaimID <> @ClaimId AND C.ReceivedDate > @ClaimReceivedDate
# ),0)
# -
# -- MR CLAIMS WHICH HAVE SANCTIONED AMOUNT AFTER THE PRESENT CLAIM
# ISNULL((
# Select SUM(IIF(isnull(CD.SanctionedAmount,0) > 0, isnull(CD.SanctionedAmount,0), isnull(CD.ClaimAmount,0)- CASE WHEN CD.StageID = 26 THEN isnull(CD.DeductionAmount,0) ELSE 0 END))
# FROM Claims C, ClaimsDetails CD Where 
# CD.ClaimID=C.ID AND C.Deleted=0 AND CD.Deleted = 0 
# AND EXISTS (SELECT 1 FROM MemberPolicy MP (NOLOCK) WHERE MP.ID = C.MemberPolicyId AND MP.MainmemberID = @MainMemberID AND MP.ID = IIF(@SITypeID=5, MP.ID,@MemberPolicyID) AND ',' + ISNULL(@RelationshipIds,'0') + ',' like '%,' + IIF(@RelationshipIds IS NULL, '0', CONVERT(VARCHAR,MP.relationshipid)) + ',%')
# AND CD.RequestTypeID >= 4 AND CD.ClaimTypeID = 2 AND CD.StageID IN (26,27)
# AND CD.ClaimID <> @ClaimId AND C.ReceivedDate > @ClaimReceivedDate
# ),0)

# -- Calculate with total sum insured

# SELECT @Totalbalance = isnull(@SumInsured,0) + isnull(@CB_Amount,0) -
# -- SETTLED CASES
# ISNULL((
# Select SUM(isnull(CU.SanctionedAmount,0))
# FROM ClaimUtilizedAmount CU, Claims C, ClaimsDetails CD Where 
# CD.ClaimID=C.ID AND C.Deleted=0 AND CU.Deleted=0 AND CU.ClaimId = CD.ClaimId AND CU.SlNo = CD.SlNo
# AND CD.ClaimId = C.ID AND CD.Deleted = 0
# AND EXISTS (SELECT 1 FROM MemberPolicy MP (NOLOCK) WHERE MP.ID = C.MemberPolicyId AND MP.MainmemberID = @MainMemberID AND MP.ID = IIF(@SITypeID=5, MP.ID,@MemberPolicyID))
# AND CD.RequestTypeID >= 4 AND CD.StageID = 27 AND CD.RequestTypeID <> 9
# AND CU.ClaimID <> @ClaimId AND C.ReceivedDate <= @ClaimReceivedDate
# ),0)
# -
# -- MR CASES WHICH ARE NOT YET SETTLED
# ISNULL((
# Select SUM(IIF(isnull(CD.SanctionedAmount,0) > 0, isnull(CD.SanctionedAmount,0), isnull(CD.ClaimAmount,0)- CASE WHEN CD.StageID = 26 THEN isnull(CD.DeductionAmount,0) ELSE 0 END))
# FROM Claims C, ClaimsDetails CD Where 
# CD.ClaimID=C.ID AND C.Deleted=0 AND CD.Deleted = 0 
# AND EXISTS (SELECT 1 FROM MemberPolicy MP (NOLOCK) WHERE MP.ID = C.MemberPolicyId AND MP.MainmemberID = @MainMemberID AND MP.ID = IIF(@SITypeID=5, MP.ID,@MemberPolicyID))
# AND CD.RequestTypeID >= 4 AND CD.ClaimTypeID = 2
# AND NOT EXISTS (SELECT 1 FROM ClaimsDetails CD3 WHERE CD3.ClaimId = C.ID AND RequestTypeid >= 4 AND CD3.StageID IN (27,23,21,25))
# AND CD.ClaimID <> @ClaimId AND C.ReceivedDate <= @ClaimReceivedDate
# ),0)
# -
# -- PP CASES WHICH ARE NOT YET SETTLED
# ISNULL((
# Select SUM(isnull(CU.SanctionedAmount,0))
# FROM ClaimUtilizedAmount CU, Claims C, ClaimsDetails CD Where 
# CD.ClaimID=C.ID AND C.Deleted=0 AND CU.Deleted=0 AND CU.ClaimId = CD.ClaimId AND CU.SlNo = CD.SlNo
# AND CD.ClaimId = C.ID AND CD.Deleted = 0
# AND EXISTS (SELECT 1 FROM MemberPolicy MP (NOLOCK) WHERE MP.ID = C.MemberPolicyId AND MP.MainmemberID = @MainMemberID AND MP.ID = IIF(@SITypeID=5, MP.ID,@MemberPolicyID))
# AND CD.RequestTypeID < 4 AND CD.StageID NOT IN (21,23,25)
# AND NOT EXISTS (SELECT 1 FROM ClaimsDetails CD3 WHERE CD3.ClaimId = C.ID AND RequestTypeid >= 4 AND CD3.StageID IN (27,23,21,25))
# AND CU.ClaimID <> @ClaimId AND C.ReceivedDate <= @ClaimReceivedDate
# ),0)
# -
# -- PP CLAIMS WHICH HAVE SANCTIONED AMOUNT AFTER THE PRESENT CLAIM
# ISNULL((
# Select SUM(isnull(CU.SanctionedAmount,0))
# FROM ClaimUtilizedAmount CU, Claims C, ClaimsDetails CD Where 
# CD.ClaimID=C.ID AND C.Deleted=0 AND CU.Deleted=0 AND CU.ClaimId = CD.ClaimId AND CU.SlNo = CD.SlNo
# AND CD.ClaimId = C.ID AND CD.Deleted = 0
# AND EXISTS (SELECT 1 FROM MemberPolicy MP (NOLOCK) WHERE MP.ID = C.MemberPolicyId AND MP.MainmemberID = @MainMemberID AND MP.ID = IIF(@SITypeID=5, MP.ID,@MemberPolicyID) AND ',' + ISNULL(@RelationshipIds,'0') + ',' like '%,' + IIF(@RelationshipIds IS NULL, '0', CONVERT(VARCHAR,MP.relationshipid)) + ',%')
# AND CD.RequestTypeID < 4 AND CD.StageID = 29
# AND NOT EXISTS (SELECT 1 FROM ClaimsDetails CD3 WHERE CD3.ClaimId = C.ID AND RequestTypeid >= 4 AND CD3.StageID IN (26,27,23,21,25))
# AND CU.ClaimID <> @ClaimId AND C.ReceivedDate > @ClaimReceivedDate
# ),0)
# -
# ISNULL((
# Select SUM(isnull(CU.SanctionedAmount,0))
# FROM ClaimUtilizedAmount CU, Claims C, ClaimsDetails CD Where 
# CD.ClaimID=C.ID AND C.Deleted=0 AND CU.Deleted=0 AND CU.ClaimId = CD.ClaimId AND CU.SlNo = CD.SlNo
# AND CD.ClaimId = C.ID AND CD.Deleted = 0
# AND EXISTS (SELECT 1 FROM MemberPolicy MP (NOLOCK) WHERE MP.ID = C.MemberPolicyId AND MP.MainmemberID = @MainMemberID AND MP.ID = IIF(@SITypeID=5, MP.ID,@MemberPolicyID) AND ',' + ISNULL(@RelationshipIds,'0') + ',' like '%,' + IIF(@RelationshipIds IS NULL, '0', CONVERT(VARCHAR,MP.relationshipid)) + ',%')
# AND CD.RequestTypeID >= 4 AND CD.StageID IN (26, 27) AND CD.RequestTypeID <> 9
# AND CU.ClaimID <> @ClaimId AND C.ReceivedDate > @ClaimReceivedDate
# ),0)
# -
# -- MR CLAIMS WHICH HAVE SANCTIONED AMOUNT AFTER THE PRESENT CLAIM
# ISNULL((
# Select SUM(IIF(isnull(CD.SanctionedAmount,0) > 0, isnull(CD.SanctionedAmount,0), isnull(CD.ClaimAmount,0)- CASE WHEN CD.StageID = 26 THEN isnull(CD.DeductionAmount,0) ELSE 0 END))
# FROM Claims C, ClaimsDetails CD Where 
# CD.ClaimID=C.ID AND C.Deleted=0 AND CD.Deleted = 0 
# AND EXISTS (SELECT 1 FROM MemberPolicy MP (NOLOCK) WHERE MP.ID = C.MemberPolicyId AND MP.MainmemberID = @MainMemberID AND MP.ID = IIF(@SITypeID=5, MP.ID,@MemberPolicyID) AND ',' + ISNULL(@RelationshipIds,'0') + ',' like '%,' + IIF(@RelationshipIds IS NULL, '0', CONVERT(VARCHAR,MP.relationshipid)) + ',%')
# AND CD.RequestTypeID >= 4 AND CD.ClaimTypeID = 2 AND CD.StageID IN (26,27)
# AND CD.ClaimID <> @ClaimId AND C.ReceivedDate > @ClaimReceivedDate
# ),0)

# RETURN IIF(@Totalbalance < @Sublimitbalance, IIF(@Totalbalance < 0,0,@Totalbalance), IIF(@Sublimitbalance < 0, 0,@Sublimitbalance))

# End

# COMMAND ----------

# DBTITLE 1,balance_sum_insured_without_present_claim
def balance_sum_insured_without_present_claim(claimID, slno):
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

# claimID = 24070402456
# slno = 3
# claims_df = spark.sql("SELECT * FROM fhpl.Claims")
# member_policy_df = spark.sql("SELECT * FROM fhpl.MemberPolicy")
# claims_details_df = spark.sql("SELECT * FROM fhpl.ClaimsDetails")
# membersi_df = spark.sql("SELECT * FROM fhpl.MemberSI")
# bpsuminsured_df = spark.sql("SELECT * FROM fhpl.BPSumInsured")
# bpsiconditions_df = spark.sql("SELECT * FROM fhpl.BPSIConditions")
# bpserviceconfigdetails_df = spark.sql("SELECT * FROM fhpl.BPServiceConfigDetails")
# claim_utilized_amount_df = spark.sql("SELECT * FROM fhpl.ClaimUtilizedAmount")

# balance = balance_sum_insured_without_present_claim(
#     claimID, slno, claims_df, member_policy_df, membersi_df, bpsuminsured_df, 
#     bpsiconditions_df, bpserviceconfigdetails_df, claim_utilized_amount_df, claims_details_df
# )
# print(balance)

# COMMAND ----------

# MAGIC %md
# MAGIC #### UFN_Spectra_BalanceSumInsured

# COMMAND ----------

# DBTITLE 1,UFN_Spectra_BalanceSumInsured
def UFN_Spectra_BalanceSumInsured(claimID=None, slno=None):

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

# COMMAND ----------

# claims_df = spark.sql("SELECT * FROM fhpl.Claims")
# member_policy_df = spark.sql("SELECT * FROM fhpl.MemberPolicy")
# claims_details_df = spark.sql("SELECT * FROM fhpl.ClaimsDetails").filter("Deleted = 0")
# membersi_df = spark.sql("SELECT * FROM fhpl.MemberSI")
# bpsuminsured_df = spark.sql("SELECT * FROM fhpl.BPSumInsured")
# bpsiconditions_df = spark.sql("SELECT * FROM fhpl.BPSIConditions")
# bp_service_config_details_df = spark.sql("SELECT * FROM fhpl.BPServiceConfigDetails")
# claim_utilized_amount_df = spark.sql("SELECT * FROM fhpl.ClaimUtilizedAmount").filter("Deleted = 0")

# claimID = 24070402597
# slno = 1
# result = UFN_Spectra_BalanceSumInsured(
#     claimID, slno, claims_df, member_policy_df, claims_details_df, membersi_df, bpsuminsured_df, 
# bpsiconditions_df, bp_service_config_details_df, claim_utilized_amount_df)
# print(result)

# COMMAND ----------

# DBTITLE 1,balance_sum_insured_without_present_claim
## Corrected Code by ANAND
# Check the commented logic that has issue and try to see whether we are applying it correct

def balance_sum_insured_without_present_claim(claimID, slno):
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

claims_df = spark.sql("SELECT * FROM fhpl.Claims")
member_policy_df = spark.sql("SELECT * FROM fhpl.MemberPolicy")
claims_details_df = spark.sql("SELECT * FROM fhpl.ClaimsDetails").filter("Deleted = 0")
membersi_df = spark.sql("SELECT * FROM fhpl.MemberSI")
bpsuminsured_df = spark.sql("SELECT * FROM fhpl.BPSumInsured")
bpsiconditions_df = spark.sql("SELECT * FROM fhpl.BPSIConditions")
bp_service_config_details_df = spark.sql("SELECT * FROM fhpl.BPServiceConfigDetails")
claim_utilized_amount_df = spark.sql("SELECT * FROM fhpl.ClaimUtilizedAmount").filter("Deleted = 0")

claimID = 24070402597
slno = 1
result = balance_sum_insured_without_present_claim(
    claimID, slno)
print(result)

# COMMAND ----------

def UDF_GetIncurredAmount(claimid, slno):

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
                SELECT balance_sum_insured_without_present_claim(ID, 3) AS BalanceSumInsured
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
                SELECT UFN_Spectra_BalanceSumInsured_Without_PresentClaim(ID, 1) AS IncurredAmount
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
                SELECT UFN_Spectra_BalanceSumInsured_Without_PresentClaim(ID, 1) AS IncurredAmount
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