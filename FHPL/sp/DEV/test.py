# Databricks notebook source
# MAGIC %sql 
# MAGIC
# MAGIC select * from fhpl.Claims  C
# MAGIC inner join fhpl.MemberPolicy Mp on c.MemberPolicyID = Mp.id
# MAGIC inner join fhpl.Membersi s  on mp.id = s.memberpolicyid
# MAGIC inner join fhpl.bpsuminsured bp on bp.id=s.BPSIID and bp.SICategoryID_P20=69
# MAGIC left join fhpl.bpsiconditions bpcon on bpcon.bpsiid = bp.id and bpcon.bpconditionid = 30

# COMMAND ----------

# MAGIC %sql
# MAGIC USE [McarePlus]
# MAGIC GO
# MAGIC /****** Object:  UserDefinedFunction [dbo].[UFN_Spectra_BalanceSumInsured_Without_PresentClaim]    Script Date: 18-07-2024 13:49:37 ******/
# MAGIC SET ANSI_NULLS ON
# MAGIC GO
# MAGIC SET QUOTED_IDENTIFIER ON
# MAGIC GO
# MAGIC
# MAGIC --SELECT [dbo].[UFN_Spectra_BalanceSumInsured_Without_PresentClaim](19030800104,1)
# MAGIC
# MAGIC ALTER FUNCTION [dbo].[UFN_Spectra_BalanceSumInsured_Without_PresentClaim] (@claimID bigint=NULL,@slno int=NULL)
# MAGIC returns money
# MAGIC as
# MAGIC begin
# MAGIC
# MAGIC declare @BPSIIDs varchar(100)='',@MainMemberID bigint,@MemberSIID varchar(100)='',@MemberPolicyID bigint,@SITypeID int,@CB_Amount money=0, @SumInsured money = 0
# MAGIC
# MAGIC Declare @SubLimitbalance money, @Totalbalance money,  @RelationshipIds varchar(50), @SubLimit money = 0, @ServiceLimit money = 0, @ClaimRelationshipID int, @ClaimReceivedDate datetime
# MAGIC
# MAGIC Select TOP 1 @MainMemberID=MainmemberID,@MemberPolicyID=C.MemberPolicyID,@SITypeID=bp.SITypeID,@CB_Amount=isnull(s.CB_Amount,0),
# MAGIC @SumInsured = bp.SumInsured, @RelationshipIds = bpcon.Relationshipid, @SubLimit = bpcon.FamilyLimit, @ClaimRelationshipID = MP.RelationshipID, @ClaimReceivedDate = C.ReceivedDate,
# MAGIC @ServiceLimit = IIF(bpscon_fam.[InternalValueAbs] < bpscon_ind.[InternalValueAbs], bpscon_fam.[InternalValueAbs], bpscon_ind.[InternalValueAbs])
# MAGIC from Claims C
# MAGIC inner join MemberPolicy Mp (NOLOCK) on c.MemberPolicyID = Mp.id
# MAGIC inner join Membersi s (NOLOCK) on mp.id = s.memberpolicyid
# MAGIC inner join bpsuminsured bp on bp.id=s.BPSIID and bp.SICategoryID_P20=69
# MAGIC left join bpsiconditions bpcon on bpcon.bpsiid = bp.id and bpcon.bpconditionid = 30
# MAGIC and (',' + bpcon.Relationshipid + ',' like '%,' + CONVERT(VARCHAR,mp.relationshipid) + ',%' OR bpcon.Relationshipid is null)
# MAGIC and bp.policyid = bpcon.policyid
# MAGIC left join [BPServiceConfigDetails] bpscon_ind on bpscon_ind.benefitplansiid = bp.id and c.servicetypeid = bpscon_ind.servicetypeid
# MAGIC and ',' + bpscon_ind.AllowedRelationids + ',' like '%,' + CONVERT(VARCHAR,mp.relationshipid) + ',%' and bpscon_ind.[LimitCatg_P29] = 108
# MAGIC left join [BPServiceConfigDetails] bpscon_fam on bpscon_fam.benefitplansiid = bp.id and c.servicetypeid = bpscon_fam.servicetypeid
# MAGIC and ',' + bpscon_fam.AllowedRelationids + ',' like '%,' + CONVERT(VARCHAR,mp.relationshipid) + ',%' and bpscon_fam.[LimitCatg_P29] = 107
# MAGIC where C.ID=@ClaimID and C.Deleted=0 and s.deleted=0
# MAGIC
# MAGIC
# MAGIC if(@SITypeID=5)
# MAGIC Begin
# MAGIC set @MemberPolicyID=@MainMemberID
# MAGIC if(@CB_Amount=0)
# MAGIC select @CB_Amount=cb_amount from membersi ms (NOLOCK),memberpolicy mp1 (NOLOCK) where ms.memberpolicyid=mp1.id and mp1.id=@mainmemberid
# MAGIC End;
# MAGIC
# MAGIC SELECT @SubLimitbalance = IIF(@ClaimRelationshipID = 2, ISNULL(@ServiceLimit, @SumInsured), COALESCE(@ServiceLimit, @SubLimit, @SumInsured)) + isnull(@CB_Amount,0) -
# MAGIC -- SETTLED CASES
# MAGIC ISNULL((
# MAGIC Select SUM(isnull(CU.SanctionedAmount,0))
# MAGIC FROM ClaimUtilizedAmount CU, Claims C, ClaimsDetails CD Where 
# MAGIC CD.ClaimID=C.ID AND C.Deleted=0 AND CU.Deleted=0 AND CU.ClaimId = CD.ClaimId AND CU.SlNo = CD.SlNo
# MAGIC AND CD.ClaimId = C.ID AND CD.Deleted = 0
# MAGIC AND EXISTS (SELECT 1 FROM MemberPolicy MP (NOLOCK) WHERE MP.ID = C.MemberPolicyId AND MP.MainmemberID = @MainMemberID AND MP.ID = IIF(@SITypeID=5, MP.ID,@MemberPolicyID) AND ',' + ISNULL(@RelationshipIds,'0') + ',' like '%,' + IIF(@RelationshipIds IS NULL, '0', CONVERT(VARCHAR,MP.relationshipid)) + ',%')
# MAGIC AND CD.RequestTypeID >= 4 AND CD.StageID = 27 AND CD.RequestTypeID <> 9
# MAGIC AND CU.ClaimID <> @ClaimId AND C.ReceivedDate <= @ClaimReceivedDate
# MAGIC ),0)
# MAGIC -
# MAGIC -- MR CASES WHICH ARE NOT YET SETTLED
# MAGIC ISNULL((
# MAGIC Select SUM(IIF(isnull(CD.SanctionedAmount,0) > 0, isnull(CD.SanctionedAmount,0), isnull(CD.ClaimAmount,0)- CASE WHEN CD.StageID = 26 THEN isnull(CD.DeductionAmount,0) ELSE 0 END))
# MAGIC FROM Claims C, ClaimsDetails CD Where 
# MAGIC CD.ClaimID=C.ID AND C.Deleted=0 AND CD.Deleted = 0 
# MAGIC AND EXISTS (SELECT 1 FROM MemberPolicy MP (NOLOCK) WHERE MP.ID = C.MemberPolicyId AND MP.MainmemberID = @MainMemberID AND MP.ID = IIF(@SITypeID=5, MP.ID,@MemberPolicyID) AND ',' + ISNULL(@RelationshipIds,'0') + ',' like '%,' + IIF(@RelationshipIds IS NULL, '0', CONVERT(VARCHAR,MP.relationshipid)) + ',%')
# MAGIC AND CD.RequestTypeID >= 4 AND CD.ClaimTypeID = 2
# MAGIC AND NOT EXISTS (SELECT 1 FROM ClaimsDetails CD3 WHERE CD3.ClaimId = C.ID AND RequestTypeid >= 4 AND CD3.StageID IN (27,23,21,25))
# MAGIC AND CD.ClaimID <> @ClaimId AND C.ReceivedDate <= @ClaimReceivedDate
# MAGIC ),0)
# MAGIC -
# MAGIC -- PP CASES WHICH ARE NOT YET SETTLED
# MAGIC ISNULL((
# MAGIC Select SUM(isnull(CU.SanctionedAmount,0))
# MAGIC FROM ClaimUtilizedAmount CU, Claims C, ClaimsDetails CD Where 
# MAGIC CD.ClaimID=C.ID AND C.Deleted=0 AND CU.Deleted=0 AND CU.ClaimId = CD.ClaimId AND CU.SlNo = CD.SlNo
# MAGIC AND CD.ClaimId = C.ID AND CD.Deleted = 0
# MAGIC AND EXISTS (SELECT 1 FROM MemberPolicy MP (NOLOCK) WHERE MP.ID = C.MemberPolicyId AND MP.MainmemberID = @MainMemberID AND MP.ID = IIF(@SITypeID=5, MP.ID,@MemberPolicyID) AND ',' + ISNULL(@RelationshipIds,'0') + ',' like '%,' + IIF(@RelationshipIds IS NULL, '0', CONVERT(VARCHAR,MP.relationshipid)) + ',%')
# MAGIC AND CD.RequestTypeID < 4 AND CD.StageID NOT IN (21,23,25)
# MAGIC AND NOT EXISTS (SELECT 1 FROM ClaimsDetails CD3 WHERE CD3.ClaimId = C.ID AND RequestTypeid >= 4 AND CD3.StageID IN (27,23,21,25))
# MAGIC AND CU.ClaimID <> @ClaimId AND C.ReceivedDate <= @ClaimReceivedDate
# MAGIC ),0)
# MAGIC -
# MAGIC -- PP CLAIMS WHICH HAVE SANCTIONED AMOUNT AFTER THE PRESENT CLAIM
# MAGIC ISNULL((
# MAGIC Select SUM(isnull(CU.SanctionedAmount,0))
# MAGIC FROM ClaimUtilizedAmount CU, Claims C, ClaimsDetails CD Where 
# MAGIC CD.ClaimID=C.ID AND C.Deleted=0 AND CU.Deleted=0 AND CU.ClaimId = CD.ClaimId AND CU.SlNo = CD.SlNo
# MAGIC AND CD.ClaimId = C.ID AND CD.Deleted = 0
# MAGIC AND EXISTS (SELECT 1 FROM MemberPolicy MP (NOLOCK) WHERE MP.ID = C.MemberPolicyId AND MP.MainmemberID = @MainMemberID AND MP.ID = IIF(@SITypeID=5, MP.ID,@MemberPolicyID) AND ',' + ISNULL(@RelationshipIds,'0') + ',' like '%,' + IIF(@RelationshipIds IS NULL, '0', CONVERT(VARCHAR,MP.relationshipid)) + ',%')
# MAGIC AND CD.RequestTypeID < 4 AND CD.StageID = 29
# MAGIC AND NOT EXISTS (SELECT 1 FROM ClaimsDetails CD3 WHERE CD3.ClaimId = C.ID AND RequestTypeid >= 4 AND CD3.StageID IN (26,27,23,21,25))
# MAGIC AND CU.ClaimID <> @ClaimId AND C.ReceivedDate > @ClaimReceivedDate
# MAGIC ),0)
# MAGIC -
# MAGIC ISNULL((
# MAGIC Select SUM(isnull(CU.SanctionedAmount,0))
# MAGIC FROM ClaimUtilizedAmount CU, Claims C, ClaimsDetails CD Where 
# MAGIC CD.ClaimID=C.ID AND C.Deleted=0 AND CU.Deleted=0 AND CU.ClaimId = CD.ClaimId AND CU.SlNo = CD.SlNo
# MAGIC AND CD.ClaimId = C.ID AND CD.Deleted = 0
# MAGIC AND EXISTS (SELECT 1 FROM MemberPolicy MP (NOLOCK) WHERE MP.ID = C.MemberPolicyId AND MP.MainmemberID = @MainMemberID AND MP.ID = IIF(@SITypeID=5, MP.ID,@MemberPolicyID) AND ',' + ISNULL(@RelationshipIds,'0') + ',' like '%,' + IIF(@RelationshipIds IS NULL, '0', CONVERT(VARCHAR,MP.relationshipid)) + ',%')
# MAGIC AND CD.RequestTypeID >= 4 AND CD.StageID IN (26,27) AND CD.RequestTypeID <> 9
# MAGIC AND CU.ClaimID <> @ClaimId AND C.ReceivedDate > @ClaimReceivedDate
# MAGIC ),0)
# MAGIC -
# MAGIC -- MR CLAIMS WHICH HAVE SANCTIONED AMOUNT AFTER THE PRESENT CLAIM
# MAGIC ISNULL((
# MAGIC Select SUM(IIF(isnull(CD.SanctionedAmount,0) > 0, isnull(CD.SanctionedAmount,0), isnull(CD.ClaimAmount,0)- CASE WHEN CD.StageID = 26 THEN isnull(CD.DeductionAmount,0) ELSE 0 END))
# MAGIC FROM Claims C, ClaimsDetails CD Where 
# MAGIC CD.ClaimID=C.ID AND C.Deleted=0 AND CD.Deleted = 0 
# MAGIC AND EXISTS (SELECT 1 FROM MemberPolicy MP (NOLOCK) WHERE MP.ID = C.MemberPolicyId AND MP.MainmemberID = @MainMemberID AND MP.ID = IIF(@SITypeID=5, MP.ID,@MemberPolicyID) AND ',' + ISNULL(@RelationshipIds,'0') + ',' like '%,' + IIF(@RelationshipIds IS NULL, '0', CONVERT(VARCHAR,MP.relationshipid)) + ',%')
# MAGIC AND CD.RequestTypeID >= 4 AND CD.ClaimTypeID = 2 AND CD.StageID IN (26,27)
# MAGIC AND CD.ClaimID <> @ClaimId AND C.ReceivedDate > @ClaimReceivedDate
# MAGIC ),0)
# MAGIC
# MAGIC -- Calculate with total sum insured
# MAGIC
# MAGIC SELECT @Totalbalance = isnull(@SumInsured,0) + isnull(@CB_Amount,0) -
# MAGIC -- SETTLED CASES
# MAGIC ISNULL((
# MAGIC Select SUM(isnull(CU.SanctionedAmount,0))
# MAGIC FROM ClaimUtilizedAmount CU, Claims C, ClaimsDetails CD Where 
# MAGIC CD.ClaimID=C.ID AND C.Deleted=0 AND CU.Deleted=0 AND CU.ClaimId = CD.ClaimId AND CU.SlNo = CD.SlNo
# MAGIC AND CD.ClaimId = C.ID AND CD.Deleted = 0
# MAGIC AND EXISTS (SELECT 1 FROM MemberPolicy MP (NOLOCK) WHERE MP.ID = C.MemberPolicyId AND MP.MainmemberID = @MainMemberID AND MP.ID = IIF(@SITypeID=5, MP.ID,@MemberPolicyID))
# MAGIC AND CD.RequestTypeID >= 4 AND CD.StageID = 27 AND CD.RequestTypeID <> 9
# MAGIC AND CU.ClaimID <> @ClaimId AND C.ReceivedDate <= @ClaimReceivedDate
# MAGIC ),0)
# MAGIC -
# MAGIC -- MR CASES WHICH ARE NOT YET SETTLED
# MAGIC ISNULL((
# MAGIC Select SUM(IIF(isnull(CD.SanctionedAmount,0) > 0, isnull(CD.SanctionedAmount,0), isnull(CD.ClaimAmount,0)- CASE WHEN CD.StageID = 26 THEN isnull(CD.DeductionAmount,0) ELSE 0 END))
# MAGIC FROM Claims C, ClaimsDetails CD Where 
# MAGIC CD.ClaimID=C.ID AND C.Deleted=0 AND CD.Deleted = 0 
# MAGIC AND EXISTS (SELECT 1 FROM MemberPolicy MP (NOLOCK) WHERE MP.ID = C.MemberPolicyId AND MP.MainmemberID = @MainMemberID AND MP.ID = IIF(@SITypeID=5, MP.ID,@MemberPolicyID))
# MAGIC AND CD.RequestTypeID >= 4 AND CD.ClaimTypeID = 2
# MAGIC AND NOT EXISTS (SELECT 1 FROM ClaimsDetails CD3 WHERE CD3.ClaimId = C.ID AND RequestTypeid >= 4 AND CD3.StageID IN (27,23,21,25))
# MAGIC AND CD.ClaimID <> @ClaimId AND C.ReceivedDate <= @ClaimReceivedDate
# MAGIC ),0)
# MAGIC -
# MAGIC -- PP CASES WHICH ARE NOT YET SETTLED
# MAGIC ISNULL((
# MAGIC Select SUM(isnull(CU.SanctionedAmount,0))
# MAGIC FROM ClaimUtilizedAmount CU, Claims C, ClaimsDetails CD Where 
# MAGIC CD.ClaimID=C.ID AND C.Deleted=0 AND CU.Deleted=0 AND CU.ClaimId = CD.ClaimId AND CU.SlNo = CD.SlNo
# MAGIC AND CD.ClaimId = C.ID AND CD.Deleted = 0
# MAGIC AND EXISTS (SELECT 1 FROM MemberPolicy MP (NOLOCK) WHERE MP.ID = C.MemberPolicyId AND MP.MainmemberID = @MainMemberID AND MP.ID = IIF(@SITypeID=5, MP.ID,@MemberPolicyID))
# MAGIC AND CD.RequestTypeID < 4 AND CD.StageID NOT IN (21,23,25)
# MAGIC AND NOT EXISTS (SELECT 1 FROM ClaimsDetails CD3 WHERE CD3.ClaimId = C.ID AND RequestTypeid >= 4 AND CD3.StageID IN (27,23,21,25))
# MAGIC AND CU.ClaimID <> @ClaimId AND C.ReceivedDate <= @ClaimReceivedDate
# MAGIC ),0)
# MAGIC -
# MAGIC -- PP CLAIMS WHICH HAVE SANCTIONED AMOUNT AFTER THE PRESENT CLAIM
# MAGIC ISNULL((
# MAGIC Select SUM(isnull(CU.SanctionedAmount,0))
# MAGIC FROM ClaimUtilizedAmount CU, Claims C, ClaimsDetails CD Where 
# MAGIC CD.ClaimID=C.ID AND C.Deleted=0 AND CU.Deleted=0 AND CU.ClaimId = CD.ClaimId AND CU.SlNo = CD.SlNo
# MAGIC AND CD.ClaimId = C.ID AND CD.Deleted = 0
# MAGIC AND EXISTS (SELECT 1 FROM MemberPolicy MP (NOLOCK) WHERE MP.ID = C.MemberPolicyId AND MP.MainmemberID = @MainMemberID AND MP.ID = IIF(@SITypeID=5, MP.ID,@MemberPolicyID) AND ',' + ISNULL(@RelationshipIds,'0') + ',' like '%,' + IIF(@RelationshipIds IS NULL, '0', CONVERT(VARCHAR,MP.relationshipid)) + ',%')
# MAGIC AND CD.RequestTypeID < 4 AND CD.StageID = 29
# MAGIC AND NOT EXISTS (SELECT 1 FROM ClaimsDetails CD3 WHERE CD3.ClaimId = C.ID AND RequestTypeid >= 4 AND CD3.StageID IN (26,27,23,21,25))
# MAGIC AND CU.ClaimID <> @ClaimId AND C.ReceivedDate > @ClaimReceivedDate
# MAGIC ),0)
# MAGIC -
# MAGIC ISNULL((
# MAGIC Select SUM(isnull(CU.SanctionedAmount,0))
# MAGIC FROM ClaimUtilizedAmount CU, Claims C, ClaimsDetails CD Where 
# MAGIC CD.ClaimID=C.ID AND C.Deleted=0 AND CU.Deleted=0 AND CU.ClaimId = CD.ClaimId AND CU.SlNo = CD.SlNo
# MAGIC AND CD.ClaimId = C.ID AND CD.Deleted = 0
# MAGIC AND EXISTS (SELECT 1 FROM MemberPolicy MP (NOLOCK) WHERE MP.ID = C.MemberPolicyId AND MP.MainmemberID = @MainMemberID AND MP.ID = IIF(@SITypeID=5, MP.ID,@MemberPolicyID) AND ',' + ISNULL(@RelationshipIds,'0') + ',' like '%,' + IIF(@RelationshipIds IS NULL, '0', CONVERT(VARCHAR,MP.relationshipid)) + ',%')
# MAGIC AND CD.RequestTypeID >= 4 AND CD.StageID IN (26, 27) AND CD.RequestTypeID <> 9
# MAGIC AND CU.ClaimID <> @ClaimId AND C.ReceivedDate > @ClaimReceivedDate
# MAGIC ),0)
# MAGIC -
# MAGIC -- MR CLAIMS WHICH HAVE SANCTIONED AMOUNT AFTER THE PRESENT CLAIM
# MAGIC ISNULL((
# MAGIC Select SUM(IIF(isnull(CD.SanctionedAmount,0) > 0, isnull(CD.SanctionedAmount,0), isnull(CD.ClaimAmount,0)- CASE WHEN CD.StageID = 26 THEN isnull(CD.DeductionAmount,0) ELSE 0 END))
# MAGIC FROM Claims C, ClaimsDetails CD Where 
# MAGIC CD.ClaimID=C.ID AND C.Deleted=0 AND CD.Deleted = 0 
# MAGIC AND EXISTS (SELECT 1 FROM MemberPolicy MP (NOLOCK) WHERE MP.ID = C.MemberPolicyId AND MP.MainmemberID = @MainMemberID AND MP.ID = IIF(@SITypeID=5, MP.ID,@MemberPolicyID) AND ',' + ISNULL(@RelationshipIds,'0') + ',' like '%,' + IIF(@RelationshipIds IS NULL, '0', CONVERT(VARCHAR,MP.relationshipid)) + ',%')
# MAGIC AND CD.RequestTypeID >= 4 AND CD.ClaimTypeID = 2 AND CD.StageID IN (26,27)
# MAGIC AND CD.ClaimID <> @ClaimId AND C.ReceivedDate > @ClaimReceivedDate
# MAGIC ),0)
# MAGIC
# MAGIC RETURN IIF(@Totalbalance < @Sublimitbalance, IIF(@Totalbalance < 0,0,@Totalbalance), IIF(@Sublimitbalance < 0, 0,@Sublimitbalance))
# MAGIC
# MAGIC End

# COMMAND ----------

sdf
amit , 20
amit , 34
basher , 30
basher , 60  
anuj, 25

redf
amit 50
anuj 10 
basheer 30


# COMMAND ----------

def (uid, spdf , reddf)
 spdf.join(red, uid).filter(uid==udi)
 return with(returnmaot, sdf.amount*redfamount/1000).collecting()



# COMMAND ----------

spdf , reddf
 spdf.join(red, uid)
 return withcolut(returnmaot, sdf.amount*redfamount/1000)


amit , 20, 10
basher , 30 , 9
anuj, 25, 2.5