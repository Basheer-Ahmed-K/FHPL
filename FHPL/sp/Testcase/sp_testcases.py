# Databricks notebook source
# MAGIC %run ./sp_review

# COMMAND ----------

import unittest
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Dis_Rej_reason
claim_rejection_reasons_data = [
    Row(claimid=24070402926, slno=1, RejectionReasonsID=1, Deleted=0, FreeText1="Sample text", Remarks=None),
    Row(claimid=24070402926, slno=1, RejectionReasonsID=0, Deleted=0, FreeText1=None, Remarks="Manual remark")
]

mst_rejection_reasons_data = [
    Row(ID=1, Name="Reason 1 |FT|", Deleted=0)
]

claims_data = [
    Row(ID=24070402926, MemberPolicyID=1, DateOfAdmission="2022-01-01", DateOfDischarge="2022-01-05", Deleted=0)
]

member_policy_data = [
    Row(ID=1, MemberCommencingDate="2020-01-01", MemberInceptionDate="2019-01-01", Deleted=0)
]

claims_details_data = [
    Row(claimid=24070402926, slno=1, reason="Detail reason", createddatetime="2016-01-01", deleted=0, remarks=None, requesttypeid=1, stageid=23)
]

claim_rejection_reasons_df = spark.createDataFrame(claim_rejection_reasons_data)
mst_rejection_reasons_df = spark.createDataFrame(mst_rejection_reasons_data)
claims_df = spark.createDataFrame(claims_data)
member_policy_df = spark.createDataFrame(member_policy_data)

# Define schema for claims_details_data
claims_details_schema = StructType([
    StructField("claimid", LongType(), True),
    StructField("slno", IntegerType(), True),
    StructField("reason", StringType(), True),
    StructField("createddatetime", StringType(), True),
    StructField("deleted", IntegerType(), True),
    StructField("remarks", StringType(), True),
    StructField("requesttypeid", IntegerType(), True),
    StructField("stageid", IntegerType(), True)
])

claims_details_df = spark.createDataFrame(claims_details_data, schema=claims_details_schema)

claimid = 24070402926
slno = 1
expected_reason = "Reason 1 Sample text, Manual remark, Detail reason"

actual_reason = udf_rejection_reason(claimid, slno, claims_df, claim_rejection_reasons_df, mst_rejection_reasons_df, member_policy_df, claims_details_df)

assert actual_reason == expected_reason, f"Expected: {expected_reason}, but got: {actual_reason}"
print("Test passed!")

# COMMAND ----------

# DBTITLE 1,udf_disallowence_reason
claim_deduction_details_data = [
    Row(claimid=24070402633, slno=4, deleted=0, DeductionReasonID=1, deductionamount=100, FreeTextValue="Sample text", serviceid=1, deductionslno=1),
    Row(claimid=24070402633, slno=4, deleted=0, DeductionReasonID=2, deductionamount=200, FreeTextValue="Another text", serviceid=2, deductionslno=2),
    Row(claimid=24070402633, slno=3, deleted=0, DeductionReasonID=1, deductionamount=150, FreeTextValue="Different text", serviceid=1, deductionslno=1)
]

mst_deduction_reasons_data = [
    Row(id=1, name="Reason 1"),
    Row(id=2, name="Reason 2")
]

claim_deduction_details_df = spark.createDataFrame(claim_deduction_details_data)
mst_deduction_reasons_df = spark.createDataFrame(mst_deduction_reasons_data)

claimid = 24070402633
slno = 4
expected_reason = "Rs., 100, Reason 1, Sample text, Rs., 200, Reason 2, Another text"

actual_reason = udf_disallowence_reason(claim_deduction_details_df, mst_deduction_reasons_df, claimid, slno)

assert actual_reason == expected_reason, f"Expected: {expected_reason}, but got: {actual_reason}"
print("Test passed!")

# COMMAND ----------

# DBTITLE 1,UFN_IRPendingReasons
from pyspark.sql import SparkSession
class TestUFN_IRPendingReasons(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local").appName("TestUFN_IRPendingReasons").getOrCreate()
        
        # Sample data for testing
        cls.claims_ir_reasons_df = cls.spark.createDataFrame([
            (24070402002, 3, 1, 0, 1, 0, "Remark 1", 0, 0, 0, 0),
            (24070402002, 3, 1, 0, 1, 1, "Remark 2", 0, 0, 0, 0)
        ], ["ClaimID", "slno", "IRDocumentID", "Deleted", "ismandatory", "IsReceived", "Remarks", "serviceid", "FreeText1", "FreeText2", "issueid"])
        
        cls.mst_irdocuments_df = cls.spark.createDataFrame([
            (0, "Document 1"),
            (1, "Document 2")
        ], ["ID", "Name"])
        
        cls.mst_issuingauthority_df = cls.spark.createDataFrame([
            (0, "Authority 1"),
            (1, "Authority 2")
        ], ["id", "Name"])
        
        cls.claims_df = cls.spark.createDataFrame([
            (24070402002, 0)
        ], ["id", "issueid"])
        
        cls.mst_services_df = cls.spark.createDataFrame([
            (0, "Service 1"),
            (1, "Service 2")
        ], ["ID", "Name"])

    def test_UFN_IRPendingReasons(self):
        pending_reasons = UFN_IRPendingReasons(24070402002, 3, self.claims_ir_reasons_df, self.mst_irdocuments_df, self.mst_issuingauthority_df, self.claims_df, self.mst_services_df, 1, 0)
        self.assertEqual(pending_reasons, "Document 2")

if __name__ == '__main__':
    unittest.main(argv=[''], verbosity=2, exit=False)

# COMMAND ----------

# DBTITLE 1,incurred_amount
class TestBalanceSumInsured(unittest.TestCase):

    def setUp(self):
        claims_data = [
            Row(ID=24070402456, MemberPolicyID=1, ServiceTypeID=1, ReceivedDate='2023-01-01', Deleted=0)
        ]
        member_policy_data = [
            Row(ID=1, MainMemberID=1, RelationshipID=1)
        ]
        membersi_data = [
            Row(MemberPolicyID=1, BPSIID=1, CB_Amount=100, Deleted=0)
        ]
        bpsuminsured_data = [
            Row(ID=1, SICategoryID_P20=69, PolicyID=1, SumInsured=1000, SITypeID=1)
        ]
        bpsiconditions_data = [
            Row(BPSIID=1, BPConditionID=30, PolicyID=1, RelationshipID=1, FamilyLimit=500)
        ]
        bpserviceconfigdetails_data = [
            Row(BenefitPlanSIID=1, ServiceTypeID=1, AllowedRelationIDs=1, LimitCatg_P29=108, InternalValueAbs=200),
            Row(BenefitPlanSIID=1, ServiceTypeID=1, AllowedRelationIDs=1, LimitCatg_P29=107, InternalValueAbs=300)
        ]
        claim_utilized_amount_data = [
            Row(ClaimID=24070402455, SlNo=1, SanctionedAmount=100, Deleted=0)
        ]
        claims_details_data = [
            Row(ClaimID=24070402455, SlNo=1, RequestTypeID=4, StageID=27, ClaimTypeID=2, SanctionedAmount=100, ClaimAmount=200, DeductionAmount=50, Deleted=0)
        ]

        # Create DataFrames from the sample data
        self.claims_df = spark.createDataFrame(claims_data)
        self.member_policy_df = spark.createDataFrame(member_policy_data)
        self.membersi_df = spark.createDataFrame(membersi_data)
        self.bpsuminsured_df = spark.createDataFrame(bpsuminsured_data)
        self.bpsiconditions_df = spark.createDataFrame(bpsiconditions_data)
        self.bpserviceconfigdetails_df = spark.createDataFrame(bpserviceconfigdetails_data)
        self.claim_utilized_amount_df = spark.createDataFrame(claim_utilized_amount_data)
        self.claims_details_df = spark.createDataFrame(claims_details_data)

    def test_balance_sum_insured(self):
        claimID = 24070402456
        slno = 1

        balance = balance_sum_insured_without_present_claim(
            claimID, slno, self.claims_df, self.member_policy_df, self.membersi_df, self.bpsuminsured_df, 
            self.bpsiconditions_df, self.bpserviceconfigdetails_df, self.claim_utilized_amount_df, self.claims_details_df
        )

        self.assertIsNotNone(balance)

if __name__ == '__main__':
    unittest.main(argv=[''], verbosity=2, exit=False)