# Databricks notebook source
# MAGIC %pip install dbldatagen faker

# COMMAND ----------

from pyspark.sql import SparkSession
import dbldatagen as dg
from pyspark.sql.types import *
from faker import Faker
faker = Faker("en_IN")
from datetime import datetime
from pyspark.sql.functions import *
import random
fake = Faker('en-IN')

# COMMAND ----------

num_rows = 1000
row_num = 1000
received_modes = [1, 2, 3]
sender_types = [1, 2]
document_types = [1, 2, 3]
request_types = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12] 
statuses = [1, 2, 3]
claim_types = [1, 2, 3]

levels = [1, 2, 3]
treatment_types = [66, 67]
icd_codes = ["I21", "I22", "I23"]
pcs_codes = ["2703", "02703DZ", "02713DZ", "02723DZ", "027034Z", "027134Z", "027234Z", "02703ZZ"]

probable_diagnoses = [
    "Typhoid fever",
    "dengue fever",
    "lrti/seticemia/uti",
    "LRTI/SETICEMIA/UTI",
    "Fever and Illness",
    "Acute appendicitis",
    "DENGUE FEVER",
    "jaundice blood infection high fever",
    "MEDICAL"
]

status_change_remarks_list = [
    "Reviewed and approved",
    "Pending further review",
    "Closed due to insufficient information",
    "Awaiting additional documentation",
    "Escalated to senior management",
    "Decision pending"
]
def generate_gmail():
    return f"{fake.user_name()}@gmail.com"
email_ids = [generate_gmail() for _ in range(num_rows)]

bank_names = [
    "State Bank of India",
    "HDFC Bank",
    "ICICI Bank",
    "Axis Bank",
    "Punjab National Bank",
    "Bank of Baroda",
    "Kotak Mahindra Bank",
    "Canara Bank",
    "Union Bank of India",
    "Syndicate Bank",
    "Yes Bank",
    "Bank of India",
    "Indian Bank",
    "Central Bank of India",
    "Standard Chartered Bank"
]
branch_names = [
    "Connaught Place",
    "Jubilee Hills",
    "Andheri East",
    "Koramangala",
    "Navi Mumbai",
    "Bengaluru East",
    "Chennai Central",
    "Kolkata South",
    "Hyderabad Main",
    "Delhi North",
    "Pune Camp",
    "Mumbai Fort",
    "Vashi",
    "Bhopal",
    "Jaipur City"
]
relation_list = [
    "Mother","Daughter","Father","Son","Husband","Spouse","Wife","Nephew"
]

def generate_policy_number():
    return "GMC" + ''.join(str(random.randint(0, 9)) for _ in range(8))

policy_numbers = [generate_policy_number() for _ in range(num_rows)]

product_names = ["Star Health & Allied","Go Digit","Acko General","Zuno General","Aditya Birla Sun","Cholamandalam MS","ManipalCigna","NAVI","Magma HDI","Aditya Birla"]

indian_names_full = [
    "Aarav Sharma", "Vivaan Patel", "Aditya Gupta", "Vihaan Singh", "Arjun Kumar", "Ayaan Desai", 
    "Reyansh Reddy", "Sai Rao", "Aryan Mehta", "Rohan Sharma", "Ishaan Jain", "Kartik Agarwal", 
    "Shiv Shah", "Siddharth Verma", "Karan Reddy", "Amit Bhardwaj", "Ravi Prasad", "Rishi Patel", 
    "Raj Kumar", "Vikram Bansal", "Mohit Yadav", "Aman Saini", "Akash Kumar", "Nikhil Mehta", 
    "Manoj Sharma", "Saurabh Sharma", "Pranav Patel", "Ankur Jain", "Deepak Yadav", "Aakash Soni", 
    "Yash Bhatia", "Shyam Singh", "Rajesh Gupta", "Sandeep Sharma", "Anil Kumar", "Gaurav Kapoor", 
    "Vivek Reddy", "Sunny Patel", "Tarun Agarwal", "Ajeet Verma", "Raghav Sharma", "Ashwin Rao", 
    "Naveen Gupta", "Vikrant Singh", "Sumit Kumar", "Harsh Sharma", "Anand Patel", "Siddhi Jain", 
    "Tanmay Agarwal", "Parth Reddy", "Aditya Sharma", "Ranjit Mehta", "Vijay Yadav", "Kunal Kapoor", 
    "Shivam Patel", "Keshav Sharma", "Ashish Kumar", "Anuj Jain", "Sanjay Reddy", "Jai Gupta", 
    "Siddhant Verma", "Gagan Kumar", "Suraj Patel", "Ritesh Singh", "Dhruv Sharma", "Vishal Agarwal", 
    "Hitesh Kumar", "Jitendra Sharma", "Amitabh Kapoor", "Rajeev Patel", "Rishabh Yadav", "Vikash Jain", 
    "Vishnu Reddy", "Nitin Kumar", "Manish Gupta", "Raman Sharma", "Sanjiv Patel", "Kapil Mehta", 
    "Gopal Kumar", "Sandeep Jain", "Vikas Yadav", "Shubham Reddy", "Gaurav Sharma", "Pankaj Gupta", 
    "Nitin Patel", "Himanshu Kumar", "Praveen Sharma", "Raghav Patel", "Rohit Yadav", "Varun Gupta", 
    "Tushar Sharma", "Nitin Reddy", "Ishwar Kumar", "Rajiv Patel", "Kishore Sharma", "Arvind Gupta", 
    "Rajat Sharma", "Mohan Patel", "Harshad Kumar", "Umesh Gupta"
]


def generate_email(product_names):
    suffix = product_names.replace(" ", "").replace("&", "").replace(",", "").lower() + ".com"
    return f"{fake.user_name()}@{suffix}"
company_email = [generate_email(random.choice(product_names)) for _ in range(num_rows)]


# Predefined Indian states (for generating state names)
indian_states = [
    'Andhra Pradesh', 'Arunachal Pradesh', 'Assam', 'Bihar', 'Chhattisgarh', 'Goa',
    'Gujarat', 'Haryana', 'Himachal Pradesh', 'Jharkhand', 'Karnataka', 'Kerala',
    'Madhya Pradesh', 'Maharashtra', 'Manipur', 'Meghalaya', 'Mizoram', 'Nagaland',
    'Odisha', 'Punjab', 'Rajasthan', 'Sikkim', 'Tamil Nadu', 'Telangana',
    'Tripura', 'Uttar Pradesh', 'Uttarakhand', 'West Bengal'
]

received_list = ["Star Health & Allied","Go Digit","Acko General","Zuno General","Aditya Birla Sun","Cholamandalam MS","ManipalCigna","NAVI","Magma HDI","Aditya Birla"]

hospital_name  = ["Apollo Hospitals", "Fortis Healthcare", "Max Healthcare", "Medanta - The Medicity", "Manipal Hospitals", "Columbia Asia Hospital", "Narayana Health", "Jaypee Hospital", "Hiranandani Hospital", "Kokilaben Dhirubhai Ambani Hospital", "Shree Harsha Hospital", "Lilavati Hospital", "Wockhardt Hospitals", "P.D. Hinduja Hospital", "Global Hospitals", "Sterling Hospitals", "CIMS Hospital", "Breach Candy Hospital", "Sahara Hospital", "Medicover Hospitals"]
remarkList = ['Claim Passed', 'For Processing', 'Medical Scrutiny', 'Main Claim Pending', 'IR Docs Updated', 'Refer to Insurer – R', '']

# COMMAND ----------

# MAGIC %md
# MAGIC # ClaimsServiceDetails

# COMMAND ----------

row_num = 1000
ClaimsServiceDetails_spec = (dg.DataGenerator(spark,name='Claims_Service_Details', seedColumnName='baseId', rows=row_num)
           .withColumn("Id", LongType(), minValue=85857022, maxValue=85858022, random=True)
           .withColumn("Claimid", LongType(), minValue=24070402000, maxValue=24070403000, random=True)
           .withColumn("Slno", IntegerType(), minValue=1, maxValue=5, random=True)
           .withColumn("ServiceID", IntegerType(), minValue=1, maxValue=50, random=True)
           .withColumn("BillAmount", IntegerType(), minValue=9202, maxValue=100000, random=True)
           .withColumn("DeductionAmount", IntegerType(), expr="BillAmount * 0.1")
           .withColumn("DiscountAmount", IntegerType(), values=[0])
           .withColumn("EligibleAmount", IntegerType(), expr="BillAmount - DeductionAmount - DiscountAmount")
           .withColumn("AdditionalAmount", IntegerType(), minValue=0, maxValue=200, random=True)
           .withColumn("AdditionalAmtReasonIDs", StringType(), values=["null"])
           .withColumn("SanctionedAmount", IntegerType(), expr="EligibleAmount")
           .withColumn("CoPayment", IntegerType(), minValue=0, maxValue=100, random=True)
           .withColumn("Remarks", StringType(), expr="'No remarks'")
           .withColumn("Deleted", IntegerType(), values=[0,1], weights=[0.9, 0.1], random=True)
           .withColumn("CreatedUserRegionID", IntegerType(), minValue=1, maxValue=10, random=True)
           .withColumn("Createddatetime", TimestampType(), expr="current_timestamp()")
           .withColumn("DeletedUserRegionID", StringType(), expr='null')
           .withColumn("DeletedDatetime", TimestampType(), expr="null")
           .withColumn("MIG_ServiceID", IntegerType(), minValue=1, maxValue=150, random=True))

ClaimsServiceDetails = ClaimsServiceDetails_spec.build()
display(ClaimsServiceDetails)

# COMMAND ----------

# MAGIC %md
# MAGIC # Claims Details

# COMMAND ----------

Claimsdetails_spec = (dg.DataGenerator(spark, name="Claimsdetails", seedColumnName='baseId', rows=row_num)
                      .withColumn("ID", IntegerType(), minValue=11380305, maxValue=11381305, random=True)
                      .withColumn("ClaimID", LongType(), minValue=24070402000, maxValue=24070403000, random=True)
                      .withColumn("Slno", IntegerType(), minValue=1, maxValue=5, random=True)
                      .withColumn("MainRefSlno", IntegerType(), expr='null')
                      .withColumn("RequestTypeID", IntegerType(), values=request_types, random=True)
                      .withColumn("ClaimTypeID", IntegerType(), minValue=1, maxValue=50, random=True)
                      .withColumn("StageID", IntegerType(), minValue=21, maxValue=27, random=True)
                      .withColumn("ReceivedDate", TimestampType(), expr="current_timestamp()")
                      .withColumn("isFinal", IntegerType(), values=[0])
                      .withColumn("Diagnosis", StringType(), expr="'Cholelithiasis-Laparoscopic cholecystectomy'", percentNulls=0.8, random=True)
                      .withColumn("ReqFacilityID", IntegerType(), minValue=1, maxValue=100, percentNulls=0.5, random=True)
                      .withColumn("ReqOtherAccm", StringType(), expr='null')
                      .withColumn("ApprovedFacilityID", IntegerType(), minValue=205, maxValue=214, random=True)
                      .withColumn("EstimatedDays", IntegerType(), minValue=1, maxValue=30, percentNulls=0.5, random=True)
                      .withColumn("EstimatedCost", IntegerType(), expr='null')
                      .withColumn("ReqBillingType_P51", IntegerType(), minValue=1, maxValue=3, percentNulls=0.9, random=True)
                      .withColumn("BillingType_P51", IntegerType(), minValue=1, maxValue=3, percentNulls=0.9, random=True)
                      .withColumn("BillAmount", IntegerType(), minValue=1000, maxValue=50000, random=True)
                      .withColumn("PackageAmount", IntegerType(), minValue=1000, maxValue=2000, percentNulls=0.5, random=True)
                      .withColumn("TariffValue", IntegerType(), minValue=1000, maxValue=2000, percentNulls=0.5, random=True)
                      .withColumn("PackageLimit", IntegerType(), minValue=1000, maxValue=2000, percentNulls=0.5, random=True)
                      .withColumn("BPCoverageLimit", IntegerType(), minValue=1000, maxValue=2000, percentNulls=0.5, random=True)
                      .withColumn("DeductionAmount", IntegerType(), minValue=1000, maxValue=2000, percentNulls=0.5, random=True)
                      .withColumn("EligibleBillAmount", IntegerType(), minValue=1000, maxValue=2000, percentNulls=0.5, random=True)
                      .withColumn("MOUDiscount", IntegerType(), minValue=5, maxValue=80, percentNulls=0.5, random=True)
                      .withColumn("DiscountByHospital", IntegerType(), minValue=5, maxValue=80, percentNulls=0.5, random=True)
                      .withColumn("EligibleAmount", IntegerType(), minValue=10000, maxValue=20000, percentNulls=0.5, random=True)
                      .withColumn("CoPayment", IntegerType(), minValue=0.0, maxValue=100, percentNulls=0.5, random=True)
                      .withColumn("Deductible", IntegerType(), minValue=1000, maxValue=2000, percentNulls=0.5, random=True)
                      .withColumn("NetEligibleAmount", IntegerType(), minValue=1000, maxValue=2000, percentNulls=0.5, random=True)
                      .withColumn("PaidByPatient", IntegerType(), minValue=1000, maxValue=2000, percentNulls=0.5, random=True)
                      .withColumn("ExcessPaidByPatient", IntegerType(), minValue=1000, maxValue=2000, percentNulls=0.5, random=True)
                      .withColumn("Excess_PreAuth", IntegerType(), minValue=1000, maxValue=2000, percentNulls=0.5, random=True)
                      .withColumn("Excess_SI", IntegerType(), minValue=1000, maxValue=2000, percentNulls=0.5, random=True)
                      .withColumn("AdmissibleAmount", IntegerType(), minValue=1000, maxValue=2000, percentNulls=0.5, random=True)
                      .withColumn("PayableAmount", IntegerType(), minValue=1000, maxValue=2000, percentNulls=0.5, random=True)
                      .withColumn("NegotiatedAmount", IntegerType(), minValue=1000, maxValue=2000, percentNulls=0.5, random=True)
                      .withColumn("SanctionedAmount", IntegerType(), minValue=1000, maxValue=2000, percentNulls=0.5, random=True)
                      .withColumn("SettledAmount", IntegerType(), minValue=1000, maxValue=2000, percentNulls=0.5, random=True)
                      .withColumn("TDSAmount", IntegerType(), minValue=1000, maxValue=2000, percentNulls=0.5, random=True)
                      .withColumn("NetAmount", IntegerType(), minValue=1000, maxValue=2000, percentNulls=0.5, random=True)
                      .withColumn("PayeeName", StringType(), values=[faker.name() for _ in range(row_num)], random=True)
                      .withColumn("ModeOfPaymentID", IntegerType(), minValue=1, maxValue=row_num, random=True)
                      .withColumn("BankTransactionNo", StringType(), expr="concat('TRX', string(ID))")
                      .withColumn("ChequeDate", TimestampType(), expr="current_timestamp() - interval 7 days")
                      .withColumn("SettledDate", TimestampType(), expr="current_timestamp()")
                      .withColumn("BankAccountNo", StringType(), expr="concat('BANK', string(id))")
                      .withColumn("BankName", StringType(), expr="'Bank Name'")
                      .withColumn("BranchName", StringType(), expr="'Branch Name'")
                      .withColumn("IFSCCode", StringType(), expr="'IFSC0000'")
                      .withColumn("MICRNo", StringType(), expr="'MICR0000'")
                      .withColumn("CoPaymentHTML", StringType(), values=['null'])
                      .withColumn("ServiceTax", IntegerType(), minValue=0, maxValue=500, random=True)
                      .withColumn("ServiceTaxClaimed", IntegerType(), expr="ServiceTax")
                      .withColumn("Notes", StringType(), expr="null")
                      .withColumn("Remarks", StringType(), expr="'null'")
                      .withColumn("Reason", StringType(), expr="'null'")
                      .withColumn("isReopen", IntegerType(), values=[0])
                      .withColumn("ReopenReason_P53", StringType(), expr="'null'")
                      .withColumn("CourtCaseID", IntegerType(), minValue=1, maxValue=100, percentNulls=0.5, random=True)
                      .withColumn("DurationOfAilment", IntegerType(), minValue=1, maxValue=30, percentNulls=0.5, random=True)
                      .withColumn("AilmentType_P18", IntegerType(), minValue=1, maxValue=10, random=True)
                      .withColumn("ICD10Code", StringType(), expr="'null'")
                      .withColumn("PCSCode", StringType(), expr="'null'")
                      .withColumn("IsPackage", BooleanType(), expr="'false'")
                      .withColumn("SurgeryDate", TimestampType(), expr="current_timestamp()")
                      .withColumn("BedNo", IntegerType(), minValue=1, maxValue=500, random=True)
                      .withColumn("TreatmentTypeID_P19", IntegerType(), minValue=1, maxValue=5, random=True)
                      .withColumn("PlanOfTreatment", StringType(), expr="'null'")
                      .withColumn("PresentComplaint", StringType(), expr="'null'")
                      .withColumn("PresentAilmentHistory", StringType(), expr="'null'")
                      .withColumn("DrugAdministration", StringType(), expr="'null'")
                      .withColumn("TypeOfAnesthesiaID", IntegerType(), minValue=1, maxValue=5, percentNulls=0.5, random=True)
                      .withColumn("BP", StringType(), expr="null")
                      .withColumn("PR", StringType(), expr="null")
                      .withColumn("LMP", StringType(), expr="null")
                      .withColumn("Temperature", StringType(), expr="null")
                      .withColumn("RS", StringType(), expr="null")
                      .withColumn("CVS", StringType(), expr="'null'")
                      .withColumn("PorA", StringType(), expr="'null'")
                      .withColumn("Others", StringType(), expr="'null'")
                      .withColumn("InvestigationResults", StringType(), expr="'null'")
                      .withColumn("ExecutiveNotes", StringType(), values=['KYC success, no medical condition disclosed'], percentNulls=0.6, random=True)
                      .withColumn("MillimanConditionID", IntegerType(), minValue=1, maxValue=5, percentNulls=0.5, random=True)
                      .withColumn("SeverityID", IntegerType(), minValue=1, maxValue=5, percentNulls=0.5, random=True)
                      .withColumn("Deleted", BooleanType(), expr="False")
                      .withColumn("CreatedUserRegionID", IntegerType(), minValue=1, maxValue=10, random=True)
                      .withColumn("CreatedDatetime", TimestampType(), expr="current_timestamp()")
                      .withColumn("ModifiedUserRegionID", IntegerType(), minValue=1, maxValue=100, percentNulls=0.5, random=True)
                      .withColumn("ModifiedDatetime", TimestampType(), expr="null")
                      .withColumn("DeletedUserRegionID", IntegerType(), expr='null')
                      .withColumn("DeletedDatetime", TimestampType(), expr='null')
                      .withColumn("MIG_Slno", IntegerType(), minValue=1, maxValue=100, percentNulls=0.5, random=True)
                      .withColumn("BillingCorrection", BooleanType(), expr="null")
                      .withColumn("DoctorNotes", StringType(), expr="null")
                      .withColumn("AdditionalRemarks", StringType(), expr="null")
                      .withColumn("InsurerClaimID", IntegerType(), expr="null")
                      .withColumn("ClaimAmount", IntegerType(), minValue=645, maxValue=50000, random=True)
                      .withColumn("MIG_AuthorizedDate", TimestampType(), expr="current_timestamp() - interval 3 days")
                      .withColumn("MIG_PreauthFirstResponseDate", TimestampType(), expr="null")
                      .withColumn("NottoDeductfromHospital", BooleanType(), expr="False")
                      .withColumn("IsNeftBounced", BooleanType(), expr="null")
                      .withColumn("RejectedPreauthID", IntegerType(), expr='null')
                      .withColumn("IsBufferUtilized", BooleanType(), expr="null")
                      .withColumn("IsFacilityChanged", BooleanType(), expr="null")
                      .withColumn("IsAutoGenerated", BooleanType(), expr="null")
                      .withColumn("EPDAmount", IntegerType(), expr='null')
                      .withColumn("SkipScrutiny", BooleanType(), expr="null")
                      .withColumn("physicalDoc", BooleanType(), expr="False")
                      .withColumn("IsAutofillby_i3", BooleanType(), expr="null")
                      .withColumn("IsCovid", BooleanType(), expr="False")
                      .withColumn("NavigateRegionID", IntegerType(), values=[1])
                      .withColumn("IsClaimForClosure", BooleanType(), expr="False")
                      .withColumn("IsRecalculated", BooleanType(), expr="null")
                      .withColumn("bufferwithoutbase", StringType(), expr='null')
                      .withColumn("MemberClaimDocsUploadRefID", IntegerType(), minValue=315807, maxValue=316807, random=True)
                      .withColumn("claimdiagnosis", StringType(), expr="null")
                      .withColumn("IsoutofSI", BooleanType(), expr="null")
                      .withColumn("PremiumDeducted", IntegerType(), expr="null")
                      .withColumn("GST", IntegerType(), expr="null")
                      .withColumn("IGST", IntegerType(), expr="null")
                      .withColumn("CGST", IntegerType(), expr="null")
                      .withColumn("SGST", IntegerType(), expr="null")
                      .withColumn("ClaimInwardReqID", IntegerType(), minValue=179432, maxValue=180432, random=True)
                      .withColumn("ParentClaimID", IntegerType(), expr="null")
                     )

Claimsdetails = Claimsdetails_spec.build()
display(Claimsdetails)

# COMMAND ----------

# MAGIC %md
# MAGIC # Claim Rejection Reasons

# COMMAND ----------

remarks_list = [
    "as per available documents admission is for diagnostic purpose hence this claim stands for rejection.",
    "on scrutiny of claim documents it is observed that, the patient hospitalization is less than 24 hours. Hence is DENY. as per policy condition. Patient DOA on 03.07.2024 at 23:04:54 pm and DOD on 04.07.2023 at 18:11:45 pm",
    "claim for investigation and evaluation. hence claim repudiated: under clause: 4.4.1 INVESTIGATION AND EVALUATION (code-Excl04)",
    "Cashless facility is denied, as the cashless falls within the 2 years waiting period.",
    "Generation error: claim registered towards cid- 24070402205-1, hence rejected"
]
ClaimRejectionReasons_spec = (
    dg.DataGenerator(spark, name='ClaimRejectionReasons', rows=row_num, seedColumnName='baseId')
    .withColumn("ID", IntegerType(), minValue=817162, maxValue=818162, random=True)
    .withColumn("ClaimID", LongType(), minValue=24070402000, maxValue=24070403000, random=True)
    .withColumn("Slno", IntegerType(), minValue=1, maxValue=5, random=True)
    .withColumn("RejectionReasonsID", IntegerType(), minValue=1,maxValue=10,random=True)
    .withColumn("FreeText1", StringType(), expr="null")
    .withColumn("FreeText2", StringType(), expr="null")
    .withColumn("Remarks", StringType(), values=remarks_list, random=True)
    .withColumn("ActionID", IntegerType(), minValue=95889822, maxValue=95890822, random=True)
    .withColumn("Deleted", BooleanType(), expr="False")
    .withColumn("CreatedUserRegionID", IntegerType(), minValue=1, maxValue=10, random=True)
    .withColumn("CreatedDatetime", TimestampType(), expr="current_timestamp()")
    .withColumn("DeletedUserRegionID", IntegerType(), expr='null')
    .withColumn("DeletedDatetime", TimestampType(), expr='null')
    .withColumn("RejectionCategory", IntegerType(), minValue=1, maxValue=99, random=True)
    .withColumn("RejectionSubCategory", IntegerType(), minValue=10, maxValue=99, random=True)
)
ClaimRejectionReasons = ClaimRejectionReasons_spec.build()
display(ClaimRejectionReasons)

# COMMAND ----------

# MAGIC %md
# MAGIC # Buffer Utilization

# COMMAND ----------

RequestRemarks_list = [
    "convert this into list buffer",
    "buffer request",
    "release buffer",
    "buffer",
    "Buffer amount"
]
BufferUtilization_spec = (
    dg.DataGenerator(spark, name='BufferUtilization', rows=row_num, seedColumnName='baseId')
    .withColumn("ID", IntegerType(), minValue=88986, maxValue=89986, random=True)
    .withColumn("MemberPolicyID", IntegerType(), minValue=77194598, maxValue=81286391, random=True)
    .withColumn("PolicyID", LongType(),minValue=4411218,maxValue=4412218, random=True)
    .withColumn("RefertoId", IntegerType(), minValue=200, maxValue=300, random=True)
    .withColumn("CorpID", IntegerType(), minValue=1, maxValue=100, random=True)
    .withColumn("ClaimID", LongType(), minValue=24070402000, maxValue=24070403000, random=True)
    .withColumn("Slno", IntegerType(), minValue=1, maxValue=5, random=True)
    .withColumn("TPAProcID", IntegerType(), minValue=4, maxValue=1147, random=True)
    .withColumn("EligibleAmount", IntegerType(), minValue=3102, maxValue=400000, random=True)
    .withColumn("RuleID", IntegerType(), minValue=2490819, maxValue=3223208, random=True)
    .withColumn("SICategoryID_P20", IntegerType(), values=[69,70], random=True)
    .withColumn("RequestedAmount", IntegerType(), minValue=3102, maxValue=409695, random=True)
    .withColumn("RequestedBy", IntegerType(), minValue=3882, maxValue=9025, random=True)
    .withColumn("RequestedDatetime", TimestampType(), expr="current_timestamp()")
    .withColumn("RequestRemarks", StringType(), values=RequestRemarks_list, random=True)
    .withColumn("ApprovedAmount", IntegerType(), minValue=3102, maxValue=400000, random=True)
    .withColumn("ApprovedBy", IntegerType(), minValue=8983, maxValue=11784, random=True)
    .withColumn("ApprovedDatetime", TimestampType(), expr="current_timestamp()")
    .withColumn("ApprovalRemarks", StringType(), values=RequestRemarks_list, percentNulls=0.3, random=True)
    .withColumn("ApprovalRecievedBy", StringType(), expr="null")
    .withColumn("UtilizedAmount", IntegerType(), minValue=64081, maxValue=400000, random=True)
    .withColumn("UtilizedDatetime", TimestampType(), expr="current_timestamp()")
    .withColumn("DMSIds", StringType(), expr="null")
    .withColumn("Deleted", BooleanType(), expr="False")
    .withColumn("CreatedUserRegionID", IntegerType(), minValue=1, maxValue=10, random=True)
    .withColumn("CreatedDatetime", TimestampType(), expr="current_timestamp()")
    .withColumn("ModifiedUserRegionID", IntegerType(), minValue=5979, maxValue=10427, percentNulls=0.3, random=True)
    .withColumn("ModifiedDatetime", TimestampType(), expr="current_timestamp()")
)

BufferUtilization = BufferUtilization_spec.build()
display(BufferUtilization)

# COMMAND ----------

# MAGIC %md
# MAGIC # Mst_Issuingauthority

# COMMAND ----------

mst_row_num = 100

code_list = ["SHAI","GDGI","AGIL","ZUNO","ABSLI","CMGICL","MCHIC","NAVI","MHDI","ABHI"]

short_names_list = [
    "Star Health & Allied",
    "Go Digit",
    "Acko General",
    "Zuno General",
    "Aditya Birla Sun",
    "Cholamandalam MS",
    "ManipalCigna",
    "NAVI",
    "Magma HDI",
    "Aditya Birla"
]

names_list = [
    "Star Health & Allied Insurance",
    "Go Digit General Insurance Company Limited",
    "Acko General Insurance Limited",
    "Zuno General Insurance Company Ltd",
    "Aditya Birla Sun Life Insurance Company Ltd",
    "Cholamandalam MS General Insurance Company Limited",
    "ManipalCigna Health Insurance Company",
    "NAVI General Insurance Company",
    "Magma HDI General Insurance Company Ltd",
    "Aditya Birla Health Insurance Company Ltd"
]
hierarchy_list = [
    "HO,RO",
    "HO",
    "HO,RO",
    "HO,RO",
    "HO,RO",
    "HO,RO",
    "HO,RO,DO,BO,SO,BC_Centre",
    "HO,RO,DO,BO",
    "HO,RO,DO,BO,SO,BC_Centre",
    "HO,RO,DO,BO"
]
remarks_list = [
    "Insurer created as per mail from Dr. Shashidhar dated 17/08/2021.", 
    "UIN No. ACKTGDP18115V021718",
    "Name Modified vide mail from Insurer dated 24/05/2019, Updated on 27/05/2019. Existing Name : CignaTTK Health Insurance Company Ltd."
]

Mst_Issuingauthority_spec = (
    dg.DataGenerator(spark, name='Mst_Issuingauthority', rows=mst_row_num, seedColumnName='baseId')
    .withColumn("ID", IntegerType(), minValue=1, maxValue=100)
    .withColumn("Code", StringType(), values=code_list, random=True)
    .withColumn("Deleted", BooleanType(), expr="False")
    .withColumn("ShortName", StringType(), values=short_names_list)
    .withColumn("Name", StringType(), values=names_list)
    .withColumn("InsurerTypeID", IntegerType(), minValue=1, maxValue=10, random=True)
    .withColumn("AutoGenUHID", BooleanType(), expr ="rand() > 300")
    .withColumn("CBPercentage", IntegerType(), expr='null')
    .withColumn("CBDeduction", IntegerType(), expr='null')
    .withColumn("CBClaimFreeYears", IntegerType(), expr='null')
    .withColumn("CBMaxPercentage", IntegerType(), expr='null')
    .withColumn("EffectiveDate", TimestampType(), expr="current_timestamp()")
    .withColumn("StatusID", IntegerType(), expr='null')
    .withColumn("Hierarchy", StringType(), values=hierarchy_list, random=True)
    .withColumn("Remarks", StringType(), values=remarks_list, percentNulls=0.4, random=True)
    .withColumn("WithInSI", BooleanType(), expr="False", percentNulls=0.8, random=True)
    .withColumn("LinkText", StringType(), values=['Aditya Birla'], percentNulls=0.8, random=True)
    .withColumn("TollFreeNo", IntegerType(), values=[faker.phone_number() for _ in range(mst_row_num)])
    .withColumn("CustomerCareMailID", StringType(), values=['care@fhpl.net', 'info@fhpl.net'], percentNulls=0.5, random=True)
    .withColumn("IRTAT", IntegerType(), minValue=1, maxValue=50, random=True)
    .withColumn("Rem1TAT", IntegerType(), minValue=1, maxValue=50, random=True)
    .withColumn("Rem2TAT", IntegerType(), minValue=1, maxValue=50, random=True)
    .withColumn("TPAFees", IntegerType(), expr='null')
    .withColumn("Createddatetime", TimestampType(), expr="current_timestamp()")
    .withColumn("ModifiedDatetime", TimestampType(), expr="current_timestamp()", percentNulls=0.4, random=True)
    .withColumn("FHPLTollFreeNo", IntegerType(), values=['18004254033'], percentNulls=0.4, random=True)
    .withColumn("FHPLEmail", StringType(), values=['info@fhpl.net'], percentNulls=0.4, random=True)
    .withColumn("ZoneValues", StringType(), expr='null')
    .withColumn("ModifiedBy", IntegerType(), minValue=11, maxValue=10503, percentNulls=0.3, random=True)
)

Mst_Issuingauthority = Mst_Issuingauthority_spec.build()
display(Mst_Issuingauthority)

# COMMAND ----------

# MAGIC %md
# MAGIC # Mst_Gender

# COMMAND ----------

Mst_Gender_spec = (dg.DataGenerator(spark, name='Mst_Gender', rows=4, seedColumnName='baseId')
                   .withColumn("ID", IntegerType(), minValue=1, maxValue=4)
                   .withColumn("Name", StringType(), values=["Male", "Female", "UnKnown", "TransGender"])
                   .withColumn("Deleted", IntegerType(), values=[0])
                   )

Mst_Gender = Mst_Gender_spec.build()
display(Mst_Gender)

# COMMAND ----------

# MAGIC %md
# MAGIC # Mst_Product

# COMMAND ----------

names_list = [
    "Test",
    "Test1",
    "Kotak Health Care",
    "Kotak Health Care - Excel",
    "Kotak Health Care - Premium",
    "Medi Prime",
    "MediPlus",
    "MediSenior",
    "Medi Raksha",
    "Individual Tailormade Product"]

Mst_Product_spec = (
    dg.DataGenerator(spark, name='Mst_Product', rows=mst_row_num, seedColumnName='baseId')
    .withColumn("ID", IntegerType(), minValue=1, maxValue=1000)
    .withColumn("Code", StringType(), minValue=2876, maxValue=2900, random=True)
    .withColumn("Name", StringType(), values=names_list, random=True)
    .withColumn("IssueID", IntegerType(), minValue=1, maxValue=100, random=True)
    .withColumn("PolicyTypeID", IntegerType(), minValue=4852805, maxValue=4853805, random=True)
    .withColumn("EffectiveDate", TimestampType(), expr="current_timestamp()")
    .withColumn("Deleted", IntegerType(), values=[0, 1], weights=[0.8, 0.2], random=True)
    .withColumn("CreatedUserRegionID", IntegerType(), minValue=1, maxValue=10, random=True)
    .withColumn("CreatedDate", TimestampType(), expr="current_timestamp()")
    .withColumn("BenefitplanID", IntegerType(), minValue=1, maxValue=1000, random=True)
    .withColumn("UIN", StringType(), expr="null")
)

Mst_Product = Mst_Product_spec.build()
display(Mst_Product)

# COMMAND ----------

# MAGIC %md
# MAGIC # Mst_Regions

# COMMAND ----------

cities_list = [
    "Hyderabad",
    "Bangalore",
    "Chennai",
    "Delhi",
    "Mumbai",
    "Kolkata",
    "Ahmedabad",
    "Coimbatore",
    "Bhopal",
    "Indore",
    "Bhubaneswar",
    "Jaipur",
    "Chandigarh",
    "Kochi",
    "Lucknow",
    "Vishakapatnam",
    "Nagpur",
    "Pune",
    "Vijayawada",
    "Madurai",
    "Trivandrum",
    "Guwahati",
    "Goa",
    "Solapur",
    "Vadodara",
    "Mohali"
]
Prov_ProcessBranch_list = [1, 2, 3]

Mst_Regions_spec = (
    dg.DataGenerator(spark, name='Mst_Regions', rows=26, seedColumnName='baseId')
    .withColumn("ID", IntegerType(), minValue=1, maxValue=row_num)
    .withColumn("Name", StringType(), values=cities_list)
    .withColumn("Prov_ProcessBranch", IntegerType(),values=Prov_ProcessBranch_list,random=True)
    .withColumn("Prov_SkipStageID", IntegerType(), expr="null")
    .withColumn("tempname", StringType(), values=[faker.first_name() for _ in range(row_num)], omit=True)
    .withColumn("Prov_CommEmailds", StringType(), expr="concat(tempname, '@fhpl.net')")
    .withColumn("Address", StringType(), values=[faker.address() for _ in range(row_num)])
    .withColumn("City", StringType(), values=[faker.city() for _ in range(row_num)])
    .withColumn("StateID", IntegerType(), minValue=101, maxValue=110, random=True)
    .withColumn("CountryID", IntegerType(), values=[1])
    .withColumn("Pincode", StringType(), values=[faker.postcode() for _ in range(row_num)])
    .withColumn("ContactNo", StringType(), values=[faker.phone_number() for _ in range(row_num)])
    .withColumn("fax", StringType(), expr="null")
    .withColumn("Deleted", IntegerType(), values=[0])
)

Mst_Regions = Mst_Regions_spec.build()
display(Mst_Regions)

# COMMAND ----------

# MAGIC %md
# MAGIC # Lnk_UserRegions

# COMMAND ----------

Lnk_UserRegions_spec = (
    dg.DataGenerator(spark, name='Lnk_UserRegions', rows=row_num, seedColumnName='baseId')
    .withColumn("ID", IntegerType(), minValue=1, maxValue=row_num)
    .withColumn("UserID", IntegerType(), minValue=1, maxValue=row_num, random=True)
    .withColumn("RegionID", IntegerType(), minValue=1, maxValue=26, random=True)
    .withColumn("Deleted", IntegerType(), values=[0])
)

Lnk_UserRegions = Lnk_UserRegions_spec.build()
display(Lnk_UserRegions)

# COMMAND ----------

# MAGIC %md
# MAGIC #  Mst_pcs																																																																																																																																																																																																																							
# MAGIC

# COMMAND ----------

categories = [
    "Medical and Surgical",
    "Obstetrics",
    "Placement",
    "Administration",
    "Measurement and Monitoring",
    "Extracorporeal Assistance and Performance",
    "Extracorporeal Therapies",
    "Osteopathic",
    "Other Procedures",
    "Chiropractic"
]

Mst_pcs_spec = (
    dg.DataGenerator(spark, name='Mst_pcs', rows=mst_row_num, seedColumnName='baseId')
    .withColumn("ID", IntegerType(), minValue=1, maxValue=row_num)
    .withColumn("Name", StringType(), values=categories, random=True)
    .withColumn("Code", IntegerType(), minValue=0, maxValue=row_num-1, random=True)
    .withColumn("ParentID", IntegerType(), minValue=0, maxValue=10, random=True)
    .withColumn("Level", IntegerType(), values=[1], random=True)
    .withColumn("Deleted", IntegerType(), values=[0])
)

Mst_pcs = Mst_pcs_spec.build()
display(Mst_pcs)

# COMMAND ----------

# MAGIC %md
# MAGIC # Mst_AdmissionType

# COMMAND ----------

Mst_AdmissionType_spec = (
    dg.DataGenerator(spark, name='Mst_AdmissionType', rows=2, seedColumnName='baseId')
    .withColumn("ID", IntegerType(), minValue=1, maxValue=2)
    .withColumn("Name", StringType(), values=['Planned', 'Emergency'])
    .withColumn("Deleted", IntegerType(), values=[0])
    .withColumn("Createddatetime", TimestampType(), expr="current_timestamp()")
)

Mst_AdmissionType = Mst_AdmissionType_spec.build()
display(Mst_AdmissionType)

# COMMAND ----------

# MAGIC %md
# MAGIC # Mst_IRDocument

# COMMAND ----------

categories_list = [
    "General",
    "Hospital Facilities",
    "General",
    "Proof of Identity",
    "TDC",
    "Prescriptions & investigations",
    "Treating Doctor certificate",
    "Treating Doctor certificate",
    "Treating Doctor certificate",
    "Discharge Summary"
]
name_list = [
    "Clarification regarding delay in submission of claim.",
    "Letter from the hospital regarding the Bed capacity, Registration details, and facilities available in the hospital.",
    "Letter from hospital regarding whether cashless facility wide PA No.|FT| authorized for this hospitalization utilized or not.",
    "Age clarification from the hospital / Insurer. As per the policy |FT| yrs, but as per the hospital record  |FT1| yrs.",
    "Treating doctors certificate for detailed case history with progression, duration and past history of the present ailment.",
    "Related prescriptions and investigation reports for the enclosed bills.",
    "Treating doctors certificate for the Obstetrics history (GPLA) details.",
    "Treating doctors certificate for details & circumstances of injury with any alcohol/drugs influence details at the time of injury, MLC copy if applicable.",
    "Treating doctors certificate for Past history duration of DM, HTN, CAD / When diagnosed for the 1st time.",
    "Original detailed discharge summary on hospital letterhead."
]

is_mandatory_list = [1, 1, 1, 1, 1, 0, 1, 1, 1, 1]
is_having_free_text_list = [0, 0, 1, 1, 0, 0, 0, 0, 0, 0]

Mst_IRDocuments_spec = (
    dg.DataGenerator(spark, name='Mst_IRDocuments', rows=mst_row_num, seedColumnName='baseId')
    .withColumn("ID", IntegerType(), minValue=1, maxValue=35)
    .withColumn("CategoryName", StringType(), values=categories_list,)
    .withColumn("Name", StringType(), values=name_list)
    .withColumn("IsHavingFreeText", IntegerType(), values=is_having_free_text_list)
    .withColumn("Deleted", IntegerType(), values=[0])
    .withColumn("IsMandatory", IntegerType(), values=is_mandatory_list)
    .withColumn("ExecScrutinyDisplay", IntegerType(), values=[0])
)

Mst_IRDocuments = Mst_IRDocuments_spec.build()
display(Mst_IRDocuments)

# COMMAND ----------

# MAGIC %md
# MAGIC # Mst_DeductionReasons

# COMMAND ----------

Name_list = [
    "GENERAL",
    "Exceeding the limit as per policy conditions |FT|",
    "Restricted to agreed tariff",
    "Found to be unreasonably high",
    "MEDICINES",
    "Not payable |FT|",
    "No prescription |FT|",
    "Medicines not related to ailment",
    "Medicines pertaining to |FT| disallowed",
    "Not within post hospitalization period as per policy"
]

is_having_free_text_list = [0, 1, 0, 0, 0, 1, 0, 0, 1, 0]
isNOP_list = [1, 1, 0, 0, 1, 1, 0, 1, 1, 1]

Mst_DeductionReasons_spec = (
    dg.DataGenerator(spark, name='Mst_DeductionReasons', rows=mst_row_num, seedColumnName='baseId')
    .withColumn("ID", IntegerType(), minValue=1, maxValue=mst_row_num)
    .withColumn("Name", StringType(), values=Name_list)
    .withColumn("ParentID", IntegerType(), minValue=0, maxValue=5, random=True)
    .withColumn("IsHavingFreeText", IntegerType(), values=is_having_free_text_list)
    .withColumn("Deleted", IntegerType(), values=[0])
    .withColumn("isNOP", IntegerType(), values=isNOP_list)
)

Mst_DeductionReasons = Mst_DeductionReasons_spec.build()
display(Mst_DeductionReasons)

# COMMAND ----------

# MAGIC %md
# MAGIC # MST_Agent

# COMMAND ----------

agent_name_list = [
    "Counter - Air Sonic Travel Pvt Ltd",
    "Pratik Naik",
    "Samir Kishore Ganatra",
    "Bela Shah",
    "Yogesh Joshi",
    "Subodh Khandelwal",
    "Akhil V Khanna",
    "Anandan D",
    "Ranjan Kumar",
    "Harpreet Singh"
]

agent_code_list = [
    10079018,
    7000,
    19000,
    8000,
    13000,
    14000,
    18000,
    29000,
    58000,
    61000
]

issue_id_list = [30, 20, 0, 8, 2, 5, 6, 19]

Mst_Agent_spec = (
    dg.DataGenerator(spark, name='Mst_Agent', rows=mst_row_num, seedColumnName='baseId')
    .withColumn("ID", IntegerType(), minValue=1, maxValue=mst_row_num)
    .withColumn("Name", StringType(), values=agent_name_list)
    .withColumn("Code", StringType(), values=agent_code_list)
    .withColumn("Deleted", IntegerType(), values=[0])
    .withColumn("Password", StringType(), values=["null"])
    .withColumn("IssueID", IntegerType(), values=issue_id_list)
    .withColumn("IsAgentsuspicious", IntegerType(), expr="NULL")
)

Mst_Agent = Mst_Agent_spec.build()
display(Mst_Agent)

# COMMAND ----------

# MAGIC %md
# MAGIC #Mst_RejectionReasons																																																																																																																																																																																																																		
# MAGIC

# COMMAND ----------

short_text_list = [
    "Pre-Existing Disease",
    "30 Days Waiting Period",
    "First 2 Years Exclusion",
    "Treatment Before Inception Date/ New Commencement Date",
    "Person Not Insured",
    "Out-Patient Treatment Sufficient",
    "Congenital Internal Or External Diseases, Defects or Anomalies",
    "Insufficient Sum Insured",
    "Treatment by not authorized medical practitioner",
    "Treatment Not In Line With Authorization"
]

name_list = [
    "The submitted claim is for the treatment of a disease/symptoms which has been started before the start of the policy with us. As per the terms and condition of the policy any pre-existing condition will not be covered until 48 months of continuous coverage with us. The disease/symptom started on |FT| and the policy start date is ~PID~. Hence we regret to inform you that your claim is repudiated under Section 3) clause d) of the policy.",
    "The submitted claim falls under first 30 days waiting period clause of policy. Hence we regret to inform you that your claim is repudiated under Section 3) clause B ) of the policy.",
    "The submitted claim is for the illness which has a specific two years of waiting period as per the policy and the policy start date is ~PID~. Hence we regret to inform you that your claim is repudiated under Section 3)c of the policy.",
    "As per the submitted documents, the treatment for this ailment is prior to the policy inception date. Hence we regret to inform you that the claim is repudiated.",
    "The submitted claim is for the treatment of a person who is not insured with us. Hence we regret to inform you that the claim is repudiated. Please submit us a copy of the TATA AIG Card and / or copy of the policy of the treated person (if insured) to proceed further.",
    "The submitted claim is only for investigation and evaluation of the ailment, which could have been done on outpatient basis without the necessity of admission. Hence we regret to inform that your claim is repudiated under Section 3e (xv) of the policy.",
    "As per the documents submitted, the insured was admitted for treatment of |FT| which falls under category of congenital defect/anomalies. Evaluation and Treatment related to a condition which is present since birth has been excluded in the policy. Hence we regret to inform you that your claim has been repudiated under clause 3.E. vi of the policy.",
    "The insured has utilized his/her sum insured during the previous treatment and there is no balance left to process this case. Hence we regret to inform you that present claim can not be processed under Clause: Def 30.",
    "As per the submitted documents, the insured was treated by a doctor who does not fulfill the criteria of a medical practitioner defined in the policy. Hence we regret to inform you that the claim is repudiated under Clause: 3.E.XXI.",
    "As per the submitted documents, the treatment was not given as per the authorization and consent for the same was not taken before the discharge of the patient. Hence we regret to inform you that the claim is repudiated."
]

is_having_free_text_list = [1,0,0,0,0,0,1,0,0,0]
issue_id_list = [30, 20, 0, 8, 2, 5, 6, 19]
clause_no_list = [
    "Section 3; Exclusions d)",
    "Section 3; Exclusions b)",
    "Section 3; Exclusions c)",
    "Section 5, Def 24",
    "Section 4, B.",
    "Section 3.E. XV.",
    "Section 3.E.VI",
    "Section 5, Def 30",
    "Section 3 E XXI",
    "NULL"
]
clause_wordings_list = [
    "Pre-existing Conditions will not be covered until 48 months of continuous coverage have elapsed, since inception of the first MediPrime Policy with Us; but: 1. If the Insured Person is presently covered and has been continuously covered without any break under: i. an individual health insurance plan with an Indian insurer for the reimbursement of medical costs for inpatient treatment in a Hospital, OR ii. any other similar health insurance plan from Us, then Section 3 d. of the Policy stands deleted and shall be replaced entirely with the following: i) The waiting period for all Pre-existing Conditions shall be reduced by the number of continuous preceding years of coverage of the Insured Person under the previous health insurance policy; ANDii) If the proposed Sum Insured for a proposed Insured Person is more than the Sum Insured applicable under the previous health insurance policy (other than as a result of the application of Benefit 2a), then the reduced waiting period shall only apply to the extent of the Sum Insured under the previous health insurance policy.",
    "We are not liable for any treatment which begins during waiting periods except if any Insured Person suffers an Accident. 30 days Waiting Periodb) A waiting period of 30 days will apply to all claims unless:i) The Insured Person has been insured under an MediPrime Policy continuously and without any break in the previous Policy Year, orii) The Insured Person was insured continuously and without interruption for at least 1 year under any other Indian insurer’s individual health insurance policy for the reimbursement of medical costs for inpatient treatment in a hospital, and he establishes to Our satisfaction that he was unaware of and had not taken any advice or medication for such Illness or treatment.iii) If the Insured person renews with Us or transfers from any other insurer and increases the Sum Insured (other than as a result of the application of Benefit 2a) upon renewal with Us), then this exclusion shall only apply in relation to the amount by which the Sum Insured has been increased.",
    "The Illnesses and treatments listed below will be covered subject to a waiting period of 2 years as long as in the third Policy Year the Insured Person has been insured under an MediPrime Policy continuously and without any break:i) Illnesses: arthritis if non infective; calculus diseases of gall bladder and urogenital system; cataract; fissure/fistula in anus, hemorrhoids, pilonidal sinus, gastric and duodenal ulcers; gout and rheumatism; internal tumors, cysts, nodules, polyps including breast lumps (each of any kind unless malignant); osteoarthritis and osteoporosis if age related; polycystic ovarian diseases; sinusitis and related disorders and skin tumors unless malignant.ii) Treatments: benign ear, nose and throat (ENT) disorders and surgeries (including but not limited to adenoidectomy, mastoidectomy, tonsillectomy and tympanoplasty); dilatation and curettage (D&C); hysterectomy for menorrhagia or fibromyoma or prolapse of uterus unless necessitated by malignancy; joint replacement; myomectomy for fibroids; surgery of gallbladder and bile duct unless necessitated by malignancy; surgery of genito urinary system unless necessitated by malignancy; surgery of benign prostatic hypertrophy; surgery of hernia; surgery of hydrocele; surgery for prolapsed inter vertebral disk; surgery of varicose veins and varicose ulcers; surgery on tonsils and sinuses; surgery for nasal septum deviation.iii) However, a waiting period of 2 years will not apply if the Insured Person was insured continuously and without interruption for at least 2 years under any other Indian insurer’s individual health insurance policy for the reimbursement of medical costs for inpatient treatment in a hospital.",
    "Policy Period means the period between the Commencement Date and the Expiry Date specified in the Schedule.",
    "Only those persons named as an Insured Person in the Schedule shall be covered under this Policy.",
    "Experimental, investigational or unproven treatment, devices and pharmacological regimens; measures primarily for diagnostic, X-ray or laboratory examinations or other diagnostic studies which are not consistent with or incidental to the diagnosis and treatment of the positive existence or presence of any illness for which confinement is required at a hospital.",
    "We will not make any payment for any claim in respect of any Insured Person directly or indirectly for, caused by, arising from or in any way attributable to any of the following unless expressly stated to the contrary in this Policy:vi) Psychiatric, mental disorders (including mental health treatments); Parkinson and Alzheimer’s disease; general debility or exhaustion (“run-down condition”); congenital internal or external diseases, defects or anomalies; genetic disorders; stem cell implantation or surgery; or growth hormone therapy; sleep-apnoea.",
    "Sum Insured means the sum shown in the Schedule which represents Our maximum liability for each Insured Person for any and all benefits claimed for during each Policy Year, and in relation to a Family Floater represents Our maximum liability for any and all claims made by You and all of Your Dependents during each Policy Year.",
    "We will not make any payment for any claim in respect of any Insured Person directly or indirectly for, caused by, arising from or in any way attributable to any of the following unless expressly stated to the contrary in this Policy:Treatment rendered by a Medical Practitioner which is outside his discipline or the discipline for which he is licensed; treatments rendered by a Medical Practitioner who shares the same residence as an Insured Person or who is a member of an Insured Person’s family, however proven material costs are eligible for reimbursement in accordance with the applicable cover.",
    "No such rejection clause is noted in the policy."
]

Mst_RejectionReasons_spec = (
    dg.DataGenerator(spark, name='Mst_RejectionReasons', rows=mst_row_num, seedColumnName='baseId')
    .withColumn("ID", IntegerType(), minValue=1, maxValue=mst_row_num)
    .withColumn("ShortText", StringType(), values=short_text_list)
    .withColumn("Name", StringType(), values=name_list)
    .withColumn("IsHavingFreeText", IntegerType(), values=is_having_free_text_list)
    .withColumn("Deleted", IntegerType(), values=[0])
    .withColumn("SequenceNo", IntegerType(), minValue=1, maxValue=10)
    .withColumn("BPConditionID", IntegerType(), minValue=1, maxValue=100, random=True)
    .withColumn("ISSUEID", IntegerType(), values=issue_id_list)
    .withColumn("Clauseno", StringType(), values=clause_no_list)
    .withColumn("Clausewordings", StringType(), values=clause_wordings_list)
)

Mst_RejectionReasons = Mst_RejectionReasons_spec.build()
display(Mst_RejectionReasons)

# COMMAND ----------

# MAGIC %md
# MAGIC # Mst_Facility

# COMMAND ----------


level2_list = ["Others", "Day Care", "HDU Room", "HDU", "ICU", "Deluxe", "Semi Delux", "Private", "Semi Private", "General Ward"]

parent_id_list = [1, 2, 3, 4, 5, 0, 204]

Mst_Facility_spec = (
    dg.DataGenerator(spark, name='Mst_Facility', rows=mst_row_num, seedColumnName='baseId')
    .withColumn("ID", IntegerType(), minValue=205, maxValue=214)
    .withColumn("Level1", StringType(), values=["TPA Facility"])
    .withColumn("Level2", StringType(), values=level2_list)
    .withColumn("Level3", StringType(), expr="null")
    .withColumn("Level4", StringType(), expr="null")
    .withColumn("isFreeText", IntegerType(), values=[0])
    .withColumn("ParentID", IntegerType(), values=parent_id_list, random=True)
    .withColumn("Deleted", IntegerType(), values=[0])
    .withColumn("Createddatetime", "timestamp", begin="2022-01-01 00:00:00", end="2022-12-31 23:59:59", random=True)
    .withColumn("CreatedUserRegionID", IntegerType(), minValue=1, maxValue=10, random=True)
    .withColumn("LastModifiedUserID", IntegerType(), expr="null")
    .withColumn("LastModifiedDatetime", "timestamp", begin="2022-01-01 00:00:00", end="2022-12-31 23:59:59", random=True)
    .withColumn("slno", IntegerType(), minValue=1, maxValue=5, random=True)
)

Mst_Facility = Mst_Facility_spec.build()
display(Mst_Facility)

# COMMAND ----------

# MAGIC %md
# MAGIC # Mst_Payer

# COMMAND ----------


insurer_id_list = [6, 5, 7, 5, 6, 6, 6, 7, 7, 7]
name_list = [
    "DO : 610300 : DAVANGERE",
    "MO : 150581 : TUNI",
    "BO : 440103 : TRIPUNITHURA",
    "DO : 180100 : ANAND",
    "BO : 104301 : PORT BLAIR",
    "DO : 104300 : KOLKATA AUTO TIE UP HUB",
    "BO : 501302 : HOSUR",
    "BO : 313201 : KADAMTALA JALPAIGURI",
    "BO : 313304 : BASIRHAT",
    "BC : 414891 : CHENGALPATTU"
]
payer_code_list = [
    610300,
    150581,
    440103,
    180100,
    104301,
    104300,
    501302,
    313201,
    313304,
    414891
]

office_type_list = ['DO','BO', 'BO', 'DO', 'BO', 'DO', 'BO', 'BO','BO', 'BO' ]
parent_id_list = [1, 2, 3, 4, 5, 0, 204]

Mst_Payer_spec = (
    dg.DataGenerator(spark, name='Mst_Payer', rows=mst_row_num, seedColumnName='baseId')
    .withColumn("ID", IntegerType(), minValue=48000865, maxValue=48000874)
    .withColumn("InsurerID", IntegerType(), values=insurer_id_list)
    .withColumn("Name", StringType(), values=name_list)
    .withColumn("PayerCode", IntegerType(), values=payer_code_list)
    .withColumn("FloatCode", StringType(), expr="NULL")
    .withColumn("OfficeType", StringType(), values=office_type_list)
    .withColumn("ParentID", IntegerType(), values=parent_id_list, random=True)
    .withColumn("WebAddress", StringType(), expr="NULL")
    .withColumn("Remarks", StringType(), expr="NULL")
    .withColumn("Deleted", IntegerType(), values=[0])
    .withColumn("Latitude", FloatType(), expr="NULL")
    .withColumn("Longitude", FloatType(), expr="NULL")
    .withColumn("TPAFees", IntegerType(), values=[0])
)

Mst_Payer = Mst_Payer_spec.build()
display(Mst_Payer)

# COMMAND ----------

# MAGIC %md
# MAGIC # Mst_PropertyValues

# COMMAND ----------

name_list = [
    "Male",
    "Female",
    "Group",
    "Retail",
    "Floater SI",
    "Individual SI",
    "Individual",
    "Group",
    "Public Sector Unit",
    "Private Sector Unit"
]

parent_id_list = [1, 2, 3, 4, 5, 0, 204]

Mst_PropertyValues_spec = (
    dg.DataGenerator(spark, name='Mst_PropertyValues', rows=mst_row_num, seedColumnName='baseId')
    .withColumn("ID", IntegerType(), minValue=1, maxValue=mst_row_num)
    .withColumn("Name", StringType(), values=name_list)
    .withColumn("PropertyID", IntegerType(), minValue=1, maxValue=5, random=True)
    .withColumn("Deleted", IntegerType(), values=[0])
    .withColumn("ParentID", IntegerType(), values=parent_id_list, random=True)
)

Mst_PropertyValues = Mst_PropertyValues_spec.build()
display(Mst_PropertyValues)

# COMMAND ----------

# MAGIC %md
# MAGIC # Mst_State

# COMMAND ----------

state_name_list = [
    'Andhra Pradesh', 'Arunachal Pradesh', 'Assam', 'Bihar', 'Chhattisgarh', 'Goa',
    'Gujarat', 'Haryana', 'Himachal Pradesh', 'Jharkhand', 'Karnataka', 'Kerala',
    'Madhya Pradesh', 'Maharashtra', 'Manipur', 'Meghalaya', 'Mizoram', 'Nagaland',
    'Odisha', 'Punjab', 'Rajasthan', 'Sikkim', 'Tamil Nadu', 'Telangana',
    'Tripura', 'Uttar Pradesh', 'Uttarakhand', 'West Bengal'
]

Mst_State_spec = (
    dg.DataGenerator(spark, name='Mst_State', rows=28, seedColumnName='baseId')
    .withColumn("ID", IntegerType(), minValue=101, maxValue=110)
    .withColumn("Name", StringType(), values=state_name_list)
    .withColumn("CountryID", IntegerType(), values=[1])
    .withColumn("Deleted", IntegerType(), values=[0])
)

Mst_State = Mst_State_spec.build()
display(Mst_State)

# COMMAND ----------

# MAGIC %md
# MAGIC # Mst_RelationShip

# COMMAND ----------

relation_name_list = [
    "Un known",
    "Self",
    "Mother",
    "Son",
    "Daughter",
    "Sister in law",
    "Brother in law",
    "Grandmother",
    "Grandfather",
    "None"
]
Mst_RelationShip_spec = (
    dg.DataGenerator(spark, name='Mst_RelationShip', rows=mst_row_num, seedColumnName='baseId')
    .withColumn("ID", IntegerType(), minValue=1, maxValue=mst_row_num)
    .withColumn("Name", StringType(), values=relation_name_list)
    .withColumn("Deleted", IntegerType(), values=[0])
    .withColumn("GroupName", StringType(), values=['Parent', 'Children'], percentNulls=0.5)
    .withColumn("FamilyCondition_P10", StringType(), values=["28,30,31"], percentNulls=0.5)
    .withColumn("GroupID_P26", IntegerType(), values=[98, 99, 254])
)

Mst_RelationShip = Mst_RelationShip_spec.build()
display(Mst_RelationShip)

# COMMAND ----------

# MAGIC %md
# MAGIC # BenefitPlan

# COMMAND ----------

name_list = [
    "Test",
    "Test Individual",
    "Kotak Health Care",
    "Kotak-Excel",
    "Kotak-Premium",
    "Medi Prime",
    "MediPlus",
    "MediSenior",
    "Medi Raksha",
    "Ratnasagar Herbals"
]

BenefitPlan_spec = (
    dg.DataGenerator(spark, name='BenefitPlan', rows=row_num, seedColumnName='baseId')
    .withColumn("ID", IntegerType(), minValue=1, maxValue=row_num)
    .withColumn("Name", StringType(), values=name_list)
    .withColumn("IssueID", IntegerType(), minValue=1, maxValue=100, random=True)
    .withColumn("PayerID", IntegerType(), minValue=1, maxValue=10, random=True)
    .withColumn("RequestID", IntegerType(), minValue=1, maxValue=345087, random=True)
    .withColumn("ProductID", IntegerType(), minValue=1, maxValue=1000)
    .withColumn("PolicyTypeID", IntegerType(), minValue=3, maxValue=10, random=True)
    .withColumn("HealthPolicyID", IntegerType(), minValue=1, maxValue=10, random=True)
    .withColumn("CorporateID", IntegerType(), minValue=0, maxValue=10, random=True)
    .withColumn("PolicyID", LongType(),minValue=4411218,maxValue=4412218, random=True)
    .withColumn("SITypeID", IntegerType(), minValue=5, maxValue=68, random=True)
    .withColumn("isWithinSI", BooleanType(), values="null")
    .withColumn("isOutofSI", BooleanType(), values="null")
    .withColumn("FamilySize", IntegerType(), minValue=1, maxValue=10, random=True)
    .withColumn("StatusID", IntegerType(), minValue=1, maxValue=10)
    .withColumn("isLock", IntegerType(), values=[0, 1], random=True)
    .withColumn("Deleted", IntegerType(), values=[0])
    .withColumn("CreatedUserRegionID", IntegerType(), minValue=1, maxValue=10, random=True)
    .withColumn("CreatedDate", "timestamp", begin="2022-01-01 00:00:00", end="2022-12-31 23:59:59", random=True)
    .withColumn("LastModifiedUserRegionID", IntegerType(), minValue=1, maxValue=10, percentNulls=0.3, random=True)
    .withColumn("LastModifiedDate", "timestamp", begin="2023-01-01 00:00:00", end="2023-12-31 23:59:59")
)

BenefitPlan = BenefitPlan_spec.build()
display(BenefitPlan)

# COMMAND ----------

# MAGIC %md
# MAGIC # ClaimStage

# COMMAND ----------


name_list = [
    "Intimation",
    "Received Requests",
    "Registration",
    "For Bill Entry",
    "For Adjudication",
    "Query to Hospital",
    "Query to Member",
    "Query to Insurer",
    "Refer to CRM",
    "Refer to Enrollment"
]

externalname_list = [
    "Intimation",
    "Received Requests",
    "Registration",
    "Under Process",
    "Under Process",
    "Under Query",
    "Under Query",
    "IR to Insurer",
    "Under Process",
    "Under Process"
]

ClaimStage_spec = (
    dg.DataGenerator(spark, name='ClaimStage', rows=row_num, seedColumnName='baseId')
    .withColumn("ID", IntegerType(), minValue=1, maxValue=row_num)
    .withColumn("Name", StringType(), values=name_list)
    .withColumn("ExternalName", StringType(), values=externalname_list)
    .withColumn("Deleted", IntegerType(), values=[0])
    .withColumn("CreatedDatetime", "timestamp", begin="2022-01-01 00:00:00", end="2022-12-31 23:59:59", random=True)
)

ClaimStage = ClaimStage_spec.build()
display(ClaimStage)

# COMMAND ----------

# MAGIC %md
# MAGIC # ClaimDeductionDetails

# COMMAND ----------

row_num=1000
FreeTextValue_list = [
    "MLC",
    "Monitor charges not payable for ICU Rs 3279/-",
    "0"
]

ClaimDeductionDetails_spec = (
    dg.DataGenerator(spark, name='ClaimDeductionDetails', rows=row_num, seedColumnName='baseId')
    .withColumn("ID", IntegerType(), minValue=48425800, maxValue=48425809)
    .withColumn("ClaimBillDetailsID", IntegerType(), minValue=104348347, maxValue=104348392, random=True)
    .withColumn("ClaimID",LongType(), minValue=24070402000, maxValue=24070403000, random=True)
    .withColumn("Slno", IntegerType(), minValue=1, maxValue=5, random=True)
    .withColumn("ServiceID", IntegerType(), minValue=0, maxValue=50, random=True)
    .withColumn("BillSlNo", IntegerType(), minValue=1, maxValue=10, random=True)
    .withColumn("DeductionSlNo", IntegerType(), minValue=1, maxValue=100, random=True)
    .withColumn("DeductionAmount", IntegerType(), minValue=0, maxValue=6500)
    .withColumn("DeductionReasonID", IntegerType(), minValue=1, maxValue=10)
    .withColumn("FreeTextValue", StringType(), values=FreeTextValue_list, random=True)
    .withColumn("Deleted", IntegerType(), values=[0])
    .withColumn("CreatedUserRegionID", IntegerType(), minValue=1, maxValue=10, random=True)
    .withColumn("Createddatetime", "timestamp", begin="2022-01-01 00:00:00", end="2022-12-31 23:59:59", random=True)
    .withColumn("DeletedUserRegionID", IntegerType(), minValue=483, maxValue=2889, percentNulls=0.3, random=True)
    .withColumn("DeletedDatetime", "timestamp", begin="2023-01-01 00:00:00", end="2023-12-31 23:59:59", percentNulls=0.3, random=True)
    .withColumn("MIG_Serviceid", IntegerType(), minValue=1, maxValue=150, random=True)
    .withColumn("IRDADeductionReasonID", IntegerType(), values=['null', 0], random=True)
)

ClaimDeductionDetails = ClaimDeductionDetails_spec.build()
display(ClaimDeductionDetails)

# COMMAND ----------

# MAGIC %md
# MAGIC # ClaimUtilizedAmount

# COMMAND ----------


ClaimUtilizedAmount_spec = (
    dg.DataGenerator(spark, name='ClaimUtilizedAmount', rows=row_num, seedColumnName='baseId')
    .withColumn("ID", IntegerType(), minValue=71563, maxValue=71563+row_num)
    .withColumn("Claimid", LongType(), minValue=24070402000, maxValue=24070403000, random=True)
    .withColumn("Slno", IntegerType(), minValue=1, maxValue=5, random=True)
    .withColumn("MemberSIID", IntegerType(), minValue=70000, maxValue=900000, random=True)
    .withColumn("InsApprovedAmountID", IntegerType(), values=[0])
    .withColumn("SanctionedAmount", IntegerType(), minValue=12000, maxValue=300000, random=True)
    .withColumn("BalanceAmount", IntegerType(), minValue=20250, maxValue=220000, random=True)
    .withColumn("Deleted", IntegerType(), values=[1,0], random=True)
    .withColumn("CreatedUserRegionID",IntegerType(), minValue=1, maxValue=10, random=True)
    .withColumn("Createddatetime", "timestamp", begin="2017-05-15 00:00:00", end="2017-08-04 23:59:59", random=True)
    .withColumn("DeletedUserRegionID", IntegerType(), minValue=1, maxValue=10, random=True)
    .withColumn("DeletedDatetime", "timestamp", begin="2017-05-15 00:00:00", end="2017-08-04 23:59:59", percentNulls=0.3, random=True)
    .withColumn("SICategoryID", IntegerType(), minValue=1, maxValue=100, random=True)
    .withColumn("MainMemberID", LongType(), minValue=86750556, maxValue=86751556, random=True)
    .withColumn("SITypeID", IntegerType(), minValue=5, maxValue=68, random=True)
)

ClaimUtilizedAmount = ClaimUtilizedAmount_spec.build()
display(ClaimUtilizedAmount)

# COMMAND ----------

# MAGIC %md
# MAGIC # ClaimsIRReasons

# COMMAND ----------


remarks_list = [
    "i) Kindly furnish the Circumstances & Etiology of the Injury / Fracture along with a copy of the MLC report, if due to RTA, and any intake of Alcohol/Drugs by the Patient at the time of the Accident.",
    "ii) Supporting investigation reports and exact line of management",
    "i) Kindly furnish the Circumstances & Etiology of the Injury / Fracture along with a copy of the MLC report, if due to RTA, and any intake of Alcohol/Drugs by the Patient at the time of the Accident.",
    "ii) Supporting investigation reports and exact line of management",
    "iii) Supporting investigation reports.",
    "iv) Admission notes with treatment done till date.",
    "v) Photo id proof of the patient"
]
ClaimsIRReasons_spec = (
    dg.DataGenerator(spark, name='ClaimsIRReasons', rows=row_num, seedColumnName='baseId')
    .withColumn("ID", IntegerType(), minValue=245714, maxValue=245793)
    .withColumn("ClaimActionID", IntegerType(), minValue=664729, maxValue=664746, random=True)
    .withColumn("ClaimID", LongType(), minValue=24070402000, maxValue=24070403000, random=True)
    .withColumn("Slno", IntegerType(), minValue=1, maxValue=5, random=True)
    .withColumn("IRDocumentID", IntegerType(), minValue=0, maxValue=35, random=True)
    .withColumn("FreeText1", StringType(), expr="null")
    .withColumn("FreeText2", StringType(), expr="null")
    .withColumn("ServiceID", IntegerType(), minValue=0, maxValue=50, random=True)
    .withColumn("Remarks", StringType(), values=remarks_list, random=True)
    .withColumn("isMandatory", IntegerType(), values=[1], random=True)
    .withColumn("isReceived", IntegerType(), values=[0, 1], random=True)
    .withColumn("Amount", IntegerType(), minValue=0, maxValue=155000, random=True)
    .withColumn("SentDate", "timestamp", expr="null")
    .withColumn("ConsignmentNo", StringType(), expr="null")
    .withColumn("ReceivedDate", "timestamp", expr='null')
    .withColumn("Deleted", IntegerType(), values=[0, 1], random=True)
    .withColumn("CreatedUserRegionID", IntegerType(), minValue=1, maxValue=10, random=True)
    .withColumn("CreatedDatetime", "timestamp", begin="2017-05-15 00:00:00", end="2017-08-04 23:59:59", random=True)
    .withColumn("DeletedUserRegionID", IntegerType(), minValue=1, maxValue=10, random=True)
    .withColumn("DeletedDatetime", "timestamp", begin="2017-05-15 00:00:00", end="2017-08-04 23:59:59", percentNulls=0.3, random=True)
)

ClaimsIRReasons = ClaimsIRReasons_spec.build()
display(ClaimsIRReasons)

# COMMAND ----------

# MAGIC %md
# MAGIC #BPServiceConfigDetails

# COMMAND ----------

AllowedRelationIDs_list = [
    "22,23,25,26,27,33,42",
    "2",
    "2,3,5,42",
    "2,42"
]

external_remark_list = [
    "Ambulance charges covered upto Rs.2,500/- per Hospitalisation.",
    "Ambulance charges covered upto Rs.2,000/- per Hospitalization.",
    "Family Transportation covered upto Rs.5,000/-.",
    "Ambulance charges covered upto Rs.2,500/- per Hospitalisation.",
    "Ambulance charges covered upto Rs.2,000/- per Hospitalization.",
    "Ambulance covered upto Rs.2,000/- per Hospitalization.",
    "Family Transportation covered upto Rs.100/- per Day upto a maximum of 15 Days with a Deductible of 2 Days upto Rs.5,000/-.",
    "Family Transportation covered upto Rs.5,000/-.",
]

BPServiceConfigDetails_spec = (
    dg.DataGenerator(spark, name='BPServiceConfigDetails', rows=row_num, seedColumnName='baseId')
    .withColumn("ID", IntegerType(), minValue=1, maxValue=row_num)
    .withColumn("BenefitPlanID", IntegerType(), minValue=1, maxValue=1000, random=True)
    .withColumn("BenefitPlanSIID", IntegerType(), minValue=51, maxValue=319, random=True)
    .withColumn("isActive", IntegerType(), values=[1])
    .withColumn("ServiceTypeID", IntegerType(), values=[1])
    .withColumn("ServiceSubTypeID", IntegerType(), values=[4])
    .withColumn("TPAProcedureID", IntegerType(), minValue=310, maxValue=1147, random=True)
    .withColumn("ServiceID", IntegerType(), minValue=1, maxValue=50, random=True)
    .withColumn("BPComparisionFromID", IntegerType(), values=[210])
    .withColumn("ExpressionID_P17", IntegerType(), values=[53])
    .withColumn("AllowedRelationIDs", StringType(), values=AllowedRelationIDs_list)
    .withColumn("ExternalValuePerc", IntegerType(), expr="null")
    .withColumn("ExternalValueAbs", IntegerType(), minValue=2000, maxValue=5000, random=True)
    .withColumn("InternalValuePerc", IntegerType(), expr="null")
    .withColumn("InternalValueAbs", IntegerType(), minValue=2000, maxValue=5000, random=True)
    .withColumn("AllowedRoles", StringType(), expr="null")
    .withColumn("LimitCatg_P29", IntegerType(), minValue=107, maxValue=109, random=True)
    .withColumn("PolicyID", IntegerType(), minValue=0, maxValue=740, random=True)
    .withColumn("ENDR_ID", IntegerType(), expr="null")
    .withColumn("ApplicableTo_P11", IntegerType(), values=[33])
    .withColumn("EffectiveDate", "timestamp", begin="2017-05-15 00:00:00", end="2017-08-04 23:59:59", random=True)
    .withColumn("iSMinValue", IntegerType(), values=[1])
    .withColumn("FCount", IntegerType(), expr="null")
    .withColumn("ICount", IntegerType(), expr="null")
    .withColumn("InternalRemarks", StringType(), expr="null")
    .withColumn("ExternalRemarks", StringType(), values=external_remark_list, random=True)
    .withColumn("isOutofSI", IntegerType(), values=[0, 1], random=True)
    .withColumn("isOutPatient", IntegerType(), values=[0, 1], random=True)
    .withColumn("isMandatory", IntegerType(), expr="null")
    .withColumn("CreatedUserRegionID", IntegerType(), minValue=1, maxValue=10, random=True)
    .withColumn("CreatedDateTime", "timestamp", begin="2017-05-15 00:00:00", end="2017-08-04 23:59:59", random=True)
    .withColumn("DeletedUserRegionID", IntegerType(), minValue=1, maxValue=10, random=True)
    .withColumn("DeletedDateTime", "timestamp", expr="null")
    .withColumn("Deleted", IntegerType(), values=[0])
)

BPServiceConfigDetails = BPServiceConfigDetails_spec.build()
display(BPServiceConfigDetails)

# COMMAND ----------

row_count = 1000

TertiaryUtilization_spec = (dg.DataGenerator(spark, name="TertiaryUtilization", seedColumnName='KID', rows=row_count)
                  .withColumn("ID", IntegerType(), minValue=4900, maxValue=5000, random=True)
                  .withColumn("MemberPolicyID", IntegerType(), minValue=42625636, maxValue=77880770, random=True)
                  .withColumn("PolicyID", LongType(),minValue=4411218,maxValue=4412218, random=True)
                  .withColumn("RefertoId", IntegerType(), minValue=200, maxValue=300, random=True)
                  .withColumn("CorpID", IntegerType(), minValue=1, maxValue=100, random=True)
                  .withColumn("ClaimID", LongType(), minValue=24070402000, maxValue=24070403000, random=True)
                  .withColumn("Slno", IntegerType(), minValue=1, maxValue=5, random=True)
                  .withColumn("TPAProcID", IntegerType(), minValue=1000, maxValue=1200, random=True)
                  .withColumn("EligibleAmount", IntegerType(), minValue=202, maxValue=41604, random=True)
                  .withColumn("RuleID", IntegerType(), minValue=1871635, maxValue=2299465, random=True)
                  .withColumn("SICategoryID_P20", IntegerType(), values=[69,70], random=True)
                  .withColumn("RequestedAmount", IntegerType(), minValue=202, maxValue=41604, random=True)
                  .withColumn("RequestedBy", StringType(), values=["6000", "6248"] ,random=True)
                  .withColumn("RequestedDatetime", TimestampType(), expr="current_timestamp()")
                  .withColumn("RequestRemarks", StringType(), values = ["41600","41604"],random=True)
                  .withColumn("ApprovedAmount", IntegerType(), minValue=202, maxValue=41604, random=True)
                  .withColumn("ApprovedBy", StringType(), minValue = 3300, maxValue = 3319, random=True)
                  .withColumn("ApprovedDatetime", TimestampType(), expr="current_timestamp()")
                  .withColumn("ApprovalRemarks", StringType(), values = ["approve","reject"], random = True)
                  .withColumn("ApprovalRecievedBy", StringType(), expr = "null")
                  .withColumn("UtilizedAmount", IntegerType(), minValue=202, maxValue=41604, random=True)
                  .withColumn("UtilizedDatetime", TimestampType(),expr="current_timestamp()")
                  .withColumn("DMSIds", StringType(),expr = "null")
                  .withColumn("Deleted", BooleanType(), values=[True, False], random=True)
                  .withColumn("CreatedUserRegionID", IntegerType(), minValue=1, maxValue=10, random=True)
                  .withColumn("CreatedDatetime", TimestampType(),expr="current_timestamp()")
                  .withColumn("ModifiedUserRegionID", IntegerType(), minValue=1, maxValue=10, random=True)
                  .withColumn("ModifiedDatetime", TimestampType(),expr="current_timestamp()"))

TertiaryUtilization = TertiaryUtilization_spec.build()
TertiaryUtilization.display()

# COMMAND ----------

row_count = 1000
policy_spec = (dg.DataGenerator(spark, name="policy_data", rows=row_count, seedColumnName='KID')
             .withColumn("ID", LongType(),minValue=4411218,maxValue=4412218)
             .withColumn("CorpID", IntegerType(), minValue=1, maxValue=100, random=True)
             .withColumn("BrokerID", IntegerType(), minValue=1, maxValue=100, random=True)
             .withColumn("IssueID", IntegerType(), minValue=1, maxValue=100, random=True)
             .withColumn("PayerID", IntegerType(), minValue=2000, maxValue=2020)
             .withColumn("BenefitPlanID", IntegerType(), minValue=1, maxValue=1000, random=True)
             .withColumn("PolicyNo", IntegerType(), minValue=1001620000, maxValue=1001900000, random=True)
             .withColumn("PayeeTypeID", IntegerType(), values=[4])
             .withColumn("ProposerName", StringType(), values=[fake.name() for _ in range(1000)])
             .withColumn("ProposerDate", DateType(), expr="null")
             .withColumn("AgentID", StringType(), expr="null")
             .withColumn("InceptionDate", DateType(), expr="null")
             .withColumn("StartDate", StringType(),begin="2023-05-15", end="2024-08-04",random=True)
             .withColumn("EndDate", DateType(), expr="null")
             .withColumn("isLatest", BooleanType(), expr="true")
             .withColumn("StatusID", IntegerType(),values=[14])
             .withColumn("PolicyIssueDate", StringType(), expr="null")
             .withColumn("PolicyClosureDate", StringType(),begin="2023-05-15", end="2024-08-04",random=True)
             .withColumn("PolicyTypeID_P2", IntegerType(), values=[4])
             .withColumn("SITypeID_P3", IntegerType(), values=[5,6])
             .withColumn("CoverageTypeID_P21", IntegerType(),values=[76,77,78,79,80,81,82,8])
             .withColumn("PolicyBreakDays", IntegerType(), expr="null")
             .withColumn("CondonedBy", StringType(), expr="null")
             .withColumn("CondonedDate", StringType(), expr="null")
             .withColumn("CondoneRemarks", StringType(), expr="null")
             .withColumn("CondoneDMSID", StringType(), expr="null")
             .withColumn("RenewedCount", IntegerType(), values=[0,1,2],random=True)
             .withColumn("BasePolicyID", IntegerType(), expr="null")
             .withColumn("PrevPolicyIDs", StringType(), minValue=4411218,maxValue=4688326, random=True)
             .withColumn("LinkedPolicyIDs", StringType(), expr="null")
             .withColumn("CoverContinueFlag", BooleanType(), expr="null")
             .withColumn("VB64ConfirmBy", StringType(), expr="null")
             .withColumn("VB64ConfirmDate", StringType(), expr="null")
             .withColumn("VB64Remarks", StringType(), expr="null")
             .withColumn("VB64DMSID", StringType(), expr="null")
             .withColumn("PolicyZone", IntegerType(), expr="null")
             .withColumn("NetPremium", FloatType(), expr="null")
             .withColumn("LoadingPremium", FloatType(), expr="null")
             .withColumn("ServiceTaxPerc", FloatType(), expr="null")
             .withColumn("ServiceTaxValue", FloatType(), expr="null")
             .withColumn("TotalPremium", FloatType(), expr="null")
             .withColumn("BankID", IntegerType(), expr="null")
             .withColumn("AccountNo", expr="null")
             .withColumn("AccountTypeID", expr="null")
             .withColumn("PayeeName", StringType(), values=[fake.name() for _ in range(100)])
             .withColumn("BufferAmount", FloatType(), expr="null")
             .withColumn("BufferEffectiveDate", StringType(), expr="null")
             .withColumn("EffectiveFrom", StringType(), expr="null")
             .withColumn("EffectiveTo", StringType(), expr="null")
             .withColumn("CardTypeID", IntegerType(), expr="null")
             .withColumn("ClaimRatioThresholdPerc", FloatType(), expr="null")
             .withColumn("ClaimRatioThresholdValue", expr="null")
             .withColumn("CardID", StringType(), expr="null")
             .withColumn("eCardTemplateID", StringType(), expr="null")
             .withColumn("RevolvingFlagID", IntegerType(), expr="null")
             .withColumn("iSDummyPolicy", BooleanType(), expr="false")
             .withColumn("RegulariseDate", StringType(), expr="null")
             .withColumn("TPAFees", FloatType(), expr="null")
             .withColumn("TPAFeeID_P21", IntegerType(), expr="null")
             .withColumn("Discount", FloatType(), expr="null")
             .withColumn("ProcessedRegions", StringType(), expr="null")
             .withColumn("MigratedID", IntegerType(), expr="null")
             .withColumn("MIG_HealthPolicyID", IntegerType(), expr="null")
             .withColumn("PolicyRecievedDate", expr="null")
             .withColumn("Deleted", IntegerType(), values=[0, 1], weights=[0.8, 0.2], random=True)
             .withColumn("CreatedUserRegionID", IntegerType(), minValue=1, maxValue=10, random=True)
             .withColumn("CreatedDatetime", StringType(), expr="null")
             .withColumn("LastModifiedUserRegionID", IntegerType(), expr="null")
             .withColumn("LastModifiedDate", StringType(), expr="null")
             .withColumn("MIG_MemberDetailsID", IntegerType(), expr="null")
             .withColumn("Deductible", FloatType(), expr="null")
             .withColumn("InstallmentFreqID", IntegerType(), expr="null")
             .withColumn("NoOfInstallments", IntegerType(), expr="null")
             .withColumn("PolicyPremium", FloatType(), expr="null")
             .withColumn("PremiumPaid", FloatType(), expr="null")
             .withColumn("PlanYears", IntegerType(), minValue=1, maxValue=3)
             .withColumn("PlanRenewalDate", StringType(), expr="null")
             .withColumn("PlanRenewalYear", IntegerType(), expr="null")
             .withColumn("ProcessBranchID", IntegerType(), expr="null")
             .withColumn("ProposerTypeID", IntegerType(), expr="null")
             .withColumn("Remarks", StringType(), expr="null")
             .withColumn("Notes", StringType(), expr="null")
             .withColumn("IDITContactId", IntegerType(), expr="null")
             .withColumn("PrevPolicyNos", StringType(), expr="null")
             .withColumn("IsPolicyNIDB", BooleanType(), expr="null")
             .withColumn("PNIDB_UpdatedDate", StringType(), expr="null")
             .withColumn("PNIDB_RemovedDate", StringType(), expr="null")
             .withColumn("PNIDB_CreatedUserRegionID", IntegerType(), expr="null")
             .withColumn("PNIDB_DeletedUserRegionID", IntegerType(), expr="null")
             .withColumn("PartyCode", IntegerType(), expr="null")
             .withColumn("Productcode", IntegerType(), expr="null")
             .withColumn("IsSuspiciousPolicy", BooleanType(), expr="false")
            )

Policy = policy_spec.build()
display(Policy)

# COMMAND ----------

provider_spec = (dg.DataGenerator(spark, name="provider", rows=row_count, seedColumnName='KID')
            .withColumn("ID", IntegerType(), minValue=214500, maxValue=214500+row_count)
            .withColumn("Name", StringType(), values=hospital_name,random=True)
            .withColumn("ShortName", StringType(), expr="array_join(transform(split(Name, ' '), x -> substring(x, 1, 1)), '')")
            .withColumn("PRCNo", StringType(), nullable=True)
            .withColumn("ServiceTAXNo", StringType(), nullable=True)
            .withColumn("ServiceTAXNoDMSID", StringType(), nullable=True)
            .withColumn("PANNo", StringType(), values=[fake.uuid4() for _ in range(row_count)],random=True)
            .withColumn("PANNumberDMSID", StringType(), nullable=True)
            .withColumn("RegistrationNo", StringType(), nullable=True)
            .withColumn("RegistrationNoDMSID", StringType(), nullable=True)
            .withColumn("YearofEstablishment", IntegerType(), minValue=1980, maxValue=2023)
            .withColumn("PayeeName", StringType(), nullable=True)
            .withColumn("DeducteeName", StringType(), nullable=True)
            .withColumn("OwnershipID_P35", IntegerType(), minValue=0, maxValue=200)
            .withColumn("OwnershipOthers", StringType(), nullable=True)
            .withColumn("Recognizedby_P36", IntegerType(), minValue=0, maxValue=200)
            .withColumn("RecognizedbyOthers", StringType(), nullable=True)
            .withColumn("HospitalType_P37", IntegerType(), minValue=0, maxValue=200)
            .withColumn("SpecialtyType_P38", IntegerType(), minValue=0, maxValue=200)
            .withColumn("SpecialtyID", IntegerType(), minValue=0, maxValue=200)
            .withColumn("Accreditation_P39", IntegerType(), minValue=0, maxValue=200)
            .withColumn("TDSExemption", StringType(), nullable=True)
            .withColumn("IRDACode", StringType(), nullable=True)
            .withColumn("CancelledChequeDMSID", StringType(), nullable=True)
            .withColumn("Address1", StringType(), values=[fake.address() for _ in range(100)])
            .withColumn("Address2", StringType(), nullable=True)
            .withColumn("CountryID", IntegerType(), minValue=1, maxValue=195)
            .withColumn("StateID", IntegerType(), minValue=1, maxValue=50)
            .withColumn("DistrictID", IntegerType(), minValue=1, maxValue=1000)
            .withColumn("CityID", IntegerType(), minValue=1, maxValue=1000)
            .withColumn("CityOthers", StringType(), nullable=True)
            .withColumn("Location", StringType(), nullable=True)
            .withColumn("PINCode", IntegerType(), minValue=100000, maxValue=999999)
            .withColumn("CountryCode", StringType(), values=[fake.country_code() for _ in range(100)])
            .withColumn("RegMobileNo", StringType(), values=[fake.phone_number() for _ in range(100)])
            .withColumn("RegFaxNo", StringType(), nullable=True)
            .withColumn("RegEmailID", StringType(), values=[fake.email() for _ in range(100)])
            .withColumn("Longitude", DoubleType(), minValue=-180.0, maxValue=180.0)
            .withColumn("Latitude", DoubleType(), minValue=-90.0, maxValue=90.0)
            .withColumn("UserName", StringType(), nullable=True)
            .withColumn("Password", StringType(), nullable=True)
            .withColumn("Website", StringType(), values=[fake.url() for _ in range(100)])
            .withColumn("OwnerName", StringType(), values=[fake.name() for _ in range(100)])
            .withColumn("OwnerDesignation_P40", StringType(), nullable=True)
            .withColumn("OwnerMobile", StringType(), values=[fake.phone_number() for _ in range(100)])
            .withColumn("OwnerEmail", StringType(), values=[fake.email() for _ in range(100)])
            .withColumn("OwnerLandline", StringType(), nullable=True)
            .withColumn("OwnerFax", StringType(), nullable=True)
            .withColumn("OwnerPhotoDMSID", StringType(), nullable=True)
            .withColumn("Deleted", BooleanType(), values=[fake.boolean() for _ in range(100)])
            .withColumn("CreatedUserRegionID", IntegerType(), minValue=1, maxValue=10, random=True)
            .withColumn("Createddatetime", TimestampType(), expr="current_timestamp()")
            .withColumn("ModifiedUserRegionID", IntegerType(), minValue=0, maxValue=20000)
            .withColumn("ModifiedDatetime", TimestampType(), expr="current_timestamp()")
            .withColumn("DeletedUserRegionID", IntegerType(), nullable=True)
            .withColumn("DeletedDatetime", TimestampType(), nullable=True)
            .withColumn("isgipsappn", BooleanType(), values=[fake.boolean() for _ in range(100)])
            .withColumn("Status", StringType(), values=["ACTIVE", "INACTIVE"])
            .withColumn("RegLandlineNo", StringType(), values=[fake.phone_number() for _ in range(100)])
            .withColumn("CityName", StringType(), values=[fake.city() for _ in range(100)])
            .withColumn("Remarks", StringType(), nullable=True)
            .withColumn("NEFTDeclarationDMSID", StringType(), nullable=True)
            .withColumn("PayeeDeclarationDMSID", StringType(), nullable=True)
            .withColumn("IsByPatriate", BooleanType(), values=[fake.boolean() for _ in range(100)])
            .withColumn("STDCode", StringType(), values=["23", "91", "44", "11", "22"])
            .withColumn("HospitalProfarmaDMSID", IntegerType(), minValue=37000, maxValue=38000,random=True)
            .withColumn("MOUDMSID", IntegerType(), minValue=0, maxValue=10)
            .withColumn("InsurerID", IntegerType(), minValue=10, maxValue=30)
            .withColumn("TotalNoofBeds", IntegerType(), minValue=50, maxValue=500)
            .withColumn("HospitalCategory_P68", IntegerType(), minValue=1, maxValue=1000,random=True)
            .withColumn("RohiniCode", StringType(), nullable=True)
            .withColumn("NABHAccreditationStatus", StringType(), values=['Accredited', 'Non-Accredited'], random=True)
            .withColumn("SOCEffectiveDate", DateType(), begin="2023-01-01", end="2024-07-23")
            .withColumn("IsFHPLPPN", BooleanType(), values=[True, False], random=True)
            .withColumn("GSTIN", StringType(), values=[fake.uuid4() for _ in range(row_count)], random=True)
            .withColumn("IsEmpanelment", BooleanType(), values=[True, False], random=True)
            .withColumn("EarlyPaymentDiscount", StringType(), nullable=True)
            .withColumn("EarlyPaymentDiscountRemarks", StringType(), nullable=True)
            .withColumn("OPServices", StringType(), nullable=True)
            .withColumn("NABHAccreditionType", StringType(), values=['Type1', 'Type2', 'Type3'], random=True)
            .withColumn("NABHCode", StringType(), nullable=True)
            .withColumn("RohiniDate", DateType(), begin="2023-01-01", end="2024-07-23")
            .withColumn("NABHExpiry", DateType(),begin="2023-01-01", end="2024-07-23")
            .withColumn("AccreditationExpiryDate", DateType(), begin="2023-01-01", end="2024-07-23")
            .withColumn("RohiniExpiryDate", DateType(), begin="2023-01-01", end="2024-07-23")
            .withColumn("RegisteredAuthority", StringType(), values=['Authority1', 'Authority2'], random=True)
            .withColumn("RegistrationExpiryDate", DateType(), begin="2023-01-01", end="2024-07-23")
            .withColumn("Empanelmentrequestreceivedfrom", StringType(), values=['Source1', 'Source2'], random=True)
            .withColumn("SPOC", StringType(), values=[fake.name() for _ in range(row_count)], random=True)
            .withColumn("SPOCmailID", StringType(), values=[fake.email() for _ in range(row_count)], random=True)
            .withColumn("SPOCdate", DateType(), begin="2023-01-01", end="2024-07-23")
            .withColumn("Reason", StringType(), nullable=True)
)

Provider = provider_spec.build()
display(Provider)

# COMMAND ----------

names_list = ["PRATIPADA UMESH RAUT", "Deepika Alahari", "Prakhar Tulsyan", "Sapna Upadhyay", "Rishika Singh", "Arun Chacko", "Susan Varghese Puthanparambil", "Harshitha Boga", "Mahvash Tanveer", "Hadi Shahid Kazi"]
addresses1_list = ["UrbanClap Technologies India Pvt Ltd", "Cubic Transportation Systems (India) Pvt Ltd", "Bharti Airtel Ltd"]
addresses2_list = ["4th Floor, Block C & D, ILABS Technology Center", "Plot No.18, SY No.64, Software Units Layout, OPP to Inorbit Mall, Madhapur"]
locations_list = ["Bangalore", "Hyderabad", "Gurgaon"]
cities_list = ["Bangalore", "Hyderabad", "Gurgaon"]
pincodes = [456225 , 500081 , 122015]
districts_list = [12920, 13705, 10618]
states_list = [129, 137, 106]
membercontacts_spec = (
    dg.DataGenerator(spark, name='MemberContacts', rows=row_num, seedColumnName='baseId')
    .withColumn("ID", IntegerType(), minValue=86848778, maxValue=86848787)
    .withColumn("EntityID", IntegerType(), minValue=86750555, maxValue=86750564)
    .withColumn("Name", StringType(), values=names_list)
    .withColumn("Designation", StringType() , expr='null')  
    .withColumn("Address1", StringType(), values=addresses1_list)
    .withColumn("Address2", StringType(), values=addresses2_list, percentNulls=0.1)
    .withColumn("Location", StringType(), values=locations_list)
    .withColumn("CityID", IntegerType(), minValue=0, maxValue=0)
    .withColumn("PinCode", IntegerType(), values=pincodes)
    .withColumn("DistrictID", IntegerType(), values=districts_list)
    .withColumn("StateID", IntegerType(), values=states_list)
    .withColumn("STDCode", StringType(), expr='null') 
    .withColumn("PhoneNo", StringType(),expr='null')
    .withColumn("MobileNo", StringType(), expr='null')
    .withColumn("SendSMS", BooleanType(), values=[False])
    .withColumn("Fax_STDCode", StringType(), expr='null')  
    .withColumn("FaxNo", StringType(), expr='null')  
    .withColumn("EmailID", StringType(), expr = 'null')  
    .withColumn("SendMail", BooleanType(), values=[False]) 
    .withColumn("Website", StringType(), expr='null')  
    .withColumn("TollfreeNo", StringType(), expr='null') 
    .withColumn("IsBaseAddress", BooleanType(), values=[True]) 
    .withColumn("IsTollfreeNo", BooleanType(), expr='null')
    .withColumn("CreatedUserRegionID",IntegerType(), minValue=1, maxValue=10, random=True)  
    .withColumn("CreatedDate", StringType(), values=["2023-07-24", "2023-07-25", "2023-07-26"])
    .withColumn("ModifiedUserRegionID", IntegerType(), expr='null')
    .withColumn("ModifiedDate", StringType(), expr='null')
    .withColumn("Deleted", BooleanType(), values=[False])
    .withColumn("AlterMobileNumber", StringType(), expr='null')
    .withColumn("CCEmailID", StringType(), expr='null')
    .withColumn("BCCEmailID", StringType(), expr='null')
    .withColumn("SendAlterSMS", BooleanType(), expr='null')
    .withColumn("SendCCEmail", BooleanType(), expr='null')
    .withColumn("SendBCCEmail", BooleanType(), expr='null')
)

MemberContacts = membercontacts_spec.build()
MemberContacts.display()

# COMMAND ----------

bpsuminsured_spec = (
    dg.DataGenerator(spark, name="bpsuminsured", rows=row_num, seedColumnName="baseId")
    .withColumn("ID", IntegerType(), minValue=1, maxValue=1000)
    .withColumn("BenefitPlanID", IntegerType(), minValue=1, maxValue=1000, random=True)
    .withColumn("SumInsured", IntegerType(), minValue=500000, maxValue=10000000)
    .withColumn("SICategoryID_P20", IntegerType(), values=[69,70,71], random=True)
    .withColumn("SITypeID", IntegerType(), values=[5])
    .withColumn("PolicyID", LongType(),minValue=4411218,maxValue=4412218, random=True)
    .withColumn("ENDR_ID", IntegerType(), minValue=4768057, maxValue=4851564)
    .withColumn("ApplicableTo_P11", IntegerType(), values=[33])
    .withColumn("EffectiveDate", DateType(), expr="date_add(current_date(), cast(rand() * 365 as int))")
    .withColumn("CopayPercentage", FloatType(), expr = 'null')
    .withColumn("CopayAmount", FloatType(), expr = 'null')
    .withColumn("isLumpSum", BooleanType(), values=[False])
    .withColumn("isAccumulate", BooleanType(), values=[False])
    .withColumn("CoInsurancePerc", FloatType(), expr = 'null')
    .withColumn("CoInsuranceAmt", FloatType(), expr = 'null')
    .withColumn("CoInsuranceRemarks", StringType(), expr = 'null')
    .withColumn("isWithinSI", BooleanType(), values=[True])
    .withColumn("isOutOFSI", BooleanType(), values=[False])
    .withColumn("Priority", IntegerType(), expr = 'null')
    .withColumn("Notes", StringType(), expr = 'null')
    .withColumn("RulesHTML", StringType(), expr = 'null')
    .withColumn("Locked", BooleanType(), values=[False])
    .withColumn("ApprovalReq_P60", BooleanType(), expr = 'null')
    .withColumn("CalculateCopy_P61", BooleanType(), expr = 'null')
    .withColumn("MIG_ProductTypeID", IntegerType(), expr = 'null')
    .withColumn("Deleted", BooleanType(), values=[False])
    .withColumn("CreatedUserRegionID", IntegerType(), minValue=1, maxValue=10, random=True)
    .withColumn("CreatedDate", DateType(), expr="current_date()")
    .withColumn("LastModifiedUserRegionID", IntegerType(), values=[3263])
    .withColumn("LastModifiedDate", DateType(), expr="current_date()")
    .withColumn("MIG_BenefitPlanID", IntegerType(), expr = 'null')
    .withColumn("isAutoRule", BooleanType(), values=[True , False])
    .withColumn("ProductSIID", IntegerType(), expr = 'null')
    .withColumn("MIG_BenefitPlanName", StringType(), values=["Plan 2", "Plan 3", "Plan 4"])
)

BPSuminsured = bpsuminsured_spec.build()
BPSuminsured.display()

# COMMAND ----------

number_1 = [
    ('218464'),
    ('218414'),
    ('218414'),
    ('218413'),
    ('218382'),
    ('218382'),
    ('218381'),
    ('217313'),
    ('228078')
]

random_number = random.randint(1000, 9999)
member_policy_spec = (
    dg.DataGenerator(spark, name="member_policy", rows=row_num, seedColumnName="baseId")
    .withColumn("Id" , LongType() , minValue=86750555, maxValue=86751564)
    .withColumn("Issueid", IntegerType(), minValue=1, maxValue=100, random=True)
    .withColumn("uhidno", StringType(), expr="concat('NIAC', lpad(cast(rand() * 100000000 as bigint), 9, '0'))")
    .withColumn("InsUhidno", StringType(), expr="null")
    .withColumn("MainmemberID", LongType(), minValue=86750556, maxValue=86751556)
    .withColumn("PolicyID", LongType(),minValue=4411218,maxValue=4412218, random=True)
    .withColumn("CorpID", IntegerType(), minValue=1, maxValue=100, random=True)
    .withColumn("EmployeeID", StringType(), values = [218464, 218414, 218414, 218413, 218382, 218382, 218381, 217313, 228078])
    .withColumn("SalutationID", IntegerType(), minValue=0, maxValue=9)
    .withColumn("FirstName", StringType(), values= ["PRATIPADA UMESH RAUT", "Deepika Alahari", "Prakhar Tulsyan", "Sapna Upadhyay", "Rishika Singh", "Arun Chacko", "Susan Varghese Puthanparambil", "Harshitha Boga", "Mahvash Tanveer", "Hadi Shahid Kazi"])
    .withColumn("MiddleName", StringType(), expr = 'null') 
    .withColumn("LastName", StringType(), expr = 'null')
    .withColumn("MemberName", StringType(), values = ["PRATIPADA UMESH RAUT", "Deepika Alahari", "Prakhar Tulsyan", "Sapna Upadhyay", "Rishika Singh", "Arun Chacko", "Susan Varghese Puthanparambil", "Harshitha Boga", "Mahvash Tanveer", "Hadi Shahid Kazi"])
    .withColumn("GenderID", IntegerType(), minValue=1, maxValue=2)
    .withColumn("DOB", DateType(), expr="date_add('2000-01-01', cast(rand() * 10000 as int) - 5000)")
    .withColumn("Age", IntegerType(), minValue=1, maxValue=31)
    .withColumn("AgeTypeID", IntegerType(), minValue=1, maxValue=2)
    .withColumn("MaritalStatusID_P25", IntegerType(), minValue=92, maxValue=96)
    .withColumn("MemberInceptionDate", DateType(), expr="date_add(current_date(), cast(rand() * 1000 as int) - 500)")
    .withColumn("MemberCommencingDate", DateType(), expr="date_add(current_date(), cast(rand() * 100 as int) - 50)")
    .withColumn("MemberEndDate", DateType(), expr="date_add(MemberCommencingDate, 365)")
    .withColumn("PolicyNo", IntegerType(), minValue=1001620000, maxValue=1001900000, random=True)
    .withColumn("PrevPolicyno", StringType(), expr = 'null')  # Assuming previous policy numbers are empty
    .withColumn("RelationshipID", IntegerType(), minValue=2, maxValue=42)
    .withColumn("Occupation", StringType(), values=["null"])  # Assuming occupations are empty
    .withColumn("DOJ", DateType(), expr="date('1900-01-01')")
    .withColumn("DOL", StringType(), values=["null"]) 
    .withColumn("Grade", StringType(), expr = 'null')
    .withColumn("Designation", StringType(),expr = 'null')
    .withColumn("Unit", StringType(), expr = 'null')
    .withColumn("Division", StringType(),expr = 'null')
    .withColumn("Renewalcount", IntegerType(), expr = 'null')
    .withColumn("StatusID", IntegerType(), values = [19])
    .withColumn("isLatest", BooleanType(), values=[True])
    .withColumn("PreviousMemberID", StringType(), expr = 'null')
    .withColumn("PreviousPolicyID", StringType(),expr = 'null' )
    .withColumn("PassportNo", StringType(), expr = 'null')
    .withColumn("PanNo", StringType(), expr = 'null')
    .withColumn("DrivingLiscenceNo", StringType(), expr ='null')
    .withColumn("AadharID", StringType(), expr = 'null')
    .withColumn("UniqueID", StringType(), expr = 'null')
    .withColumn("RationCardNo", StringType(), expr = 'null')
    .withColumn("VoterID", StringType(), expr ='null')
    .withColumn("BloodGroup", StringType(), expr = 'null')
    .withColumn("ScannedUserRegionID", IntegerType(), expr = 'null')
    .withColumn("ScannedDatetime", DateType(),expr = 'null')
    .withColumn("PhotoreceivedDate", DateType(), expr='null')
    .withColumn("PayeeName", StringType(), values=[fake.name() for i in range(row_num)],random=True)
    .withColumn("BankID", IntegerType(), expr = 'null')
    .withColumn("AccountNo", StringType(), values = [0])  
    .withColumn("AcountTypeID", IntegerType(), minValue=0, maxValue=1)
    .withColumn("isVIP", IntegerType(), values=[1, 183, 184],random=True)
    .withColumn("isNIDB", BooleanType(), values=[False])
    .withColumn("isSuspicious", BooleanType(), values=[False])
    .withColumn("InvestigationID", IntegerType(), values = [0])
    .withColumn("ProposerName", StringType(), expr = 'null')
    .withColumn("NomineeName", StringType(), expr = 'null')
    .withColumn("AssigneeName", StringType(), expr = 'null')
    .withColumn("PayScale", StringType(), expr = 'null')
    .withColumn("BatchID", IntegerType(), expr = 'null')
    .withColumn("Notes", StringType(), values=["Policy Data"]*row_num)
    .withColumn("Remarks", StringType(), values=["Member enrolled as per mail received from Kruthika R on Thu 04-07-2024 19:53" for _ in range(row_num)])
    .withColumn('number', StringType(), values=number_1 , omit = True)
    .withColumn("UserName", StringType(), expr="concat(number,'@ctspl')")
    .withColumn("Password", StringType(), expr = 'null')
    .withColumn("Ins_Familyno", IntegerType(),values = number_1)
    .withColumn("Ins_SerialNo", IntegerType(), minValue=0, maxValue=2)
    .withColumn("Ins_Personid",StringType(), expr="concat('MEMBER', cast(number as int))")
    .withColumn("Ins_EmployeeID", IntegerType(), expr = 'null')
    .withColumn("Partycode", IntegerType(), expr = 'null')
    .withColumn("FamilySlNo", IntegerType(), expr = 'null')
    .withColumn("FamilyNo", IntegerType(), expr = 'null')
    .withColumn("RiskID", IntegerType(), values = [0])
    .withColumn("Branch", StringType(), expr = 'null')
    .withColumn("Zone", StringType(), expr = 'null')
    .withColumn("DeviationIDs_P32", StringType(), expr = 'null')
    .withColumn("Deleted", IntegerType(), values = [0,1], weights=[0.8, 0.2], random = True )
    .withColumn("MIG_MemberDetailsID", IntegerType(), expr = 'null')
    .withColumn("CreatedBy", IntegerType(), minValue=2996, maxValue=10248 , random = True)
    .withColumn("CreatedDateTime", DateType(), expr="null")
    .withColumn("ModifiedBy", StringType(), expr = 'null')
    .withColumn("ModifiedDateTime", DateType(), expr="null")
    .withColumn("DeletedBy" , StringType(), expr = "null")
    .withColumn("DeletedDatetime" , StringType() , expr = 'null')
    .withColumn("PortingId", IntegerType() , values = [0])
    .withColumn("Deductible", IntegerType(), values = [0])
    .withColumn("Ecard_ChangePassword", BooleanType() , values = [False])
    .withColumn("AgentCode", StringType(), expr = 'null')
    .withColumn("PortingFileID" , IntegerType() , minValue = 476760 , maxValue = 476762)
    .withColumn("PortabilityNotes", StringType() , expr = 'null')
    .withColumn("LegalFlag", StringType() , expr = 'null')
    .withColumn("NIDB_RemovedDate", StringType() , expr = 'null')
    .withColumn("NIDB_UpdatedDate", StringType() , expr = 'null')
    .withColumn("DiscontinueReasonID_P80" , StringType() , expr = 'null')
    .withColumn("NIDB_RemovedBy" , StringType() , expr = 'null')
    .withColumn("TXT_Category", StringType() , expr = 'null')
    .withColumn("DummyUHID" , StringType() , expr = 'null')
    .withColumn("ABHA_ID", StringType() , expr = 'null')
    .withColumn("UserLoginRemarks" , StringType() , expr = 'null')
    .withColumn("OptionalCover" , StringType() , expr = 'null')
    .withColumn("IsOptionalCover" , StringType() , expr = 'null')
   )

MemberPolicy = member_policy_spec.build()
MemberPolicy.display()


# COMMAND ----------

row_count = 10 
Mst_Broker_spec = (
    dg.DataGenerator(spark, name="Mst_Broker", rows=row_count, seedColumnName="baseId")
    .withColumn("ID", IntegerType(), minValue=1, maxValue=10)
    .withColumn("Name", StringType(), values=[
        'Birla Sunlife Insurance Brokers', 'Vantage India Brokers', 'Aditya Birla Insurance Brokers',
        'AON Global Insurance Brokers', 'jn', 'A Siluvai Muthu', 'Achyut Corporate',
        'ACME Insurance Broking', 'Alegion Insurance Broking', 'Alert Insurance Brokers'])
    .withColumn("Deleted", BooleanType(), values=[0, 1])
    .withColumn("BrokerCode", StringType(), expr="null")
    .withColumn("McareAgentID", IntegerType(), minValue=1, maxValue=10, percentNulls=0.5 , random= True)
    .withColumn("Username", StringType(), expr="null")
    .withColumn("Password", StringType(), expr="null")
    .withColumn("ParentID", IntegerType(), expr="null")
    .withColumn("IRDACert_No", StringType(), expr="null")
    .withColumn("IRDALicense_No", StringType(), expr="null")
    .withColumn("DetailedAnalysis", StringType(), expr="null")
    .withColumn("McareAgentDetailsID", StringType(), expr="null")
    .withColumn("CreatedDateTime", DateType(), expr="null")
    .withColumn("CreatedUserRegionID", IntegerType(), minValue=1, maxValue=10, random=True)
    .withColumn("ModifiedDateTime", DateType(), expr="null")
    .withColumn("ModifiedUserRegionID", IntegerType(), expr="null")
    .withColumn("DeletedDateTime", DateType(), expr="null")
    .withColumn("DeletedUserRegionID", IntegerType(), expr="null")
)

Mst_Broker = Mst_Broker_spec.build()
display(Mst_Broker)

# COMMAND ----------

num_rows=1000

# COMMAND ----------

data_gen = dg.DataGenerator(spark,name="intimation_data",rows = num_rows,seedColumnName="unique_id") \
    .withColumn("ID", IntegerType(), minValue=1156000,maxValue = 1156000+num_rows) \
    .withColumn("PresentComplaint", StringType(), values=probable_diagnoses,random = True ) \
    .withColumn("ProbableDiagnosis", StringType(), values=probable_diagnoses,random = True ) \
    .withColumn("Ailment", StringType(), values=probable_diagnoses,random = True ) \
    .withColumn("Remarks", StringType(),values = probable_diagnoses,random = True ) \
    .withColumn("IntimationDate", TimestampType(), begin="2023-01-01 00:00:01", end="2024-07-18 23:59:59") \
    .withColumn("ProbableDOA", TimestampType(), begin="2023-01-01 00:00:01", end="2024-07-18 23:59:59") \
    .withColumn("DurationOfAilment", IntegerType(), minValue=0, maxValue=7,random = True ) \
    .withColumn("EstimatedCost", IntegerType(), minValue=10000, maxValue=100000,random=True) \
    .withColumn("CreatedDateTime", TimestampType(), begin="2023-01-01 00:00:01", end="2024-07-18 23:59:59") \
    .withColumn("CreatedOperatorID", IntegerType(), minValue=1, maxValue=13000,random = True) \
    .withColumn("ModifiedDateTime", TimestampType(), begin="2023-01-01 00:00:01", end="2024-07-18 23:59:59") \
    .withColumn("ModifiedOperatorID", IntegerType(), minValue=1, maxValue=13000,random = True) \
    .withColumn("Deleted", IntegerType(),values= [0,1],weights = [0.90,0.10], random=True) \
    .withColumn("MemberPolicyID", IntegerType(), minValue=4442000, maxValue=4443000,random = True ) \
    .withColumn("ProviderID", IntegerType(),minValue=214500, maxValue=214500+num_rows,random = True ) \
    .withColumn("ClaimID", LongType(), minValue=24070402000, maxValue=24070403000, random=True ) \
    .withColumn("OtherProviderName", StringType(), values = hospital_name, random=True) \
    .withColumn("OtherProviderAddress", StringType(), values=[fake.address() for _ in range(num_rows)],random = True) \
    .withColumn("ModeOfCommunicationID", IntegerType(), values = [3,5,6],random=True) \
    .withColumn("ContactPersonName", StringType(), values = [fake.name() for _ in range(num_rows)],random = True ) \
    .withColumn("ContactPersonMobileNo", StringType(), values= [fake.phone_number() for i in range(num_rows)],random = True ) \
    .withColumn("ContactPersonEmailID", StringType(),values = email_ids,random = True ) \
    .withColumn("ContactPersonRelationShipID", IntegerType(), minValue=1, maxValue=40,random = True ) \
    .withColumn("IsClosed", BooleanType(), random=True) \
    .withColumn("StatusReasonID", IntegerType(), minValue=1, maxValue=20,random = True ) \
    .withColumn("ISSUEID", IntegerType(), minValue=1, maxValue=100,random = True ) \
    .withColumn("OldProviderID", IntegerType(), minValue=1, maxValue=20,random = True ) \
    .withColumn("ClaimTypeID", IntegerType(),values=claim_types,random = True) \
    .withColumn("StatusID", IntegerType(), values=[14,19,279],random = True) \
    .withColumn("StatusChangeDatetime", TimestampType(), begin="2023-01-01 00:00:01", end="2024-07-18 23:59:59") \
    .withColumn("StatusChangedBy", IntegerType(), values=[1,191,12602,12603,12843],random = True) \
    .withColumn("StatusChangeRemarks", StringType(), values = status_change_remarks_list,random=True) \
    .withColumn("ClosedOrRejectionReasonsID_P69_P70", IntegerType(), minValue=1, maxValue=20,random = True ) \
    .withColumn("InsurerClaimID", IntegerType(),minValue=1, maxValue=30,random = True ) \
    .withColumn("InsurerIntimationID", IntegerType(),minValue=1, maxValue=30,random = True  )

Intimation = data_gen.build()
Intimation.display()

# COMMAND ----------

titles = [1, 2]  # 1 for Mr., 2 for Ms.
genders = [1, 2]  # 1 for Male, 2 for Female
blood_groups = [1, 2, 3, 4]  # 1 for A+, 2 for B+, etc.
marital_statuses = [1, 2]  # 1 for Single, 2 for Married
designations = [1, 2, 3]  # 1 for Manager, 2 for Developer, etc.
departments = [1, 2, 3]  # 1 for HR, 2 for Finance, etc.
operator_categories = [1, 2]  # 1 for Admin, 2 for User
states = [
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26
]  # IDs for Indian states
countries = [1]  # 1 for India
employment_statuses = [1, 2]  # 1 for Active, 2 for Inactive
roles = [1, 2]  # 1 for Admin, 2 for User
login_types = [1, 2]  # 1 for Internal, 2 for External

data_gen = dg.DataGenerator(spark, name="mst_users_data", rows=num_rows, seedColumnName="unique_id") \
    .withColumn("ID", IntegerType(), minValue=1, maxValue=num_rows) \
    .withColumn("TitleID", IntegerType(), values=titles, random=True) \
    .withColumn("Name", StringType(), values=[fake.name() for _ in range(num_rows)], random=True) \
    .withColumn("GenderID", IntegerType(), values=genders, random=True) \
    .withColumn("BloodGroupID", IntegerType(), values=blood_groups, random=True) \
    .withColumn("EmployeePassportNo", StringType(), values=[fake.ssn() for _ in range(num_rows)], random=True) \
    .withColumn("IssuedDate", TimestampType(), begin="2000-01-01 00:00:00", end="2024-12-31 23:59:59") \
    .withColumn("PlaceOfIssue", StringType(), values=[fake.city() for _ in range(num_rows)], random=True) \
    .withColumn("ExpiryDate", TimestampType(), begin="2025-01-01 00:00:00", end="2040-12-31 23:59:59") \
    .withColumn("IssuedCountry", StringType(), values=['India' for _ in range(num_rows)]) \
    .withColumn("DateOfBirth", TimestampType(), begin="1900-01-01 00:00:00", end="2005-12-31 23:59:59") \
    .withColumn("MaritalStatusID", IntegerType(), values=marital_statuses, random=True) \
    .withColumn("DesignationID", IntegerType(), values=designations, random=True) \
    .withColumn("DepartmentID", IntegerType(), values=departments, random=True) \
    .withColumn("OperatorCategoryID", IntegerType(), values=operator_categories, random=True) \
    .withColumn("STDCode", StringType(), values=[fake.country_code() for _ in range(num_rows)], random=True) \
    .withColumn("PhoneNo", StringType(), values=[fake.phone_number() for _ in range(num_rows)], random=True) \
    .withColumn("MobileNo", StringType(), values=[fake.phone_number() for _ in range(num_rows)], random=True) \
    .withColumn("Email", StringType(), values=email_ids, random=True) \
    .withColumn("AddressLine1", StringType(), values=[fake.street_address() for _ in range(num_rows)], random=True) \
    .withColumn("AddressLine2", StringType(), values=[fake.address() for _ in range(num_rows)], random=True) \
    .withColumn("Place", StringType(), values=[random.choice(indian_states) for _ in range(num_rows)], random=True) \
    .withColumn("AdminUnitID", IntegerType(), minValue=1, maxValue=1000, random=True) \
    .withColumn("DistrictID", IntegerType(), minValue=1, maxValue=1000, random=True) \
    .withColumn("StateID", IntegerType(), values=states, random=True) \
    .withColumn("PinCode", StringType(), values=[fake.postcode() for _ in range(num_rows)], random=True) \
    .withColumn("CountryID", IntegerType(), values=countries, random=True) \
    .withColumn("EmployeeNo", StringType(), values=[fake.random_number(digits=6) for _ in range(num_rows)], random=True) \
    .withColumn("TPAID", IntegerType(), minValue=1, maxValue=1000, random=True) \
    .withColumn("EmploymentStatus", IntegerType(), values=employment_statuses, random=True) \
    .withColumn("DateOfJoining", TimestampType(), begin="2000-01-01 00:00:00", end="2024-12-31 23:59:59") \
    .withColumn("ESIC", StringType(), values=[fake.random_number(digits=12) for _ in range(num_rows)], random=True) \
    .withColumn("BankID", IntegerType(), minValue=10000, maxValue=1000000, random=True) \
    .withColumn("BankAccountNo", StringType(), values=[fake.random_number(digits=10) for _ in range(num_rows)], random=True) \
    .withColumn("PanNo", StringType(), values=[fake.bban() for _ in range(num_rows)], random=True) \
    .withColumn("EPFNo", StringType(), values=[fake.random_number(digits=12) for _ in range(num_rows)], random=True) \
    .withColumn("UniqueSmartCardNo", StringType(), values=[fake.random_number(digits=16) for _ in range(num_rows)], random=True) \
    .withColumn("LoginID", StringType(), values=[fake.user_name() for _ in range(num_rows)], random=True) \
    .withColumn("Password", StringType(), values=[fake.password() for _ in range(num_rows)], random=True) \
    .withColumn("CreatedDateTime", TimestampType(), begin="2023-01-01 00:00:00", end="2024-07-23 23:59:59") \
    .withColumn("CreatedOperatorID", IntegerType(), minValue=1, maxValue=1000, random=True) \
    .withColumn("ModifiedDateTime", TimestampType(), begin="2023-01-01 00:00:00", end="2024-07-23 23:59:59") \
    .withColumn("ModifiedOperatorID", IntegerType(), minValue=1, maxValue=1000, random=True) \
    .withColumn("DeletedDateTime", TimestampType(), begin="2023-01-01 00:00:00", end="2024-07-23 23:59:59") \
    .withColumn("DeletedOperatorID", IntegerType(), minValue=1, maxValue=1000, random=True) \
    .withColumn("Deleted", IntegerType(), values = [0,1],weights = [0.90,0.10], random=True) \
    .withColumn("rowguid", StringType(), values= [None]) \
    .withColumn("rowguid50", StringType(), values= [None]) \
    .withColumn("LastLoginDateTime", TimestampType(), begin="2023-01-01 00:00:00", end="2024-07-23 23:59:59") \
    .withColumn("LastLogOutDateTime", TimestampType(), begin="2023-01-01 00:00:00", end="2024-07-23 23:59:59") \
    .withColumn("DashboardFlag", IntegerType(),values= [None]) \
    .withColumn("OfficeID", IntegerType(), values= [None]) \
    .withColumn("ReportingToUserID", IntegerType(), values= [None for _ in range(num_rows)]) \
    .withColumn("EffectiveDate", DateType(), begin="2023-01-01", end="2024-07-23") \
    .withColumn("authorizedMinAmount", IntegerType(), minValue=0, maxValue=10000, random=True) \
    .withColumn("AuthorizedMaxAmount", IntegerType(), minValue=0, maxValue=100000, random=True) \
    .withColumn("DefaultRoleID", IntegerType(), values=[13, 17], random=True) \
    .withColumn("ISSUEID", IntegerType(), minValue=1, maxValue=100,random = True)\
    .withColumn("LoginType", IntegerType(), values=login_types, random=True) \
    .withColumn("IsAdmin", IntegerType(),values = [0,1],weights = [0.10,0.90], random=True) \
    .withColumn("LoginFailedCount", IntegerType(), minValue=0, maxValue=10, random=True) \
    .withColumn("AccountLockedDatetime", TimestampType(), begin="2023-01-01 00:00:00", end="2024-07-23 23:59:59") \
    .withColumn("PasswordResetDatetime", TimestampType(), begin="2023-01-01 00:00:00", end="2024-07-23 23:59:59") \
    .withColumn("IsCaptchaRequired", BooleanType(), random=True) \
    .withColumn("ClaimTypes", StringType(), values=[None for _ in range(num_rows)])

Mst_Users = data_gen.build()
Mst_Users.display()

# COMMAND ----------

data_gen = dg.DataGenerator(spark, name="tbl_claims_inward_request_data", rows=num_rows, seedColumnName="unique_id") \
    .withColumn("ID", IntegerType(), minValue=1, maxValue=num_rows) \
    .withColumn("ReceivedBy", StringType(), values=received_list, random=True) \
    .withColumn("ReceivedDate", TimestampType(), begin="2020-01-01 00:00:00", end="2024-12-31 23:59:59") \
    .withColumn("ReceivedModeID", IntegerType(), values=received_modes, random=True) \
    .withColumn("SenderTypeID", IntegerType(), values=sender_types, random=True) \
    .withColumn("SenderName", StringType(), values=[fake.name() for _ in range(num_rows)], random=True) \
    .withColumn("ConsignmentNo", StringType(), values=[None for _ in range(num_rows)], random=True) \
    .withColumn("CourierCompanyID", IntegerType(), minValue=1, maxValue=10, random=True) \
    .withColumn("DocumentTypeID", IntegerType(), values=document_types, random=True) \
    .withColumn("ClaimID", LongType(), minValue=24070402000, maxValue=24070403000, random=True) \
    .withColumn("SlNo", IntegerType(), minValue=1, maxValue=5, random=True) \
    .withColumn("ClaimTypeID", IntegerType(), values=claim_types, random=True) \
    .withColumn("RequestTypeID", IntegerType(), values=request_types, random=True) \
    .withColumn("UhidNo", StringType(), values = [fake.uuid4().upper() for _ in range(num_rows)], random=True) \
    .withColumn("EmployeeID", IntegerType(), minValue=1, maxValue=1000, random=True) \
    .withColumn("MemberName", StringType(), values=[fake.name() for _ in range(num_rows)], random=True) \
    .withColumn("HospitalName", StringType(), values=hospital_name, random=True) \
    .withColumn("CorpID", IntegerType(), minValue=1, maxValue=100, random=True) \
    .withColumn("IssueID", IntegerType(),minValue=1, maxValue=100, random=True) \
    .withColumn("IsSoftCopyAvailable", values = [0,1],weights = [0.10,0.90], random=True) \
    .withColumn("StatusID", IntegerType(), values=statuses, random=True) \
    .withColumn("ClaimedAmount", IntegerType(), minValue=1000, maxValue=100000, random=True) \
    .withColumn("MobileNo", StringType(), values=[fake.phone_number() for _ in range(num_rows)], random=True) \
    .withColumn("ClaimDocUploadRefID", IntegerType(),minValue=32000, maxValue=33000, random=True) \
    .withColumn("Deleted",IntegerType(), values = [0,1],weights = [0.90,0.10], random=True) \
    .withColumn("CreatedUserRegionID", IntegerType(), minValue=1, maxValue=10, random=True) \
    .withColumn("CreatedDateTime", TimestampType(), begin="2023-01-01 00:00:00", end="2024-12-31 23:59:59") \
    .withColumn("ModifiedUserRegionID", IntegerType(), minValue=1, maxValue=5, random=True) \
    .withColumn("ModifiedDateTime", TimestampType(), begin="2023-01-01 00:00:00", end="2024-12-31 23:59:59") \
    .withColumn("DeletedUserRegionID", IntegerType(), minValue=1, maxValue=5, random=True) \
    .withColumn("DeletedDateTime", TimestampType(), begin="2023-01-01 00:00:00", end="2024-12-31 23:59:59") \
    .withColumn("DateOfAdmission", TimestampType(), begin="2020-01-01 00:00:00", end="2024-12-31 23:59:59") \
    .withColumn("PolicyNumber", StringType(), values=policy_numbers, random=True)

tbl_ClaimsInwardRequest = data_gen.build()
tbl_ClaimsInwardRequest.display()

# COMMAND ----------


data_gen = dg.DataGenerator(spark, name="TPAProcedures_data", rows=num_rows, seedColumnName="unique_id") \
    .withColumn("ID",IntegerType(), minValue=300, maxValue=300+num_rows) \
    .withColumn("Level1", StringType(), values=[
        "Cardiothorasic Surgery and Cardiology Procedures", "General Surgery Procedures", 
        "Orthopedic Surgery Procedures", "Neurological Surgery Procedures", 
        "Gastroenterology Procedures", "Urological Procedures", "Endocrinology Procedures", 
        "Pulmonary Medicine Procedures", "Dermatology Procedures", "Ophthalmology Procedures"], random=True) \
    .withColumn("Level2", StringType(), values=[
        "CAD-Angioplasty", "CAD-Coronary Artery Bypass Grafting", "CAD-Heart Valve Replacement",
        "CAD-Heart Valve Repair", "CAD-Transcatheter Aortic Valve Replacement (TAVR)",
        "Cardiomyopathy Treatment", "Myocarditis Treatment", "Pericardial Effusion Treatment",
        "Arrhythmia Management", "Heart Failure Management", "Appendectomy", "Cholecystectomy",
        "Hernia Repair", "Gastrectomy", "Colorectal Resection", "Thyroidectomy", "Mastectomy",
        "Bariatric Surgery", "Abdominal Aortic Aneurysm Repair", "Splenectomy", "Hip Replacement",
        "Knee Replacement", "Shoulder Arthroscopy", "Spinal Fusion", "Ankle Arthroscopy",
        "Fracture Fixation", "Meniscus Repair", "Rotator Cuff Repair", "Elbow Arthroscopy",
        "Osteotomy", "Craniotomy", "Spinal Cord Surgery", "Deep Brain Stimulation", 
        "Epilepsy Surgery", "Brain Tumor Resection", "Pituitary Tumor Surgery", 
        "Carpal Tunnel Release", "Peripheral Nerve Surgery", "Vascular Neurosurgery", 
        "Endoscopic Neurosurgery", "Endoscopy", "Colonoscopy", "ERCP (Endoscopic Retrograde Cholangiopancreatography)", 
        "Gastroscopy", "Capsule Endoscopy", "Liver Biopsy", "Paracentesis", "Esophageal Dilation", 
        "Gastrostomy Tube Placement", "Polypectomy", "Cystoscopy", "Ureteroscopy", "Prostate Biopsy", 
        "Nephrectomy", "TURP (Transurethral Resection of the Prostate)", "Vasectomy", "Lithotripsy", 
        "Bladder Augmentation", "Urethral Reconstruction", "Kidney Stone Removal", "Thyroid Biopsy", 
        "Adrenalectomy", "Pituitary Gland Surgery", "Insulin Pump Therapy", "Hormone Replacement Therapy", 
        "Parathyroidectomy", "Diabetes Management", "Cushing's Syndrome Treatment", "Growth Hormone Therapy", 
        "Hyperthyroidism Treatment", "Bronchoscopy", "Pulmonary Function Testing", "Chest Tube Insertion", 
        "Lung Biopsy", "Sleep Study", "Oxygen Therapy", "Ventilator Management", "Asthma Management", 
        "COPD Management", "Pleural Drainage", "Skin Biopsy", "Mohs Micrographic Surgery", 
        "Laser Skin Resurfacing", "Cryotherapy", "Electrosurgery", "Chemical Peels", "Acne Treatment", 
        "Psoriasis Treatment", "Eczema Treatment", "Tattoo Removal", "Cataract Surgery", 
        "LASIK Eye Surgery", "Retinal Detachment Surgery", "Glaucoma Treatment", "Corneal Transplant", 
        "Vitrectomy", "Eye Exam and Refraction", "Pterygium Surgery", "Strabismus Surgery", 
        "Macular Degeneration Treatment"], random=True) \
    .withColumn("Level3", StringType(), values=[
        "Cardiothoracic Surgery and Cardiology Procedures", "Angioplasty with IABP Pump", 
        "Angioplasty with Stent - Single Site", "Angioplasty with Stent - Two Sites", 
        "Angioplasty with Stent - Three Sites", "Angioplasty with Drug Eluting Stent - Single Site", 
        "Angioplasty with Drug Eluting Stent - Two Sites", "Angioplasty with Drug Eluting Stent - Three Sites", 
        "Balloon Angioplasty without Stent - Single Site", "Coronary Artery Bypass Grafting", 
        "Heart Valve Replacement", "Heart Valve Repair", "Transcatheter Aortic Valve Replacement (TAVR)", 
        "Cardiomyopathy Treatment", "Myocarditis Treatment", "Pericardial Effusion Treatment", 
        "Arrhythmia Management", "Heart Failure Management", "Myocardial Infarction Treatment", 
        "Endovascular Aneurysm Repair"], random=True) \
    .withColumn("Code", StringType(), values=["1001", "1002","1003"], random=True) \
    .withColumn("FHPLCode", StringType(), values=["0", "1.1.1", "1.1.2", "1.1.3", "1.1.4", "1.1.5", "1.1.6", "1.1.7", "1.1.8"], random=True) \
    .withColumn("ParentID", IntegerType(), minValue=0, maxValue=20,random=True) \
    .withColumn("TreatmentType_P19", IntegerType(), values=treatment_types, random=True) \
    .withColumn("Inclusions", StringType(), values=[None]) \
    .withColumn("Exclusions", StringType(), values=[None]) \
    .withColumn("LOS", StringType(), values=[None]) \
    .withColumn("EffectiveDate", TimestampType(), begin="2023-01-01 00:00:00", end="2024-07-23 23:59:59") \
    .withColumn("isCI",values = [0,1],weights = [0.05,0.95], random=True) \
    .withColumn("isDayCare", values = [0,1],weights = [0.05,0.95], random=True) \
    .withColumn("isPED", values = [0,1],weights = [0.05,0.95], random=True) \
    .withColumn("isGIPSA", values = [0,1],weights = [0.10,0.90], random=True) \
    .withColumn("ICDCode", StringType(), values=icd_codes, random=True) \
    .withColumn("PCSCode", StringType(), values=pcs_codes, random=True) \
    .withColumn("Deleted", values = [0,1],weights = [0.90,0.10], random=True) \
    .withColumn("TypeofTreatment_P19", StringType(), values=treatment_types, random=True) \
    .withColumn("PPNCode", StringType(),values=["Test Code" for _ in range(num_rows)], random=True) \
    .withColumn("PPNDescription", StringType(), values=["Test Desc" for _ in range(num_rows)], random=True) \
    .withColumn("Level", IntegerType(), values=levels, random=True)

TPAProcedures = data_gen.build()
TPAProcedures.display()

# COMMAND ----------

data_gen = dg.DataGenerator(spark, name="ProviderBankDetails_data", rows=num_rows, seedColumnName="unique_id") \
    .withColumn("ID", IntegerType(), minValue=1117000, maxValue=1117000+num_rows) \
    .withColumn("ProviderID", IntegerType(), minValue=214500, maxValue=214500+num_rows,random = True) \
    .withColumn("BankID", IntegerType(), minValue=10000, maxValue=1000000, random=True) \
    .withColumn("AccountNumber", StringType(), minValue=1000000000,maxValue=9999999999, random=True) \
    .withColumn("AccountTypeID", IntegerType(), values=[1, 2], random=True) \
    .withColumn("ChequeDMSID", IntegerType(), minValue=10000,maxValue=99999, random=True) \
    .withColumn("Payeename", StringType(), values=[
        "Aasha Hospitals", "APOLLO HOSPITALS ENTERPRISE LTD", "Balaji Multispeciality Hospital",
        "Dbr Sk Super Speciality Hosptials", "DBR SANJANA KRISHNA HOSPITALS PRIVATE LIMITED",
        "Sree Ramadevi Multi Super Speciality Hospital"
    ], random=True) \
    .withColumn("EffectiveDate", TimestampType(), begin="2023-01-01 00:00:00", end="2024-07-23 23:59:59") \
    .withColumn("Deleted", IntegerType(), values=[0, 1], weights=[0.9, 0.1], random=True) \
    .withColumn("Createddatetime", StringType(), values=[datetime.now().strftime("%H:%M:%S") for _ in range(num_rows)], random=True) \
    .withColumn("CreatedUserRegionID", IntegerType(), minValue=1, maxValue=10, random=True) \
    .withColumn("DeletedUserRegionID", IntegerType(), minValue=1, maxValue=5, random=True) \
    .withColumn("Deleteddatetime", StringType(), values=[datetime.now().strftime("%H:%M:%S") for _ in range(num_rows)], random=True) \
    .withColumn("MobileNo", StringType(), values= [fake.phone_number() for i in range(num_rows)],random = True) \
    .withColumn("AltMobileNo", StringType(), values= [fake.phone_number() for i in range(num_rows)],random = True) \
    .withColumn("Emailid", StringType(), values=email_ids, random=True) \
    .withColumn("SecEmailID", StringType(), values=email_ids, random=True) \
    .withColumn("IssueID", IntegerType(), minValue=1, maxValue=100,random = True) \
    .withColumn("ModifiedUserRegionID", IntegerType(),minValue=1, maxValue=5,random=True) \
    .withColumn("Modifieddatetime", StringType(), values=[datetime.now().strftime("%H:%M:%S") for _ in range(num_rows)], random=True)

ProviderBankDetails = data_gen.build()
ProviderBankDetails.display()

# COMMAND ----------

data_gen = dg.DataGenerator(spark, name="Mst_Corporate_data", rows=num_rows, seedColumnName="unique_id") \
    .withColumn("ID", IntegerType(), minValue=1, maxValue=num_rows) \
    .withColumn("Name", StringType(), values=[
        "Test Corporate", "Uttaranchal Deep Publication", "Boston Software Consultant India Pvt Ltd",
        "Sabmiller India Ltd", "Engineering Graphics Services", "Gramya Vikas Mancha",
        "The Future Kids School", "Corona Bus Manufacturers Pvt Ltd", "Wittybrains Software Technologies Pvt Ltd",
        "Doaba Public Senior Secondary School"
    ], random=True) \
    .withColumn("ParentID", IntegerType(), minValue=0, maxValue=20,random=True) \
    .withColumn("Hierarchy", StringType(), values=["HO", "Branch"], random=True) \
    .withColumn("RegistrationNo", StringType(), values=[None] + [str(random.randint(100000000, 999999999)) for _ in range(num_rows)], random=True) \
    .withColumn("PanNo", StringType(), values=[None] + [f"{''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=5))}{random.randint(1000, 9999)}{random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}" for _ in range(num_rows)], random=True) \
    .withColumn("CODE", StringType(), values=[None] + [str(random.randint(100, 999)) for _ in range(num_rows)], random=True) \
    .withColumn("Preference", IntegerType(), minValue=1, maxValue=100) \
    .withColumn("SmallName", StringType(), values=["TC", "UTDP", None], random=True) \
    .withColumn("Grades", StringType(), values=["G1", "G2", "G3"], random=True) \
    .withColumn("Designations", StringType(), values=["Manager", "Executive", "Clerk"], random=True) \
    .withColumn("Units", StringType(), values=["Unit1", "Unit2", "Unit3"], random=True) \
    .withColumn("Divisions", StringType(), values=["Div1", "Div2", "Div3"], random=True) \
    .withColumn("EffectiveDate", TimestampType(), begin="2023-01-01 00:00:00", end="2024-07-23 23:59:59") \
    .withColumn("Remarks", StringType(), values=[None, "Test"], random=True) \
    .withColumn("UserName", StringType(), values=[None] + ["User" + str(i) for i in range(num_rows)], random=True) \
    .withColumn("Password", StringType(), values=[None] + ["Pwd" + str(i) for i in range(num_rows)], random=True) \
    .withColumn("TPAFees", IntegerType(), minValue=1, maxValue=100) \
    .withColumn("PrimaryLogo", StringType(), values=[None, "Logo1", "Logo2"], random=True) \
    .withColumn("SecondaryLogo", StringType(), values=[None, "Logo1", "Logo2"], random=True) \
    .withColumn("CreatedDateTime", TimestampType(), begin="2015-01-01 00:00:00", end="2016-12-31 23:59:59") \
    .withColumn("CreatedOperatorID", IntegerType(), minValue=1, maxValue=100) \
    .withColumn("ModifyDateTime", TimestampType(), begin="2015-01-01 00:00:00", end="2016-12-31 23:59:59") \
    .withColumn("ModifyOperatorID", IntegerType(), minValue=1, maxValue=100) \
    .withColumn("DeletedDateTime", TimestampType(), begin="2015-01-01 00:00:00", end="2016-12-31 23:59:59", values=[None], random=True) \
    .withColumn("DeletedOperatorID", IntegerType(), minValue=1, maxValue=100, values=[None], random=True) \
    .withColumn("Deleted", IntegerType(), values=[0, 1], weights=[0.9, 0.1], random=True) \
    .withColumn("MIG_InsuredGroupID", IntegerType(), minValue=1, maxValue=100, values=[None], random=True) \
    .withColumn("LocationId", IntegerType(), minValue=1, maxValue=100) \
    .withColumn("IndustryTypeId", IntegerType(), minValue=1, maxValue=100) \
    .withColumn("FHPlusLoginEnabled", IntegerType(), values=[0, 1], random=True) \
    .withColumn("PwdPolicyEnabled", IntegerType(), values=[0, 1], random=True)

Mst_Corporate = data_gen.build()
Mst_Corporate.display()

# COMMAND ----------

data_gen = dg.DataGenerator(spark, name="Request", rows=num_rows, seedColumnName="unique_id") \
    .withColumn("ID", IntegerType(), minValue=1, maxValue=num_rows) \
    .withColumn("ReceivedDate", TimestampType(), begin="2023-01-01 00:00:00", end="2024-07-23 23:59:59") \
    .withColumn("ReceivedBy", StringType(), values=["Administrator", "Padma G", "Hyndavi Toka"], random=True) \
    .withColumn("ModeofDeliverID", IntegerType(), minValue=1, maxValue=3) \
    .withColumn("DeliveryService", StringType(), values=email_ids, random=True) \
    .withColumn("SenderTypeID", IntegerType(), minValue=1, maxValue=10) \
    .withColumn("SenderID", IntegerType(), minValue=1, maxValue=100, random=True) \
    .withColumn("ConsignmentNo", StringType(), values=[None]) \
    .withColumn("LotNo", StringType(), values=[None, "Lot1", "Lot2", "Lot3"], random=True) \
    .withColumn("LotReceivedNos", IntegerType(), minValue=1, maxValue=100, random=True) \
    .withColumn("IssueID", IntegerType(), minValue=1, maxValue=100, random=True) \
    .withColumn("CorpID", IntegerType(), minValue=1, maxValue=10) \
    .withColumn("PolicyID", IntegerType(), minValue=4411218,maxValue=4688326, random=True) \
    .withColumn("BrokerID", IntegerType(), minValue=1, maxValue=100, random=True) \
    .withColumn("BenefitPlanID", IntegerType(), minValue=1, maxValue=100, random=True) \
    .withColumn("PolicyNo", IntegerType(), minValue=1001620000, maxValue=1001900000, random=True) \
    .withColumn("StartDate", TimestampType(), begin="2023-01-01 00:00:00", end="2024-07-23 23:59:59") \
    .withColumn("EndDate", TimestampType(), begin="2023-01-01 00:00:00", end="2024-07-23 23:59:59") \
    .withColumn("CoverageTypeID_P21", IntegerType(), minValue=1, maxValue=100) \
    .withColumn("PayeeName", StringType(), values=["Sabmiller India Ltd", None], random=True) \
    .withColumn("BankName", StringType(), values=[None, "Bank1", "Bank2", "Bank3"], random=True) \
    .withColumn("BranchName", StringType(), values=[None, "Branch1", "Branch2", "Branch3"], random=True) \
    .withColumn("BankAddress", StringType(), values=[None, "Address1", "Address2", "Address3"], random=True) \
    .withColumn("IFSCCode", StringType(), values=[None, "IFSC1", "IFSC2", "IFSC3"], random=True) \
    .withColumn("MICRCode", StringType(), values=[None, "MICR1", "MICR2", "MICR3"], random=True) \
    .withColumn("AccountNo", StringType(), values=[None, "52071925449", "52071925450", "52071925451"], random=True) \
    .withColumn("AccountTypeID", IntegerType(), minValue=1, maxValue=5) \
    .withColumn("BusinessSourceID", IntegerType(), minValue=1, maxValue=5) \
    .withColumn("PayeeTypeID", IntegerType(), minValue=1, maxValue=5) \
    .withColumn("CardTypeID", IntegerType(), minValue=1, maxValue=5) \
    .withColumn("DocumentTypeid", IntegerType(), minValue=1, maxValue=5) \
    .withColumn("RenewalFlag", IntegerType(), values=[0, 1], random=True) \
    .withColumn("ToEmailID", StringType(), values=[None, "email1@example.com", "email2@example.com", "email3@example.com"], random=True) \
    .withColumn("CCEmailID", StringType(), values=[None, "ccemail1@example.com", "ccemail2@example.com", "ccemail3@example.com"], random=True) \
    .withColumn("BCCEmailID", StringType(), values=[None, "bccemail1@example.com", "bccemail2@example.com", "bccemail3@example.com"], random=True) \
    .withColumn("MobileNumber", StringType(), values=[None, "1234567890", "9876543210", "1122334455"], random=True) \
    .withColumn("StatusID", IntegerType(), minValue=1, maxValue=5) \
    .withColumn("ForwardToRegionID", IntegerType(), minValue=1, maxValue=5) \
    .withColumn("ForwardToDeptID", IntegerType(), minValue=1, maxValue=5) \
    .withColumn("ActivityID", IntegerType(), minValue=1, maxValue=5) \
    .withColumn("Remarks", StringType(), values=[None, "Testing", "Pending", "Completed"], random=True) \
    .withColumn("PriorityID", IntegerType(), minValue=1, maxValue=5) \
    .withColumn("AutoLock", IntegerType(), values=[0, 1], random=True) \
    .withColumn("CreatedUserRegionID", IntegerType(), minValue=1, maxValue=10, random=True) \
    .withColumn("CreatedDate", TimestampType(), begin="2015-01-01 00:00:00", end="2016-12-31 23:59:59") \
    .withColumn("ModifiedUserID", IntegerType(), minValue=1, maxValue=100, random=True) \
    .withColumn("ModifiedDate", TimestampType(), begin="2015-01-01 00:00:00", end="2016-12-31 23:59:59") \
    .withColumn("ProcessBranchID", IntegerType(), minValue=1, maxValue=100, random=True) \
    .withColumn("ProposerTypeID", IntegerType(), minValue=1, maxValue=5) \
    .withColumn("PreviousPolicyID", IntegerType(), minValue=4442000, maxValue=4443000, random=True) \
    .withColumn("IssuingOfficeID", IntegerType(), minValue=1, maxValue=100, random=True) \
    .withColumn("IsPolicyNIDB", IntegerType(), values=[0, 1], random=True) \
    .withColumn("LoginTypeID", IntegerType(), minValue=1, maxValue=5) \
    .withColumn("CommunicationStatusID", IntegerType(), minValue=1, maxValue=5)

dim_request = data_gen.build()
dim_request.display()

# COMMAND ----------

num_rows=1000

data_gen = dg.DataGenerator(spark, name="MemberPortingExt", rows=num_rows, seedColumnName="unique_id") \
    .withColumn("PortingFileID", IntegerType(), minValue=1980, maxValue=2980) \
    .withColumn("IssueID", IntegerType(), minValue=1, maxValue=100, random=True) \
    .withColumn("PayerID", IntegerType(), minValue=1, maxValue=10) \
    .withColumn("PolicyNumber", StringType(), values=policy_numbers, random=True) \
    .withColumn("PlanName", StringType(), values=["EXCEL", "PREMIUM"], weights=[0.9,0.1], random=True) \
    .withColumn("IsRenewedPolicy", StringType(), values=["Yes", "No"],weights=[0.1, 0.9], random=True) \
    .withColumn("PreviousPolicyNumber", StringType(), expr="null") \
    .withColumn("PolicyStatus", StringType(), values=["Enforced"], random=True) \
    .withColumn("PolicyType", StringType(), values=["Individual", "Family Floater"], random=True) \
    .withColumn("IRDAPolicyType", IntegerType(), values=[1,2,3], random=True) \
    .withColumn("PolicyTerm_Year", IntegerType(), minValue=1, maxValue=10,random=True) \
    .withColumn("PolicyIssueDate", DateType(), begin="2023-01-01", end="2024-07-23") \
    .withColumn("Zone", StringType(), values=["North", "South", "East", "West"], random=True) \
    .withColumn("TotalCountOfInsured", IntegerType(), minValue=1, maxValue=10) \
    .withColumn("MaxAdults", IntegerType(), minValue=1, maxValue=5) \
    .withColumn("MaxDependentChildren", IntegerType(), minValue=0, maxValue=5) \
    .withColumn("SI_FloaterPolicy", IntegerType(), values=[0,300000,1000000], random=True) \
    .withColumn("ServiceType", StringType(), values=["New Business", "Renewal"], random=True) \
    .withColumn("TPAID", IntegerType(), values=[1000000001,1000000002],random=True) \
    .withColumn("TPAName", StringType(), values=["Family Health Plan (TPA) Limited"], random=True) \
    .withColumn("SubChannel", StringType(), expr="null") \
    .withColumn("CoPayPerClaim", IntegerType(), expr="null") \
    .withColumn("DeductiblePerPolicyYear", IntegerType(), values=[0]) \
    .withColumn("PPN", LongType(), minValue=201607220000000, maxValue=201607220000999 , random=True) \
    .withColumn("IFSCCode", StringType(), values=[fake.bban() for i in range(num_rows)], random=True) \
    .withColumn("MICRCode", StringType(), values=[fake.bban() for i in range(num_rows)], random=True) \
    .withColumn("BankName", StringType(), values=bank_names, random=True) \
    .withColumn("Branch", StringType(), values=branch_names, random=True) \
    .withColumn("City", StringType(), values=[fake.city() for i in range(num_rows)], random=True) \
    .withColumn("AccountNumber", StringType(), values=[random.randint(100000000000, 999999999999) for i in range(num_rows)], random=True) \
    .withColumn("NomineeName1", StringType(), values=[fake.name() for i in range(num_rows)], random=True) \
    .withColumn("Relationship1", StringType(), values=relation_list, random=True) \
    .withColumn("ProposerFullName", StringType(), values=[fake.name() for i in range(num_rows)], random=True) \
    .withColumn("ProposerTitle", StringType(), values=["Mr", "Mrs", "Ms"], random=True) \
    .withColumn("ProposerGender", StringType(), values=["Male", "Female"], random=True) \
    .withColumn("ProposerOccupation", StringType(), values=["BUSINESS", "PROFESSIONAL", "SALARIED"], random=True) \
    .withColumn("ProposerID", StringType(), minValue = 1000020000,maxValue = 1000030000, random=True) \
    .withColumn("MailingAddressLine1", StringType(), values=[fake.street_address() for i in range(num_rows)], random=True) \
    .withColumn("MailingAddressLine2", StringType(), values=[fake.address() for i in range(num_rows)], random=True) \
    .withColumn("MailingCity", StringType(), values=[fake.city() for i in range(num_rows)], random=True) \
    .withColumn("MailingTownDistrict", StringType(), values=[fake.city() for i in range(num_rows)], random=True) \
    .withColumn("MailingState", StringType(), values=indian_states, random=True) \
    .withColumn("MailingPinCode", StringType(), values=[fake.postcode() for i in range(num_rows)], random=True) \
    .withColumn("MobileNumber", StringType(),values=[fake.phone_number() for i in range(num_rows)], random=True) \
    .withColumn("ResidenceNumber", StringType(), expr="null", random=True) \
    .withColumn("OfficeNumber", StringType(), expr="null", random=True) \
    .withColumn("EmailID", StringType(), values=email_ids, random=True) \
    .withColumn("PolicyStartDate", DateType(), begin="2023-01-01", end="2024-07-23") \
    .withColumn("PolicyEndDate", DateType(), begin="2023-01-01", end="2024-07-23")\
    .withColumn("BasicCovers", StringType(), values=["YES"]) \
    .withColumn("CoverageAmt1", IntegerType(), values=[0]) \
    .withColumn("DonorExpensesCover", StringType(), values=["YES"]) \
    .withColumn("CoverageAmt2", IntegerType(),minValue=30000,maxValue=5000000, random=True) \
    .withColumn("HospitalDailyCashCover", StringType(), values=["YES"]) \
    .withColumn("CoverageAmt3", IntegerType(), values=[5000]) \
    .withColumn("RoomRentCap", StringType(), values=["NO"]) \
    .withColumn("CoverageAmt4", IntegerType(), values=[0]) \
    .withColumn("DoubleSumInsuredForHospitalizationDueToAccidentCover", StringType(), values=["NO"]) \
    .withColumn("CoverageAmt5", IntegerType(), values=[0]) \
    .withColumn("ConvalescenceBenefitCover", StringType(), values=["YES"]) \
    .withColumn("CoverageAmt6", IntegerType(), values=[10000]) \
    .withColumn("CriticalIllnessCover", StringType(), values=["NO"]) \
    .withColumn("CoverageAmt7", IntegerType(), values=[0]) \
    .withColumn("AlternateTreatmentCover", StringType(), values=["NO"]) \
    .withColumn("CoverageAmt8", IntegerType(), values=[0]) \
    .withColumn("MaternityBenefitCover", StringType(), values=["NO"]) \
    .withColumn("CoverageAmt9", IntegerType(), values=[0]) \
    .withColumn("NewBornBabyCover", StringType(), values=["NO"]) \
    .withColumn("CoverageAmt10", IntegerType(), values=[0]) \
    .withColumn("CompassionateVisitCover", StringType(), values=["NO"]) \
    .withColumn("CoverageAmt11", IntegerType(), values=[0]) \
    .withColumn("RestorationOfSumInsuredCover", StringType(), values=["NO"]) \
    .withColumn("CoverageAmt12", IntegerType(), values=[0]) \
    .withColumn("CoverageName13", StringType(), expr="null") \
    .withColumn("CoverageAmt13", IntegerType(), expr="null") \
    .withColumn("CoverageName14", StringType(), expr="null") \
    .withColumn("CoverageAmt14", IntegerType(), expr="null") \
    .withColumn("CoverageName15", StringType(), expr="null") \
    .withColumn("CoverageAmt15", IntegerType(), expr="null") \
    .withColumn("EndorsementReferenceNumber", StringType(), expr="null") \
    .withColumn("EffectiveDateTime", TimestampType(), begin="2023-01-01 00:00:00", end="2024-07-23 23:59:59") \
    .withColumn("EndorsementType", StringType(), expr="null") \
    .withColumn("TypeOfCalculation", StringType(), expr="null") \
    .withColumn("EndorsementRemarks", StringType(), expr="null") \
    .withColumn("KotakGeneralBranch", StringType(), values=branch_names,random=True) \
    .withColumn("Channel", StringType(), values=["BANCASSURANCE"]) \
    .withColumn("ProductCode", StringType(), values=[2011]) \
    .withColumn("ProductName", StringType(), values=product_names, random=True) \
    .withColumn("AgentName", StringType(), values=product_names,random=True) \
    .withColumn("AgentID", StringType(), minValue=1129811000,maxValue=1129812000,random=True) \
    .withColumn("AgentMobileNumber", StringType(), values=[fake.phone_number() for i in range(num_rows)],random=True) \
    .withColumn("AgentEmailID", StringType(), values=company_email,random=True) \
    .withColumn("isVIP", StringType(), expr="null") \
    .withColumn("InsuredName1", StringType(), values=[fake.name().upper() for i in range(num_rows)],random=True) \
    .withColumn("DateOfBirthI1", DateType(), begin="1960-01-01", end="2024-07-23") \
    .withColumn("SumInsuredOnlyIndividual1", IntegerType(),minValue=200000,maxValue=5000000,random=True) \
    .withColumn("CBAmountI1", IntegerType(), values=[0]) \
    .withColumn("RelationshipI1", StringType(), values=relation_list,random=True) \
    .withColumn("GenderI1", StringType(), values=["Male","Female"], random=True) \
    .withColumn("MemberID1", StringType(), minValue=1000010000, maxValue = 1000020000, random=True) \
    .withColumn("InsuredOccupation1", StringType(), values =["BUSINESS", "PROFESSIONAL", "SALARIED"] , random=True) \
    .withColumn("Portability1", StringType(), values=["NO"]) \
    .withColumn("PortabilitySumInsured1", IntegerType(), expr="null") \
    .withColumn("Portability30DaysWaitingPeriod1", IntegerType(), expr="null") \
    .withColumn("Portability2YearsWaitingPeriod1", IntegerType(), expr="null") \
    .withColumn("Portability4YearsPEDWaitingPeriod1", IntegerType(), expr="null") \
    .withColumn("PEDName_UW1", StringType(), values=["DIABETES MELLITUS"]) \
    .withColumn("PEDDescription_UW1", StringType(), values=["HIGH BLOOD SUGAR"]) \
    .withColumn("PEDRemarks_UW1", StringType(), values=["Diabetes and its complications"]) \
    .withColumn("ICDCode1", StringType(), values=icd_codes,random=True) \
    .withColumn("ICDDescription1", StringType(), values=["Diabetes mellitus"]) \
    .withColumn("UWRemarks1", StringType(), values=["Diabetes mellitus"]) \
    .withColumn("InsuredName2", StringType(), values=indian_names_full) \
    .withColumn("DateOfBirthI2", DateType(), begin="2023-01-01", end="2024-07-23") \
    .withColumn("SumInsuredOnlyIndividual2", IntegerType(), minValue=200000,maxValue=5000000,random=True) \
    .withColumn("CBAmountI2", IntegerType(),minValue=200000,maxValue=5000000,random=True) \
    .withColumn("RelationshipI2", StringType(), values=relation_list,random=True) \
    .withColumn("GenderI2", StringType(), values=["Male","Female"],random=True) \
    .withColumn("MemberID2", StringType(), minValue=1000010000, maxValue=1000020000) \
    .withColumn("InsuredOccupation2", StringType(), expr="null") \
    .withColumn("Portability2", StringType(), values=["YES","NO"],weights=[0.20,0.80],random=True) \
    .withColumn("PortabilitySumInsured2", IntegerType(), expr="null") \
    .withColumn("Portability30DaysWaitingPeriod2", IntegerType(), expr="null") \
    .withColumn("Portability2YearsWaitingPeriod2", IntegerType(), expr="null") \
    .withColumn("Portability4YearsPEDWaitingPeriod2", IntegerType(), expr="null") \
    .withColumn("PEDName_UW2", StringType(), expr="null") \
    .withColumn("PEDDescription_UW2", StringType(), expr="null") \
    .withColumn("PEDRemarks_UW2", StringType(), expr="null") \
    .withColumn("ICDCode2", StringType(), expr="null") \
    .withColumn("ICDDescription2", StringType(), expr="null") \
    .withColumn("UWRemarks2", StringType(), expr="null") \
    .withColumn("InsuredName3", StringType(), values=indian_names_full,random=True) \
    .withColumn("DateOfBirthI3", DateType(), begin="2023-01-01", end="2024-07-23") \
    .withColumn("SumInsuredOnlyIndividual3", IntegerType(), expr="null") \
    .withColumn("CBAmountI3", IntegerType(), expr="null") \
    .withColumn("RelationshipI3", StringType(), expr="null") \
    .withColumn("GenderI3", StringType(), expr="null") \
    .withColumn("MemberID3", StringType(), expr="null") \
    .withColumn("InsuredOccupation3", StringType(), expr="null") \
    .withColumn("Portability3", StringType(), expr="null") \
    .withColumn("PortabilitySumInsured3", IntegerType(), expr="null") \
    .withColumn("Portability30DaysWaitingPeriod3", IntegerType(), expr="null") \
    .withColumn("Portability2YearsWaitingPeriod3", IntegerType(), expr="null") \
    .withColumn("Portability4YearsPEDWaitingPeriod3", IntegerType(), expr="null") \
    .withColumn("PEDName_UW3", StringType(), expr="null") \
    .withColumn("PEDDescription_UW3", StringType(), expr="null") \
    .withColumn("PEDRemarks_UW3", StringType(), expr="null") \
    .withColumn("ICDCode3", StringType(), expr="null") \
    .withColumn("ICDDescription3", StringType(), expr="null") \
    .withColumn("UWRemarks3", StringType(), expr="null") \
    .withColumn("InsuredName4", StringType(), values=indian_names_full,random=True) \
    .withColumn("DateOfBirthI4", DateType(), begin="2023-01-01", end="2024-07-23") \
    .withColumn("SumInsuredOnlyIndividual4", IntegerType(), expr="null") \
    .withColumn("CBAmountI4", IntegerType(), expr="null") \
    .withColumn("RelationshipI4", StringType(), expr="null") \
    .withColumn("GenderI4", StringType(), expr="null") \
    .withColumn("MemberID4", StringType(), expr="null") \
    .withColumn("InsuredOccupation4", StringType(), expr="null") \
    .withColumn("Portability4", StringType(), expr="null") \
    .withColumn("PortabilitySumInsured4", IntegerType(), expr="null") \
    .withColumn("Portability30DaysWaitingPeriod4", IntegerType(), expr="null") \
    .withColumn("Portability2YearsWaitingPeriod4", IntegerType(), expr="null") \
    .withColumn("Portability4YearsPEDWaitingPeriod4", IntegerType(), expr="null") \
    .withColumn("PEDName_UW4", StringType(), expr="null") \
    .withColumn("PEDDescription_UW4", StringType(), expr="null") \
    .withColumn("PEDRemarks_UW4", StringType(), expr="null") \
    .withColumn("ICDCode4", StringType(), expr="null") \
    .withColumn("ICDDescription4", StringType(), expr="null") \
    .withColumn("UWRemarks4", StringType(), expr="null") \
    .withColumn("InsuredName5", StringType(), values=indian_names_full,random=True) \
    .withColumn("DateOfBirthI5", DateType(), begin="2023-01-01", end="2024-07-23") \
    .withColumn("SumInsuredOnlyIndividual5", IntegerType(), expr="null") \
    .withColumn("CBAmountI5", IntegerType(), expr="null") \
    .withColumn("RelationshipI5", StringType(), expr="null") \
    .withColumn("GenderI5", StringType(), expr="null") \
    .withColumn("MemberID5", StringType(), expr="null") \
    .withColumn("InsuredOccupation5", StringType(), expr="null") \
    .withColumn("Portability5", StringType(), expr="null") \
    .withColumn("PortabilitySumInsured5", IntegerType(), expr="null") \
    .withColumn("Portability30DaysWaitingPeriod5", IntegerType(), expr="null") \
    .withColumn("Portability2YearsWaitingPeriod5", IntegerType(), expr="null") \
    .withColumn("Portability4YearsPEDWaitingPeriod5", IntegerType(), expr="null") \
    .withColumn("PEDName_UW5", StringType(), expr="null") \
    .withColumn("PEDDescription_UW5", StringType(), expr="null") \
    .withColumn("PEDRemarks_UW5", StringType(), expr="null") \
    .withColumn("ICDCode5", StringType(), expr="null") \
    .withColumn("ICDDescription5", StringType(), expr="null") \
    .withColumn("UWRemarks5", StringType(), expr="null") \
    .withColumn("InsuredName6", StringType(), expr="null") \
    .withColumn("DateOfBirthI6", DateType(), begin="2023-01-01", end="2024-07-23") \
    .withColumn("SumInsuredOnlyIndividual6", IntegerType(), expr="null") \
    .withColumn("CBAmountI6", IntegerType(), expr="null") \
    .withColumn("RelationshipI6", StringType(), expr="null") \
    .withColumn("GenderI6", StringType(), expr="null") \
    .withColumn("MemberID6", StringType(), expr="null") \
    .withColumn("InsuredOccupation6", StringType(), expr="null") \
    .withColumn("Portability6", StringType(), expr="null") \
    .withColumn("PortabilitySumInsured6", IntegerType(), expr="null") \
    .withColumn("Portability30DaysWaitingPeriod6", IntegerType(), expr="null") \
    .withColumn("Portability2YearsWaitingPeriod6", IntegerType(), expr="null") \
    .withColumn("Portability4YearsPEDWaitingPeriod6", IntegerType(), expr="null") \
    .withColumn("PEDName_UW6", StringType(), expr="null") \
    .withColumn("PEDDescription_UW6", StringType(), expr="null") \
    .withColumn("PEDRemarks_UW6", StringType(), expr="null") \
    .withColumn("ICDCode6", StringType(), expr="null") \
    .withColumn("ICDDescription6", StringType(), expr="null") \
    .withColumn("UWRemarks6", StringType(), expr="null") \
    .withColumn("InsuredName7", StringType(), expr="null") \
    .withColumn("DateOfBirthI7", DateType(), begin="2023-01-01", end="2024-07-23") \
    .withColumn("SumInsuredOnlyIndividual7", IntegerType(), expr="null") \
    .withColumn("CBAmountI7", IntegerType(), expr="null") \
    .withColumn("RelationshipI7", StringType(), expr="null") \
    .withColumn("GenderI7", StringType(), expr="null") \
    .withColumn("MemberID7", StringType(), expr="null") \
    .withColumn("InsuredOccupation7", StringType(), expr="null") \
    .withColumn("Portability7", StringType(), expr="null") \
    .withColumn("PortabilitySumInsured7", IntegerType(), expr="null") \
    .withColumn("Portability30DaysWaitingPeriod7", IntegerType(), expr="null") \
    .withColumn("Portability2YearsWaitingPeriod7", IntegerType(), expr="null") \
    .withColumn("Portability4YearsPEDWaitingPeriod7", IntegerType(), expr="null") \
    .withColumn("PEDName_UW7", StringType(), expr="null") \
    .withColumn("PEDDescription_UW7", StringType(), expr="null") \
    .withColumn("PEDRemarks_UW7", StringType(), expr="null") \
    .withColumn("ICDCode7", StringType(), expr="null") \
    .withColumn("ICDDescription7", StringType(), expr="null") \
    .withColumn("UWRemarks7", StringType(), expr="null") \
    .withColumn("InsuredName8", StringType(), expr="null") \
    .withColumn("DateOfBirthI8", DateType(), begin="2023-01-01", end="2024-07-23") \
    .withColumn("SumInsuredOnlyIndividual8", IntegerType(), expr="null") \
    .withColumn("CBAmountI8", IntegerType(), expr="null") \
    .withColumn("RelationshipI8", StringType(), values=relation_list,random=True) \
    .withColumn("GenderI8", StringType(), expr="null") \
    .withColumn("MemberID8", StringType(), expr="null") \
    .withColumn("InsuredOccupation8", StringType(), expr="null") \
    .withColumn("Portability8", StringType(), expr="null") \
    .withColumn("PortabilitySumInsured8", IntegerType(), expr="null") \
    .withColumn("Portability30DaysWaitingPeriod8", IntegerType(), expr="null") \
    .withColumn("Portability2YearsWaitingPeriod8", IntegerType(), expr="null") \
    .withColumn("Portability4YearsPEDWaitingPeriod8", IntegerType(), expr="null") \
    .withColumn("PEDName_UW8", StringType(), expr="null") \
    .withColumn("PEDDescription_UW8", StringType(), expr="null") \
    .withColumn("PEDRemarks_UW8", StringType(), expr="null") \
    .withColumn("ICDCode8", StringType(), expr="null") \
    .withColumn("ICDDescription8", StringType(), expr="null") \
    .withColumn("UWRemarks8", StringType(), expr="null") \
    .withColumn("MainMemberID", LongType(), minValue=86750556, maxValue=86751556, random=True) \
    .withColumn("FloaterCB", StringType(), expr="null") \
    .withColumn("Policy_Source", StringType(), expr="null") \
    .withColumn("Partner_Application_No", StringType(), expr="null") \
    .withColumn("Branch_Inward_No", StringType(), expr="null") \
    .withColumn("Installment_Yes_No", StringType(), expr="null") \
    .withColumn("Installment_Frequency", StringType(), expr="null") \
    .withColumn("QWP", StringType(), expr="null") \
    .withColumn("Member_EntryDate_I1", DateType(), begin="2023-01-01", end="2024-07-23") \
    .withColumn("Member_EntryDate_I2", DateType(), begin="2023-01-01", end="2024-07-23") \
    .withColumn("Member_EntryDate_I3", DateType(), begin="2023-01-01", end="2024-07-23") \
    .withColumn("Member_EntryDate_I4", DateType(), begin="2023-01-01", end="2024-07-23") \
    .withColumn("Member_EntryDate_I5", DateType(), begin="2023-01-01", end="2024-07-23") \
    .withColumn("Member_EntryDate_I6", DateType(), begin="2023-01-01", end="2024-07-23") \
    .withColumn("Member_EntryDate_I7", DateType(), begin="2023-01-01", end="2024-07-23") \
    .withColumn("Member_EntryDate_I8", DateType(), begin="2023-01-01", end="2024-07-23") \
    .withColumn("PED_WaitingPeriod_I1", IntegerType(), expr="null") \
    .withColumn("PED_WaitingPeriod_I2", IntegerType(), expr="null") \
    .withColumn("PED_WaitingPeriod_I3", IntegerType(), expr="null") \
    .withColumn("PED_WaitingPeriod_I4", IntegerType(), expr="null") \
    .withColumn("PED_WaitingPeriod_I5", IntegerType(), expr="null") \
    .withColumn("PED_WaitingPeriod_I6", IntegerType(), expr="null") \
    .withColumn("PED_WaitingPeriod_I7", IntegerType(), expr="null") \
    .withColumn("PED_WaitingPeriod_I8", IntegerType(), expr="null") \
    .withColumn("AnnualHealthCheckupCover", StringType(), expr="null") \
    .withColumn("Value_Added_Service_Cover", StringType(), expr="null") \
    .withColumn("PreHospital_Days_Cover", StringType(), expr="null") \
    .withColumn("PostHospital_Days_Cover", StringType(), expr="null") \
    .withColumn("Second_E_Opinion_Cover", StringType(), expr="null") \
    .withColumn("Vaccn_Expenses_Cover", StringType(), expr="null") \
    .withColumn("Vaccn_Expenses_Cover_Amt", IntegerType(), expr="null") \
    .withColumn("Air_Ambulance_Cover", StringType(), expr="null") \
    .withColumn("Home_Nursing_Benefit_Cover", StringType(), expr="null") \
    .withColumn("OthCoverDtls_Maternity", StringType(), expr="null") \
    .withColumn("OthCoverDtls_Critical_Illness", StringType(), expr="null") \
    .withColumn("TXT_CATEGORY", StringType(), expr="null") \
    .withColumn("Claim_Protect", StringType(), expr="null") \
    .withColumn("Inflation_Protect", StringType(), expr="null") \
    .withColumn("Coverage_amt16", IntegerType(), expr="null") \
    .withColumn("Super_NCB", StringType(), expr="null") \
    .withColumn("Coverage_amt17", IntegerType(), expr="null") \
    .withColumn("Restoration_Benefit_Plus", StringType(), expr="null") \
    .withColumn("Coverage_amt18", IntegerType(), expr="null") \
    .withColumn("CKYC_Available", StringType(), expr="null") \
    .withColumn("CKYC_ID_No", StringType(), expr="null") \
    .withColumn("Request_Date", DateType(), begin="2023-01-01", end="2024-07-23") \
    .withColumn("InflationSI1", IntegerType(), expr="null") \
    .withColumn("SuperNCBSI1", IntegerType(), expr="null") \
    .withColumn("InflationSI2", IntegerType(), expr="null") \
    .withColumn("SuperNCBSI2", IntegerType(), expr="null") \
    .withColumn("InflationSI3", IntegerType(), expr="null") \
    .withColumn("SuperNCBSI3", IntegerType(), expr="null") \
    .withColumn("InflationSI4", IntegerType(), expr="null") \
    .withColumn("SuperNCBSI4", IntegerType(), expr="null") \
    .withColumn("InflationSI5", IntegerType(), expr="null") \
    .withColumn("SuperNCBSI5", IntegerType(), expr="null") \
    .withColumn("InflationSI6", IntegerType(), expr="null") \
    .withColumn("SuperNCBSI6", IntegerType(), expr="null") \
    .withColumn("InflationSI7", IntegerType(), expr="null") \
    .withColumn("SuperNCBSI7", IntegerType(), expr="null") \
    .withColumn("InflationSI8", IntegerType(), expr="null") \
    .withColumn("SuperNCBSI8", IntegerType(), expr="null") \
    .withColumn("Reciept_Status", StringType(), expr="null")

MemberPortingExt = data_gen.build()
MemberPortingExt.display()

# COMMAND ----------

data_gen = dg.DataGenerator(spark, name="Membersi", rows=num_rows, seedColumnName="unique_id") \
    .withColumn("ID", IntegerType(), minValue=70000, maxValue=71000) \
    .withColumn("memberpolicyid", LongType() , minValue=86750555, maxValue=86751564,random=True) \
    .withColumn("BPSIID", IntegerType(), minValue=1, maxValue=1000,random=True) \
    .withColumn("endorsementid", IntegerType(), values=[None]) \
    .withColumn("EffectiveFrom", TimestampType(), begin="2023-01-01 00:00:00", end="2024-07-23 23:59:59") \
    .withColumn("EffectiveTo", TimestampType(), begin="2023-01-01 00:00:00", end="2024-07-23 23:59:59") \
    .withColumn("StatusID_P27", IntegerType(), minValue=1, maxValue=200,random=True) \
    .withColumn("StatusRemarksID_P28", IntegerType(), values=[None, 0], random=True) \
    .withColumn("statusRemarks", StringType(), values=[None, "Floater - 300000"], random=True) \
    .withColumn("CB_Amount", IntegerType(), minValue=0, maxValue=30000) \
    .withColumn("CB_Perc", FloatType(), values=[None]) \
    .withColumn("CB_Effectivedate", TimestampType(), begin="2023-01-01 00:00:00", end="2024-07-23 23:59:59") \
    .withColumn("BasicPremium", IntegerType(), minValue=0, maxValue=30000,random=True) \
    .withColumn("ServiceTax", IntegerType(), values=[None]) \
    .withColumn("ServiceTaxValue", IntegerType(), values=[None]) \
    .withColumn("NetPremium", IntegerType(), minValue=0, maxValue=30000,random=True) \
    .withColumn("loadingPremium", IntegerType(), values=[None]) \
    .withColumn("Deleted", IntegerType(), values=[0, 1],weights=[0.9,0.10], random=True) \
    .withColumn("LastModifiedBy", IntegerType(), values=[None,1],random=True) \
    .withColumn("ModifiedDateTime", TimestampType(), begin="2023-01-01 00:00:00", end="2024-07-23 23:59:59") \
    .withColumn("DeletedBy", IntegerType(), values=[None,1], random=True) \
    .withColumn("DeletedDatetime", TimestampType(), begin="2023-01-01 00:00:00", end="2024-07-23 23:59:59")

Membersi = data_gen.build()
Membersi.display()

# COMMAND ----------

data_gen = dg.DataGenerator(spark, name="bpsiconditions", rows=num_rows, seedColumnName="unique_id") \
    .withColumn("ID", IntegerType(), minValue=4820000, maxValue=4830000, random=True) \
    .withColumn("BPSIID", IntegerType(), minValue=1, maxValue=1000, random=True) \
    .withColumn("BPConditionID", IntegerType(), minValue=1, maxValue=100, random=True) \
    .withColumn("isCovered", IntegerType(), values=[0, 1],weights=[0.9,0.1], random=True) \
    .withColumn("CoverageType_P49", IntegerType(), values=[192]) \
    .withColumn("ServiceTypeID", IntegerType(), values=[1]) \
    .withColumn("ServiceSubTypeID", IntegerType(), values=[4]) \
    .withColumn("ClaimTypeID", IntegerType(), values=claim_types) \
    .withColumn("RequestTypeID", IntegerType(), values=request_types, random=True) \
    .withColumn("RelGroupID_P26", IntegerType(), values=[None]) \
    .withColumn("RelationshipID", IntegerType(), minValue=1, maxValue=40,random = True) \
    .withColumn("BPComparisionFrom_P52", IntegerType(), values=[None]) \
    .withColumn("ExpressionID_P17", IntegerType(), values=[None]) \
    .withColumn("BPComparisionTo_P52", IntegerType(), values=[None]) \
    .withColumn("ExternalValuePerc", IntegerType(), values=[None]) \
    .withColumn("ExternalValueAbs", IntegerType(), values=[None]) \
    .withColumn("InternalValuePerc", IntegerType(), values=[None]) \
    .withColumn("InternalValueAbs", IntegerType(), values=[None]) \
    .withColumn("Duration", IntegerType(), values=[0]) \
    .withColumn("DurationType_P18", IntegerType(), values=[None]) \
    .withColumn("CorporateLimit", IntegerType(), values=[None]) \
    .withColumn("CorporatePerc", IntegerType(), values=[None]) \
    .withColumn("CorporateClaimCount", IntegerType(), values=[None]) \
    .withColumn("PolicyLimit", IntegerType(), values=[None]) \
    .withColumn("Policyperc", IntegerType(), values=[None]) \
    .withColumn("PolicyClaimCount", IntegerType(), values=[None]) \
    .withColumn("FamilyLimit", IntegerType(), minValue=1, maxValue=5, random=True) \
    .withColumn("FamilyPerc", IntegerType(), values=[None]) \
    .withColumn("FamilyClaimCount", IntegerType(), values=[None]) \
    .withColumn("IndividualLimit", IntegerType(), values=[None]) \
    .withColumn("IndividualPerc", IntegerType(), values=[None]) \
    .withColumn("IndividualClaimCount", IntegerType(), values=[None]) \
    .withColumn("ClaimLimit", IntegerType(), values=[None]) \
    .withColumn("ClaimPerc", IntegerType(), values=[None]) \
    .withColumn("TPAProcedureID", IntegerType(), minValue=300, maxValue=1200, random=True) \
    .withColumn("CopayPerc", IntegerType(), values=[None]) \
    .withColumn("CopayValue", IntegerType(), values=[None]) \
    .withColumn("isLess", IntegerType(), values=[None]) \
    .withColumn("Age", IntegerType(), minValue=0, maxValue=100, random=True) \
    .withColumn("AgeTypeID", IntegerType(), values=[None]) \
    .withColumn("InsZone", IntegerType(), values=[None]) \
    .withColumn("Grade", IntegerType(), values=[None]) \
    .withColumn("Designation", IntegerType(), values=[None]) \
    .withColumn("NetworkType_P50", IntegerType(), values=[None]) \
    .withColumn("NatureofTreatment_P43", IntegerType(), values=[None]) \
    .withColumn("AdmissionTypeID", IntegerType(), values=[None]) \
    .withColumn("CityID", IntegerType(), values=[None]) \
    .withColumn("isICRCopay", IntegerType(), values=[None]) \
    .withColumn("PayerID", IntegerType(), values=[None]) \
    .withColumn("BPSubConditionID", IntegerType(), values=[None]) \
    .withColumn("APerc", IntegerType(), values=[None]) \
    .withColumn("AValue", IntegerType(), values=[None]) \
    .withColumn("BPerc", IntegerType(), values=[None]) \
    .withColumn("BValue", IntegerType(), values=[None]) \
    .withColumn("CPerc", IntegerType(), values=[None]) \
    .withColumn("CValue", IntegerType(), values=[None]) \
    .withColumn("ApprovalReq_P60", IntegerType(), values=[None]) \
    .withColumn("isAccident", IntegerType(), values=[None]) \
    .withColumn("LimitCatg_P29", IntegerType(), values=[None]) \
    .withColumn("ApplicableTo_P11", IntegerType(), values=[None]) \
    .withColumn("PolicyID", LongType(),minValue=4411218,maxValue=4412218, random=True) \
    .withColumn("ENDR_ID", IntegerType(), values=[None]) \
    .withColumn("EffectiveDate", TimestampType(), begin="2023-01-01 00:00:00", end="2024-07-23 23:59:59") \
    .withColumn("Remarks", StringType(), values=[None, "Domiciliary Hospitalisation is covered.", "No Limit for ICU.", "No Limit for Normal.", "Maternity Not Covered.", "Deductible Rs.3,00,000/- per Family.", "Terrorism is covered.", "Internal Congenital treatment is covered.", "Alternative Treatment is covered.", "All Other Terms & Conditions as per Kotak Mahindra General Insurance Company Ltd."], random=True) \
    .withColumn("Deleted", IntegerType(), values=[0, 1], weights=[0.9, 0.1], random=True) \
    .withColumn("CreatedDatetime", TimestampType(), begin="2023-01-01 00:00:00", end="2024-07-23 23:59:59") \
    .withColumn("CreatedUserRegionID",IntegerType(), minValue=1, maxValue=10, random=True) \
    .withColumn("DeletedDatetime", TimestampType(), begin="2023-01-01 00:00:00", end="2024-07-23 23:59:59") \
    .withColumn("DeletedUserRegionID", IntegerType(),  minValue=1, maxValue=5, random=True) \
    .withColumn("Accomdation", StringType(), values=[None]) \
    .withColumn("CityType", StringType(), values=[None]) \
    .withColumn("SpecialRuleCondition", StringType(), values=[None]) \
    .withColumn("GroupLimit", IntegerType(), values=[None]) \
    .withColumn("GroupPerc", IntegerType(), values=[None]) \
    .withColumn("GroupClaimCount", IntegerType(), values=[None])

BPSIConditions = data_gen.build()
BPSIConditions.display()

# COMMAND ----------

data_gen = dg.DataGenerator(spark, name="Providermou", rows=num_rows, seedColumnName="unique_id") \
    .withColumn("ID", IntegerType(), minValue=1117000, maxValue=1117000+num_rows) \
    .withColumn("ProviderID", IntegerType(), minValue=214500, maxValue=214500+num_rows,random = True) \
    .withColumn("MOUTypeID_P44", IntegerType(), values=[167,168]) \
    .withColumn("MOUEntityID", IntegerType(), minValue=1, maxValue=50, random=True) \
    .withColumn("PRCNo", IntegerType(), minValue=18931, maxValue=18940, random=True) \
    .withColumn("MOUNo", StringType(), values=[None]) \
    .withColumn("StartDate", TimestampType(), begin="2023-01-01 00:00:00", end="2024-07-23 23:59:59") \
    .withColumn("EndDate", TimestampType(), begin="2023-01-01 00:00:00", end="2024-07-23 23:59:59") \
    .withColumn("ExpiryDate", TimestampType(), values=[None]) \
    .withColumn("MOUDiscountPerc", FloatType(), values=[None]) \
    .withColumn("MOUDiscountAbs", FloatType(), values=[None]) \
    .withColumn("MOUReceivedDate", TimestampType(), values=[None]) \
    .withColumn("EffectiveDate", TimestampType(), values=[None]) \
    .withColumn("DMSID", IntegerType(), values=[0,1], weights=[0.9, 0.1], random=True) \
    .withColumn("TariffDMSID", IntegerType(), values=[None]) \
    .withColumn("PackageDMSID", IntegerType(), values=[None]) \
    .withColumn("isLatest", IntegerType(), values=[1,0],weights=[0.9, 0.1], random=True) \
    .withColumn("Deleted", IntegerType(), values=[0, 1], weights=[0.9, 0.1], random=True) \
    .withColumn("Createddatetime", TimestampType(), begin="2023-01-01 00:00:00", end="2024-07-23 23:59:59") \
    .withColumn("CreatedUserRegionID",IntegerType(), minValue=1, maxValue=10, random=True) \
    .withColumn("DeletedUserRegionID", IntegerType(),  minValue=1, maxValue=5, random=True) \
    .withColumn("Deleteddatetime", TimestampType(), values=[None]) \
    .withColumn("IPPercentage", FloatType(), values=[None]) \
    .withColumn("OPPercentage", FloatType(), values=[None]) \
    .withColumn("PackagePercentage", FloatType(), values=[None]) \
    .withColumn("AgreementType", StringType(), values=["T"]) \
    .withColumn("AgreementID", IntegerType(), minValue=1116000, maxValue=1118000, random=True) \
    .withColumn("ModifiedUserRegionID", IntegerType(), minValue=1, maxValue=5,random=True) \
    .withColumn("Modifieddatetime", TimestampType(), values=[None]) \
    .withColumn("DiscountUpdateType", StringType(), values=[None]) \
    .withColumn("Remarks", StringType(), values=["New Empanelment"]) \
    .withColumn("PackageDiscountRemarks", StringType(), values=[None]) \
    .withColumn("ServiceDiscountRemarks", StringType(), values=[None]) \
    .withColumn("PkgType", IntegerType(), values=[None]) \
    .withColumn("TempMou", StringType(), values=[0,1], weights=[0.9, 0.1], random=True)

Providermou = data_gen.build()
Providermou.display()

# COMMAND ----------

data_gen = dg.DataGenerator(spark, name="Claims", rows=num_rows, seedColumnName="unique_id") \
    .withColumn("ID", LongType(), minValue=24070402000, maxValue=24070403000, random=True) \
    .withColumn("IssueID", IntegerType(), minValue=1, maxValue=100, random=True) \
    .withColumn("EventNo", IntegerType(), values=[None]) \
    .withColumn("InsurerClaimID", IntegerType(),minValue=1, maxValue=30, random=True) \
    .withColumn("MemberPolicyID",LongType() , minValue=86750555, maxValue=86751564,random=True) \
    .withColumn("StageID", IntegerType(), minValue=21, maxValue=27, random=True) \
    .withColumn("ReceivedDate", TimestampType(), begin="2023-07-04 00:00:00", end="2024-07-04 23:59:59") \
    .withColumn("ReceivedMode_P23", IntegerType(), values=[465]) \
    .withColumn("ServiceTypeID", IntegerType(), values=[1]) \
    .withColumn("ServiceSubTypeID", IntegerType(), values=[4]) \
    .withColumn("HospitalizationType", IntegerType(), values=[18]) \
    .withColumn("AdmissionTypeID", IntegerType(), values=[None]) \
    .withColumn("ProbableDiagnosis", IntegerType(), values=probable_diagnoses,random=True) \
    .withColumn("ProbableDOA", TimestampType(), begin="2023-04-17 00:00:00", end="2024-07-04 23:59:59") \
    .withColumn("DateofAdmission", TimestampType(), begin="2023-04-17 00:00:00", end="2024-07-04 23:59:59") \
    .withColumn("DateofDischarge", TimestampType(), begin="2023-04-17 00:00:00", end="2024-07-04 23:59:59") \
    .withColumn("ProviderID", IntegerType(), minValue=214500, maxValue=214500+num_rows,random = True) \
    .withColumn("IllnessStartDate", TimestampType(), values=[None]) \
    .withColumn("NatureofTreatmentType_P43", IntegerType(), values=[None]) \
    .withColumn("TreatmentUndertaken", IntegerType(), values=[None]) \
    .withColumn("ICUDays", IntegerType(), values=[None]) \
    .withColumn("RoomDays", IntegerType(), values=[None]) \
    .withColumn("BillNo", IntegerType(), minValue=2998, maxValue=299098, random=True) \
    .withColumn("BillDate", TimestampType(), begin="2023-04-17 00:00:00", end="2024-07-04 23:59:59") \
    .withColumn("IPNo", IntegerType(), values=[None]) \
    .withColumn("ClaimedAmount", IntegerType(), minValue=1000, maxValue=100000, random=True) \
    .withColumn("isfarceClaim", IntegerType(), values=[0,1],weights=[0.9, 0.1], random=True) \
    .withColumn("PatientName", StringType(), values=[fake.name() for i in range(num_rows)], random=True) \
    .withColumn("GenderID", IntegerType(), values=[1, 2], random=True) \
    .withColumn("Age", IntegerType(), minValue=25, maxValue=82, random=True) \
    .withColumn("AgetypeID", IntegerType(), values=[1]) \
    .withColumn("DOB", TimestampType(), begin="1942-01-01 00:00:00", end="2024-06-26 23:59:59") \
    .withColumn("EmployeeID", IntegerType(), minValue=631, maxValue=117269, random=True) \
    .withColumn("EmployeeName", StringType(), values=[fake.name() for i in range(num_rows)], random=True) \
    .withColumn("RelationShipID", IntegerType(), minValue=1, maxValue=40,random = True) \
    .withColumn("Mobile", StringType(), values=[fake.phone_number() for i in range(num_rows)], random=True) \
    .withColumn("OtherMobileNo", StringType(), values=[fake.phone_number() for i in range(num_rows)], random=True) \
    .withColumn("Email", StringType(), values=[None]) \
    .withColumn("Grade", IntegerType(), values=[None]) \
    .withColumn("Designation", IntegerType(), values=[None]) \
    .withColumn("FirstConsultation", IntegerType(), values=[None]) \
    .withColumn("PastHistory", IntegerType(), values=[None]) \
    .withColumn("MedicalHistory", IntegerType(), values=[None]) \
    .withColumn("PreExistingDisease", IntegerType(), values=[None]) \
    .withColumn("isAccident_RTA", IntegerType(), values=[None]) \
    .withColumn("DateOfInjury", TimestampType(), values=[None]) \
    .withColumn("isInjury_Alcohol", IntegerType(), values=[None]) \
    .withColumn("isAlcoholTested", IntegerType(), values=[None]) \
    .withColumn("Injury_Occured", IntegerType(), values=[None]) \
    .withColumn("ReportedToPolice", IntegerType(), values=[None]) \
    .withColumn("FIRNo", IntegerType(), values=[None]) \
    .withColumn("FIRDate", TimestampType(), values=[None]) \
    .withColumn("FIRLocation", IntegerType(), values=[None]) \
    .withColumn("isMaternity", IntegerType(), values=[None]) \
    .withColumn("Maternity_G", IntegerType(), values=[None]) \
    .withColumn("Maternity_P", IntegerType(), values=[None]) \
    .withColumn("Maternity_L", IntegerType(), values=[None]) \
    .withColumn("Maternity_A", IntegerType(), values=[None]) \
    .withColumn("Maternity_D", IntegerType(), values=[None]) \
    .withColumn("ExpDateofDelivery", TimestampType(), values=[None]) \
    .withColumn("NoOfLivingChildren", IntegerType(), values=[None]) \
    .withColumn("Deleted", StringType(), values=[0, 1], weights=[0.9, 0.1], random=True) \
    .withColumn("MIG_FHPCodes", IntegerType(), values=[None]) \
    .withColumn("MIG_TreatmentTypeID", IntegerType(), values=[None]) \
    .withColumn("MIG_IsPreAuthorization", IntegerType(), values=[None]) \
    .withColumn("MIG_IsDischargeSummary", IntegerType(), values=[None]) \
    .withColumn("MIG_IsHospitalizationBills", IntegerType(), values=[None]) \
    .withColumn("MIG_IsBillBreakup", IntegerType(), values=[None]) \
    .withColumn("MIG_IsClaimForm", IntegerType(), values=[None]) \
    .withColumn("MIG_IsBillPaid", IntegerType(), values=[None]) \
    .withColumn("MIG_FHPFlag", IntegerType(), values=[None]) \
    .withColumn("MIG_MainClaimID", IntegerType(), values=[None]) \
    .withColumn("MIG_BatchNo", IntegerType(), values=[None]) \
    .withColumn("MIG_PreauthID", IntegerType(), values=[None]) \
    .withColumn("MIG_Cancelled", IntegerType(), values=[None]) \
    .withColumn("MIG_InsurerID", IntegerType(), values=[None]) \
    .withColumn("CreatedUserRegionID", IntegerType(), minValue=1, maxValue=10, random=True) \
    .withColumn("CreatedDatetime", TimestampType(), begin="2024-07-04 00:00:00", end="2024-07-04 23:59:59") \
    .withColumn("ModifiedUserRegionID", IntegerType(), minValue=1, maxValue=5, random=True) \
    .withColumn("ModifiedDatetime", TimestampType(), values=[None]) \
    .withColumn("DeletedUserRegionID", IntegerType(), minValue=1, maxValue=5, random=True) \
    .withColumn("DeletedDatetime", TimestampType(), values=[None]) \
    .withColumn("IsUnderProcess", StringType(), values=[1,0],weights=[0.9, 0.1], random=True) \
    .withColumn("PatientConditionID", IntegerType(), values=[None]) \
    .withColumn("PatientConditionDate", TimestampType(), values=[None]) \
    .withColumn("OldProviderID",  IntegerType(), minValue=1, maxValue=20,random = True) \
    .withColumn("EstimatedDays", IntegerType(), values=[None]) \
    .withColumn("physicalDoc", IntegerType(), values=[None]) \
    .withColumn("LegalFlag", IntegerType(), values=[None]) \
    .withColumn("LegalRemarks", StringType(), values=[None])

Claims = data_gen.build()
Claims.display()

# COMMAND ----------

data_gen = dg.DataGenerator(spark, name="ClaimActionItems", rows=num_rows, seedColumnName="unique_id") \
    .withColumn("ID", IntegerType(), minValue=95889000, maxValue=95889000+num_rows, random=True) \
    .withColumn("ClaimID", LongType(), minValue=24070402000, maxValue=24070403000, random=True) \
    .withColumn("Slno", IntegerType(),minValue=1, maxValue=5, random=True) \
    .withColumn("ClaimTypeID", IntegerType(), values=claim_types, random=True) \
    .withColumn("RequestTypeID", IntegerType(), values=request_types, random=True) \
    .withColumn("ServiceTypeID", IntegerType(), values=[1]) \
    .withColumn("ServiceSubTypeID", IntegerType(), values=[4]) \
    .withColumn("ClaimStageID", IntegerType(), minValue=21, maxValue=27, random=True) \
    .withColumn("ClaimLandingIDs", IntegerType(), values=[None]) \
    .withColumn("RoleID", IntegerType(), values=[13, 17], random=True) \
    .withColumn("RegionID", IntegerType(), minValue=1, maxValue=5, random=True) \
    .withColumn("ClaimedAmount", IntegerType(), minValue=1000, maxValue=100000, random=True) \
    .withColumn("OpenDate", TimestampType(), begin="2023-07-05 00:00:00", end="2024-07-05 00:02:00") \
    .withColumn("CloseDate", TimestampType(), begin="2023-07-08 00:00:00", end="2024-07-08 00:02:00") \
    .withColumn("TATHrs", IntegerType(), values=[0]) \
    .withColumn("TATExceedHrs", IntegerType(), values=[0]) \
    .withColumn("OpenBy", IntegerType(), minValue=9386, maxValue=12304, random=True) \
    .withColumn("ClosedBy", IntegerType(), values=[None, 9386], random=True) \
    .withColumn("Remarks", StringType(), values=remarkList, random=True) \
    .withColumn("ReasonIDs_P",IntegerType(), minValue=1, maxValue=20,random = True) \
    .withColumn("UserLoggedID", IntegerType(), values=[None]) \
    .withColumn("Deleted", StringType(), values=[0,1],weights=[0.9, 0.1], random=True)

ClaimActionItems = data_gen.build()
display(ClaimActionItems)

# COMMAND ----------

data_gen = dg.DataGenerator(spark, name="ClaimsCoding", rows=num_rows, seedColumnName="unique_id") \
    .withColumn("ID", IntegerType(), minValue=16739136, maxValue=16739136+num_rows, random=True) \
    .withColumn("ClaimID", LongType(), minValue=24070402000, maxValue=24070403000, random=True) \
    .withColumn("Slno", IntegerType(), minValue=1, maxValue=5, random=True) \
    .withColumn("BillingType_P51", IntegerType(), values=[202]) \
    .withColumn("TPAProcedureID", IntegerType(), minValue=300, maxValue=1200, random=True) \
    .withColumn("PackageRate", IntegerType(), values=[0]) \
    .withColumn("PackageRatio", IntegerType(), values=[100]) \
    .withColumn("AdditionalAmount", IntegerType(), values=[0, 65], random=True) \
    .withColumn("TreatementTypeID_19", IntegerType(), minValue=1, maxValue=100, random=True) \
    .withColumn("isGipsa", StringType(), values=[1, 0], weights=[0.1, 0.9], random=True) \
    .withColumn("isDayCare", StringType(), values=[0,1],weights=[0.9, 0.1], random=True) \
    .withColumn("isCI", StringType(), values=[0,1],weights=[0.9, 0.1], random=True) \
    .withColumn("isPED", StringType(), values=[0,1],weights=[0.9, 0.1], random=True) \
    .withColumn("TypeOfAnesthesiaID", IntegerType(), values=[0,1],weights=[0.9, 0.1], random=True) \
    .withColumn("Exclusions", StringType(), values=[None]) \
    .withColumn("SurgeryDate", TimestampType(), begin="2023-07-04 00:00:00", end="2024-07-04 23:59:59") \
    .withColumn("BillAmount", IntegerType(), minValue=1000, maxValue=100000, random=True) \
    .withColumn("DisallowedAmount", IntegerType(), values=[0]) \
    .withColumn("DisallowedReasonIDs", IntegerType(), values=[1,2,3],weights=[0.1, 0.1, 0.8], random=True) \
    .withColumn("BPCoverageLimit", IntegerType(), values=[None]) \
    .withColumn("EligibleAmount", IntegerType(), minValue=1000, maxValue=100000, random=True) \
    .withColumn("PayableAmount", IntegerType(), minValue=1000, maxValue=100000, random=True) \
    .withColumn("BufferAmount", IntegerType(), values=[0]) \
    .withColumn("AdditionalreasonIDs", StringType(), values=[None]) \
    .withColumn("Discount", IntegerType(), minValue=0, maxValue=100, random=True) \
    .withColumn("Copay", IntegerType(), values=[0]) \
    .withColumn("Remarks", StringType(), values=[None]) \
    .withColumn("ICDCode", StringType(), values=icd_codes, random=True) \
    .withColumn("PCSCode", IntegerType(), values=pcs_codes, random=True) \
    .withColumn("PCSDescription", StringType(), values=["Resection of Prostate, Via Natural or Artificial Opening Endoscopic"], random=True) \
    .withColumn("TPALevel1", IntegerType(), minValue=200, maxValue=1200, random=True) \
    .withColumn("TPALevel2", IntegerType(), minValue=200, maxValue=1200, random=True) \
    .withColumn("TPALevel3", IntegerType(), minValue=200, maxValue=1200, random=True) \
    .withColumn("Deleted", StringType(), values=[0, 1], weights=[0.9, 0.1], random=True) \
    .withColumn("CreatedUserRegionID", IntegerType(), minValue=1, maxValue=5, random=True) \
    .withColumn("Createddatetime", TimestampType(), begin="2023-07-04 00:00:00", end="2024-07-04 23:59:59") \
    .withColumn("ModifiedUserRegionID", IntegerType(), minValue=1, maxValue=5, random=True) \
    .withColumn("ModifiedDatetime", TimestampType(), values=[None]) \
    .withColumn("DeletedUserRegionID", IntegerType(), minValue=1, maxValue=5, random=True) \
    .withColumn("DeletedDatetime", TimestampType(), values=[None]) \
    .withColumn("ProcessHTML", StringType(), values=[fake.ipv4_network_class() for i in range(num_rows)], random=True)

ClaimsCoding = data_gen.build()
display(ClaimsCoding)

# COMMAND ----------

data_gen = dg.DataGenerator(spark, name="ClaimRequesttype", rows=num_rows, seedColumnName="unique_id") \
    .withColumn("ID", IntegerType(), minValue=1,maxValue=num_rows) \
    .withColumn("Name", StringType(), values=["Preauth Request", "Preauth Enhancement", "Preauth Final", "Claim File", "Query Response", "Re-Open", "Pre-Post", "Re-settlement", "HCB", "Convalescence"]) \
    .withColumn("ClaimTypeID", IntegerType(), values=claim_types,random=True) \
    .withColumn("Deleted", IntegerType(), values=[0,1],weights=[0.9,0.1],random=True) \
    .withColumn("CreatedDatetime", TimestampType(), begin="2023-07-04 00:00:00", end="2024-07-04 23:59:59")

ClaimRequesttype = data_gen.build()
display(ClaimRequesttype)

# COMMAND ----------

data_gen = dg.DataGenerator(spark, name="ClaimServiceType", rows=num_rows, seedColumnName="unique_id") \
    .withColumn("ID", IntegerType(), values=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]) \
    .withColumn("Name", StringType(), values=["In-Patient", "Out-Patient", "Day Care", "Hospitalization", "Out of SI Benefits", "HCB/DCB", "Critical Illness", "HCB/DCB", "Critical Illness", "Personal Accident"]) \
    .withColumn("ParentID", IntegerType(), minValue=0, maxValue=20,random=True) \
    .withColumn("Deleted", IntegerType(), values=[0,1],weights=[0.9,0.1],random=True) \
    .withColumn("CreatedDatetime", TimestampType(), begin="2023-07-04 00:00:00", end="2024-07-04 23:59:59")

ClaimServiceType = data_gen.build()
display(ClaimServiceType)

# COMMAND ----------

data_gen = dg.DataGenerator(spark, name="ICD10", rows=num_rows, seedColumnName="unique_id") \
    .withColumn("ID", IntegerType(), minValue=12000, maxValue=12000+num_rows) \
    .withColumn("DiseaseCode", StringType(), values=["A00-B99", "C00-D49", "D50-D89", "E00-E89", "F01-F99", "G00-G99", "H00-H59", "H60-H95", "I00-I99", "J00-J99"],random=True) \
    .withColumn("Name", StringType(), values=[
        "Certain infectious and parasitic diseases", 
        "Neoplasms", 
        "Diseases of the blood and blood-forming organs and certain disorders involving the immune mechanism", 
        "Endocrine, nutritional and metabolic diseases", 
        "Mental, Behavioral and Neurodevelopmental disorders", 
        "Diseases of the nervous system", 
        "Diseases of the eye and adnexa", 
        "Diseases of the ear and mastoid process", 
        "Diseases of the circulatory system", 
        "Diseases of the respiratory system"
    ],random=True) \
    .withColumn("ParentID", IntegerType(), values=[0,1,2,3,4,5],random=True) \
    .withColumn("Createddatetime", TimestampType(), begin="2023-07-04 00:00:00", end="2024-07-04 23:59:59") \
    .withColumn("Modifieddatetime", TimestampType(), begin="2023-01-01 00:00:00", end="2024-12-31 23:59:59") \
    .withColumn("Deleted", IntegerType(), values=[0,1],weights=[0.9,0.1],random=True) \
    .withColumn("Level", IntegerType(), values=levels, weights=[0.6,0.2,0.2], random=True) \
    .withColumn("GCCode", StringType(), values=["A00-B99", "C00-D48", "D50-D89", "E00-E90", "F00-F99", "G00-G99", "H00-H59", "H60-H95", "I00-I99", "J00-J99"]) \
    .withColumn("GCDescription", StringType(), values=[
        "Certain infectious and parasitic diseases", 
        "Neoplasms", 
        "Diseases of blood, blood-forming organs and certain disorders involving the immune mechanism", 
        "Endocrine, nutritional and metabolic diseases", 
        "Mental and behavioural disorders", 
        "Diseases of the nervous system", 
        "Diseases of the eye and adnexa", 
        "Diseases of the ear and mastoid process", 
        "Diseases of the circulatory system", 
        "Diseases of the respiratory system"
    ]) \
    .withColumn("OrderID", IntegerType(), expr="NULL") \
    .withColumn("CommonName", StringType(), expr="NULL")

ICD10 = data_gen.build()
display(ICD10)

# COMMAND ----------

# DBTITLE 1,Mst_Services
Name_list = [
    "Accommodation Charges",
    "ICU Charges",
    "Room Rent",
    "Nursing Charges",
    "DMO/RMO Charges",
    "Others",
    "Professional Charges",
    "Surgeon/Physician",
    "Assistant Surgeon",
    "Anaesthetist"
]
small_name_list = [
    "Accommodation Charges",
    "ICU",
    "Roo",
    "Nursin",
    "DMO/RMO",
    "Others",
    "Professional Charges",
    "Surgeon/Physician",
    "Assistant Surgeon",
    "Anaesthetist"
]
num_rows = 10
mst_services_spec = dg.DataGenerator(spark, name="Mst_Services", rows=num_rows, seedColumnName="unique_id") \
    .withColumn("ID", IntegerType(), minValue=0, maxValue=50) \
    .withColumn("Name", StringType(), values=Name_list) \
    .withColumn("SmallName", StringType(), values=small_name_list) \
    .withColumn("ParentID", IntegerType(), minValue=0, maxValue=10, random=True) \
    .withColumn("ServiceLevel", IntegerType(), values=[1, 2], random=True) \
    .withColumn("Deleted", IntegerType(), values=[0]) \
    .withColumn("SERVICECODE", StringType(), expr="null") \
    .withColumn("Createddatetime", TimestampType(), begin="2020-07-04 00:00:00", end="2021-07-04 23:59:59") \
    .withColumn("Modifieddatetime", TimestampType(), expr="null") \
    .withColumn("IProviderServices", IntegerType(), values=[1, "null"], random=True) \
    .withColumn("UnitType_P67", IntegerType(), values=["null", 282], random=True) \
    .withColumn("DisplayOrder", IntegerType(), minValue=1, maxValue=50)

Mst_Services_df = mst_services_spec.build()
display(Mst_Services_df)

# COMMAND ----------

num_rows = 1000
insurer_dump_spec = dg.DataGenerator(spark, name="InsurerDumpRetrievalTimeStamp_AllInsurers", rows=num_rows, seedColumnName="unique_id") \
    .withColumn("ISSUEID", IntegerType(), minValue=1, maxValue=100, random=True) \
    .withColumn("IsActive", IntegerType(), values=[1]) \
    .withColumn("LastDumpRetrievalDate", DateType(), expr="current_date()")

DBA_Reports_InsurerDumpRetrievalTimeStamp_AllInsurers_df = insurer_dump_spec.build()
display(DBA_Reports_InsurerDumpRetrievalTimeStamp_AllInsurers_df)

# COMMAND ----------

incremental_claims_spec = dg.DataGenerator(spark, name="IncrementalClaims_AllInsurers", rows=num_rows, seedColumnName="unique_id") \
    .withColumn("Claimid", LongType(), minValue=24070402000, maxValue=24070403000, random=True) \
    .withColumn("isprocessed", IntegerType(), values=[0])

DBA_Reports_IncrementalClaims_AllInsurers_df = incremental_claims_spec.build()
display(DBA_Reports_IncrementalClaims_AllInsurers_df)

reprocess_claims_spec = dg.DataGenerator(spark, name="ReprocessClaimList", rows=num_rows, seedColumnName="unique_id") \
    .withColumn("Claimid", LongType(), minValue=24070402000, maxValue=24070403000, random=True) \
    .withColumn("isprocessed", IntegerType(), values=[0])

DBA_Reports_ReprocessClaimList_df = reprocess_claims_spec.build()
display(DBA_Reports_ReprocessClaimList_df)

# COMMAND ----------

dataframes = {
    "Policy": Policy,
    "Claims": Claims,
    "Claimsdetails": Claimsdetails,
    "ClaimActionItems": ClaimActionItems,
    "ClaimsServiceDetails": ClaimsServiceDetails,
    "ClaimRejectionReasons": ClaimRejectionReasons,
    "ClaimsCoding": ClaimsCoding,
    "BufferUtilization": BufferUtilization,
    "TertiaryUtilization": TertiaryUtilization,
    "Mst_Issuingauthority": Mst_Issuingauthority,
    "Provider": Provider,
    "BPSuminsured": BPSuminsured,
    "MemberPolicy": MemberPolicy,
    "MemberContacts": MemberContacts,
    "Intimation": Intimation,
    "ClaimRequesttype": ClaimRequesttype,
    "ICD10": ICD10,
    "Mst_Gender": Mst_Gender,
    "Mst_RelationShip": Mst_RelationShip,
    "ClaimServiceType": ClaimServiceType,
    "Mst_State": Mst_State,
    "Providermou": Providermou,
    "Mst_PropertyValues": Mst_PropertyValues,
    "Mst_Payer": Mst_Payer,
    "Mst_Facility": Mst_Facility,
    "Mst_RejectionReasons": Mst_RejectionReasons,
    "ClaimStage": ClaimStage,
    "ClaimDeductionDetails": ClaimDeductionDetails,
    "Mst_DeductionReasons": Mst_DeductionReasons,
    "BPSIConditions": BPSIConditions,
    "Membersi": Membersi,
    "ClaimUtilizedAmount": ClaimUtilizedAmount,
    "ClaimsIRReasons": ClaimsIRReasons,
    "Mst_IRDocuments": Mst_IRDocuments,
    "BenefitPlan": BenefitPlan,
    "Mst_Agent": Mst_Agent,
    "Mst_pcs": Mst_pcs,
    "ProviderBankDetails": ProviderBankDetails,
    "Lnk_UserRegions": Lnk_UserRegions,
    "Mst_Users": Mst_Users,
    "Mst_Regions": Mst_Regions,
    "Mst_Product": Mst_Product,
    "MemberPortingExt": MemberPortingExt,
    "Mst_AdmissionType": Mst_AdmissionType,
    "dim_request": dim_request,
    "tbl_ClaimsInwardRequest": tbl_ClaimsInwardRequest,
    "TPAProcedures": TPAProcedures,
    "Mst_Corporate": Mst_Corporate,
    "Mst_Broker": Mst_Broker,
    "BPServiceConfigDetails": BPServiceConfigDetails,
    "Mst_Services": Mst_Services_df
}

# COMMAND ----------

dataframes = {
    "MemberPolicy": MemberPolicy
    }

# COMMAND ----------

def write_dataframes_to_database(dataframes, database_name):
    
    for table_name, dataframe in dataframes.items():
        full_table_name = f"{database_name}.{table_name}"
        dataframe.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_table_name)
        print(f"Written {table_name} to {full_table_name}")

write_dataframes_to_database(dataframes, "fhpl")