# Databricks notebook source
from pyspark.sql.functions import col, lit, row_number, concat_ws, when
from pyspark.sql.window import Window
import pandas as pd

spark.sql('use database fhpl')

# COMMAND ----------

# DBTITLE 1,USP_CRITICALILLINESS_SUMINSUREDAMOUNTGETTING
from pyspark.sql.functions import col, lit, sum as pyspark_sum, when,row_number,max,coalesce
from pyspark.sql.window import Window

def USP_CRITICALILLINESS_SUMINSUREDAMOUNTGETTING(claims_df, member_policy_df, member_si_df, 
                                     bp_sum_insured_df, bp_si_conditions_df, 
                                     claim_utilized_amount_df, claims_details_df, 
                                     claim_id):
    # Perform joins and filtering
    available_sub_limit = 0
    table_df = (claims_df.alias("c").join(member_policy_df.alias("mp"), col("c.MemberPolicyID") == col("mp.Id"), "inner")
                .join(member_si_df.alias("s"), col("mp.Id") == col("s.memberpolicyid"), "inner")
                .join(bp_sum_insured_df.alias("bp"), (col("s.BPSIID") == col("bp.ID")) & 
                                          (col("bp.SICategoryID_P20")==71), "inner")
                .join(bp_si_conditions_df.alias("bpcon"), (col("bpcon.BPSIID") == col("bp.ID")) & 
                                          (col("bpcon.BPConditionID") == 30) & 
                                          (col("bpcon.Deleted") == 0), "left")
                .filter((col("c.ID") == claim_id) & (col("c.Deleted") == 0) & (col("s.Deleted") == 0))
                .select(
                    col("mp.MainmemberID"),
                    col("c.MemberPolicyID").alias("MPolicyID"),
                    col("bp.SITypeID"),
                    col("bpcon.ClaimTypeID").alias("ClaimTypeID"),
                    col("bp.SumInsured"),
                    col("bpcon.RelationshipID"),
                    col("bpcon.FamilyLimit"),
                    col("mp.RelationShipID").alias("Mp_RelationshipID")
                ))

    # Add a row number to simulate the identity column
    window_spec = Window.orderBy("MainmemberID")
    table_df = table_df.withColumn("totalval", row_number().over(window_spec))
    min_val = 0
    max_val = table_df.agg({"totalval": "max"}).collect()[0][0]

    while min_val <= max_val:
        # Filter rows where totalval equals min_val
        row = table_df.filter(col("totalval") == min_val).first()

        if row is None:
            min_val += 1
            continue

        sum_insured = row["SumInsured"]
        if (sum_insured is not None) or (sum_insured != ''):
            available_sum_insured = sum_insured
        else:
            sum_insured = 0
        min_val += 1

    return available_sum_insured

# COMMAND ----------

# DBTITLE 1,USP_CRITICALILLINESS_SUMINSUREDAMOUNTGETTING
claims_df = spark.sql("select * from fhpl.claims") 
member_policy_df = spark.sql('select * from fhpl.memberpolicy') 
member_si_df = spark.sql('select * from fhpl.membersi') 
bp_sum_insured_df = spark.sql('select * from fhpl.bpsuminsured') 
bp_si_conditions_df = spark.sql('select * from fhpl.bpsiconditions')
claim_utilized_amount_df = spark.sql('select * from fhpl.claimutilizedamount') 
claims_details_df = spark.sql('select * from fhpl.claimsdetails') 
claim_ID = 24070402335
available_sum_insured =  USP_CRITICALILLINESS_SUMINSUREDAMOUNTGETTING(claims_df,member_policy_df,member_si_df,bp_sum_insured_df,bp_si_conditions_df,claim_utilized_amount_df,claims_details_df,claim_ID)
print(available_sum_insured)

# COMMAND ----------

# DBTITLE 1,USP_SUBLIMITUTILIZATIONAMOUNTGETTING
from pyspark.sql.functions import col, lit, sum as pyspark_sum, when,row_number,max,coalesce
from pyspark.sql.window import Window

def USP_SUBLIMITUTILIZATIONAMOUNTGETTING(claims_df, member_policy_df, member_si_df, 
                                     bp_sum_insured_df, bp_si_conditions_df, 
                                     claim_utilized_amount_df, claims_details_df, 
                                     claim_id):
    # Perform joins and filtering
    available_sub_limit = 0
    table_df = (claims_df.alias("c").join(member_policy_df.alias("mp"), col("c.MemberPolicyID") == col("mp.Id"), "inner")
                .join(member_si_df.alias("s"), col("mp.Id") == col("s.memberpolicyid"), "inner")
                .join(bp_sum_insured_df.alias("bp"), (col("s.BPSIID") == col("bp.ID")) & 
                                          (col("bp.SICategoryID_P20")==69), "inner")
                .join(bp_si_conditions_df.alias("bpcon"), (col("bpcon.BPSIID") == col("bp.ID")) & 
                                          (col("bpcon.BPConditionID") == 30) & 
                                          (col("bpcon.Deleted") == 0), "left")
                .filter((col("c.ID") == claim_id) & (col("c.Deleted") == 0) & (col("s.Deleted") == 0))
                .select(
                    col("mp.MainmemberID"),
                    col("c.MemberPolicyID").alias("MPolicyID"),
                    col("bp.SITypeID"),
                    col("bpcon.ClaimTypeID").alias("ClaimTypeID"),
                    col("bp.SumInsured"),
                    col("bpcon.RelationshipID"),
                    col("bpcon.FamilyLimit"),
                    col("c.RelationShipID").alias("CRelationshipID")
                ))

    # Add a row number to simulate the identity column
    window_spec = Window.orderBy("MainmemberID")
    table_df = table_df.withColumn("totalval", row_number().over(window_spec))
    min_val = 0
    max_val = table_df.agg({"totalval": "max"}).collect()[0][0]

    while min_val <= max_val:
        # Filter rows where totalval equals min_val
        row = table_df.filter(col("totalval") == min_val).first()

        if row is None:
            min_val += 1
            continue

        if row["FamilyLimit"] is None:
            min_val += 1
            continue

        main_member_id = row["MainmemberID"]
        m_policy_id = row["MPolicyID"]
        si_type_id = row["SITypeID"]
        claim_type = row["ClaimTypeID"]
        sum_insured = row["SumInsured"]
        bp_relation_id = str(row["RelationshipID"])
        sub_limit = row["FamilyLimit"]
        c_relation_id = row["CRelationshipID"]

        

        if sub_limit < sum_insured:
            if sub_limit is not None and sub_limit != '':
                # Amount1 Calculation
                amount1_df = (claim_utilized_amount_df.alias("CU")
                              .join(claims_details_df.alias("CD"), 
                                    (col("CU.Claimid") == col("CD.ClaimID")) & 
                                    (col("CU.Slno") == col("CD.Slno")))
                              .join(claims_df.alias("CC"), 
                                    col("CC.ID") == col("CD.ClaimID"))
                              .filter((col("CC.RelationShipID").isin(bp_relation_id.split(','))) & 
                                      (col("CU.Deleted") == 0) & 
                                      (col("CU.SicategoryID") == 69) & 
                                      (main_member_id == col("CU.MainMemberID")) &
                                      (col("CD.ID").isin(claims_details_df.alias("cd").filter((col("cd.RequestTypeID").isin(1,2,3,4)) & (~col("cd.StageID").isin(21, 25, 23)) & (claim_id == col("cd.Claimid"))).sort(col("cd.Slno").desc()).select(col("cd.id")).first().id)))
                              .groupBy("CU.Claimid", "CU.Slno")
                              .agg(_sum("CU.SanctionedAmount").alias("Amount1")))

                amount1 = amount1_df.agg(sum("CU.SanctionedAmount")).collect()[0][0] or 0

                # Amount2 Calculation
                amount2_df = (claim_utilized_amount_df.alias("CU")
                              .join(claims_details_df.alias("CD"), 
                                    (col("CU.Claimid") == col("CD.ClaimID")) & 
                                    (col("CU.Slno") == col("CD.Slno")))
                              .join(claims_df.alias("CC"), 
                                    col("CC.ID") == col("CD.ClaimID"))
                              .filter((col("CC.RelationShipID").isin(bp_relation_id.split(','))) & 
                                      (col("CU.Deleted") == 0) & 
                                      (col("CU.SicategoryID") == 69) & 
                                      (main_member_id == col("CU.MainMemberID")) &
                                      (col("CD.ID").isin(claims_details_df.alias("cd").filter((col("cd.RequestTypeID").isin(7,8,9,10,11,12)) & (~col("cd.StageID").isin(21, 25, 23)) & (claim_id == col("cd.Claimid"))).sort(col("cd.Slno").desc()).select(col("cd.id")).first().id))))
                            #   .groupBy("CU.Claimid", "CU.Slno")
                            #   .agg(_sum("CU.SanctionedAmount").alias("Amount2")))

                amount2 = amount2_df.agg(sum("CU.SanctionedAmount")).collect()[0][0] or 0
                
            else:
                sub_limit = 0
        else:
            sub_limit = 0

        total_amt = amount1 + amount2
        available_sub_limit = total_amt
        min_val += 1

    return available_sub_limit

# COMMAND ----------

claims_df = spark.sql("select * from fhpl.claims") 
member_policy_df = spark.sql('select * from fhpl.memberpolicy') 
member_si_df = spark.sql('select * from fhpl.membersi') 
bp_sum_insured_df = spark.sql('select * from fhpl.bpsuminsured') 
bp_si_conditions_df = spark.sql('select * from fhpl.bpsiconditions')
claim_utilized_amount_df = spark.sql('select * from fhpl.claimutilizedamount') 
claims_details_df = spark.sql('select * from fhpl.claimsdetails') 
claim_ID = 24070402978
available_sum_limit =  USP_SUBLIMITUTILIZATIONAMOUNTGETTING(claims_df,member_policy_df,member_si_df,bp_sum_insured_df,bp_si_conditions_df,claim_utilized_amount_df,claims_details_df,claim_ID)
print(available_sum_limit)

# COMMAND ----------

bp_sum_insured_df = spark.sql('select * from fhpl.bpsuminsured') 
bp_sum_insured_df.filter(col("SICategoryID_P20")==71).count()

bp_si_conditions_df = spark.sql('select * from fhpl.bpsiconditions')
bp_si_conditions_df.filter(col("BPConditionID")==30).count()

# COMMAND ----------

claim_id = 24070402651

table_df = (claims_df.alias("c").join(member_policy_df.alias("mp"), col("c.MemberPolicyID") == col("mp.Id"), "inner")
                .join(member_si_df.alias("s"), col("mp.Id") == col("s.memberpolicyid"), "inner")
                .join(bp_sum_insured_df.alias("bp"), (col("s.BPSIID") == col("bp.ID")) & 
                                          (col("bp.SICategoryID_P20")==71), "inner")
                .join(bp_si_conditions_df.alias("bpcon"), (col("bpcon.BPSIID") == col("bp.ID")) & 
                                          (col("bpcon.BPConditionID") == 30) & 
                                          (col("bpcon.Deleted") == 0), "left")
                .filter((col("c.Deleted") == 0) & (col("s.Deleted") == 0))
                .select( col("c.ID"),
                    col("mp.MainmemberID"),
                    col("c.MemberPolicyID").alias("MPolicyID"),
                    col("bp.SITypeID"),
                    col("bpcon.ClaimTypeID").alias("ClaimTypeID"),
                    col("bp.SumInsured"),
                    col("bpcon.RelationshipID"),
                    col("bpcon.FamilyLimit"),
                    col("c.RelationShipID").alias("CRelationshipID")
                ))
# (col("c.ID") == claim_id) & 

# table_df.select("spark_catalog.fhpl.claims.ID").show()
#table_df.show()
claims_details_df = claims_details_df.select(col("ClaimID"), col("RequestTypeID"), col("StageID"),col("Slno"))
claim_utilized_amount_df = claim_utilized_amount_df.select(col("Claimid"), col("Slno"), col("SanctionedAmount"))
table_df.alias('tb').join(claims_details_df.alias('cd'), col('tb.ID') == col('cd.ClaimID'), 'inner').join(claim_utilized_amount_df.alias("CU"),(col("CU.Claimid") == col("cd.ClaimID")) & 
                                    (col("CU.Slno") == col("cd.Slno"))).filter(col('cd.RequestTypeID').isin(1,2,3,4,7,8,9,10,11,12) &  (~col("cd.StageID").isin(21, 25, 23))).show()



# COMMAND ----------

# DBTITLE 1,USP_CRITICALILLINESS_SUMINSURED
from pyspark.sql.functions import col, lit, sum, when,row_number,max
from pyspark.sql.window import Window

def USP_CRITICALILLINESS_SUMINSURED(claims_df, member_policy_df, member_si_df, 
                                     bp_sum_insured_df, bp_si_conditions_df, 
                                     claim_utilized_amount_df, claims_details_df, 
                                     claim_id):
    # Perform joins and filtering
    table_df = (claims_df.alias("c").join(member_policy_df.alias("mp"), col("c.MemberPolicyID") == col("mp.Id"), "inner")
                .join(member_si_df.alias("s"), col("mp.Id") == col("s.memberpolicyid"), "inner")
                .join(bp_sum_insured_df.alias("bp"), (col("s.BPSIID") == col("bp.ID")) & 
                                          (col("bp.SICategoryID_P20")==71), "inner")
                .join(bp_si_conditions_df.alias("bpcon"), (col("bpcon.BPSIID") == col("bp.ID")) & 
                                          (col("bpcon.BPConditionID") == 30) & 
                                          (col("bpcon.Deleted") == 0), "left")
                .filter((col("c.ID") == claim_id) & (col("c.Deleted") == 0) & (col("s.Deleted") == 0))
                .select(
                    col("mp.MainmemberID"),
                    col("c.MemberPolicyID").alias("MPolicyID"),
                    col("bp.SITypeID"),
                    col("bpcon.ClaimTypeID").alias("ClaimTypeID"),
                    col("bp.SumInsured"),
                    col("bpcon.RelationshipID"),
                    col("bpcon.FamilyLimit"),
                    col("mp.RelationShipID").alias("Mp_RelationshipID")
                ))

    # Add a row number to simulate the identity column
    window_spec = Window.orderBy("MainmemberID")
    table_df = table_df.withColumn("totalval", row_number().over(window_spec))
    min_val = 0
    max_val = table_df.agg({"totalval": "max"}).collect()[0][0]

    while min_val <= max_val:
        # Filter rows where totalval equals min_val
        row = table_df.filter(col("totalval") == min_val).first()

        if row is None:
            min_val += 1
            continue

        main_member_id = row["MainmemberID"]
        m_policy_id = row["MPolicyID"]
        si_type_id = row["SITypeID"]
        claim_type = row["ClaimTypeID"]
        sum_insured = row["SumInsured"]

        if sum_insured is not None:
            if  sum_insured != '':
                # Amount1 Calculation
                amount1_df = (claim_utilized_amount_df.alias("CU")
                              .join(claims_details_df.alias("CD"), 
                                    (col("CU.Claimid") == col("CD.ClaimID")) & 
                                    (col("CU.Slno") == col("CD.Slno")))
                              .join(claims_df.alias("CC"), 
                                    col("CC.ID") == col("CD.ClaimID"))
                              .filter(
                                      (col("CU.Deleted") == 0) & 
                                      (col("CU.SicategoryID") == 71) & 
                                      (main_member_id == col("CU.MainMemberID")) &
                                      (col("CD.ID").isin(claims_details_df.alias("CD").filter((col("CD.RequestTypeID").isin(1,2,3,4)) & (~col("CD.StageID").isin(21, 25, 23)) & (claim_id == col("CD.ClaimID"))).sort(col("CD.Slno").desc()).select(col("CD.id")).first().id))))
                            #   .groupBy("CU.Claimid", "CU.Slno")
                            #   .agg(sum("CU.SanctionedAmount").cast("IntegerType").alias("Amount1")))

                amount1 = amount1_df.agg(sum("CU.SanctionedAmount")).collect()[0][0] or 0

                # Amount2 Calculation
                amount2_df = (claim_utilized_amount_df.alias("CU")
                              .join(claims_details_df.alias("CD"), 
                                    (col("CU.Claimid") == col("CD.ClaimID")) & 
                                    (col("CU.Slno") == col("CD.Slno")))
                              .join(claims_df.alias("CC"), 
                                    col("CC.ID") == col("CD.ClaimID"))
                              .filter(
                                      (col("CU.Deleted") == 0) & 
                                      (col("CU.SicategoryID") == 71) & 
                                      (main_member_id == col("CU.MainMemberID")) &
                                      (col("CD.ID").isin(claims_details_df.alias("CD").filter((col("CD.RequestTypeID").isin(7,8,9,10,11,12)) & (~col("CD.StageID").isin(21, 25, 23)) & (claim_id == col("CD.ClaimID"))).sort(col("CD.Slno").desc()).select(col("CD.id")).first().id))))
                            #   .groupBy("CU.Claimid", "CU.Slno")
                            #   .agg(sum("CU.SanctionedAmount").cast("IntegerType").alias("Amount2")))

                amount2 = amount2_df.agg(sum("CU.SanctionedAmount")).collect()[0][0] or 0

                
            else:
                sum_insured = 0
        else:
            sum_insured = 0
        total_amt = amount1 + amount2
        available_sub_insured = sum_insured-total_amt

        min_val += 1

    return available_sub_insured

# COMMAND ----------

claims_df = spark.sql("select * from fhpl.claims") 
member_policy_df = spark.sql('select * from fhpl.memberpolicy') 
member_si_df = spark.sql('select * from fhpl.membersi') 
bp_sum_insured_df = spark.sql('select * from fhpl.bpsuminsured') 
bp_si_conditions_df = spark.sql('select * from fhpl.bpsiconditions')
claim_utilized_amount_df = spark.sql('select * from fhpl.claimutilizedamount') 
claims_details_df = spark.sql('select * from fhpl.claimsdetails') 
claim_ID = 24070402335
available_sum_insured =  USP_CRITICALILLINESS_SUMINSURED(claims_df,member_policy_df,member_si_df,bp_sum_insured_df,bp_si_conditions_df,claim_utilized_amount_df,claims_details_df,claim_ID)
print(available_sum_insured)

# COMMAND ----------

from pyspark.sql.functions import col, lit, sum as _sum, when,row_number,max
from pyspark.sql.window import Window
claims_details_df = spark.sql('select * from fhpl.claimsdetails') 
claims_details_df.alias('cd').filter((col("cd.RequestTypeID").isin(1,2,3,4)) & (~col("cd.StageID").isin(21, 25, 23))).sort(col("cd.Slno").desc()).select(col("cd.claimid")).first()

# claim_utilized_amount_df.select(_sum(col('SanctionedAmount'))).show()


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fhpl.claimutilizedamount
# MAGIC where SanctionedAmount is null

# COMMAND ----------

# DBTITLE 1,USP_CRITICALILLINESS_SUMINSUREDUTILIZATION
from pyspark.sql.functions import col, lit, sum as pyspark_sum, when,row_number,max
from pyspark.sql.window import Window

def USP_CRITICALILLINESS_SUMINSUREDUTILIZATION(claims_df, member_policy_df, member_si_df, 
                                     bp_sum_insured_df, bp_si_conditions_df, 
                                     claim_utilized_amount_df, claims_details_df, 
                                     claim_id):
    # Perform joins and filtering
    table_df = (claims_df.alias("c").join(member_policy_df.alias("mp"), col("c.MemberPolicyID") == col("mp.Id"), "inner")
                .join(member_si_df.alias("s"), col("mp.Id") == col("s.memberpolicyid"), "inner")
                .join(bp_sum_insured_df.alias("bp"), (col("s.BPSIID") == col("bp.ID")) & 
                                          (col("bp.SICategoryID_P20")==71), "inner")
                .join(bp_si_conditions_df.alias("bpcon"), (col("bpcon.BPSIID") == col("bp.ID")) & 
                                          (col("bpcon.BPConditionID") == 30) & 
                                          (col("bpcon.Deleted") == 0), "left")
                .filter((col("c.ID") == claim_id) & (col("c.Deleted") == 0) & (col("s.Deleted") == 0))
                .select(
                    col("mp.MainmemberID"),
                    col("c.MemberPolicyID").alias("MPolicyID"),
                    col("bp.SITypeID"),
                    col("bpcon.ClaimTypeID").alias("ClaimTypeID"),
                    col("bp.SumInsured"),
                    col("bpcon.RelationshipID"),
                    col("bpcon.FamilyLimit"),
                    col("mp.RelationShipID").alias("Mp_RelationshipID")
                ))

    # Add a row number to simulate the identity column
    window_spec = Window.orderBy("MainmemberID")
    table_df = table_df.withColumn("totalval", row_number().over(window_spec))
    min_val = 0
    max_val = table_df.agg({"totalval": "max"}).collect()[0][0]

    while min_val <= max_val:
        # Filter rows where totalval equals min_val
        row = table_df.filter(col("totalval") == min_val).first()

        if row is None:
            min_val += 1
            continue

        main_member_id = row["MainmemberID"]
        m_policy_id = row["MPolicyID"]
        si_type_id = row["SITypeID"]
        claim_type = row["ClaimTypeID"]
        sum_insured = row["SumInsured"]

        if sum_insured is not None:
            if  sum_insured != '':
                # Amount1 Calculation
                amount1_df = (claim_utilized_amount_df.alias("CU")
                              .join(claims_details_df.alias("CD"), 
                                    (col("CU.Claimid") == col("CD.ClaimID")) & 
                                    (col("CU.Slno") == col("CD.Slno")))
                              .join(claims_df.alias("CC"), 
                                    col("CC.ID") == col("CD.ClaimID"))
                              .filter(
                                      (col("CU.Deleted") == 0) & 
                                      (col("CU.SicategoryID") == 71) & 
                                      (main_member_id == col("CU.MainMemberID")) &
                                      (col("CD.ID").isin(claims_details_df.alias('cd').filter((col("cd.RequestTypeID").isin(1,2,3,4)) & (~col("cd.StageID").isin(21, 25, 23)) & (claim_id == col("cd.Claimid"))).sort(col("cd.Slno").desc()).select(col("cd.id")).first().id))))
                            #   .groupBy("CU.Claimid", "CU.Slno")
                            #   .agg(_sum("CU.SanctionedAmount").alias("Amount1")))

                amount1 = amount1_df.agg(sum("CU.SanctionedAmount")).collect()[0][0] or 0

                # Amount2 Calculation
                amount2_df = (claim_utilized_amount_df.alias("CU")
                              .join(claims_details_df.alias("CD"), 
                                    (col("CU.Claimid") == col("CD.ClaimID")) & 
                                    (col("CU.Slno") == col("CD.Slno")))
                              .join(claims_df.alias("CC"), 
                                    col("CC.ID") == col("CD.ClaimID"))
                              .filter(
                                      (col("CU.Deleted") == 0) & 
                                      (col("CU.SicategoryID") == 71) & 
                                      (main_member_id == col("CU.MainMemberID")) &
                                      (col("CD.ID").isin(claims_details_df.alias('cd').filter((col("cd.RequestTypeID").isin(7,8,9,10,11,12)) & (~col("cd.StageID").isin(21, 25, 23)) & (claim_id == col("cd.Claimid"))).sort(col("cd.Slno").desc()).select(col("cd.id")).first().id))))
                            #   .groupBy("CU.Claimid", "CU.Slno")
                            #   .agg(_sum("CU.SanctionedAmount").alias("Amount2")))

                amount2 = amount2_df.agg(sum("CU.SanctionedAmount")).collect()[0][0] or 0

                
            else:
                sum_insured = 0
        else:
            sum_insured = 0
        total_amt = amount1 + amount2
        available_sub_insured = total_amt

        min_val += 1

    return available_sub_insured

# COMMAND ----------

claims_df = spark.sql("select * from fhpl.claims") 
member_policy_df = spark.sql('select * from fhpl.memberpolicy') 
member_si_df = spark.sql('select * from fhpl.membersi') 
bp_sum_insured_df = spark.sql('select * from fhpl.bpsuminsured') 
bp_si_conditions_df = spark.sql('select * from fhpl.bpsiconditions')
claim_utilized_amount_df = spark.sql('select * from fhpl.claimutilizedamount') 
claims_details_df = spark.sql('select * from fhpl.claimsdetails') 
claim_ID = 24070402335
available_sum_insured =  USP_CRITICALILLINESS_SUMINSUREDUTILIZATION(claims_df,member_policy_df,member_si_df,bp_sum_insured_df,bp_si_conditions_df,claim_utilized_amount_df,claims_details_df,claim_ID)
print(available_sum_insured)

# COMMAND ----------

# DBTITLE 1,ACKOSUB_LIMITBALANCESUMINSURED
from pyspark.sql.functions import col, lit, sum as pyspark_sum, when,row_number,max,coalesce
from pyspark.sql.window import Window

def ACKOSUB_LIMITBALANCESUMINSURED(claims_df, member_policy_df, member_si_df, 
                                     bp_sum_insured_df, bp_si_conditions_df, 
                                     claim_utilized_amount_df, claims_details_df, 
                                     claim_id):
    # Perform joins and filtering
    available_sub_limit = 0
    table_df = (claims_df.alias("c").join(member_policy_df.alias("mp"), col("c.MemberPolicyID") == col("mp.Id"), "inner")
                .join(member_si_df.alias("s"), col("mp.Id") == col("s.memberpolicyid"), "inner")
                .join(bp_sum_insured_df.alias("bp"), (col("s.BPSIID") == col("bp.ID")) & 
                                          (col("bp.SICategoryID_P20")==69), "inner")
                .join(bp_si_conditions_df.alias("bpcon"), (col("bpcon.BPSIID") == col("bp.ID")) & 
                                          (col("bpcon.BPConditionID") == 30) & 
                                          (col("bpcon.Deleted") == 0), "left")
                .filter((col("c.ID") == claim_id) & (col("c.Deleted") == 0) & (col("s.Deleted") == 0))
                .select(
                    col("mp.MainmemberID"),
                    col("c.MemberPolicyID").alias("MPolicyID"),
                    col("bp.SITypeID"),
                    col("bpcon.ClaimTypeID").alias("ClaimTypeID"),
                    col("bp.SumInsured"),
                    col("bpcon.RelationshipID"),
                    col("bpcon.FamilyLimit"),
                    col("mp.RelationShipID").alias("Mp_RelationshipID")
                ))

    # Add a row number to simulate the identity column
    window_spec = Window.orderBy("MainmemberID")
    table_df = table_df.withColumn("totalval", row_number().over(window_spec))
    min_val = 0
    max_val = table_df.agg({"totalval": "max"}).collect()[0][0]

    while min_val <= max_val:
        # Filter rows where totalval equals min_val
        row = table_df.filter(col("totalval") == min_val).first()

        if row is None:
            min_val += 1
            continue

        if row["FamilyLimit"] is None:
            min_val += 1
            continue

        main_member_id = row["MainmemberID"]
        m_policy_id = row["MPolicyID"]
        si_type_id = row["SITypeID"]
        claim_type = row["ClaimTypeID"]
        sum_insured = row["SumInsured"]
        bp_relation_id = str(row["RelationshipID"])
        sub_limit = row["FamilyLimit"]
        c_relation_id = row["CRelationshipID"]

        

        if sub_limit < sum_insured:
            if sub_limit is not None and sub_limit != '':
                # Amount1 Calculation
                amount1_df = (claim_utilized_amount_df.alias("CU")
                              .join(claims_details_df.alias("CD"), 
                                    (col("CU.Claimid") == col("CD.ClaimID")) & 
                                    (col("CU.Slno") == col("CD.Slno")))
                              .join(claims_df.alias("CC"), 
                                    col("CC.ID") == col("CD.ClaimID"))
                              .filter((col("CC.RelationShipID").isin(bp_relation_id.split(','))) & 
                                      (col("CU.Deleted") == 0) & 
                                      (col("CU.SicategoryID") == 69) & 
                                      (main_member_id == col("CU.MainMemberID")) &
                                      (col("CD.ID").isin(claims_details_df.alias("cd").filter((col("cd.RequestTypeID").isin(1,2,3,4)) & (~col("cd.StageID").isin(21, 25, 23)) & (claim_id == col("cd.Claimid"))).sort(col("cd.Slno").desc()).select(col("cd.id")).first().id)))
                              .groupBy("CU.Claimid", "CU.Slno")
                              .agg(_sum("CU.SanctionedAmount").alias("Amount1")))

                amount1 = amount1_df.agg(sum("CU.SanctionedAmount")).collect()[0][0] or 0

                # Amount2 Calculation
                amount2_df = (claim_utilized_amount_df.alias("CU")
                              .join(claims_details_df.alias("CD"), 
                                    (col("CU.Claimid") == col("CD.ClaimID")) & 
                                    (col("CU.Slno") == col("CD.Slno")))
                              .join(claims_df.alias("CC"), 
                                    col("CC.ID") == col("CD.ClaimID"))
                              .filter((col("CC.RelationShipID").isin(bp_relation_id.split(','))) & 
                                      (col("CU.Deleted") == 0) & 
                                      (col("CU.SicategoryID") == 69) & 
                                      (main_member_id == col("CU.MainMemberID")) &
                                      (col("CD.ID").isin(claims_details_df.alias("cd").filter((col("cd.RequestTypeID").isin(7,8,9,10,11,12)) & (~col("cd.StageID").isin(21, 25, 23)) & (claim_id == col("cd.Claimid"))).sort(col("cd.Slno").desc()).select(col("cd.id")).first().id))))
                            #   .groupBy("CU.Claimid", "CU.Slno")
                            #   .agg(_sum("CU.SanctionedAmount").alias("Amount2")))

                amount2 = amount2_df.agg(sum("CU.SanctionedAmount")).collect()[0][0] or 0
                
            else:
                sub_limit = 0
        else:
            sub_limit = 0

        total_amt = amount1 + amount2
        available_sub_limit = sub_limit - total_amt
        min_val += 1

    return available_sub_limit

# COMMAND ----------

claims_df = spark.sql("select * from fhpl.claims") 
member_policy_df = spark.sql('select * from fhpl.memberpolicy') 
member_si_df = spark.sql('select * from fhpl.membersi') 
bp_sum_insured_df = spark.sql('select * from fhpl.bpsuminsured') 
bp_si_conditions_df = spark.sql('select * from fhpl.bpsiconditions')
claim_utilized_amount_df = spark.sql('select * from fhpl.claimutilizedamount') 
claims_details_df = spark.sql('select * from fhpl.claimsdetails') 
claim_ID = 24070402978
available_sum_insured =  ACKOSUB_LIMITBALANCESUMINSURED(claims_df,member_policy_df,member_si_df,bp_sum_insured_df,bp_si_conditions_df,claim_utilized_amount_df,claims_details_df,claim_ID)
print(available_sum_insured)

# COMMAND ----------

# DBTITLE 1,USP_ALLINSURERWISE_CLAIMWISE_TOTAL_SUBLIMIT
from pyspark.sql.functions import col, lit, sum as pyspark_sum, when,row_number,max,coalesce
from pyspark.sql.window import Window

def USP_ALLINSURERWISE_CLAIMWISE_TOTAL_SUBLIMIT(claims_df, member_policy_df, member_si_df, 
                                     bp_sum_insured_df, bp_si_conditions_df, 
                                     claim_utilized_amount_df, claims_details_df, 
                                     claim_id):
    # Perform joins and filtering
    available_sub_limit = 0
    table_df = (claims_df.alias("c").join(member_policy_df.alias("mp"), col("c.MemberPolicyID") == col("mp.Id"), "inner")
                .join(member_si_df.alias("s"), col("mp.Id") == col("s.memberpolicyid"), "inner")
                .join(bp_sum_insured_df.alias("bp"), (col("s.BPSIID") == col("bp.ID")) & 
                                          (col("bp.SICategoryID_P20")==69), "inner")
                .join(bp_si_conditions_df.alias("bpcon"), (col("bpcon.BPSIID") == col("bp.ID")) & 
                                          (col("bpcon.BPConditionID") == 30) & 
                                          (col("bpcon.Deleted") == 0), "left")
                .filter((col("c.ID") == claim_id) & (col("c.Deleted") == 0) & (col("s.Deleted") == 0))
                .select(
                    col("mp.MainmemberID"),
                    col("c.MemberPolicyID").alias("MPolicyID"),
                    col("bp.SITypeID"),
                    col("bpcon.ClaimTypeID").alias("ClaimTypeID"),
                    col("bp.SumInsured"),
                    col("bpcon.RelationshipID"),
                    col("bpcon.FamilyLimit"),
                    col("mp.RelationShipID").alias("Mp_RelationshipID")
                ))

    # Add a row number to simulate the identity column
    window_spec = Window.orderBy("MainmemberID")
    table_df = table_df.withColumn("totalval", row_number().over(window_spec))
    min_val = 0
    max_val = table_df.agg({"totalval": "max"}).collect()[0][0]

    while min_val <= max_val:
        # Filter rows where totalval equals min_val
        row = table_df.filter(col("totalval") == min_val).first()

        if row is None:
            min_val += 1
            continue
        if row["FamilyLimit"] is None:
            min_val += 1
            continue
        
        sum_insured = row["SumInsured"]
        bp_relation_id = str(row["RelationshipID"])
        sub_limit = row["FamilyLimit"]

        if sub_limit < sum_insured:
            available_sub_limit = sub_limit
        else:
            sub_limit = 0


        min_val += 1

    return available_sub_limit

# COMMAND ----------

claims_df = spark.sql("select * from fhpl.claims") 
member_policy_df = spark.sql('select * from fhpl.memberpolicy') 
member_si_df = spark.sql('select * from fhpl.membersi') 
bp_sum_insured_df = spark.sql('select * from fhpl.bpsuminsured') 
bp_si_conditions_df = spark.sql('select * from fhpl.bpsiconditions')
claim_utilized_amount_df = spark.sql('select * from fhpl.claimutilizedamount') 
claims_details_df = spark.sql('select * from fhpl.claimsdetails') 
claim_ID = 24070402978
available_sum_insured =  USP_ALLINSURERWISE_CLAIMWISE_TOTAL_SUBLIMIT(claims_df,member_policy_df,member_si_df,bp_sum_insured_df,bp_si_conditions_df,claim_utilized_amount_df,claims_details_df,claim_ID)
print(available_sum_insured)