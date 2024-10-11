# Databricks notebook source
# MAGIC %md
# MAGIC ## TX

# TX_DF ----------

tx_df = spark.sql("SELECT * FROM telco")
DATEHEURE String,EVENT_TYPE String,QUANTITY String,MSISDN_A String,MSISDN_B String,DIRECTION String,CELLCODE String,IMEI String,dummy1 String,dummy2 String,

# SITE_DF ----------

site_df = spark.sql("SELECT * FROM locations")
site_schema = 'CELLNAME String,LAC String,CI String,YEAR String,MONTH String,REGION String,SITE_NAME String,CELLCODE String,TOWNNAME  String'

site_df = spark.createDataFrame(site_df.rdd, site_schema)
# ----------
display(site_df)

# CUST_DF ----------

customers_df = spark.sql("SELECT * FROM customers")
cust_schema = 'CUSTOMER_ID String,CUSTCODE String,CUSTOMER_ID_HIGH String,TITRE String,SOCIETE String,PRENOM String,NOM String,RUE String,NO_RUE String,QUARTIER String,CODE_POSTAL String,VILLE String,ADRESSE_NOTE_1 String,ADRESSE_NOTE_2 String,ADRESSE_NOTE_3 String,TEL String,FAX String,EMAIL String,SMS String,SOCIALSECNO String,PASSPORTNO String,COMPREGNO String,ENTDATE String,MODDATE String,DEALER_ID String,BIRTHDATE String'
customers_df = spark.createDataFrame(customers_df.rdd, cust_schema)
#  ----------
display(customers_df)

# contracts_df ----------

contracts_df = spark.sql("SELECT * FROM contracts")

contract_schema = "HID String,CUSTOMER_ID String,CO_ID String,RATEPLAN String,CO_TYPE String,DNNUM String,SIMNO String,PORTNO String,CO_ACTIVATED String,COSTAT String,COREASON String,CO_MODDATE"
#  ----------
display(contracts_df) # type: ignore

# PROCESS_CONTRACT_x_CUSTOMERS ----------

from pyspark.sql import functions as F # type: ignore

# First, process the contracts dataframe
contracts_filtered = contracts_df.filter(
    (F.col("COSTAT") == 'a') &
    (F.date_format("CO_MODDATE", "yyyy-MM-dd") != "0000-00-00") &
    (F.date_format("CO_ACTIVATED", "yyyy-MM-dd") != "0000-00-00")
)

contracts_grouped = contracts_filtered.groupBy("CUSTOMER_ID", "DNNUM").agg(
    F.max("CO_MODDATE").alias("MODDATE")
)



# COMMAND ----------

# Now, join the processed contracts with the customers dataframe
customers_final = customers_df.join(
    contracts_grouped,
    on="CUSTOMER_ID",
    how="left"
).select(
    customers_df.CUSTOMER_ID,
    customers_df.NOM,
    customers_df.PRENOM,
    contracts_grouped.DNNUM.alias("MSISDN")
)

# If you want to create a table from this DataFrame
customers_final.write.mode("overwrite").saveAsTable("CUSTOMERS_FINAL")

# COMMAND ----------

display(customers_final)

# COMMAND ----------

customers_df = customers_df.withColumnRenamed("NOM", "CUST_NAME")
customers_A = customers_df.withColumnRenamed("CUSTOMER_ID", "CUSTOMER_ID_A").withColumnRenamed("CUST_NAME", "A_NAME")
customers_B = customers_df.withColumnRenamed("CUSTOMER_ID", "CUSTOMER_ID_B").withColumnRenamed("CUST_NAME", "B_NAME")

es_df = (tx_df.join(site_df, tx_df["CELLCODE"] == site_df["CELLCODE"]).join(customers_A, tx_df["MSISDN_A"] == customers_A["CUSTOMER_ID_A"]).join(customers_B, tx_df["MSISDN_B"] == customers_B["CUSTOMER_ID_B"]))



######################################################################
# MISE A JOUR DU 01/10/2024
#
from pyspark.sql import functions as F

# Step 1: Filter contracts DataFrame based on the conditions
filtered_contracts_df = contracts_df.filter(
    (contracts_df.COSTAT == 'a') &
    (F.date_format(contracts_df.CO_MODDATE, 'yyyy-MM-dd') != '0000-00-00') &
    (F.date_format(contracts_df.CO_ACTIVATED, 'yyyy-MM-dd') != '0000-00-00') &
    (F.date_format(contracts_df.CO_MODDATE, 'yyyy-MM-dd') <=
        F.date_format(
            spark.sql("SELECT DATEHEURE FROM transactions_telco LIMIT 1").collect()[0][0], 'yyyy-MM-dd'
        )
    )
)

# Step 2: Group by CUSTOMER_ID and DNNUM to get the max CO_MODDATE
grouped_contracts_df = filtered_contracts_df.groupBy('CUSTOMER_ID', 'DNNUM').agg(
    F.max('CO_MODDATE').alias('MODDATE')
)

# Step 3: Join the customers DataFrame with the grouped contracts DataFrame
customers_x_contracts_df = customers_df.alias("cus").join(
    grouped_contracts_df.alias("con"),
    on=F.col("cus.CUSTOMER_ID") == F.col("con.CUSTOMER_ID"),
    how='left'
)

# Step 4: Select the relevant columns for the final DataFrame
customers_final_df = customers_final_df.select(
    "cus.CUSTOMER_ID",
    "NOM",
    "PRENOM",
    F.col("SOCIALSECNO").alias("NO_CNI"),
    "PASSPORTNO",
    "con.DNNUM"
)

# Display the final DataFrame
show(customers_x_contracts_df) # type: ignore

customers_x_contracts_df.write.parquet("file:///data_enriched/ref/parquet//customers_x_contracts")

############################################################################################################
# COMMAND ----------

display(es_df)

# COMMAND ----------

tx_schema =
A_NAME
A_NUMBER
B_NAME
B_NUMBER
DIRECTION
EQUIPMENT_ID
EVENT_DATETIME
EVENT_QUANTITY
EVENT_TYPE
GEO_LOC
LOCATION_ID
LOCATION_NAME_A
LOCATION_NAME_B
REGION
SOLDE
SUBSCRIBER_TYPE
TRANSACTION_TYPE

# COMMAND ----------

from pyspark.sql.functions import concat, lit, col

# Join tx_df with customers_final_df for A_NAME and B_NAME
es_df = tx_df.join(
    customers_df.select(
        "MSISDN",
        concat("PRENOM", lit(" "), "NOM").alias("A_NAME")
    ).withColumnRenamed("MSISDN", "MSISDN_A"),
    on="MSISDN_A",
    how="left"
).join(
    customers_df.select(
        "MSISDN",
        concat("PRENOM", lit(" "), "NOM").alias("B_NAME")
    ).withColumnRenamed("MSISDN", "MSISDN_B"),
    on="MSISDN_B",
    how="left"
).join(
    site_df.select("CELLCODE", "SITE_NAME"),
    on="CELLCODE",
    how="left"
)

# Join with site_df (assuming this is the same as locations_df)
es_df = es_df.join(
    site_df.select("CELLCODE", "SITE_NAME"),
    on="CELLCODE",
    how="left"
)

# Select and rename columns according to the mapping
es_df = es_df.select(
    "A_NAME",
    tx_df.MSISDN_A.alias("A_NUMBER"),
    "B_NAME",
    tx_df.MSISDN_B.alias("B_NUMBER"),
    "DIRECTION",
    tx_df.IMEI.alias("EQUIPMENT_ID"),
    tx_df.DATEHEURE.alias("EVENT_DATETIME"),
    tx_df.QUANTITY.alias("EVENT_QUANTITY"),
    "EVENT_TYPE",
    lit(None).cast("string").alias(""),
    tx_df.CELLCODE.alias("LOCATION_ID"),
    site_df.SITE_NAME.alias("LOCATION_NAME_A"),
    lit(None).cast("string").alias("TRANSACTION_TYPE")
)

ESEARCH :
A_NAME
A_NUMBER
B_NAME
B_NUMBER
DIRECTION
EQUIPMENT_ID
EVENT_DATETIME
EVENT_QUANTITY
EVENT_TYPE
GEO_LOC
LOCATION_ID
LOCATION_NAME_A
LOCATION_NAME_B
REGION
SOLDE
SUBSCRIBER_TYPE
TRANSACTION_TYPE

ES_DF
CELLCODE
MSISDN_B
MSISDN_A
DATEHEURE
EVENT_TYPE
QUANTITY
DIRECTION
IMEI
A_NAME
B_NAME
SITE_NAME
# COMMAND ----------

display(es_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## OM

# COMMAND ----------

om_df = spark.sql("SELECT * FROM om")
om_schema = 'SENDER_MSISDN String, RECEIVER_MSISDN String, TRANSACTION_AMOUNT String, SERVICE_TYPE String, SENDER_POST_BALANCE String, DATEHEURE String, SENDER_CITY String, RECEIVER_CITY String, SENDER_NAME String, RECEIVER_NAME String'

om_df = spark.createDataFrame(om_df.rdd, om_schema)

# COMMAND ----------

display(om_df)

# COMMAND ----------

from pyspark.sql.functions import lit

es_om_df = om_df.select(
    lit(None).cast("string").alias("RECEIVER_CATEGORY_CODE"),
    "RECEIVER_MSISDN",
    lit(None).cast("string").alias("RECEIVER_POST_BAL"),
    lit(None).cast("string").alias("RECEIVER_PRE_BAL"),
    om_df.RECEIVER_NAME.alias("RECEIVER_USER_NAME"),
    lit(None).cast("string").alias("SENDER_CATEGORY_CODE"),
    "SENDER_MSISDN",
    om_df.SENDER_POST_BALANCE.alias("SENDER_POST_BAL"),
    lit(None).cast("string").alias("SENDER_PRE_BAL"),
    om_df.SENDER_NAME.alias("SENDER_USER_NAME"),
    lit(None).cast("string").alias("SERVICE_CHARGE_RECEIVED"),
    "SERVICE_TYPE",
    lit(None).cast("string").alias("TNO_MSISDN"),
    "TRANSACTION_AMOUNT",
    om_df.DATEHEURE.alias("TRANSFER_DATETIME"),
    lit(None).cast("string").alias("TRANSFER_ID"),
    lit(None).cast("string").alias("TRANSFER_STATUS")
)

# COMMAND ----------

display(es_om_df)
