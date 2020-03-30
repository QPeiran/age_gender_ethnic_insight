from google.cloud import bigquery

# Construct a BigQuery client object.
bq_client = bigquery.Client()

table_id = "countdownintervewtest.CDI_test.intermedia_table" # intermedia tabel

job_config = bigquery.QueryJobConfig(destination=table_id, write_disposition="WRITE_TRUNCATE")

#### we only care about the data we are interested in
query_value_data = """
SELECT * FROM `countdownintervewtest.CDI_test.raw_main_table`
WHERE (Area IN ("01", "02", "03", "04", "05", "06", "07", "08", "09", "12", "13", "14", "15", "16", "17", "18"))
    AND (Age IN ("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21"))
    AND (Sex = "9") # total sex
    AND (Ethnic = "9999")   # total ethnic
    AND (Year = "2018") # year 2018
"""

query_job = bq_client.query(query_value_data, job_config=job_config)
query_job.result()  
print("Step 1 Complete")
#### refine data and convert Count to int
query_refine_data = """
SELECT Area, Age, CAST(Count AS INT64) as Count FROM `countdownintervewtest.CDI_test.intermedia_table`
"""
query_job = bq_client.query(query_refine_data, job_config=job_config)
query_job.result()  

print("Step 2 Complete")
print("Query results loaded to the table {}".format(table_id))

# print("The query data:")
# for row in query_job:
#     print("name={}, count={}".format(row[0], row["total_people"]))
