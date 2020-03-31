from google.cloud import bigquery

# Construct a BigQuery client object.
bq_client = bigquery.Client()

table_id = "countdownintervewtest.CDI_test.intermedia_table" # intermedia tabel

job_config = bigquery.QueryJobConfig(destination=table_id, write_disposition="WRITE_TRUNCATE")

#### 1.we only care about the data we are interested in -- use "total" indicator for "Sex" and "Ethnic", "Year" = "2018" for filtering them out
#### 2.Due to the granularities of the "income_table" -- "Area" indicated by "Region" is what we want to focus
#### 3.We want to analysis the labour market, so I assume the age/age group should be between "15 - 70 years old" (people too young or old cannot enter the labour market)
query_value_data = """
SELECT Area, Age, CAST(Count AS INT64) as Count_n FROM `countdownintervewtest.CDI_test.raw_main_table`
WHERE (Area IN ("01", "02", "03", "04", "05", "06", "07", "08", "09", "12", "13", "14", "15", "16", "17", "18"))
    AND (Age IN ("015", "016", "017", "018", "019", "020", "021", "022", "023", "024", "025", "026", "027", "028", "029", "030", "031", "032", "033", "034", 
                 "035", "036", "037", "038", "039", "040", "041", "042", "043", "044", "045", "046", "047", "048", "049", "050", "051", "052", "053", "054",
                 "055", "056", "057", "058", "059", "060", "061", "062", "063", "064", "065", "066", "067", "068", "069"))
    AND (Sex = "9") # total sex
    AND (Ethnic = "9999")   # total ethnic
    AND (Year = "2018") # year 2018
"""

query_job = bq_client.query(query_value_data, job_config=job_config)
query_job.result()  
print("Step 1 Complete")

#### 
query_refine_data = """

"""
query_job = bq_client.query(query_refine_data, job_config=job_config)
query_job.result()  
print("Step 2 Complete")

#### {query 3}



print("Query results loaded to the table {}".format(table_id))
# print("The query data:")
# for row in query_job:
#     print("name={}, count={}".format(row[0], row["total_people"]))
