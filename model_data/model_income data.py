from google.cloud import bigquery

# Construct a BigQuery client object.
bq_client = bigquery.Client()

table_id = "countdownintervewtest.data_model.model_income_data" # intermedia tabel

job_config = bigquery.QueryJobConfig(destination=table_id, write_disposition="WRITE_TRUNCATE")

#seperate income type with median & average
query_value_data = """    
WITH median AS ( SELECT Region, Age_Group, Ethnic_Group, Measure, Value as Median_income FROM `countdownintervewtest.CDI_test.income_table`
WHERE Measure = "Median Weekly Earnings"),
     average AS (SELECT Region, Age_Group, Ethnic_Group, Measure, Value as Average_income FROM `countdownintervewtest.CDI_test.income_table`
WHERE Measure = "Average Weekly Earnings")
SELECT median.Region, median.Age_GRoup, median.Median_income, average.Average_income FROM median
FULL JOIN
average
ON
(
  median.Region = average.Region AND
  median.Age_Group = average.Age_Group AND
  median.Ethnic_Group = average.Ethnic_Group
)
"""

query_job = bq_client.query(query_value_data, job_config=job_config)
query_job.result()  
print("Step 1 Complete")



print("Query results loaded to the table {}".format(table_id))
# print("The query data:")
# for row in query_job:
#     print("name={}, count={}".format(row[0], row["total_people"]))
