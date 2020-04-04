from google.cloud import bigquery

# Construct a BigQuery client object.
bq_client = bigquery.Client()

table_id = "countdownintervewtest.data_model.model_census_data" # intermedia tabel

job_config = bigquery.QueryJobConfig(destination=table_id, write_disposition="WRITE_TRUNCATE")

#### 1.we only care about the data we are interested in -- use "total" indicator for "Sex" and "Ethnic", "Year" = "2018" for filtering them out
#### 2.Due to the granularities of the "income_table" -- "Area" indicated by "Region" is what we want to focus
#### 3.We want to analysis the labour market, so I assume the age/age group should be between "15 - 70 years old" (people too young or old cannot enter the labour market)
query_value_data = """
SELECT Area as Area_code, CAST(Age AS INT64) as Age, CAST(Count AS INT64) as count FROM `countdownintervewtest.CDI_test.raw_main_table`
WHERE (Area IN ("01", "02", "03", "04", "05", "06", "07", "08", "09", "12", "13", "14", "15", "16", "17", "18"))
    AND (Age IN ("015", "016", "017", "018", "019", "020", "021", "022", "023", "024", "025", "026", "027", "028", "029", "030", "031", "032", "033", "034", 
                 "035", "036", "037", "038", "039", "040", "041", "042", "043", "044", "045", "046", "047", "048", "049", "050", "051", "052", "053", "054",
                 "055", "056", "057", "058", "059", "060", "061", "062", "063", "064", "065", "066", "067", "068", "069"))
    AND (Sex = "9") # total sex
    AND (Ethnic = "9999")   # total ethnic
    AND (Year = "2018") # year 2018
    ORDER BY Age
"""

query_job = bq_client.query(query_value_data, job_config=job_config)
query_job.result()  
print("Step 1 Complete")

#### to align granularity with the income data
query_refine_data = """
WITH aggrate_area AS (SELECT Age, sum(count) AS count, "56" AS Area_code
FROM `countdownintervewtest.CDI_test.intermedia_table` 
WHERE Area_code IN ("05","06")
GROUP BY Age
UNION ALL
SELECT Age, sum(count) AS count, "12161718" AS Area_code
FROM `countdownintervewtest.CDI_test.intermedia_table` 
WHERE Area_code IN ("12","16","17","18")
GROUP BY Age
UNION ALL
SELECT Age, count, Area_code
FROM `countdownintervewtest.CDI_test.intermedia_table` 
WHERE Area_code NOT IN ("05","06","12","16","17","18")
)
SELECT Age, count, Area_code, "15 to 19" AS Age_group
FROM  aggrate_area
WHERE Age IN (15, 16, 17, 18, 19) 
UNION ALL
SELECT Age, count, Area_code, "20 to 24" AS Age_group
FROM aggrate_area 
WHERE Age IN (20, 21, 22, 23, 24) 
UNION ALL
SELECT Age, count, Area_code, "25 to 29" AS Age_group
FROM aggrate_area 
WHERE Age IN (25, 26, 27, 28, 29) 
UNION ALL
SELECT Age, count, Area_code, "30 to 34" AS Age_group
FROM aggrate_area 
WHERE Age IN (30, 31, 32, 33, 34)
UNION ALL
SELECT Age, count, Area_code, "35 to 39" AS Age_group
FROM aggrate_area 
WHERE Age IN (35, 36, 37, 38, 39) 
UNION ALL
SELECT Age, count, Area_code, "40 to 44" AS Age_group
FROM aggrate_area 
WHERE Age IN (40, 41, 42, 43, 44)
UNION ALL
SELECT Age, count, Area_code, "45 to 49" AS Age_group
FROM aggrate_area 
WHERE Age IN (45, 46, 47, 48, 49) 
UNION ALL
SELECT Age, count, Area_code, "50 to 54" AS Age_group
FROM aggrate_area 
WHERE Age IN (50, 51, 52, 53, 54)
UNION ALL
SELECT Age, count, Area_code, "55 to 59" AS Age_group
FROM aggrate_area 
WHERE Age IN (55, 56, 57, 58, 59) 
UNION ALL
SELECT Age, count, Area_code, "60 to 64" AS Age_group
FROM aggrate_area 
WHERE Age IN (60, 61, 62, 63, 64) 
UNION ALL
SELECT Age, count, Area_code, "65 plus" AS Age_group
FROM aggrate_area 
WHERE Age IN (65, 66, 67, 68, 69) 
ORDER BY Age
"""
query_job = bq_client.query(query_refine_data, job_config=job_config)
query_job.result()  
print("Step 2 Complete")

#### Join Lookup Area table
query_lookup_data = """
SELECT Region, Age_group, Age, count FROM `countdownintervewtest.data_model.model_census_data` AS A
LEFT JOIN `countdownintervewtest.CDI_test.LookupArea` AS B
ON (
  A.Area_code = B.Code
)
"""
query_job = bq_client.query(query_lookup_data, job_config=job_config)
query_job.result()  
print("Step 3 Complete")


print("Query results loaded to the table {}".format(table_id))
# print("The query data:")
# for row in query_job:
#     print("name={}, count={}".format(row[0], row["total_people"]))
