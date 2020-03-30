from google.cloud import bigquery

# Construct a BigQuery client object.
bq_client = bigquery.Client()

# we only care about the data we are interested in
query_value_data = """
    #Filter census data from 2018. Choose Age, Ethnic, Area as analysis factors
    SELECT
    Age, Ethnic, Area, SUM(CAST(Count AS FLOAT64)) AS Count_NEW
    FROM
    `countdownintervewtest`.CDI_test.raw_main_table
    WHERE
    Year = "2018"
    GROUP BY Age, Ethnic, Area
"""
query_job = bq_client.query(query_value_data)  # Make an API request.

print("The query data:")
for row in query_job:
    # Row values can be accessed by field name or index.
    print("name={}, count={}".format(row[0], row["total_people"]))