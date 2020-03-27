from google.cloud import storage
from google.cloud import bigquery

storage_client = storage.Client()
bucket = storage_client.get_bucket("age_sex_ethnic_2018")
buckets = list(storage_client.list_buckets())
print(buckets)
blob = bucket.get_blob(f"Age-sex-by-ethnic-group-grouped-total-responses-census-usually-resident-population-counts-2006-2013-2018-Censuses-RC-TA-SA2-DHB/DimenLookupYear8277.csv")
print(blob)
bt = blob.download_as_string()
print(bt)

bigquery_client = bigquery.Client()

def query_stackoverflow():
    client = bigquery.Client()
    query_job = client.query("""
        SELECT
          CONCAT(
            'https://stackoverflow.com/questions/',
            CAST(id as STRING)) as url,
          view_count
        FROM `bigquery-public-data.stackoverflow.posts_questions`
        WHERE tags like '%google-bigquery%'
        ORDER BY view_count DESC
        LIMIT 10""")

    results = query_job.result()  # Waits for job to complete.

    for row in results:
        print("{} : {} views".format(row.url, row.view_count))

query_stackoverflow()