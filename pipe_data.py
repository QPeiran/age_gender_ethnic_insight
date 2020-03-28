from google.cloud import bigquery

bq_client = bigquery.Client()


################### Step2: pull .csv from cloud storage then load ########

def create_table(dataset_id,client,SCHEMA,URI):
    dataset_ref = client.dataset(dataset_id)
    job_config = bigquery.LoadJobConfig()
    job_config.schema = SCHEMA
    job_config.skip_leading_rows = 1
    # The source format defaults to CSV, so the line below is optional.
    job_config.source_format = bigquery.SourceFormat.CSV
    uri = URI

    load_job = client.load_table_from_uri(
        uri, dataset_ref.table("myTable"), job_config=job_config
    )  # API request
    print("Starting job {}".format(load_job.job_id))

    load_job.result()  # Waits for table load to complete.
    print("Job finished.")

    destination_table = client.get_table(dataset_ref.table("myTable"))
    print("Loaded {} rows.".format(destination_table.num_rows))


# ["2018_AESA_raw", "LookupAge", "LookupEthnic", "LookupSex", "LookupArea"]
dataset_name = "CDI_test"  # <-- my dataset's name
#load lookup_age
schema_lookup_age = [
    bigquery.SchemaField("Code", "STRING","REQUIRED"),
    bigquery.SchemaField("Description", "STRING","REQUIRED"),
    bigquery.SchemaField("SortOrder", "STRING","REQUIRED"),
    ]
URI_lookup_age = "gs://age_sex_ethnic_2018/Age-sex-by-ethnic-group-grouped-total-responses-census-usually-resident-population-counts-2006-2013-2018-Censuses-RC-TA-SA2-DHB/DimenLookupAge8277.csv"

create_table(dataset_name, bq_client, schema_lookup_age,URI_lookup_age)