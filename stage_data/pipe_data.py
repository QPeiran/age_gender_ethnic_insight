from google.cloud import bigquery

bq_client = bigquery.Client()


################### Step2: pull .csv from cloud storage then load ########

def create_table(dataset_id,client,SCHEMA,URI,tabel_name):
    dataset_ref = client.dataset(dataset_id)
    job_config = bigquery.LoadJobConfig()
    job_config.schema = SCHEMA
    job_config.skip_leading_rows = 1
    # The source format defaults to CSV, so the line below is optional.
    job_config.source_format = bigquery.SourceFormat.CSV
    uri = URI

    load_job = client.load_table_from_uri(
        uri, dataset_ref.table("%s"%tabel_name), job_config=job_config
    )  # API request
    print("Starting job {}".format(load_job.job_id))

    load_job.result()  # Waits for table load to complete.
    print("Job finished.")

    destination_table = client.get_table(dataset_ref.table("%s"%tabel_name))
    print("Loaded {} rows.".format(destination_table.num_rows))


dataset_name = "CDI_test"  # <-- my dataset's name

schema_lookup_age = [
    bigquery.SchemaField("Code_age", "STRING"),
    bigquery.SchemaField("Description_age", "STRING"),
    bigquery.SchemaField("SortOrder_age", "STRING"),
    ]

schema_lookup_ethnic = [
    bigquery.SchemaField("Code_ethnic", "STRING"),
    bigquery.SchemaField("Description_ethnic", "STRING"),
    bigquery.SchemaField("SortOrder_ethnic", "STRING"),
    ]

schema_lookup_sex = [
    bigquery.SchemaField("Code_sex", "STRING"),
    bigquery.SchemaField("Description_sex", "STRING"),
    bigquery.SchemaField("SortOrder_sex", "STRING"),
    ]

schema_lookup_area = [
    bigquery.SchemaField("Code_area", "STRING"),
    bigquery.SchemaField("Description_area", "STRING"),
    bigquery.SchemaField("SortOrder_area", "STRING"),
    ]



#load lookup_age
URI_lookup_age = "gs://age_sex_ethnic_2018/Age-sex-by-ethnic-group-grouped-total-responses-census-usually-resident-population-counts-2006-2013-2018-Censuses-RC-TA-SA2-DHB/DimenLookupAge8277.csv"
lookup_age_table = "lookup_age_table"

#load lookup_ethnic
URI_lookup_ethnic = "gs://age_sex_ethnic_2018/Age-sex-by-ethnic-group-grouped-total-responses-census-usually-resident-population-counts-2006-2013-2018-Censuses-RC-TA-SA2-DHB/DimenLookupEthnic8277.csv"
lookup_ethnic_table = "lookup_ethnic_table"

#load lookup_sex
URI_lookup_sex = "gs://age_sex_ethnic_2018/Age-sex-by-ethnic-group-grouped-total-responses-census-usually-resident-population-counts-2006-2013-2018-Censuses-RC-TA-SA2-DHB/DimenLookupSex8277.csv"
lookup_sex_table = "lookup_sex_table"

#load lookup_area
URI_lookup_area = "gs://age_sex_ethnic_2018/Age-sex-by-ethnic-group-grouped-total-responses-census-usually-resident-population-counts-2006-2013-2018-Censuses-RC-TA-SA2-DHB/DimenLookupArea8277.csv"
lookup_area_table = "lookup_area_table"

#load raw_main data
schema_raw_main = [
    bigquery.SchemaField("Year", "STRING"),
    bigquery.SchemaField("Age", "STRING"),
    bigquery.SchemaField("Ethnic", "STRING"),
    bigquery.SchemaField("Sex", "STRING"),
    bigquery.SchemaField("Area", "STRING"),
    bigquery.SchemaField("Count", "STRING"),
    ]
URI_raw_main = "gs://age_sex_ethnic_2018/Age-sex-by-ethnic-group-grouped-total-responses-census-usually-resident-population-counts-2006-2013-2018-Censuses-RC-TA-SA2-DHB/Data8277.csv"
raw_main_table = "raw_main_table"

create_table(dataset_name, bq_client, schema_raw_main, URI_raw_main, raw_main_table)
create_table(dataset_name, bq_client, schema_lookup_age, URI_lookup_age, lookup_age_table)
create_table(dataset_name, bq_client, schema_lookup_ethnic, URI_lookup_ethnic, lookup_ethnic_table)
create_table(dataset_name, bq_client, schema_lookup_sex, URI_lookup_sex, lookup_sex_table)
create_table(dataset_name, bq_client, schema_lookup_area, URI_lookup_area, lookup_area_table)