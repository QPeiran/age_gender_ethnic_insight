from google.cloud import bigquery

bq_client = bigquery.Client()

############### Setp1: Create datasets ##############

def createDatasets(dataset_name, client):
    dataset_id = "countdownintervewtest.%s" % dataset_name.format(client.project)
    print(dataset_id)
    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)
    # Specify the geographic location where the dataset should reside.
    dataset.location = "australia-southeast1"
    dataset = bq_client.create_dataset(dataset)  # Make an API request.
    print("succ")


################### Step2: pull .csv from cloud storage ########

def create_table(dataset_id,client,SECHMA):
    dataset_ref = client.dataset(dataset_id)
    job_config = bigquery.LoadJobConfig()
    job_config.schema = SECHMA
    job_config.skip_leading_rows = 1
    # The source format defaults to CSV, so the line below is optional.
    job_config.source_format = bigquery.SourceFormat.CSV
    uri = "gs://cloud-samples-data/bigquery/us-states/us-states.csv"

    load_job = client.load_table_from_uri(
        uri, dataset_ref.table("us_states"), job_config=job_config
    )  # API request
    print("Starting job {}".format(load_job.job_id))

    load_job.result()  # Waits for table load to complete.
    print("Job finished.")

    destination_table = client.get_table(dataset_ref.table("us_states"))
    print("Loaded {} rows.".format(destination_table.num_rows))

###############################################


# ["2018_AESA_raw", "LookupAge", "LookupEthnic", "LookupSex", "LookupArea"]
dataset_name = "CDI_test"  # <-- my dataset's name

createDatasets(dataset_name, bq_client)
sechema_lookup_age = [
    bigquery.SchemaField("Code", "STRING","REQUIRED"),
    bigquery.SchemaField("Description", "STRING","REQUIRED"),
    bigquery.SchemaField("Description", "STRING","REQUIRED"),
    ]
