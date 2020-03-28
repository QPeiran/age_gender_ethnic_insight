from google.cloud import bigquery

bq_client = bigquery.Client()

###############Create datasets##############
def createDatasets(dataset_name, client):
    for i in dataset_name:
        dataset_id = "countdownintervewtest.%s"%i.format(client.project)
        print (dataset_id)
        dataset = bigquery.Dataset(dataset_id) # Construct a full Dataset object to send to the API.
        dataset.location = "australia-southeast1" #Specify the geographic location where the dataset should reside.
        dataset = bq_client.create_dataset(dataset)  # Make an API request.
        print("succ")
        #print("Created dataset {}.{}".format(client.project, dataset.dataset_id))

    # Send the dataset to the API for creation.
    # Raises google.api_core.exceptions.Conflict if the Dataset already
    # exists within the project.


dataset_name = ["test"]#["2018_AESA_raw", "LookupAge", "LookupEthnic", "LookupSex", "LookupArea"]

createDatasets(dataset_name,bq_client)

############################################

"""
dataset_id = 'my_dataset'

dataset_ref = client.dataset(dataset_id)
job_config = bigquery.LoadJobConfig()
job_config.schema = [
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("post_abbr", "STRING"),
]
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
"""