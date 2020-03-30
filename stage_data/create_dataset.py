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

dataset_name = "CDI_test"  # <-- my dataset's name
createDatasets(dataset_name, bq_client)
