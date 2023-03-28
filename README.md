# TODOs
- add logging
- handle exceptions
    - do not use exists_ok=True in create_bq_dataset > create_dataset, but log existence
    - do not use exists_ok=True in create_bq_table > create_table, but log existence
    - do not use not_found_ok=True in create_bq_table > delete_table, but log inexistence
- extract authorization into private function e.g. _authorize_gcloud_services

# Setup
Before running the code you need to follow the steps below.

## Google Cloud Credentials
This application communicates with several APIs provided by the Google Cloud Platform.

### Enable the APIs
Make sure you have enabled the BigQuery API and the Cloud Storage API. 
1. Go to [Cloud Storage API](https://console.cloud.google.com/apis/library/storage-component.googleapis.com) and enable it.
2. Go to [BigQuery API](https://console.cloud.google.com/apis/library/storage-component.googleapis.com) and enable it.

### Provide a JSON Credentials File
For programmatic access to Google Cloud - in our case by using some of their APIs - you need a service account.
If you don't have one, see [Create Service Accounts](https://cloud.google.com/iam/docs/service-accounts-create) for details on how to set it up.

If not done yet, create a key JSON file for your service account. See [Create Service Account Key](https://cloud.google.com/iam/docs/keys-create-delete). If you already have a Service Account key on your local drive, use that one.

Place the JSON credentials file for your service account into the root directory of this project (where README.md and
jobparams.toml are located).

### Configuring Job Parameters
Modify the [jobparams.toml](./jobparams.toml) by setting the `json_credentials_path` to your JSON key file.

# Running the Code
1. Clone this repository
2. `cd` into the root directory of this project
3. create a virtual environment using your platform specific syntax. For macOS this is
```bash
python3 -m venv .venv-data-eng-zoomcamp  # name the venv whatever you like
```
4. activate the venv by 
```bash
source .venv-data-eng-zoomcamp/bin/activate
```
5. Install all necessary packages
```bash
pip install -r requirements.txt
```
6. Run the application 
```bash
python app.py
```

# How This Application Works
TODO: proceed here