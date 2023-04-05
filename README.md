- [Welcome](#welcome)
- [Setup](#setup)
  - [Google Cloud Credentials](#google-cloud-credentials)
    - [Enable the APIs](#enable-the-apis)
    - [Provide a JSON Credentials File](#provide-a-json-credentials-file)
- [Running the Code](#running-the-code)
- [How This Application Works](#how-this-application-works)
- [Dashboard Result](#dashboard-result)

# Welcome
This project contains an end-to-end data pipeline, written in Python. It is my capstone project to finalize the [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp#data-engineering-zoomcamp) in the 2023 Cohort. 

The application reads local CSV data - into bronze layer - about trending Youtube videos in Germany, transforms it with Apache Spark - into silver layer - and uploads it to Google Cloud Storage. Then it is loaded from GCS into BigQuery. At the very end is a dashboard in Looker Studio, that connects to the transformed table in BigQuery.

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
jobparams.toml are located). give it exactly the name `gcp_keys.json`.

# Running the Code
*Note: these instructions are used for macOS/Linux/WSL, for Windows it may differ*
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
*There is an additional option to execute the unit tests via*
```bash
python -m test.test_pipeline
```

# How This Application Works
Entrypoint to this application is the file [app.py](./app.py). It contains the Prefect flow and the function invocations which are Prefect tasks at the same time. The function definitions are located in [pipeline.py](./src/pipeline.py). The job parameters are defined in the config file [jobparams.toml](./jobparams.toml).

# Dashboard Result
The data pipeline ultimately feeds into a Looker Studio dashboard. It can be viewed [here](https://lookerstudio.google.com/reporting/77a47276-3a27-4fe3-a57a-337966fcefe7). 