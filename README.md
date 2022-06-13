# Clickstream Data Pipeline

## Problem Statement

Our website writes CSV data to S3 in our AWS account with the activity (the clickstream) of our users. We require a system to:


* Wrangle the data (see "Data Wrangling" below)

* Make the resulting data queryable to our users using SQL

* (Stretch) Put the resulting onto a stream, and calculate summary statistics (see "Data Streaming" below)

 
## Requirements for running
- Python3 
- AWS account

## Datalake schema

The data is a CSV containing individual page views on our various websites. The column definitions are as follows:

- `anonymous_user_id` - unique identifier for a visitor to the website
- `url` - the full URL of the page
- `time` - the Unix time of the page view
- `browser` - user's web browser
- `os` - user's operating system
- `screen_resolution` - the user's screen resolution in pixels

### Clickstream parquet

Clickstream csv data is cleaned and stored in parquet format. See [etl.py](src/scripts/etl.py). It is partitioned by year and month.

## Instructions for running locally

#### Install Requirements

Clone repository to local machine
```
git clone https://github.com/spiyer99/clickstream-data-pipeline.git
```

#### Change directory to local repository
```
cd clickstream-data-pipeline
```

#### Create python virtual environment
```
python3 -m venv venv             # create virtualenv
source venv/bin/activate         # activate virtualenv
pip install -r requirements.txt  # install requirements
```

#### Go to source directory
```
cd src/
```

#### Run etl script in local mode
```
python scripts/etl.py
```

## Instructions for running on AWS
#### Edit dl.cfg file
```
[AWS]
AWS_ACCESS_KEY_ID=<AWS CREDENTIALS HERE>
AWS_SECRET_ACCESS_KEY=<AWS CREDENTIALS HERE>

[S3]
CODE_BUCKET=<CODE BUCKET NAME>
OUTPUT_BUCKET=<OUTPUT BUCKET NAME>

[DATALAKE]
INPUT_DATA=s3a://<INPUT_BUCKET NAME>
OUTPUT_DATA=s3a://<OUTPUT_BUCKET NAME>/
```

#### Deploy to AWS
The script automatically creates two S3 buckets (code_bucket and output_bucket), an IAM Role for EMR to access S3, and finally initiates the creation of the EMR cluster.
```
python scripts/deploy.py
```

#### Check results
Go to the AWS management console and to the EMR service and check the cluster and job status. 
After confirming the job run successfully, then run steps below to query the datalake.
```
jupyter notebook  # launch jupyter notebook app

# The notebook interface will appear in a new browser window or tab.
# Navigate to src/nb/notebook.ipynb and run sql queries against the datalake
```

#### Shut down AWS resources
Go to the AWS management console (Oregon region) and terminate the EMR cluster to avoid further costs.