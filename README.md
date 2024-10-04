## 🗂️ ETL Medallion Lakehouse

This project includes the AWS Serverless Application Model (SAM) template for the ETL pipeline that splits product IoT telemetry readings into daily parquet files.

It takes raw data files with IoT telemetry readings in JSON format as input and produces daily parquet files as output.

Raw data files can be uploaded to the ETL pipeline using the Python script `src/data_asset_uploader/raw_data_files_S3_uploader.py`. Daily parquet files can be downloaded from the s3silver bucket (the bucket URL and credentials are available in the SAM deployment output).

Resources:

* The [ETL_example_map.png](ETL_example_map.png) describes the supported use case on a functional level.

* The [setup.py](setup.py) file is used to package the project with `setuptools`.

* The [template.yaml](template.yaml) file contains the SAM pipeline definition, all the necessary permissions, and the roles required for the services to work properly.


## How to setup for local development

Install the Python version with [asdf](https://asdf-vm.com/guide/getting-started.html) package manager,
[direnv](https://direnv.net/docs/installation.html) and [make](https://www.gnu.org/software/make/manual/make.html).
On macOS this can be donw with Homebrew:

```
brew install asdf direnv make
```

Then install the dependencies of the project with:

```
asdf install
make deps
```

Lint the Python and SAM stack code and run unit tests with the following commands:

```
make lint
make test
```


## Deploying the ETL pipeline on localstack

Complete all the steps in the "How to set up for local development" section.

Rename `.envrc-template` to `.envrc` and set the value of the `LOCALSTACK_AUTH_TOKEN` environment variable 
to run localstack pro version with IAM policy checks enabled. Otherwise, this value can be removed 
to run localstack in free mode without IAM checks.

Activate the environment with direnv:

```
direnv allow
```

Run localstack **in a separate terminal** with the following command and make sure you see the message 
"Successfully requested and activated new license" in the localstack console output.

```
make localstack
```

Deploy the pipeline to the localstack **in another terminal**:

```
make deploy
```

Update the `UPLOADER_AWS_*` and `DOWNLOADER_AWS_*` credentials in `.envrc` from the corresponding SAM stack output
from the previous step and run `direnv allow` again.

Run integration tests with the following command:

```
make test_integration
```


## Running the ETL pipeline locally

After the ETL pipeline is deployed on the localstack, Raw data JSON files from the project's `/data` 
directory can be uploaded into the pipeline using the following script:

```
python src/data_asset_uploader/raw_data_files_S3_uploader.py --job_uuid 1001
```

If the `raw_data_files_S3_uploader.py` script is run without the `--job_uuid` argument, 
it will automatically generate a random job UUID.

Once the pipeline is complete (which can be monitored using localstack's console logs),
the daily parquet files can be downloaded from the s3silver bucket using the following command:

```
awslocal s3 sync s3://medallion-lakehouse-s3silver/job_1001 ./
```

The downloaded daily Parquet files can be explored with interactive Python as the following:

```
> python

import pandas as pd

df = pd.read_parquet("medallion-lakehouse-s3bronze/jupiter/2024/01/01/2024-01-01.1001.snappy.parquet")
print(df)

df = pd.read_parquet("medallion-lakehouse-s3bronze/mars/2023/04/01/2023-04-01.1001.snappy.parquet")
print(df)
```


## Risks and Missing Information

* The order of the rows in the daily parquet files is not guaranteed, they are only segmented by day.
* Entire processed batch of Raw data files can be failed if there is any malformed file in the batch.

## Error handling

Names of Raw data files that can't be processed are end up in the `RawSQSDeadLetterQueue` dead letter queue.
The URL of the dead letter queue is available in the SAM stack output.


## Copyright

Copyright © 2024 Ivan Rublev.

This project is licensed under the [MIT license](https://github.com/IvanRublev/Domo/blob/master/LICENSE.md).
