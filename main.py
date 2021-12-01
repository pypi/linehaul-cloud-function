import arrow
import cattr

import base64
import datetime
import os
import json
import gzip

from tempfile import NamedTemporaryFile
from contextlib import ExitStack

from linehaul.events.parser import parse, Download, Simple

from google.api_core import exceptions
from google.cloud import bigquery, storage

_cattr = cattr.Converter()
_cattr.register_unstructure_hook(
    arrow.Arrow, lambda o: o.format("YYYY-MM-DD HH:mm:ss ZZ")
)

DEFAULT_PROJECT = os.environ.get("GCP_PROJECT", "the-psf")
RESULT_BUCKET = os.environ.get("RESULT_BUCKET")

# Multiple datasets can be specified by separating them with whitespace
# Datasets in other projects can be referenced by using the full dataset id:
#   <project_id>.<dataset_name>
# If only the dataset name is provided (no separating period) the
# DEFAULT_PROJECT will be used as the project ID.
DATASETS = os.environ.get("BIGQUERY_DATASET", "").strip().split()
SIMPLE_TABLE = os.environ.get("BIGQUERY_SIMPLE_TABLE")
DOWNLOAD_TABLE = os.environ.get("BIGQUERY_DOWNLOAD_TABLE")
MAX_BLOBS_PER_RUN = int(
    os.environ.get("MAX_BLOBS_PER_RUN", "5000")
)  # Cannot exceed 10,000

prefix = {Simple.__name__: "simple_requests", Download.__name__: "file_downloads"}


def process_fastly_log(data, context):
    storage_client = storage.Client()
    file_name = os.path.basename(data["name"]).rstrip(".log.gz")

    print(f"Beginning processing for gs://{data['bucket']}/{data['name']}")

    bob_logs_log_blob = storage_client.bucket(data["bucket"]).get_blob(data["name"])
    if bob_logs_log_blob is None:
        return  # This has already been processed?

    unprocessed_lines = 0
    simple_lines = 0
    download_lines = 0

    with ExitStack() as stack:
        input_file_obj = stack.enter_context(NamedTemporaryFile())
        bob_logs_log_blob.download_to_file(input_file_obj)
        input_file_obj.flush()

        input_file = stack.enter_context(gzip.open(input_file_obj.name, "rb"))
        unprocessed_file = stack.enter_context(NamedTemporaryFile())
        simple_results_file = stack.enter_context(NamedTemporaryFile())
        download_results_file = stack.enter_context(NamedTemporaryFile())

        min_timestamp = arrow.utcnow()
        for line in input_file:
            try:
                res = parse(line.decode())
                min_timestamp = min(min_timestamp, res.timestamp)
                if res is not None:
                    if res.__class__.__name__ == Simple.__name__:
                        simple_results_file.write(
                            json.dumps(_cattr.unstructure(res)).encode() + b"\n"
                        )
                        simple_lines += 1
                    elif res.__class__.__name__ == Download.__name__:
                        download_results_file.write(
                            json.dumps(_cattr.unstructure(res)).encode() + b"\n"
                        )
                        download_lines += 1
                    else:
                        unprocessed_file.write(line)
                        unprocessed_lines += 1
                else:
                    unprocessed_file.write(line)
                    unprocessed_lines += 1
            except Exception:
                unprocessed_file.write(line)
                unprocessed_lines += 1

        total = unprocessed_lines + simple_lines + download_lines
        print(
            f"Processed gs://{data['bucket']}/{data['name']}: {total} lines, {simple_lines} simple_requests, {download_lines} file_downloads, {unprocessed_lines} unprocessed"
        )

        bucket = storage_client.bucket(RESULT_BUCKET)
        partition = min_timestamp.strftime("%Y%m%d")

        if simple_lines > 0:
            blob = bucket.blob(f"processed/{partition}/simple-{file_name}.json")
            blob.upload_from_file(simple_results_file, rewind=True)
        if download_lines > 0:
            blob = bucket.blob(f"processed/{partition}/downloads-{file_name}.json")
            blob.upload_from_file(download_results_file, rewind=True)

        if unprocessed_lines > 0:
            blob = bucket.blob(f"unprocessed/{partition}/{file_name}.txt")
            try:
                blob.upload_from_file(unprocessed_file, rewind=True)
            except Exception:
                # Be opprotunistic about unprocessed files...
                pass

        # Remove the log file we processed
        try:
            bob_logs_log_blob.delete()
        except exceptions.NotFound:
            # Sometimes we try to delete twice
            pass


def load_processed_files_into_bigquery(event, context):
    if "attributes" in event and "partition" in event["attributes"]:
        # Check to see if we've manually triggered the function and provided a partition
        partition = event["attributes"]["partition"]
    else:
        # Otherwise, this was triggered via cron, use the current time
        partition = datetime.datetime.utcnow().strftime("%Y%m%d")

    folder = f"processed/{partition}"

    # Load the data into the dataset(s)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.ignore_unknown_values = True

    storage_client = storage.Client()
    bucket = storage_client.bucket(RESULT_BUCKET)

    bigquery_client = bigquery.Client()

    # Get the processed files we're loading
    download_prefix = f"{folder}/downloads-"
    download_source_blobs = list(
        bucket.list_blobs(prefix=download_prefix, max_results=MAX_BLOBS_PER_RUN)
    )
    download_source_uris = [
        f"gs://{blob.bucket.name}/{blob.name}" for blob in download_source_blobs
    ]
    simple_prefix = f"{folder}/simple-"
    simple_source_blobs = list(
        bucket.list_blobs(prefix=simple_prefix, max_results=MAX_BLOBS_PER_RUN)
    )
    simple_source_uris = [
        f"gs://{blob.bucket.name}/{blob.name}" for blob in simple_source_blobs
    ]

    for DATASET in DATASETS:
        dataset_ref = bigquery.dataset.DatasetReference.from_string(
            DATASET, default_project=DEFAULT_PROJECT
        )

        if len(download_source_uris) > 0:
            # Load the files for the downloads table
            load_job = bigquery_client.load_table_from_uri(
                download_source_uris,
                dataset_ref.table(DOWNLOAD_TABLE),
                job_id_prefix="linehaul_file_downloads",
                location="US",
                job_config=job_config,
            )
            load_job.result()
            print(f"Loaded {load_job.output_rows} rows into {DATASET}:{DOWNLOAD_TABLE}")

        if len(simple_source_uris) > 0:
            # Load the files for the simple table
            load_job = bigquery_client.load_table_from_uri(
                simple_source_uris,
                dataset_ref.table(SIMPLE_TABLE),
                job_id_prefix="linehaul_simple_requests",
                location="US",
                job_config=job_config,
            )
            load_job.result()
            print(f"Loaded {load_job.output_rows} rows into {DATASET}:{SIMPLE_TABLE}")

    with storage_client.batch():
        for blob in download_source_blobs:
            blob.delete()
        for blob in simple_source_blobs:
            blob.delete()
