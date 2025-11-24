import cattr

import datetime
import os
import json
import gzip
import shlex

from tempfile import NamedTemporaryFile
from contextlib import ExitStack

from linehaul.events.parser import parse, Download, Simple
from linehaul.ua.datastructures import Installer

from cattr.gen import make_dict_unstructure_fn, override
import sentry_sdk
from sentry_sdk.integrations.serverless import serverless_function
from google.api_core import exceptions
from google.api_core.retry import Retry
from google.cloud import bigquery, storage, pubsub_v1

if dsn := os.environ.get("SENTRY_DSN"):
    sentry_sdk.init(dsn=dsn, enable_tracing=True)

_cattr = cattr.Converter()
_cattr.register_unstructure_hook(
    datetime.datetime, lambda o: o.strftime("%Y-%m-%d %H:%M:%S +00:00")
)


def _unstructure_subcommand(subcommand: list[str] | None) -> str | None:
    if subcommand is None:
        return None
    return shlex.join(subcommand)


_cattr.register_unstructure_hook(
    Installer,
    make_dict_unstructure_fn(
        Installer, _cattr, subcommand=override(unstruct_hook=_unstructure_subcommand)
    ),
)

DEFAULT_PROJECT = os.environ.get("GCP_PROJECT", "the-psf")
RESULT_BUCKET = os.environ.get("RESULT_BUCKET")
PUBSUB_TOPIC = os.environ.get("PUBSUB_TOPIC")

# Multiple datasets can be specified by separating them with whitespace
# Datasets in other projects can be referenced by using the full dataset id:
#   <project_id>.<dataset_name>
# If only the dataset name is provided (no separating period) the
# DEFAULT_PROJECT will be used as the project ID.
DATASETS = os.environ.get("BIGQUERY_DATASET", "").strip().split()
SIMPLE_TABLE = os.environ.get("BIGQUERY_SIMPLE_TABLE")
DOWNLOAD_TABLE = os.environ.get("BIGQUERY_DOWNLOAD_TABLE")
MAX_BLOBS_PER_RUN = int(
    os.environ.get("MAX_BLOBS_PER_RUN", "1000")
)  # Cannot exceed 10,000 per load, or 1,000 per batch call to delete blobs

prefix = {Simple.__name__: "simple_requests", Download.__name__: "file_downloads"}


@serverless_function
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

        min_timestamp = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
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


@Retry()
def _delete_blobs(
    storage_client,
    download_source_blobs,
    download_prefix,
    simple_source_blobs,
    simple_prefix,
):
    if len(download_source_blobs) > 0:
        with storage_client.batch():
            for blob in download_source_blobs:
                blob.delete()
        print(
            f"Deleted {len(download_source_blobs)} blobs from gs://{RESULT_BUCKET}/{download_prefix}"
        )
    if len(simple_source_blobs) > 0:
        with storage_client.batch():
            for blob in simple_source_blobs:
                blob.delete()
        print(
            f"Deleted {len(simple_source_blobs)} blobs from gs://{RESULT_BUCKET}/{simple_prefix}"
        )


def _fetch_blobs(bucket, blob_type="downloads", past_partition=None, partition=None):
    # Get the processed files we're loading

    if past_partition is not None:
        folder = f"processed/{past_partition}"
        prefix = f"{folder}/{blob_type}-"
        source_blobs = list(
            bucket.list_blobs(prefix=prefix, max_results=MAX_BLOBS_PER_RUN)
        )
        if len(source_blobs) > 0:
            return (source_blobs, prefix)

    folder = f"processed/{partition}"
    prefix = f"{folder}/{blob_type}-"
    source_blobs = list(bucket.list_blobs(prefix=prefix, max_results=MAX_BLOBS_PER_RUN))
    return (source_blobs, prefix)


@serverless_function
def load_processed_files_into_bigquery(event, context):
    continue_publishing = False
    if "attributes" in event and "partition" in event["attributes"]:
        # Check to see if we've manually triggered the function and provided a partition
        past_partition = None
        partition = event["attributes"]["partition"]
        if "continue_publishing" in event["attributes"]:
            continue_publishing = bool(event["attributes"]["continue_publishing"])
    else:
        # Otherwise, this was triggered via cron, use the current time
        # checking the past day first
        past_partition = (
            datetime.datetime.utcnow() - datetime.timedelta(days=1)
        ).strftime("%Y%m%d")
        partition = datetime.datetime.utcnow().strftime("%Y%m%d")

    # Load the data into the dataset(s)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.ignore_unknown_values = True

    storage_client = storage.Client()
    bucket = storage_client.bucket(RESULT_BUCKET)

    bigquery_client = bigquery.Client()

    download_source_blobs, download_prefix = _fetch_blobs(
        bucket,
        blob_type="downloads",
        past_partition=past_partition,
        partition=partition,
    )
    download_source_uris = [
        f"gs://{blob.bucket.name}/{blob.name}" for blob in download_source_blobs
    ]
    simple_source_blobs, simple_prefix = _fetch_blobs(
        bucket, blob_type="simple", past_partition=past_partition, partition=partition
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

    _delete_blobs(
        storage_client,
        download_source_blobs,
        download_prefix,
        simple_source_blobs,
        simple_prefix,
    )

    if continue_publishing and (
        len(download_source_blobs) > 0 or len(simple_source_blobs) > 0
    ):
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(DEFAULT_PROJECT, PUBSUB_TOPIC)
        print(
            f"Publishing to {topic_path}: partition={partition},continue_publishing={str(continue_publishing)}"
        )
        future = publisher.publish(
            topic_path,
            b"",
            partition=partition,
            continue_publishing=str(continue_publishing),
        )
        print(future.result())
