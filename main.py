import cattr

import datetime
import os
import json
import gzip
import zlib
import shlex
import uuid

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

PUBLISHER_STATE = "publisher-state/active.json"

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
        try:
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
        except (gzip.BadGzipFile, EOFError, zlib.error) as exc:
            print(
                f"Skipping malformed gzip gs://{data['bucket']}/{data['name']}: "
                f"{type(exc).__name__}: {exc}"
            )
            try:
                bob_logs_log_blob.delete()
            except exceptions.NotFound:
                pass
            return

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
        with storage_client.batch(raise_exception=False) as batch:
            for blob in download_source_blobs:
                blob.delete()
        # Batch exposes individual responses only through _responses. Inspect
        # every response: catching a batch-wide NotFound could hide other errors.
        for response in batch._responses:
            if response.status_code != 404 and not 200 <= response.status_code < 300:
                raise exceptions.from_http_response(response)
        print(
            f"Deleted {len(download_source_blobs)} blobs from gs://{RESULT_BUCKET}/{download_prefix}"
        )
    if len(simple_source_blobs) > 0:
        with storage_client.batch(raise_exception=False) as batch:
            for blob in simple_source_blobs:
                blob.delete()
        for response in batch._responses:
            if response.status_code != 404 and not 200 <= response.status_code < 300:
                raise exceptions.from_http_response(response)
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


def _read_publisher_state(bucket):
    blob = bucket.get_blob(PUBLISHER_STATE)
    if blob is None:
        return {"version": 1, "batch": None, "followup": None}, 0
    generation = blob.generation
    state = json.loads(blob.download_as_text(if_generation_match=generation))
    if state["version"] != 1:
        raise ValueError("Unsupported publisher state version")
    return state, generation


def _write_publisher_state(bucket, state, generation):
    try:
        bucket.blob(PUBLISHER_STATE).upload_from_string(
            json.dumps(state),
            content_type="application/json",
            if_generation_match=generation,
        )
    except exceptions.PreconditionFailed:
        return False
    return True


def _new_publisher_batch(bucket, bigquery_client, partition, past_partition, followup):
    sources = {}
    for kind in ("downloads", "simple"):
        blobs, source_prefix = _fetch_blobs(
            bucket, blob_type=kind, past_partition=past_partition, partition=partition
        )
        sources[kind] = {
            "prefix": source_prefix,
            "blobs": [
                {"name": blob.name, "generation": blob.generation} for blob in blobs
            ],
        }
    if not any(source["blobs"] for source in sources.values()):
        return None
    if not DATASETS:
        raise ValueError("BIGQUERY_DATASET must be configured before publishing")

    batch_id = uuid.uuid4().hex
    created_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
    jobs = []
    for dataset in DATASETS:
        dataset_ref = bigquery.dataset.DatasetReference.from_string(
            dataset, default_project=DEFAULT_PROJECT
        )
        for kind, table in (("downloads", DOWNLOAD_TABLE), ("simple", SIMPLE_TABLE)):
            if not sources[kind]["blobs"]:
                continue
            jobs.append(
                {
                    "kind": kind,
                    "table": str(dataset_ref.table(table)),
                    "job_id": f"linehaul_{batch_id}_{len(jobs)}",
                    "attempt": 0,
                    "created_at": created_at,
                    "complete": False,
                }
            )
    return {
        "partition": partition,
        "sources": sources,
        "jobs": jobs,
        "project": bigquery_client.project,
        "location": "US",
        "followup": followup,
    }


def _get_publisher_job(bucket, bigquery_client, batch, job):
    job_id = f"{job['job_id']}_{job['attempt']}"
    identity = {"project": batch["project"], "location": batch["location"]}
    try:
        return bigquery_client.get_job(job_id, **identity)
    except exceptions.NotFound:
        # Job history is finite. An old missing job may have committed; never
        # guess by submitting it again. Existing jobs can still be recovered.
        age = datetime.datetime.now(
            datetime.timezone.utc
        ) - datetime.datetime.fromisoformat(job["created_at"])
        if age >= datetime.timedelta(days=1):
            raise RuntimeError(
                f"Cannot safely submit missing job {job_id}: reconcile publisher state"
            )

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.ignore_unknown_values = True
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    source_uris = [
        f"gs://{bucket.name}/{blob['name']}"
        for blob in batch["sources"][job["kind"]]["blobs"]
    ]
    try:
        return bigquery_client.load_table_from_uri(
            source_uris, job["table"], job_id=job_id, job_config=job_config, **identity
        )
    except exceptions.Conflict:
        # Another invocation submitted this exact attempt. Conflict is not
        # success: its result must still be checked before deleting anything.
        return bigquery_client.get_job(job_id, **identity)


def _publish_followup(followup):
    publisher = pubsub_v1.PublisherClient()
    future = publisher.publish(
        followup["topic"],
        b"",
        partition=followup["partition"],
        continue_publishing="True",
    )
    print(future.result())


def _run_publisher(
    storage_client, bigquery_client, bucket, partition, past_partition, followup
):
    finished = False
    while True:
        try:
            state, generation = _read_publisher_state(bucket)
        except (exceptions.NotFound, exceptions.PreconditionFailed):
            # The state changed between its metadata and content reads.
            continue

        if state["followup"] is not None:
            # Durable outbox: a crash may duplicate a notification, but cannot
            # lose the request to continue a historical partition.
            _publish_followup(state["followup"])
            state["followup"] = None
            if _write_publisher_state(bucket, state, generation) and finished:
                return
            continue
        if finished:
            return

        batch = state["batch"]
        if batch is None:
            # Read the idle generation BEFORE listing. A stale listing cannot
            # replace a newer active or idle state, even after another run ends.
            batch = _new_publisher_batch(
                bucket, bigquery_client, partition, past_partition, followup
            )
            if batch is None:
                return
            state["batch"] = batch
            _write_publisher_state(bucket, state, generation)
            continue

        if (
            followup is not None
            and batch["partition"] == partition
            and batch["followup"] is None
        ):
            batch["followup"] = followup
            _write_publisher_state(bucket, state, generation)
            continue

        pending = next((job for job in batch["jobs"] if not job["complete"]), None)
        if pending is not None:
            load_job = _get_publisher_job(bucket, bigquery_client, batch, pending)
            try:
                load_job.result()
            except Exception:
                if load_job.state == "DONE" and load_job.error_result is not None:
                    # A failed load commits no rows. Only a confirmed terminal
                    # failure permits a fresh attempt on a subsequent invocation.
                    pending["attempt"] += 1
                    pending["created_at"] = datetime.datetime.now(
                        datetime.timezone.utc
                    ).isoformat()
                    _write_publisher_state(bucket, state, generation)
                raise
            pending["complete"] = True
            if _write_publisher_state(bucket, state, generation):
                print(f"Loaded {load_job.output_rows} rows into {pending['table']}")
            continue

        # Every destination's success is durable before any source is deleted.
        downloads = batch["sources"]["downloads"]
        simple = batch["sources"]["simple"]
        _delete_blobs(
            storage_client,
            [
                bucket.blob(blob["name"], generation=blob["generation"])
                for blob in downloads["blobs"]
            ],
            downloads["prefix"],
            [
                bucket.blob(blob["name"], generation=blob["generation"])
                for blob in simple["blobs"]
            ],
            simple["prefix"],
        )
        state["batch"] = None
        state["followup"] = batch["followup"]
        if _write_publisher_state(bucket, state, generation):
            # Recover unrelated historical work first, then honor this request.
            finished = batch["partition"] == partition


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

    storage_client = storage.Client()
    bucket = storage_client.bucket(RESULT_BUCKET)
    followup = None
    if continue_publishing:
        followup = {
            "topic": f"projects/{DEFAULT_PROJECT}/topics/{PUBSUB_TOPIC}",
            "partition": partition,
        }
    _run_publisher(
        storage_client, bigquery.Client(), bucket, partition, past_partition, followup
    )
