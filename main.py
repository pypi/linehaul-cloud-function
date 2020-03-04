import arrow
import cattr

import datetime
import os
import json
import gzip
import sys
import tempfile

from collections import defaultdict
from contextlib import ExitStack
from pathlib import Path

from linehaul.events.parser import parse, Download, Simple

from google.cloud import bigquery, storage

_cattr = cattr.Converter()
_cattr.register_unstructure_hook(
    arrow.Arrow, lambda o: o.format("YYYY-MM-DD HH:mm:ss ZZ")
)


class OutputFiles(defaultdict):
    def __init__(self, temp_dir, stack, *args, **kwargs):
        self.temp_dir = temp_dir
        self.stack = stack
        super(OutputFiles, self).__init__(*args, **kwargs)

    def __missing__(self, key):
        path = os.path.join(self.temp_dir, key)
        Path(os.path.dirname(path)).mkdir(parents=True, exist_ok=True)
        ret = self[key] = self.stack.enter_context(open(path, "wb"))
        return ret


prefix = {Simple.__name__: "simple_requests", Download.__name__: "downloads"}


def process_fastly_log(data, context):
    storage_client = storage.Client()
    bigquery_client = bigquery.Client()

    bob_logs_log_blob = storage_client.bucket(data["bucket"]).get_blob(data["name"])
    identifier = os.path.basename(data["name"]).split("-", 3)[-1].rstrip(".log.gz")
    _, temp_local_filename = tempfile.mkstemp()
    temp_output_dir = tempfile.mkdtemp()
    bob_logs_log_blob.download_to_filename(temp_local_filename)

    with ExitStack() as stack:
        f = stack.enter_context(gzip.open(temp_local_filename, "rb"))
        output_files = OutputFiles(temp_output_dir, stack)
        default_partition = datetime.datetime.utcnow().strftime("%Y%m%d")
        unprocessable = f"results/unprocessed/{default_partition}/{identifier}.txt"
        for line in f:
            try:
                res = parse(line.decode())
                if res is not None:
                    partition = res.timestamp.format("YYYYMMDD")
                    output_files[
                        f"results/{prefix[res.__class__.__name__]}/{partition}/{identifier}.json"
                    ].write(json.dumps(_cattr.unstructure(res)).encode() + b"\n")
                else:
                    output_files[unprocessable].write(line)
            except Exception as e:
                output_files[unprocessable].write(line)
        result_files = output_files.keys()

    dataset = os.environ.get("BIGQUERY_DATASET")
    table = os.environ.get("BIGQUERY_TABLE")
    dataset_ref = bigquery_client.dataset(dataset)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.ignore_unknown_values = True

    for result_file in result_files:
        if not os.path.relpath(result_file, "results").startswith("unprocessed"):
            with open(os.path.join(temp_output_dir, result_file)) as f:
                load_job = bigquery_client.load_table_from_file(
                    f,
                    dataset_ref.table(table),
                    job_id_prefix="linehaul_",
                    location="US",
                    job_config=job_config,
                )
            load_job.result()
            print(f"Loaded {load_job.output_rows} rows into {dataset}:{table}")

    bucket = storage_client.bucket(os.environ.get("RESULT_BUCKET"))
    for result_file in result_files:
        blob_name = os.path.relpath(result_file, "results")
        if blob_name.startswith("unprocessed/"):
            blob = bucket.blob(blob_name)
            blob.upload_from_filename(os.path.join(temp_output_dir, result_file))

    bob_logs_log_blob.delete()
