import contextlib
import datetime
from pathlib import Path
from importlib import reload

import pretend
import pytest

import main

GCP_PROJECT = "my-gcp-project"
RESULT_BUCKET = "my-result-bucket"


@pytest.mark.parametrize(
    "log_filename, expected_data, expected_unprocessed, expected_unprocessed_filename, expected_data_filename",
    [
        (
            "downloads-2021-01-07-20-55-2021-01-07T20-55-00.000-B8Hs_G6d6xN61En2ypwk.log.gz",
            b'{"timestamp": "2021-01-07 20:54:54 +00:00", "url": "/packages/f7/12/ec3f2e203afa394a149911729357aa48affc59c20e2c1c8297a60f33f133/threadpoolctl-2.1.0-py3-none-any.whl", "project": "threadpoolctl", "file": {"filename": "threadpoolctl-2.1.0-py3-none-any.whl", "project": "threadpoolctl", "version": "2.1.0", "type": "bdist_wheel"}, "tls_protocol": "TLSv1.2", "tls_cipher": "ECDHE-RSA-AES128-GCM-SHA256", "country_code": "US", "details": {"installer": {"name": "pip", "version": "20.1.1"}, "python": "3.7.9", "implementation": {"name": "CPython", "version": "3.7.9"}, "distro": {"name": "Debian GNU/Linux", "version": "9", "id": "stretch", "libc": {"lib": "glibc", "version": "2.24"}}, "system": {"name": "Linux", "release": "4.15.0-112-generic"}, "cpu": "x86_64", "openssl_version": "OpenSSL 1.1.0l  10 Sep 2019", "setuptools_version": "47.1.0", "ci": null}}\n'
            b'{"timestamp": "2021-01-07 20:54:54 +00:00", "url": "/packages/cd/f9/8fad70a3bd011a6be7c5c6067278f006a25341eb39d901fbda307e26804c/django_crum-0.7.9-py2.py3-none-any.whl", "project": "django-crum", "file": {"filename": "django_crum-0.7.9-py2.py3-none-any.whl", "project": "django-crum", "version": "0.7.9", "type": "bdist_wheel"}, "tls_protocol": "TLSv1.2", "tls_cipher": "ECDHE-RSA-AES128-GCM-SHA256", "country_code": "US", "details": {"installer": {"name": "pip", "version": "20.0.2"}, "python": "3.8.5", "implementation": {"name": "CPython", "version": "3.8.5"}, "distro": {"name": "Ubuntu", "version": "16.04", "id": "xenial", "libc": {"lib": "glibc", "version": "2.23"}}, "system": {"name": "Linux", "release": "4.4.0-1113-aws"}, "cpu": "x86_64", "openssl_version": "OpenSSL 1.0.2g  1 Mar 2016", "setuptools_version": "44.1.0", "ci": null}}\n',
            b"download|Thu, 07 Jan 2021 20:54:56 GMT|US|/packages/c5/db/e56e6b4bbac7c4a06de1c50de6fe1ef3810018ae11732a50f15f62c7d050/enum34-1.1.6-py2-none-any.whl|TLSv1.2|ECDHE-RSA-AES128-GCM-SHA256|enum34|1.1.6|bdist_wheel|(null)\n",
            "unprocessed/20210107/downloads-2021-01-07-20-55-2021-01-07T20-55-00.000-B8Hs_G6d6xN61En2ypwk.txt",
            "processed/20210107/downloads-downloads-2021-01-07-20-55-2021-01-07T20-55-00.000-B8Hs_G6d6xN61En2ypwk.json",
        ),
        (
            "simple-2021-01-07-20-55-2021-01-07T20-55-00.000-3wuB00t9tqgbGLFI2fSI.log.gz",
            b'{"timestamp": "2021-01-07 20:54:52 +00:00", "url": "/simple/azureml-model-management-sdk/", "project": "azureml-model-management-sdk", "tls_protocol": "TLSv1.3", "tls_cipher": "AES256-GCM", "country_code": "US", "details": {"installer": {"name": "pip", "version": "20.0.2"}, "python": "3.7.5", "implementation": {"name": "CPython", "version": "3.7.5"}, "distro": {"name": "Ubuntu", "version": "18.04", "id": "bionic", "libc": {"lib": "glibc", "version": "2.27"}}, "system": {"name": "Linux", "release": "4.15.0-1092-azure"}, "cpu": "x86_64", "openssl_version": "OpenSSL 1.1.1  11 Sep 2018", "setuptools_version": "45.2.0", "ci": null}}\n'
            b'{"timestamp": "2021-01-07 20:54:52 +00:00", "url": "/simple/pyrsistent/", "project": "pyrsistent", "tls_protocol": "TLSv1.3", "tls_cipher": "AES256-GCM", "country_code": "US", "details": {"installer": {"name": "pip", "version": "20.0.2"}, "python": "3.8.5", "implementation": {"name": "CPython", "version": "3.8.5"}, "distro": {"name": "Ubuntu", "version": "20.04", "id": "focal", "libc": {"lib": "glibc", "version": "2.31"}}, "system": {"name": "Linux", "release": "5.4.72-flatcar"}, "cpu": "x86_64", "openssl_version": "OpenSSL 1.1.1f  31 Mar 2020", "setuptools_version": "45.2.0", "ci": true}}\n',
            b"simple|Thu, 07 Jan 2021 20:54:52 GMT|US|/simple/numpy/|TLSv1.2|ECDHE-RSA-AES128-GCM-SHA256||||(null)\n",
            "unprocessed/20210107/simple-2021-01-07-20-55-2021-01-07T20-55-00.000-3wuB00t9tqgbGLFI2fSI.txt",
            "processed/20210107/simple-simple-2021-01-07-20-55-2021-01-07T20-55-00.000-3wuB00t9tqgbGLFI2fSI.json",
        ),
    ],
)
def test_process_fastly_log(
    monkeypatch,
    log_filename,
    expected_data,
    expected_unprocessed,
    expected_data_filename,
    expected_unprocessed_filename,
):
    monkeypatch.setenv("GCP_PROJECT", GCP_PROJECT)
    monkeypatch.setenv("RESULT_BUCKET", RESULT_BUCKET)

    reload(main)

    def _download_to_file(file_handler):
        with open(Path(".") / "fixtures" / log_filename, "rb") as f:
            file_handler.write(f.read())

    get_blob_stub = pretend.stub(
        download_to_file=_download_to_file,
        delete=pretend.call_recorder(lambda: None),
    )

    blobs = {}

    class Blob(object):
        def __init__(self, blob_uri):
            self.uri = blob_uri
            self.data = None
            blobs[blob_uri] = self

        def upload_from_file(self, file_handler, rewind=False):
            if rewind:
                file_handler.seek(0)
            self.data = file_handler.read()

    bucket_stub = pretend.stub(
        get_blob=pretend.call_recorder(lambda a: get_blob_stub),
        blob=pretend.call_recorder(lambda a: Blob(a)),
    )
    storage_client_stub = pretend.stub(
        bucket=pretend.call_recorder(lambda a: bucket_stub),
    )
    monkeypatch.setattr(
        main, "storage", pretend.stub(Client=lambda: storage_client_stub)
    )

    data = {
        "name": log_filename,
        "bucket": "my-bucket",
    }
    context = pretend.stub()

    main.process_fastly_log(data, context)

    assert storage_client_stub.bucket.calls == [pretend.call("my-bucket")] + [
        pretend.call(RESULT_BUCKET),
    ]
    assert bucket_stub.get_blob.calls == [pretend.call(log_filename)]
    assert bucket_stub.blob.calls == [
        pretend.call(expected_data_filename),
        pretend.call(expected_unprocessed_filename),
    ]
    assert get_blob_stub.delete.calls == [pretend.call()]
    assert blobs[expected_data_filename].data == expected_data
    assert blobs[expected_unprocessed_filename].data == expected_unprocessed


GCP_PROJECT = "my-gcp-project"
BIGQUERY_DATASET = "my-bigquery-dataset"
BIGQUERY_SIMPLE_TABLE = "my-simple-table"
BIGQUERY_DOWNLOAD_TABLE = "my-download-table"
RESULT_BUCKET = "my-result-bucket"


@pytest.mark.parametrize(
    "bigquery_dataset",
    [
        ("my-bigquery-dataset"),
        ("my-bigquery-dataset some-other-dataset"),
    ],
)
@pytest.mark.parametrize(
    "blobs, simple_fetch_current, expected_load_jobs, expected_delete_calls",
    [
        ({"simple": [], "downloads": ["blob0", "blob1", "blob2"]}, True, 1, 3),
        ({"simple": ["blob0", "blob1", "blob2"], "downloads": []}, True, 1, 3),
        (
            {
                "simple": ["blob0", "blob1", "blob2"],
                "downloads": ["blob0", "blob1", "blob2"],
            },
            True,
            2,
            6,
        ),
        (
            {"simple": ["pastblob0", "pastblob1"], "downloads": ["blob0", "blob1"]},
            False,
            2,
            4,
        ),
    ],
)
def test_load_processed_files_into_bigquery(
    monkeypatch,
    bigquery_dataset,
    blobs,
    simple_fetch_current,
    expected_load_jobs,
    expected_delete_calls,
):
    monkeypatch.setenv("GCP_PROJECT", GCP_PROJECT)
    monkeypatch.setenv("BIGQUERY_DATASET", bigquery_dataset)
    monkeypatch.setenv("BIGQUERY_SIMPLE_TABLE", BIGQUERY_SIMPLE_TABLE)
    monkeypatch.setenv("BIGQUERY_DOWNLOAD_TABLE", BIGQUERY_DOWNLOAD_TABLE)
    monkeypatch.setenv("RESULT_BUCKET", RESULT_BUCKET)

    reload(main)

    bucket = pretend.stub(name=RESULT_BUCKET)

    blob_stub = pretend.stub(
        name="blobname", bucket=bucket, delete=pretend.call_recorder(lambda: None)
    )

    past_partition = (datetime.datetime.utcnow() - datetime.timedelta(days=1)).strftime(
        "%Y%m%d"
    )
    partition = datetime.datetime.utcnow().strftime("%Y%m%d")

    def _generate_blob_list(prefix, max_results):
        if "simple" in prefix:
            if past_partition in prefix:
                _blobs = [b for b in blobs["simple"] if b.startswith("past")]
            else:
                _blobs = blobs["simple"]
        elif "downloads" in prefix:
            if past_partition in prefix:
                _blobs = [b for b in blobs["downloads"] if b.startswith("past")]
            else:
                _blobs = blobs["downloads"]
        else:
            _blobs = []
        blob_list = [blob_stub for b in _blobs]
        return blob_list

    bucket_stub = pretend.stub(
        list_blobs=pretend.call_recorder(_generate_blob_list),
    )

    @contextlib.contextmanager
    def fake_batch(*a, **kw):
        yield True

    storage_client_stub = pretend.stub(
        bucket=pretend.call_recorder(lambda a: bucket_stub),
        batch=fake_batch,
    )
    monkeypatch.setattr(
        main, "storage", pretend.stub(Client=lambda: storage_client_stub)
    )

    table_stub = pretend.stub()
    dataset_stub = pretend.stub(table=pretend.call_recorder(lambda a: table_stub))
    load_job_stub = pretend.stub(
        result=pretend.call_recorder(lambda: None),
        output_rows=pretend.stub(),
    )

    bigquery_client_stub = pretend.stub(
        load_table_from_uri=pretend.call_recorder(lambda *a, **kw: load_job_stub),
    )
    job_config_stub = pretend.stub()
    dataset_reference_stub = pretend.stub(
        from_string=pretend.call_recorder(lambda *a, **kw: dataset_stub)
    )

    monkeypatch.setattr(
        main,
        "bigquery",
        pretend.stub(
            Client=lambda: bigquery_client_stub,
            LoadJobConfig=lambda: job_config_stub,
            SourceFormat=pretend.stub(NEWLINE_DELIMITED_JSON=pretend.stub()),
            dataset=pretend.stub(DatasetReference=dataset_reference_stub),
        ),
    )

    event = {}
    context = pretend.stub()

    main.load_processed_files_into_bigquery(event, context)

    assert storage_client_stub.bucket.calls == [
        pretend.call(RESULT_BUCKET),
    ]
    expected_list_blob_calls = [
        pretend.call(prefix=f"processed/{past_partition}/downloads-", max_results=1000),
        pretend.call(prefix=f"processed/{partition}/downloads-", max_results=1000),
        pretend.call(prefix=f"processed/{past_partition}/simple-", max_results=1000),
        pretend.call(prefix=f"processed/{partition}/simple-", max_results=1000),
    ]
    if not simple_fetch_current:
        expected_list_blob_calls = expected_list_blob_calls[:3]
    assert bucket_stub.list_blobs.calls == expected_list_blob_calls
    assert (
        load_job_stub.result.calls
        == [pretend.call()] * len(bigquery_dataset.split()) * expected_load_jobs
    )
    assert blob_stub.delete.calls == [pretend.call()] * expected_delete_calls
