from pathlib import Path
from importlib import reload

import pretend
import pytest

import main

GCP_PROJECT = "my-gcp-project"
BIGQUERY_DATASET = "my-bigquery-dataset"
BIGQUERY_SIMPLE_TABLE = "my-simple-table"
BIGQUERY_DOWNLOAD_TABLE = "my-download-table"
RESULT_BUCKET = "my-result-bucket"


@pytest.mark.parametrize(
    "bigquery_dataset, expected_from_string_calls",
    [
        (
            "my-bigquery-dataset",
            [pretend.call("my-bigquery-dataset", default_project=GCP_PROJECT)],
        ),
        (
            "my-bigquery-dataset some-other-dataset",
            [
                pretend.call("my-bigquery-dataset", default_project=GCP_PROJECT),
                pretend.call("some-other-dataset", default_project=GCP_PROJECT),
            ],
        ),
    ],
)
@pytest.mark.parametrize(
    "log_filename, table_name, expected",
    [
        (
            "downloads-2021-01-07-20-55-2021-01-07T20-55-00.000-B8Hs_G6d6xN61En2ypwk.log.gz",
            BIGQUERY_DOWNLOAD_TABLE,
            b'{"timestamp": "2021-01-07 20:54:54 +00:00", "url": "/packages/f7/12/ec3f2e203afa394a149911729357aa48affc59c20e2c1c8297a60f33f133/threadpoolctl-2.1.0-py3-none-any.whl", "project": "threadpoolctl", "file": {"filename": "threadpoolctl-2.1.0-py3-none-any.whl", "project": "threadpoolctl", "version": "2.1.0", "type": "bdist_wheel"}, "tls_protocol": "TLSv1.2", "tls_cipher": "ECDHE-RSA-AES128-GCM-SHA256", "country_code": "US", "details": {"installer": {"name": "pip", "version": "20.1.1"}, "python": "3.7.9", "implementation": {"name": "CPython", "version": "3.7.9"}, "distro": {"name": "Debian GNU/Linux", "version": "9", "id": "stretch", "libc": {"lib": "glibc", "version": "2.24"}}, "system": {"name": "Linux", "release": "4.15.0-112-generic"}, "cpu": "x86_64", "openssl_version": "OpenSSL 1.1.0l  10 Sep 2019", "setuptools_version": "47.1.0", "ci": null}}\n'
            b'{"timestamp": "2021-01-07 20:54:54 +00:00", "url": "/packages/cd/f9/8fad70a3bd011a6be7c5c6067278f006a25341eb39d901fbda307e26804c/django_crum-0.7.9-py2.py3-none-any.whl", "project": "django-crum", "file": {"filename": "django_crum-0.7.9-py2.py3-none-any.whl", "project": "django-crum", "version": "0.7.9", "type": "bdist_wheel"}, "tls_protocol": "TLSv1.2", "tls_cipher": "ECDHE-RSA-AES128-GCM-SHA256", "country_code": "US", "details": {"installer": {"name": "pip", "version": "20.0.2"}, "python": "3.8.5", "implementation": {"name": "CPython", "version": "3.8.5"}, "distro": {"name": "Ubuntu", "version": "16.04", "id": "xenial", "libc": {"lib": "glibc", "version": "2.23"}}, "system": {"name": "Linux", "release": "4.4.0-1113-aws"}, "cpu": "x86_64", "openssl_version": "OpenSSL 1.0.2g  1 Mar 2016", "setuptools_version": "44.1.0", "ci": null}}\n',
        ),
        (
            "simple-2021-01-07-20-55-2021-01-07T20-55-00.000-3wuB00t9tqgbGLFI2fSI.log.gz",
            BIGQUERY_SIMPLE_TABLE,
            b'{"timestamp": "2021-01-07 20:54:52 +00:00", "url": "/simple/azureml-model-management-sdk/", "project": "azureml-model-management-sdk", "tls_protocol": "TLSv1.3", "tls_cipher": "AES256-GCM", "country_code": "US", "details": {"installer": {"name": "pip", "version": "20.0.2"}, "python": "3.7.5", "implementation": {"name": "CPython", "version": "3.7.5"}, "distro": {"name": "Ubuntu", "version": "18.04", "id": "bionic", "libc": {"lib": "glibc", "version": "2.27"}}, "system": {"name": "Linux", "release": "4.15.0-1092-azure"}, "cpu": "x86_64", "openssl_version": "OpenSSL 1.1.1  11 Sep 2018", "setuptools_version": "45.2.0", "ci": null}}\n'
            b'{"timestamp": "2021-01-07 20:54:52 +00:00", "url": "/simple/pyrsistent/", "project": "pyrsistent", "tls_protocol": "TLSv1.3", "tls_cipher": "AES256-GCM", "country_code": "US", "details": {"installer": {"name": "pip", "version": "20.0.2"}, "python": "3.8.5", "implementation": {"name": "CPython", "version": "3.8.5"}, "distro": {"name": "Ubuntu", "version": "20.04", "id": "focal", "libc": {"lib": "glibc", "version": "2.31"}}, "system": {"name": "Linux", "release": "5.4.72-flatcar"}, "cpu": "x86_64", "openssl_version": "OpenSSL 1.1.1f  31 Mar 2020", "setuptools_version": "45.2.0", "ci": true}}\n',
        ),
    ],
)
def test_function(
    monkeypatch,
    log_filename,
    table_name,
    expected,
    bigquery_dataset,
    expected_from_string_calls,
):
    monkeypatch.setenv("GCP_PROJECT", GCP_PROJECT)
    monkeypatch.setenv("BIGQUERY_DATASET", bigquery_dataset)
    monkeypatch.setenv("BIGQUERY_SIMPLE_TABLE", BIGQUERY_SIMPLE_TABLE)
    monkeypatch.setenv("BIGQUERY_DOWNLOAD_TABLE", BIGQUERY_DOWNLOAD_TABLE)
    monkeypatch.setenv("RESULT_BUCKET", RESULT_BUCKET)

    reload(main)

    def _download_to_file(file_handler):
        with open(Path(".") / "fixtures" / log_filename, "rb") as f:
            file_handler.write(f.read())

    blob_stub = pretend.stub(
        download_to_file=_download_to_file, delete=pretend.call_recorder(lambda: None),
    )
    bucket_stub = pretend.stub(get_blob=pretend.call_recorder(lambda a: blob_stub),)
    storage_client_stub = pretend.stub(
        bucket=pretend.call_recorder(lambda a: bucket_stub),
    )
    monkeypatch.setattr(
        main, "storage", pretend.stub(Client=lambda: storage_client_stub)
    )

    table_stub = pretend.stub()
    dataset_stub = pretend.stub(table=pretend.call_recorder(lambda a: table_stub))
    load_job_stub = pretend.stub(
        result=pretend.call_recorder(lambda: None), output_rows=pretend.stub(),
    )

    def _load_table_from_file(fh, *a, **kw):
        fh.flush()
        with open(fh.name, "rb") as f:
            load_job_stub._result = f.read()
        return load_job_stub

    bigquery_client_stub = pretend.stub(
        load_table_from_file=pretend.call_recorder(_load_table_from_file),
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

    data = {
        "name": log_filename,
        "bucket": "my-bucket",
    }
    context = pretend.stub()

    main.process_fastly_log(data, context)

    assert storage_client_stub.bucket.calls == [pretend.call("my-bucket")] + [
        pretend.call(RESULT_BUCKET),
    ] * len(expected_from_string_calls)
    assert bucket_stub.get_blob.calls == [pretend.call(log_filename)]
    assert dataset_reference_stub.from_string.calls == expected_from_string_calls
    assert bigquery_client_stub.load_table_from_file.calls == [
        pretend.call(
            bigquery_client_stub.load_table_from_file.calls[0].args[0],  # shh
            table_stub,
            job_id_prefix="linehaul_file_downloads",
            location="US",
            job_config=job_config_stub,
            rewind=True,
        )
    ] * len(expected_from_string_calls)
    assert dataset_stub.table.calls == [pretend.call(table_name)] * len(
        expected_from_string_calls
    )
    assert blob_stub.delete.calls == [pretend.call()]
    assert load_job_stub.result.calls == [pretend.call()] * len(
        expected_from_string_calls
    )
    assert load_job_stub._result == expected
