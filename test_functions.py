from importlib import reload
from pathlib import Path

import pretend
import pytest
import requests
from google.api_core import exceptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import storage

import main

GCP_PROJECT = "my-gcp-project"
RESULT_BUCKET = "my-result-bucket"


@pytest.mark.parametrize(
    "log_filename, expected_data, expected_unprocessed, expected_unprocessed_filename, expected_data_filename",
    [
        (
            "downloads-2021-01-07-20-55-2021-01-07T20-55-00.000-B8Hs_G6d6xN61En2ypwk.log.gz",
            b'{"timestamp": "2021-01-07 20:54:54 +00:00", "url": "/packages/f7/12/ec3f2e203afa394a149911729357aa48affc59c20e2c1c8297a60f33f133/threadpoolctl-2.1.0-py3-none-any.whl", "project": "threadpoolctl", "file": {"filename": "threadpoolctl-2.1.0-py3-none-any.whl", "project": "threadpoolctl", "version": "2.1.0", "type": "bdist_wheel"}, "tls_protocol": "TLSv1.2", "tls_cipher": "ECDHE-RSA-AES128-GCM-SHA256", "country_code": "US", "details": {"installer": {"name": "pip", "version": "20.1.1", "subcommand": null}, "python": "3.7.9", "implementation": {"name": "CPython", "version": "3.7.9"}, "distro": {"name": "Debian GNU/Linux", "version": "9", "id": "stretch", "libc": {"lib": "glibc", "version": "2.24"}}, "system": {"name": "Linux", "release": "4.15.0-112-generic"}, "cpu": "x86_64", "openssl_version": "OpenSSL 1.1.0l  10 Sep 2019", "setuptools_version": "47.1.0", "rustc_version": null, "ci": null}}\n'
            b'{"timestamp": "2021-01-07 20:54:54 +00:00", "url": "/packages/cd/f9/8fad70a3bd011a6be7c5c6067278f006a25341eb39d901fbda307e26804c/django_crum-0.7.9-py2.py3-none-any.whl", "project": "django-crum", "file": {"filename": "django_crum-0.7.9-py2.py3-none-any.whl", "project": "django-crum", "version": "0.7.9", "type": "bdist_wheel"}, "tls_protocol": "TLSv1.2", "tls_cipher": "ECDHE-RSA-AES128-GCM-SHA256", "country_code": "US", "details": {"installer": {"name": "pip", "version": "20.0.2", "subcommand": ""}, "python": "3.8.5", "implementation": {"name": "CPython", "version": "3.8.5"}, "distro": {"name": "Ubuntu", "version": "16.04", "id": "xenial", "libc": {"lib": "glibc", "version": "2.23"}}, "system": {"name": "Linux", "release": "4.4.0-1113-aws"}, "cpu": "x86_64", "openssl_version": "OpenSSL 1.0.2g  1 Mar 2016", "setuptools_version": "44.1.0", "rustc_version": null, "ci": null}}\n'
            b'{"timestamp": "2021-01-07 20:54:54 +00:00", "url": "/packages/cd/f9/8fad70a3bd011a6be7c5c6067278f006a25341eb39d901fbda307e26804c/django_crum-0.7.9-py2.py3-none-any.whl", "project": "django-crum", "file": {"filename": "django_crum-0.7.9-py2.py3-none-any.whl", "project": "django-crum", "version": "0.7.9", "type": "bdist_wheel"}, "tls_protocol": "TLSv1.2", "tls_cipher": "ECDHE-RSA-AES128-GCM-SHA256", "country_code": "US", "details": {"installer": {"name": "pip", "version": "22.0.3", "subcommand": "install \'something with a space\'"}, "python": "3.9.10", "implementation": {"name": "CPython", "version": "3.9.10"}, "distro": {"name": "macOS", "version": "12.3", "id": null, "libc": null}, "system": {"name": "Darwin", "release": "21.4.0"}, "cpu": "arm64", "openssl_version": "OpenSSL 1.1.1m  14 Dec 2021", "setuptools_version": "60.9.0", "rustc_version": "1.59.0", "ci": true}}\n'
            b'{"timestamp": "2021-01-07 20:54:54 +00:00", "url": "/packages/cd/f9/8fad70a3bd011a6be7c5c6067278f006a25341eb39d901fbda307e26804c/django_crum-0.7.9-py2.py3-none-any.whl", "project": "django-crum", "file": {"filename": "django_crum-0.7.9-py2.py3-none-any.whl", "project": "django-crum", "version": "0.7.9", "type": "bdist_wheel"}, "tls_protocol": "TLSv1.2", "tls_cipher": "ECDHE-RSA-AES128-GCM-SHA256", "country_code": "US", "details": {"installer": {"name": "uv", "version": "0.9.11", "subcommand": "pip install"}, "python": "3.9.10", "implementation": {"name": "CPython", "version": "3.9.10"}, "distro": {"name": "macOS", "version": "12.3", "id": null, "libc": null}, "system": {"name": "Darwin", "release": "21.4.0"}, "cpu": "arm64", "openssl_version": "OpenSSL 1.1.1m  14 Dec 2021", "setuptools_version": "60.9.0", "rustc_version": "1.59.0", "ci": true}}\n',
            b"download|Thu, 07 Jan 2021 20:54:56 GMT|US|/packages/c5/db/e56e6b4bbac7c4a06de1c50de6fe1ef3810018ae11732a50f15f62c7d050/enum34-1.1.6-py2-none-any.whl|TLSv1.2|ECDHE-RSA-AES128-GCM-SHA256|enum34|1.1.6|bdist_wheel|(null)\n",
            "unprocessed/20210107/downloads-2021-01-07-20-55-2021-01-07T20-55-00.000-B8Hs_G6d6xN61En2ypwk.txt",
            "processed/20210107/downloads-downloads-2021-01-07-20-55-2021-01-07T20-55-00.000-B8Hs_G6d6xN61En2ypwk.json",
        ),
        (
            "simple-2021-01-07-20-55-2021-01-07T20-55-00.000-3wuB00t9tqgbGLFI2fSI.log.gz",
            b'{"timestamp": "2021-01-07 20:54:52 +00:00", "url": "/simple/azureml-model-management-sdk/", "project": "azureml-model-management-sdk", "tls_protocol": "TLSv1.3", "tls_cipher": "AES256-GCM", "country_code": "US", "details": {"installer": {"name": "pip", "version": "20.0.2", "subcommand": null}, "python": "3.7.5", "implementation": {"name": "CPython", "version": "3.7.5"}, "distro": {"name": "Ubuntu", "version": "18.04", "id": "bionic", "libc": {"lib": "glibc", "version": "2.27"}}, "system": {"name": "Linux", "release": "4.15.0-1092-azure"}, "cpu": "x86_64", "openssl_version": "OpenSSL 1.1.1  11 Sep 2018", "setuptools_version": "45.2.0", "rustc_version": null, "ci": null}}\n'
            b'{"timestamp": "2021-01-07 20:54:52 +00:00", "url": "/simple/pyrsistent/", "project": "pyrsistent", "tls_protocol": "TLSv1.3", "tls_cipher": "AES256-GCM", "country_code": "US", "details": {"installer": {"name": "pip", "version": "20.0.2", "subcommand": null}, "python": "3.8.5", "implementation": {"name": "CPython", "version": "3.8.5"}, "distro": {"name": "Ubuntu", "version": "20.04", "id": "focal", "libc": {"lib": "glibc", "version": "2.31"}}, "system": {"name": "Linux", "release": "5.4.72-flatcar"}, "cpu": "x86_64", "openssl_version": "OpenSSL 1.1.1f  31 Mar 2020", "setuptools_version": "45.2.0", "rustc_version": null, "ci": true}}\n',
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


def test_process_fastly_log_deletes_malformed_gzip(monkeypatch):
    monkeypatch.setenv("GCP_PROJECT", GCP_PROJECT)
    monkeypatch.setenv("RESULT_BUCKET", RESULT_BUCKET)

    reload(main)

    def _download_to_file(file_handler):
        file_handler.write(b"not gzip data")

    get_blob_stub = pretend.stub(
        download_to_file=_download_to_file,
        delete=pretend.call_recorder(lambda: None),
    )

    bucket_stub = pretend.stub(
        get_blob=pretend.call_recorder(lambda a: get_blob_stub),
    )
    storage_client_stub = pretend.stub(
        bucket=pretend.call_recorder(lambda a: bucket_stub),
    )
    monkeypatch.setattr(
        main, "storage", pretend.stub(Client=lambda: storage_client_stub)
    )

    data = {
        "name": "poison.log.gz",
        "bucket": "my-bucket",
    }
    context = pretend.stub()

    main.process_fastly_log(data, context)

    assert storage_client_stub.bucket.calls == [pretend.call("my-bucket")]
    assert bucket_stub.get_blob.calls == [pretend.call("poison.log.gz")]
    assert get_blob_stub.delete.calls == [pretend.call()]


@pytest.fixture
def deletion_client():
    # Exercise the real storage batch encoder/decoder without network access.
    responses = []
    sent_requests = []

    def request(method, url, **kwargs):
        sent_requests.append((method, url))
        status, parts = responses.pop(0)
        response = requests.Response()
        response.status_code = status
        response.request = requests.Request(method, url).prepare()
        if status != 200:
            response._content = b'{"error": {"message": "Batch request failed"}}'
            return response
        response.headers["Content-Type"] = "multipart/mixed; boundary=batch"
        response._content = (
            "".join(
                "--batch\r\n"
                "Content-Type: application/http\r\n\r\n"
                f"HTTP/1.1 {code} Response\r\n"
                "Content-Type: application/json\r\n\r\n"
                f'{{"error": {{"message": "Object response {code}"}}}}\r\n'
                for code in parts
            )
            + "--batch--\r\n"
        ).encode()
        return response

    client = storage.Client(
        project=GCP_PROJECT,
        credentials=AnonymousCredentials(),
        _http=pretend.stub(request=request),
    )
    return client, responses, sent_requests


@pytest.mark.parametrize("blob_type", ["downloads", "simple"])
def test_delete_blobs_ignores_missing_objects(deletion_client, blob_type):
    client, responses, sent_requests = deletion_client
    responses.append((200, [204, 404, 204]))
    bucket = client.bucket(RESULT_BUCKET)
    blobs = [bucket.blob(f"{blob_type}-{i}") for i in range(3)]

    main._delete_blobs(
        client,
        blobs if blob_type == "downloads" else [],
        "downloads-",
        blobs if blob_type == "simple" else [],
        "simple-",
    )

    assert len(sent_requests) == 1
    assert not responses


def test_delete_blobs_does_not_hide_other_errors(deletion_client):
    client, responses, _ = deletion_client
    responses.append((200, [404, 403]))
    bucket = client.bucket(RESULT_BUCKET)

    with pytest.raises(exceptions.Forbidden):
        main._delete_blobs(
            client,
            [bucket.blob("missing"), bucket.blob("forbidden")],
            "downloads-",
            [],
            "simple-",
        )


def test_delete_blobs_retries_partial_failure_and_continues(deletion_client):
    client, responses, sent_requests = deletion_client
    responses.extend(
        [
            (200, [404, 503]),
            (200, [404, 204]),
            (200, [204]),
        ]
    )
    bucket = client.bucket(RESULT_BUCKET)

    main._delete_blobs(
        client,
        [bucket.blob("missing"), bucket.blob("retry")],
        "downloads-",
        [bucket.blob("simple")],
        "simple-",
    )

    assert len(sent_requests) == 3
    assert not responses


def test_delete_blobs_does_not_ignore_batch_endpoint_not_found(deletion_client):
    client, responses, _ = deletion_client
    responses.append((404, []))

    with pytest.raises(exceptions.NotFound):
        main._delete_blobs(
            client,
            [client.bucket(RESULT_BUCKET).blob("download")],
            "downloads-",
            [],
            "simple-",
        )
