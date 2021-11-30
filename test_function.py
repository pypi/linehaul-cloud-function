from pathlib import Path
from importlib import reload

import pretend
import pytest

import main

GCP_PROJECT = "my-gcp-project"
RESULT_BUCKET = "my-result-bucket"

@pytest.mark.parametrize(
    "log_filename, expected_data, expected_unprocessed_filename, expected_result_filename",
    [
        (
            "downloads-2021-01-07-20-55-2021-01-07T20-55-00.000-B8Hs_G6d6xN61En2ypwk.log.gz",
            b'{"timestamp": "2021-01-07 20:54:54 +00:00", "url": "/packages/f7/12/ec3f2e203afa394a149911729357aa48affc59c20e2c1c8297a60f33f133/threadpoolctl-2.1.0-py3-none-any.whl", "project": "threadpoolctl", "file": {"filename": "threadpoolctl-2.1.0-py3-none-any.whl", "project": "threadpoolctl", "version": "2.1.0", "type": "bdist_wheel"}, "tls_protocol": "TLSv1.2", "tls_cipher": "ECDHE-RSA-AES128-GCM-SHA256", "country_code": "US", "details": {"installer": {"name": "pip", "version": "20.1.1"}, "python": "3.7.9", "implementation": {"name": "CPython", "version": "3.7.9"}, "distro": {"name": "Debian GNU/Linux", "version": "9", "id": "stretch", "libc": {"lib": "glibc", "version": "2.24"}}, "system": {"name": "Linux", "release": "4.15.0-112-generic"}, "cpu": "x86_64", "openssl_version": "OpenSSL 1.1.0l  10 Sep 2019", "setuptools_version": "47.1.0", "ci": null}}\n'
            b'{"timestamp": "2021-01-07 20:54:54 +00:00", "url": "/packages/cd/f9/8fad70a3bd011a6be7c5c6067278f006a25341eb39d901fbda307e26804c/django_crum-0.7.9-py2.py3-none-any.whl", "project": "django-crum", "file": {"filename": "django_crum-0.7.9-py2.py3-none-any.whl", "project": "django-crum", "version": "0.7.9", "type": "bdist_wheel"}, "tls_protocol": "TLSv1.2", "tls_cipher": "ECDHE-RSA-AES128-GCM-SHA256", "country_code": "US", "details": {"installer": {"name": "pip", "version": "20.0.2"}, "python": "3.8.5", "implementation": {"name": "CPython", "version": "3.8.5"}, "distro": {"name": "Ubuntu", "version": "16.04", "id": "xenial", "libc": {"lib": "glibc", "version": "2.23"}}, "system": {"name": "Linux", "release": "4.4.0-1113-aws"}, "cpu": "x86_64", "openssl_version": "OpenSSL 1.0.2g  1 Mar 2016", "setuptools_version": "44.1.0", "ci": null}}\n',
            "unprocessed/20210107/downloads-2021-01-07-20-55-2021-01-07T20-55-00.000-B8Hs_G6d6xN61En2ypwk.txt",
            "processed/20210107/downloads-downloads-2021-01-07-20-55-2021-01-07T20-55-00.000-B8Hs_G6d6xN61En2ypwk.json",
        ),
        (
            "simple-2021-01-07-20-55-2021-01-07T20-55-00.000-3wuB00t9tqgbGLFI2fSI.log.gz",
            b'{"timestamp": "2021-01-07 20:54:52 +00:00", "url": "/simple/azureml-model-management-sdk/", "project": "azureml-model-management-sdk", "tls_protocol": "TLSv1.3", "tls_cipher": "AES256-GCM", "country_code": "US", "details": {"installer": {"name": "pip", "version": "20.0.2"}, "python": "3.7.5", "implementation": {"name": "CPython", "version": "3.7.5"}, "distro": {"name": "Ubuntu", "version": "18.04", "id": "bionic", "libc": {"lib": "glibc", "version": "2.27"}}, "system": {"name": "Linux", "release": "4.15.0-1092-azure"}, "cpu": "x86_64", "openssl_version": "OpenSSL 1.1.1  11 Sep 2018", "setuptools_version": "45.2.0", "ci": null}}\n'
            b'{"timestamp": "2021-01-07 20:54:52 +00:00", "url": "/simple/pyrsistent/", "project": "pyrsistent", "tls_protocol": "TLSv1.3", "tls_cipher": "AES256-GCM", "country_code": "US", "details": {"installer": {"name": "pip", "version": "20.0.2"}, "python": "3.8.5", "implementation": {"name": "CPython", "version": "3.8.5"}, "distro": {"name": "Ubuntu", "version": "20.04", "id": "focal", "libc": {"lib": "glibc", "version": "2.31"}}, "system": {"name": "Linux", "release": "5.4.72-flatcar"}, "cpu": "x86_64", "openssl_version": "OpenSSL 1.1.1f  31 Mar 2020", "setuptools_version": "45.2.0", "ci": true}}\n',
            "unprocessed/20210107/simple-2021-01-07-20-55-2021-01-07T20-55-00.000-3wuB00t9tqgbGLFI2fSI.txt",
            "processed/20210107/simple-simple-2021-01-07-20-55-2021-01-07T20-55-00.000-3wuB00t9tqgbGLFI2fSI.json",
        ),
    ],
)
def test_function(
    monkeypatch,
    log_filename,
    expected_data,
    expected_unprocessed_filename,
    expected_result_filename,
):
    monkeypatch.setenv("GCP_PROJECT", GCP_PROJECT)
    monkeypatch.setenv("RESULT_BUCKET", RESULT_BUCKET)

    reload(main)

    def _download_to_file(file_handler):
        with open(Path(".") / "fixtures" / log_filename, "rb") as f:
            file_handler.write(f.read())

    def _upload_from_file(file_handler, rewind=False):
        print(file_handler.read())

    blob_stub = pretend.stub(
        download_to_file=_download_to_file, delete=pretend.call_recorder(lambda: None), upload_from_file=_upload_from_file
    )
    bucket_stub = pretend.stub(get_blob=pretend.call_recorder(lambda a: blob_stub), blob=pretend.call_recorder(lambda a: blob_stub),)
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
    assert bucket_stub.blob.calls == [pretend.call(expected_result_filename), pretend.call(expected_unprocessed_filename)]
    assert blob_stub.delete.calls == [pretend.call()]
