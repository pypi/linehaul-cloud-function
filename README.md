# About

Implementation of linehaul to feed the PyPI public BigQuery dataset via Google Cloud Functions.

The function is triggerd by the CDN logs created in a Cloud Storage bucket. It parses the logs and streams them to the BigQuery public dataset.

# Deploy

These functions auto-deploy on merge to the `main` branch via a Cloud Build trigger on this repository.
