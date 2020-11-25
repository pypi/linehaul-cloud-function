# About

Implementation of linehaul to feed the PyPI public BigQuery dataset via Google Cloud Functions.

The function is triggerd by the CDN logs created in a Cloud Storage bucket. It parses the logs and streams them to the BigQuery public dataset.

# Deploy

This function auto-deploys on merge to the `main` branch. Originally configured with the following commands:

Staging:
```
gcloud functions deploy linehaul-staging-ingestor --source https://source.developers.google.com/projects/the-psf/repos/github_pypa_linehaul-cloud-function/moveable-aliases/main/paths// --runtime python37 --trigger-bucket linehaul-logs-staging --project the-psf --retry
```

Production:
```
gcloud functions deploy linehaul-ingestor --source https://source.developers.google.com/projects/the-psf/repos/github_pypa_linehaul-cloud-function/moveable-aliases/main/paths// --runtime python38 --trigger-bucket linehaul-logs --project the-psf --retry
```
