# About

Implementation of linehaul to feed the PyPI public BigQuery dataset via Google Cloud Functions.

The function is triggerd by the CDN logs created in a Cloud Storage bucket. It parses the logs and streams them to the BigQuery public dataset.

# Deploy

These functions auto-deploy on merge to the `main` branch via a Cloud Build trigger on this repository.

## Publisher recovery

The publisher records its active batch in
`gs://$RESULT_BUCKET/publisher-state/active.json` before submitting any BigQuery
loads. Retries recover the recorded jobs and finish deleting that batch before
listing fresh files. Each destination's successful load is saved before deletion;
cleanup targets the recorded object generations and tolerates missing objects.
Historical-partition continuation messages are saved before publication and can
be delivered more than once.

Never delete or reset the state object, including when its `batch` is null. Its
GCS generation prevents delayed invocations from reclaiming stale file lists.
Cloud Build applies `result-bucket-lifecycle.json` before deploying the publisher:
the existing 14-day expiry covers `processed/` and `unprocessed/` only. The build
service account needs `storage.buckets.update`; the publisher needs read/write
access to the state object and access to its recorded BigQuery jobs.

Pending source contents must remain unchanged until their loads finish; BigQuery
does not support generation-qualified GCS load URIs. This protocol does not
deduplicate source files recreated by the ingestor or repair existing public rows.

### First rollout and rollback

Before merging the first recovery deployment:

1. Stop publisher delivery, including scheduled and manual triggers and retries.
2. Wait for old publisher invocations and their BigQuery jobs to finish. A
   function timeout does not cancel a submitted BigQuery job.
3. Reconcile files already loaded but left undeleted by the old publisher. Its
   randomly identified jobs are not recorded in the new state.
4. Deploy, verify the lifecycle exception and the new revision, then resume
   delivery and observe a complete load-and-cleanup cycle.

Old and new publishers must not overlap: old code ignores the state object.
Drain active recovery work before rolling back to a revision without this protocol.

### Stalled batches

A confirmed failed load permits a new job attempt on a later invocation. A
submission or polling error with an unknown outcome keeps the same job identity.
If BigQuery cannot find a job whose attempt was allocated at least a day ago,
the publisher stops for reconciliation instead of risking another append after
job history expires. Inspect the state object's job project, location, base job
ID and attempt suffix to locate the job. Do not clear the record or delete job
metadata to bypass this check. Data files still expire after 14 days, so repair
stalled batches before that deadline.
