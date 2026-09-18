import collections
import datetime
import json
from types import SimpleNamespace

import pretend
import pytest
import requests
from google.api_core import exceptions

import main


PARTITION = "20260917"
DOWNLOADS = "project.dataset.downloads"
SIMPLE = "project.dataset.simple"


class Clock(datetime.datetime):
    @classmethod
    def now(cls, tz=None):
        value = cls(2026, 9, 17, 12, tzinfo=datetime.timezone.utc)
        return value.astimezone(tz) if tz else value.replace(tzinfo=None)

    @classmethod
    def utcnow(cls):
        return cls.now()


class Blob:
    def __init__(self, bucket, name, generation=None):
        self.bucket = bucket
        self.name = name
        self.generation = generation

    def download_as_text(self, if_generation_match):
        generation, content = self.bucket.objects[self.name]
        if generation != if_generation_match:
            raise exceptions.PreconditionFailed("State changed")
        return content.decode()

    def upload_from_string(self, content, *, content_type, if_generation_match):
        self.bucket.before_write(json.loads(content))
        current = self.bucket.objects.get(self.name, (0, b""))[0]
        if current != if_generation_match:
            raise exceptions.PreconditionFailed("State changed")
        self.bucket.put(self.name, content.encode())
        self.bucket.after_write(json.loads(content))

    def delete(self):
        self.bucket.pending_deletes.append(self)


class Storage:
    def __init__(self):
        self.name = "results"
        self.objects = {}
        self.generation = 0
        self.pending_deletes = []
        self.delete_failures = {}
        self.before_write = lambda state: None
        self.after_write = lambda state: None
        self.after_list = lambda: None

    def put(self, name, content):
        self.generation += 1
        self.objects[name] = (self.generation, content)

    def bucket(self, name):
        assert name == self.name
        return self

    def blob(self, name, generation=None):
        return Blob(self, name, generation)

    def get_blob(self, name):
        if name not in self.objects:
            return None
        return self.blob(name, self.objects[name][0])

    def list_blobs(self, prefix, max_results):
        snapshot = [
            self.get_blob(name)
            for name in sorted(self.objects)
            if name.startswith(prefix)
        ][:max_results]
        self.after_list()
        return snapshot

    def batch(self, raise_exception):
        return Batch(self)


class Batch:
    def __init__(self, storage):
        self.storage = storage
        self._responses = []

    def __enter__(self):
        self.storage.pending_deletes = []
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type:
            return
        for blob in self.storage.pending_deletes:
            status = self.storage.delete_failures.pop(blob.name, None)
            if status is None:
                if self.storage.objects.get(blob.name, (None,))[0] == blob.generation:
                    del self.storage.objects[blob.name]
                    status = 204
                else:
                    status = 404
            response = requests.Response()
            response.status_code = status
            response.request = requests.Request(
                "DELETE", f"https://storage.invalid/{blob.name}"
            ).prepare()
            response._content = b'{"error":{"message":"Deletion failed"}}'
            self._responses.append(response)


class Job:
    def __init__(self, client, uris, table, outcomes):
        self.client = client
        self.uris = tuple(uris)
        self.table = table
        self.outcomes = collections.deque(outcomes)
        self.state = "RUNNING"
        self.error_result = None
        self.output_rows = None

    def result(self):
        if self.state != "DONE":
            outcome = self.outcomes.popleft() if self.outcomes else "success"
            if outcome == "poll_error":
                raise TimeoutError("Polling failed")
            self.state = "DONE"
            if outcome == "failure":
                self.error_result = {"reason": "invalid"}
            else:
                rows = []
                for uri in self.uris:
                    name = uri.split("/", 3)[3]
                    rows.extend(
                        json.loads(line)
                        for line in self.client.storage.objects[name][1].splitlines()
                    )
                self.client.tables[self.table].extend(rows)
                self.output_rows = len(rows)
        if self.error_result:
            raise exceptions.BadRequest("Load failed without committing")


class BigQuery:
    def __init__(self, storage):
        self.storage = storage
        self.project = "project"
        self.jobs = {}
        self.tables = collections.defaultdict(list)
        self.outcomes = collections.defaultdict(collections.deque)
        self.before_submit = lambda key, uris, table: None

    def create_job(self, key, uris, table):
        outcomes = (
            self.outcomes[table].popleft() if self.outcomes[table] else ["success"]
        )
        self.jobs[key] = Job(self, uris, table, outcomes)
        return self.jobs[key]

    def get_job(self, job_id, *, project, location):
        try:
            return self.jobs[(project, location, job_id)]
        except KeyError:
            raise exceptions.NotFound("Unknown job")

    def load_table_from_uri(
        self, uris, table, *, job_id, project, location, job_config
    ):
        key = (project, location, job_id)
        self.before_submit(key, uris, table)
        if key in self.jobs:
            raise exceptions.Conflict("Existing job")
        return self.create_job(key, uris, table)


class Publisher:
    def __init__(self):
        self.messages = []
        self.lose_response = False

    def publish(self, topic, data, **attrs):
        self.messages.append((topic, attrs))

        def result():
            if self.lose_response:
                self.lose_response = False
                raise TimeoutError("Publish response lost")
            return "message-id"

        return pretend.stub(result=result)


class Environment:
    def __init__(self):
        self.storage = Storage()
        self.bq = BigQuery(self.storage)
        self.publisher = Publisher()

    def put(self, name, kind="downloads", partition=PARTITION):
        path = f"processed/{partition}/{kind}-{name}.json"
        self.storage.put(path, json.dumps({"id": name}).encode() + b"\n")
        return path

    def run(self, partition=PARTITION, continuation=False):
        attrs = {"partition": partition}
        if continuation:
            attrs["continue_publishing"] = "True"
        main.load_processed_files_into_bigquery({"attributes": attrs}, None)

    def state(self):
        return json.loads(self.storage.objects[main.PUBLISHER_STATE][1])

    def crash_on_completion(self):
        def crash(state):
            batch = state["batch"]
            if batch and any(job["complete"] for job in batch["jobs"]):
                self.storage.before_write = lambda state: None
                raise SystemExit("Crash after commit")

        self.storage.before_write = crash

    def age_pending_jobs(self):
        state = self.state()
        for job in state["batch"]["jobs"]:
            job["created_at"] = "2026-09-15T12:00:00+00:00"
        self.storage.put(main.PUBLISHER_STATE, json.dumps(state).encode())


@pytest.fixture
def env(monkeypatch):
    env = Environment()
    for name, value in {
        "DEFAULT_PROJECT": "project",
        "RESULT_BUCKET": "results",
        "DATASETS": ["project.dataset"],
        "DOWNLOAD_TABLE": "downloads",
        "SIMPLE_TABLE": "simple",
        "PUBSUB_TOPIC": "publisher",
        "MAX_BLOBS_PER_RUN": 1000,
    }.items():
        monkeypatch.setattr(main, name, value)
    monkeypatch.setattr(
        main,
        "datetime",
        SimpleNamespace(
            datetime=Clock, timedelta=datetime.timedelta, timezone=datetime.timezone
        ),
    )
    monkeypatch.setattr(main.storage, "Client", lambda: env.storage)
    monkeypatch.setattr(main.bigquery, "Client", lambda: env.bq)
    monkeypatch.setattr(main.pubsub_v1, "PublisherClient", lambda: env.publisher)
    return env


def test_empty_historical_partition_does_not_publish_forever(env):
    env.run(continuation=True)
    assert env.publisher.messages == []
    assert env.bq.tables == {}


def test_previous_partition_precedes_current_partition(env):
    old = env.put("old", partition="20260916")
    fresh = env.put("fresh")
    main.load_processed_files_into_bigquery({}, None)
    assert env.bq.tables[DOWNLOADS] == [{"id": "old"}]
    assert old not in env.storage.objects
    assert fresh in env.storage.objects
    main.load_processed_files_into_bigquery({}, None)
    assert env.bq.tables[DOWNLOADS] == [{"id": "old"}, {"id": "fresh"}]


def test_committed_load_survives_crash_before_completion_record(env):
    source = env.put("a")
    env.crash_on_completion()
    with pytest.raises(SystemExit):
        env.run()
    assert env.bq.tables[DOWNLOADS] == [{"id": "a"}]
    assert source in env.storage.objects
    env.run()
    assert env.bq.tables[DOWNLOADS] == [{"id": "a"}]
    assert source not in env.storage.objects


def test_claim_survives_lost_storage_write_response(env):
    env.put("a")

    def lose_response(state):
        env.storage.after_write = lambda state: None
        raise TimeoutError("State write response lost")

    env.storage.after_write = lose_response
    with pytest.raises(TimeoutError):
        env.run()
    assert env.bq.jobs == {}
    env.run()
    assert env.bq.tables[DOWNLOADS] == [{"id": "a"}]


def test_submission_with_unknown_outcome_recovers_committed_job(env):
    env.put("a")

    def lose_response(key, uris, table):
        env.bq.before_submit = lambda *args: None
        env.bq.create_job(key, uris, table).result()
        raise TimeoutError("Submission response lost")

    env.bq.before_submit = lose_response
    with pytest.raises(TimeoutError):
        env.run()
    env.run()
    assert env.bq.tables[DOWNLOADS] == [{"id": "a"}]


def test_conflict_waits_for_original_job_before_cleanup(env):
    source = env.put("a")
    env.bq.outcomes[DOWNLOADS].append(["poll_error", "success"])

    def concurrent_submit(key, uris, table):
        env.bq.before_submit = lambda *args: None
        env.bq.create_job(key, uris, table)

    env.bq.before_submit = concurrent_submit
    with pytest.raises(TimeoutError):
        env.run()
    assert source in env.storage.objects
    assert env.bq.tables == {}
    env.run()
    assert env.bq.tables[DOWNLOADS] == [{"id": "a"}]
    assert len(env.bq.jobs) == 1
    assert source not in env.storage.objects


def test_terminal_failure_permits_new_attempt(env):
    source = env.put("a")
    env.bq.outcomes[DOWNLOADS].append(["failure"])
    with pytest.raises(exceptions.BadRequest):
        env.run()
    assert env.bq.tables == {}
    assert source in env.storage.objects
    env.run()
    assert len(env.bq.jobs) == 2
    assert env.bq.tables[DOWNLOADS] == [{"id": "a"}]


def test_polling_failure_keeps_original_attempt(env):
    env.put("a")
    env.bq.outcomes[DOWNLOADS].append(["poll_error", "success"])
    with pytest.raises(TimeoutError):
        env.run()
    env.run()
    assert len(env.bq.jobs) == 1
    assert env.bq.tables[DOWNLOADS] == [{"id": "a"}]


def test_failed_destination_does_not_reload_successful_destinations(env, monkeypatch):
    monkeypatch.setattr(main, "DATASETS", ["project.dataset", "other.project"])
    download = env.put("a")
    simple = env.put("b", kind="simple")
    env.bq.outcomes["other.project.simple"].append(["failure"])
    with pytest.raises(exceptions.BadRequest):
        env.run()
    assert download in env.storage.objects
    assert simple in env.storage.objects
    env.run()
    assert dict(env.bq.tables) == {
        DOWNLOADS: [{"id": "a"}],
        SIMPLE: [{"id": "b"}],
        "other.project.downloads": [{"id": "a"}],
        "other.project.simple": [{"id": "b"}],
    }
    assert download not in env.storage.objects
    assert simple not in env.storage.objects


def test_stale_listing_cannot_reclaim_a_retired_batch(env):
    env.put("a")

    def concurrent_run():
        env.storage.after_list = lambda: None
        env.run()
        env.put("b")

    env.storage.after_list = concurrent_run
    env.run()
    assert env.bq.tables[DOWNLOADS] == [{"id": "a"}, {"id": "b"}]
    assert list(env.storage.objects) == [main.PUBLISHER_STATE]


def test_competing_claimant_adopts_active_batch(env):
    env.put("a")
    env.bq.outcomes[DOWNLOADS].append(["poll_error", "success"])

    def concurrent_run(state):
        env.storage.before_write = lambda state: None
        with pytest.raises(TimeoutError):
            env.run()

    env.storage.before_write = concurrent_run
    env.run()
    assert env.bq.tables[DOWNLOADS] == [{"id": "a"}]
    assert len(env.bq.jobs) == 1


def test_partial_deletion_resumes_without_loading_survivors_or_arrivals(env):
    deleted = env.put("a")
    survivor = env.put("b")
    simple = env.put("s", kind="simple")
    env.storage.delete_failures[survivor] = 403
    with pytest.raises(exceptions.Forbidden):
        env.run()
    assert deleted not in env.storage.objects
    assert survivor in env.storage.objects
    assert simple in env.storage.objects
    fresh = env.put("c")
    env.run()
    assert dict(env.bq.tables) == {
        DOWNLOADS: [{"id": "a"}, {"id": "b"}],
        SIMPLE: [{"id": "s"}],
    }
    assert fresh in env.storage.objects
    assert survivor not in env.storage.objects
    assert simple not in env.storage.objects
    env.run()
    assert env.bq.tables[DOWNLOADS] == [{"id": "a"}, {"id": "b"}, {"id": "c"}]


def test_cleanup_does_not_delete_replacement_generation(env):
    source = env.put("original")

    def replace_after_commit(state):
        if state["batch"] and state["batch"]["jobs"][0]["complete"]:
            env.storage.before_write = lambda state: None
            env.storage.put(source, b'{"id":"replacement"}\n')

    env.storage.before_write = replace_after_commit
    env.run()
    assert env.bq.tables[DOWNLOADS] == [{"id": "original"}]
    assert env.storage.objects[source][1] == b'{"id":"replacement"}\n'


def test_old_missing_job_stops_without_deleting_sources(env):
    source = env.put("a")

    def crash_before_submission(*args):
        env.bq.before_submit = lambda *args: None
        raise SystemExit("Crash before submission")

    env.bq.before_submit = crash_before_submission
    with pytest.raises(SystemExit):
        env.run()
    env.age_pending_jobs()
    with pytest.raises(RuntimeError):
        env.run()
    assert env.bq.jobs == {}
    assert source in env.storage.objects


def test_existing_old_job_can_still_be_recovered(env):
    source = env.put("a")
    env.crash_on_completion()
    with pytest.raises(SystemExit):
        env.run()
    env.age_pending_jobs()
    env.run()
    assert env.bq.tables[DOWNLOADS] == [{"id": "a"}]
    assert source not in env.storage.objects


def test_historical_recovery_preserves_config_then_honors_new_request(env, monkeypatch):
    old_partition = "20240102"
    old = env.put("old", partition=old_partition)
    env.bq.outcomes[DOWNLOADS].append(["poll_error", "success"])
    with pytest.raises(TimeoutError):
        env.run(partition=old_partition, continuation=True)
    monkeypatch.setattr(main, "DEFAULT_PROJECT", "new-project")
    monkeypatch.setattr(main, "DATASETS", ["new_dataset"])
    monkeypatch.setattr(main, "PUBSUB_TOPIC", "new-topic")
    env.bq.project = "new-project"
    fresh = env.put("new")
    env.run()
    assert dict(env.bq.tables) == {
        DOWNLOADS: [{"id": "old"}],
        "new-project.new_dataset.downloads": [{"id": "new"}],
    }
    assert env.publisher.messages == [
        (
            "projects/project/topics/publisher",
            {"partition": old_partition, "continue_publishing": "True"},
        )
    ]
    assert old not in env.storage.objects
    assert fresh not in env.storage.objects


def test_incoming_continuation_survives_another_interruption(env):
    env.put("a")
    env.bq.outcomes[DOWNLOADS].append(["poll_error", "poll_error", "success"])
    with pytest.raises(TimeoutError):
        env.run()
    with pytest.raises(TimeoutError):
        env.run(continuation=True)
    env.run()
    assert env.bq.tables[DOWNLOADS] == [{"id": "a"}]
    assert env.publisher.messages == [
        (
            "projects/project/topics/publisher",
            {"partition": PARTITION, "continue_publishing": "True"},
        )
    ]


def test_lost_followup_response_republishes_without_reloading(env):
    env.put("a")
    env.publisher.lose_response = True
    with pytest.raises(TimeoutError):
        env.run(continuation=True)
    env.run()
    assert env.bq.tables[DOWNLOADS] == [{"id": "a"}]
    message = (
        "projects/project/topics/publisher",
        {"partition": PARTITION, "continue_publishing": "True"},
    )
    assert env.publisher.messages == [message, message]
