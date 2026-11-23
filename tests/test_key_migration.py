"""Tests for the collision-free session key and legacy-database migration.

Covers:
* the original bug: event ids containing commas must not collide between a
  one-event session ["a,b"] and a two-event session ["a", "b"];
* full, ordered event ids on adds/retracts (JSON storage);
* opening a pre-upgrade database: original revision numbers and keys are
  preserved, ambiguous legacy rows are flagged "need review" with an empty
  id list, and the first write migrates keys once per old key;
* a failed migration keeps the previous revision and the old watermark and
  can be retried with the same request.
"""
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

from app import service
from app.main import app, get_db
from app.models import fmt_ts


def _point_app_at(engine):
    """Repoint both the module-level engine and the request DB dependency."""
    from app import main as main_module

    main_module.engine = engine
    test_session = sessionmaker(bind=engine, future=True)

    def override_get_db():
        db = test_session()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = override_get_db


def _iso(y, mo, d, h, mi=0, s=0):
    return datetime(y, mo, d, h, mi, s, tzinfo=timezone.utc).isoformat().replace(
        "+00:00", "Z"
    )


def import_events(client, uid, pairs):
    payload = {
        "events": [
            {"user_id": uid, "event_id": eid, "event_time": ts}
            for eid, ts in pairs
        ]
    }
    return client.post("/api/events/import", json=payload)


def wm(client, uid, ts):
    return client.post(
        "/api/watermark/advance", json={"user_id": uid, "watermark": ts}
    )


# ------------------------------------------------- fresh DB: comma collision
def test_comma_inside_event_id_does_not_share_session_key(client):
    uid = "comma-fresh"
    client.put(f"/api/users/{uid}/config", json={"idle_gap_seconds": 600})
    r = import_events(
        client,
        uid,
        [
            ("a,b", _iso(2026, 11, 22, 10, 0)),
            ("a", _iso(2026, 11, 22, 10, 20)),
            ("b", _iso(2026, 11, 22, 10, 22)),
        ],
    )
    assert r.status_code == 200, r.text
    pending = client.get(f"/api/users/{uid}/sessions").json()
    assert len(pending) == 2
    key_single = pending[0]["session_key"]
    key_double = pending[1]["session_key"]
    assert key_single != key_double, '["a,b"] and ["a","b"] must not share a key'
    assert pending[0]["event_ids"] == ["a,b"]
    assert pending[1]["event_ids"] == ["a", "b"]
    assert all(s["ids_need_review"] is False for s in pending)

    # Publish the first session; the second must stay pending even though the
    # old rule would have treated both sessions as the same key.
    r = wm(client, uid, _iso(2026, 11, 22, 10, 12))
    body = r.json()
    assert body["new_revision_no"] == 1
    assert len(body["changes"]) == 1
    assert body["changes"][0]["event_ids"] == ["a,b"]
    assert len(body["published"]) == 1 and len(body["pending"]) == 1

    # The second session then publishes with its own key and full ids.
    r2 = wm(client, uid, _iso(2026, 11, 22, 10, 34))
    b2 = r2.json()
    assert b2["new_revision_no"] == 2
    assert [c["event_ids"] for c in b2["changes"]] == [["a", "b"]]
    assert {tuple(s["event_ids"]) for s in b2["published"]} == {("a,b",), ("a", "b")}


def test_add_and_retract_carry_full_ordered_ids(client):
    uid = "comma-retract"
    client.put(f"/api/users/{uid}/config", json={"idle_gap_seconds": 600})
    import_events(
        client,
        uid,
        [("x,1", _iso(2026, 11, 22, 10, 0)), ("x,2", _iso(2026, 11, 22, 10, 5))],
    )
    wm(client, uid, _iso(2026, 11, 22, 10, 18))
    r = import_events(client, uid, [("L", _iso(2026, 11, 22, 10, 3))])
    changes = {c["change_type"]: c for c in r.json()["changes"]}
    assert changes["retract"]["event_ids"] == ["x,1", "x,2"]
    assert changes["add"]["event_ids"] == ["x,1", "L", "x,2"]


# ------------------------------------------------- legacy database helpers
def _legacy_key(event_ids):
    import hashlib

    return hashlib.sha256(",".join(event_ids).encode()).hexdigest()[:16]


def _build_legacy_db(
    path,
    user_id,
    sessions,
    watermark_dt,
    idle_gap=600,
    revision_no=1,
):
    """Create a database in the exact pre-upgrade schema and content.

    ``sessions`` is a list of dicts with start/end datetimes, event_ids
    (joined with commas for storage like the old code did) and status.
    """
    import sqlalchemy as sa

    eng = sa.create_engine(f"sqlite:///{path}")
    with eng.begin() as conn:
        conn.execute(
            text(
                "CREATE TABLE user_config ("
                "user_id VARCHAR(255) NOT NULL PRIMARY KEY, "
                "idle_gap_seconds INTEGER NOT NULL, "
                "watermark VARCHAR(33) NOT NULL, "
                "created_at VARCHAR(33) NOT NULL, "
                "updated_at VARCHAR(33) NOT NULL, "
                "CHECK (idle_gap_seconds > 0))"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE event ("
                "id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, "
                "user_id VARCHAR(255) NOT NULL, "
                "event_id VARCHAR(255) NOT NULL, "
                "event_time VARCHAR(33) NOT NULL, "
                "created_at VARCHAR(33) NOT NULL, "
                "UNIQUE (user_id, event_id))"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE revision ("
                "id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, "
                "user_id VARCHAR(255) NOT NULL, "
                "revision_no INTEGER NOT NULL, "
                "reason VARCHAR(64) NOT NULL, "
                "watermark VARCHAR(33) NOT NULL, "
                "idle_gap_seconds INTEGER NOT NULL, "
                "created_at VARCHAR(33) NOT NULL, "
                "detail TEXT, UNIQUE (user_id, revision_no))"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE session_snapshot ("
                "id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, "
                "revision_id INTEGER NOT NULL, "
                "session_key VARCHAR(64) NOT NULL, "
                "start_time VARCHAR(33) NOT NULL, "
                "end_time VARCHAR(33) NOT NULL, "
                "event_count INTEGER NOT NULL, "
                "event_ids TEXT NOT NULL, "
                "status VARCHAR(16) NOT NULL)"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE revision_change ("
                "id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, "
                "revision_id INTEGER NOT NULL, "
                "change_type VARCHAR(16) NOT NULL, "
                "session_key VARCHAR(64) NOT NULL, "
                "start_time VARCHAR(33) NOT NULL, "
                "end_time VARCHAR(33) NOT NULL, "
                "event_count INTEGER NOT NULL, "
                "event_ids TEXT NOT NULL)"
            )
        )
        now = fmt_ts(datetime.now(timezone.utc))
        conn.execute(
            text(
                "INSERT INTO user_config "
                "(user_id, idle_gap_seconds, watermark, created_at, updated_at) "
                "VALUES (:u, :g, :w, :n, :n)"
            ),
            {
                "u": user_id,
                "g": idle_gap,
                "w": fmt_ts(watermark_dt),
                "n": now,
            },
        )
        conn.execute(
            text(
                "INSERT INTO revision "
                "(user_id, revision_no, reason, watermark, idle_gap_seconds, "
                "created_at, detail) VALUES (:u, 1, 'publish', :w, :g, :n, :d)"
            ),
            {
                "u": user_id,
                "w": fmt_ts(watermark_dt),
                "g": idle_gap,
                "n": now,
                "d": "legacy revision",
            },
        )
        rev_id = conn.execute(
            text("SELECT id FROM revision WHERE user_id=:u"), {"u": user_id}
        ).scalar_one()
        for idx, sess in enumerate(sessions, start=1):
            joined = ",".join(sess["event_ids"])
            conn.execute(
                text(
                    "INSERT INTO session_snapshot "
                    "(revision_id, session_key, start_time, end_time, "
                    "event_count, event_ids, status) VALUES "
                    "(:r, :k, :s, :e, :c, :ids, :st)"
                ),
                {
                    "r": rev_id,
                    "k": _legacy_key(sess["event_ids"]),
                    "s": fmt_ts(sess["start"]),
                    "e": fmt_ts(sess["end"]),
                    "c": sess["event_count"],
                    "ids": joined,
                    "st": sess["status"],
                },
            )
            for eid in sess["event_ids"]:
                # Find event time: ambiguous rows deliberately only carry the
                # joined string, so derive times from explicit mapping.
                etime = sess["event_times"][eid]
                conn.execute(
                    text(
                        "INSERT INTO event "
                        "(user_id, event_id, event_time, created_at) "
                        "VALUES (:u, :e, :t, :n)"
                    ),
                    {"u": user_id, "e": eid, "t": fmt_ts(etime), "n": now},
                )


# ----------------------------- legacy DB: open, read, migrate on next write
def test_legacy_db_opens_with_flag_and_migrates_on_write(client, tmp_path):
    uid = "legacy-ambiguous"
    path = str(tmp_path / "legacy.db")
    t = lambda h, mi=0: datetime(2026, 11, 22, h, mi, tzinfo=timezone.utc)

    # Two sessions that collide under the old key rule.
    sessions = [
        {
            "event_ids": ["a,b"],
            "event_count": 1,
            "start": t(10, 0),
            "end": t(10, 0),
            "status": "published",
            "event_times": {"a,b": t(10, 0)},
        },
        {
            "event_ids": ["a", "b"],
            "event_count": 2,
            "start": t(10, 20),
            "end": t(10, 22),
            "status": "published",
            "event_times": {"a": t(10, 20), "b": t(10, 22)},
        },
        {
            "event_ids": ["c1", "c2"],
            "event_count": 2,
            "start": t(10, 40),
            "end": t(10, 44),
            "status": "published",
            "event_times": {"c1": t(10, 40), "c2": t(10, 44)},
        },
    ]
    _build_legacy_db(path, uid, sessions, watermark_dt=t(10, 34))

    # Point the app at the legacy file and run the additive migration.
    from sqlalchemy import create_engine

    legacy_engine = create_engine(
        f"sqlite:///{path}", connect_args={"check_same_thread": False}
    )
    from app.models import run_migrations

    run_migrations(legacy_engine)
    _point_app_at(legacy_engine)

    # Reads work immediately: old revision number/keys preserved; ambiguous
    # row returns an empty id list with the review marker.
    cur = client.get(f"/api/users/{uid}/sessions").json()
    by_ids = {tuple(s["event_ids"]): s for s in cur}
    assert by_ids[()]["ids_need_review"] is True
    assert by_ids[()]["event_count"] == 1
    assert by_ids[("a", "b")]["ids_need_review"] is False
    assert by_ids[("c1", "c2")]["ids_need_review"] is False

    revs = client.get(f"/api/users/{uid}/revisions").json()
    assert [r["revision_no"] for r in revs] == [1]
    rev1 = client.get(f"/api/users/{uid}/revisions/1").json()
    old_keys = {tuple(s["event_ids"]): s["session_key"] for s in rev1["sessions"]}

    # First write triggers the key migration: ambiguous old key retracted
    # exactly once; both colliding sessions re-publish with distinct new
    # keys; the untouched session keeps its old key.
    r = wm(client, uid, _iso(2026, 11, 22, 10, 56))
    body = r.json()
    assert body["new_revision_no"] == 2
    retracts = [c for c in body["changes"] if c["change_type"] == "retract"]
    adds = [c for c in body["changes"] if c["change_type"] == "add"]
    assert len(retracts) == 1
    assert retracts[0]["ids_need_review"] is True
    assert retracts[0]["event_ids"] == []
    added_keys = {c["session_key"] for c in adds}
    assert len(added_keys) == 2
    new_pub = {tuple(s["event_ids"]): s for s in body["published"]}
    assert new_pub[("a,b",)]["session_key"] != new_pub[("a", "b")]["session_key"]
    # unchanged session kept its original legacy key
    assert new_pub[("c1", "c2")]["session_key"] == old_keys[("c1", "c2")]

    # Replaying the same watermark is a no-op: the old key is not retracted a
    # second time and no extra revision is created.
    again = wm(client, uid, _iso(2026, 11, 22, 10, 56))
    assert again.json()["new_revision_no"] == 2
    assert again.json()["changes"] == []

    # Revision numbers continue from the legacy history.
    revs2 = client.get(f"/api/users/{uid}/revisions").json()
    assert [r["revision_no"] for r in revs2] == [2, 1]
    rev1_again = client.get(f"/api/users/{uid}/revisions/1").json()
    assert rev1_again["sessions"]  # history untouched and still queryable


def test_legacy_db_without_ambiguous_rows_keeps_all_keys(client, tmp_path):
    from sqlalchemy import create_engine
    from app.models import run_migrations

    uid = "legacy-clean"
    path = str(tmp_path / "legacy.db")
    t = lambda h, mi=0: datetime(2026, 11, 22, h, mi, tzinfo=timezone.utc)
    sessions = [
        {
            "event_ids": ["a1", "a2"],
            "event_count": 2,
            "start": t(10, 0),
            "end": t(10, 8),
            "status": "published",
            "event_times": {"a1": t(10, 0), "a2": t(10, 8)},
        },
        {
            "event_ids": ["b1"],
            "event_count": 1,
            "start": t(10, 20),
            "end": t(10, 20),
            "status": "pending",
            "event_times": {"b1": t(10, 20)},
        },
    ]
    _build_legacy_db(path, uid, sessions, watermark_dt=t(10, 20))
    eng = create_engine(f"sqlite:///{path}", connect_args={"check_same_thread": False})
    run_migrations(eng)
    _point_app_at(eng)

    before = client.get(f"/api/users/{uid}/sessions").json()
    old_key = next(s["session_key"] for s in before if s["event_ids"] == ["a1", "a2"])

    # Watermark far enough to publish the pending session: published set only
    # gains one session; the legacy published session is not retracted.
    r = wm(client, uid, _iso(2026, 11, 22, 10, 40))
    body = r.json()
    assert body["new_revision_no"] == 2
    assert [c["change_type"] for c in body["changes"]] == ["add"]
    kept = next(s for s in body["published"] if s["event_ids"] == ["a1", "a2"])
    assert kept["session_key"] == old_key
    assert all(s["ids_need_review"] is False for s in body["published"])


def test_migration_failure_keeps_legacy_revision_and_watermark(client, tmp_path):
    from sqlalchemy import create_engine
    from app.models import run_migrations

    uid = "legacy-fail"
    path = str(tmp_path / "legacy.db")
    t = lambda h, mi=0: datetime(2026, 11, 22, h, mi, tzinfo=timezone.utc)
    sessions = [
        {
            "event_ids": ["a,b"],
            "event_count": 1,
            "start": t(10, 0),
            "end": t(10, 0),
            "status": "published",
            "event_times": {"a,b": t(10, 0)},
        }
    ]
    _build_legacy_db(path, uid, sessions, watermark_dt=t(10, 12))
    eng = create_engine(f"sqlite:///{path}", connect_args={"check_same_thread": False})
    run_migrations(eng)
    _point_app_at(eng)

    service.FAIL_BEFORE_COMMIT = True
    try:
        failed = wm(client, uid, _iso(2026, 11, 22, 10, 30))
        assert failed.status_code == 500
    finally:
        service.FAIL_BEFORE_COMMIT = False

    # Previous revision and original watermark survive.
    revs = client.get(f"/api/users/{uid}/revisions").json()
    assert [r["revision_no"] for r in revs] == [1]
    cfg = client.get(f"/api/users/{uid}/config").json()
    assert cfg["watermark"].startswith("2026-11-22T10:12:00")

    # The exact same request retries successfully.
    ok = wm(client, uid, _iso(2026, 11, 22, 10, 30))
    body = ok.json()
    assert body["new_revision_no"] == 2
    assert [c["change_type"] for c in body["changes"]] == ["retract", "add"]
    assert body["changes"][0]["ids_need_review"] is True
