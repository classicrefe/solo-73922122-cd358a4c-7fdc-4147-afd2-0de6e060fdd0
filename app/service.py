"""Core sessionization and revision logic.

Write operations for one user are serialized by an in-process lock and each
runs inside a single SQLite transaction. Any failure (including the
test-injected :data:`FAIL_BEFORE_COMMIT`) rolls the whole transaction back,
so the previous revision remains the latest visible one.
"""
from __future__ import annotations

import hashlib
import json
import threading
from datetime import datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models import (
    EPOCH,
    ENCODING_JSON,
    Event,
    Revision,
    RevisionChange,
    SessionSnapshot,
    UserConfig,
    decode_event_ids,
    encode_event_ids,
    to_utc,
)

# Test hook: when True, the next write that would create a revision raises
# before commit, exercising the "publish failure keeps the old revision" rule.
FAIL_BEFORE_COMMIT = False


class ApiError(Exception):
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(message)


_user_locks: dict[str, threading.Lock] = {}
_locks_guard = threading.Lock()


def user_lock(user_id: str) -> threading.Lock:
    with _locks_guard:
        lock = _user_locks.get(user_id)
        if lock is None:
            lock = threading.Lock()
            _user_locks[user_id] = lock
        return lock


def get_or_create_user(db: Session, user_id: str, idle_gap_seconds: int | None = None) -> UserConfig:
    cfg = db.get(UserConfig, user_id)
    if cfg is None:
        cfg = UserConfig(
            user_id=user_id,
            idle_gap_seconds=idle_gap_seconds or 600,
            watermark=EPOCH,
        )
        db.add(cfg)
        db.flush()
    return cfg


def set_idle_gap(db: Session, user_id: str, idle_gap_seconds: int) -> UserConfig:
    cfg = get_or_create_user(db, user_id)
    cfg.idle_gap_seconds = idle_gap_seconds
    db.flush()
    return cfg


def _session_key(event_ids: list[str]) -> str:
    """Collision-free key for one user's sessions.

    The key is derived from the canonical JSON array of the ordered event
    ids, not from a comma-joined string. That keeps a single-event session
    whose id is ``"a,b"`` distinct from a two-event session
    ``["a", "b"]`` — they shared one key under the old rule, which could
    suppress a publish and corrupt reads.
    """
    payload = json.dumps(event_ids, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def _legacy_session_key(event_ids: list[str]) -> str:
    """The key the pre-upgrade code would have computed (migration only)."""
    digest = hashlib.sha256(",".join(event_ids).encode("utf-8")).hexdigest()
    return digest[:16]


def sessionize(events: list[Event], idle_gap_seconds: int) -> list[dict]:
    """Group events ordered by (event_time, event_id) into sessions.

    A new session starts whenever the gap between consecutive events is
    strictly greater than the user's idle interval; equal-to-gap events stay
    in the same session (gap <= idle interval means connected).
    """
    ordered = sorted(events, key=lambda e: (e.event_time, e.event_id))
    sessions: list[dict] = []
    current: dict | None = None
    gap = timedelta(seconds=idle_gap_seconds)

    for ev in ordered:
        if current is None or ev.event_time - current["end"] > gap:
            current = {
                "session_key": "",
                "legacy_key": "",
                "start": ev.event_time,
                "end": ev.event_time,
                "event_ids": [ev.event_id],
            }
            sessions.append(current)
        else:
            current["end"] = ev.event_time
            current["event_ids"].append(ev.event_id)

    for sess in sessions:
        sess["session_key"] = _session_key(sess["event_ids"])
        sess["legacy_key"] = _legacy_session_key(sess["event_ids"])
    return sessions


def classify_sessions(
    sessions: list[dict], watermark: datetime, idle_gap_seconds: int
) -> list[dict]:
    """Publish only sessions whose end is earlier than watermark - gap."""
    threshold = watermark - timedelta(seconds=idle_gap_seconds)
    for sess in sessions:
        sess["status"] = "published" if sess["end"] < threshold else "pending"
    return sessions


def _load_user_events(db: Session, user_id: str) -> list[Event]:
    return list(
        db.scalars(select(Event).where(Event.user_id == user_id)).all()
    )


def latest_revision(db: Session, user_id: str) -> Revision | None:
    return db.scalars(
        select(Revision)
        .where(Revision.user_id == user_id)
        .order_by(Revision.revision_no.desc())
        .limit(1)
    ).first()


def get_revision(db: Session, user_id: str, revision_no: int) -> Revision | None:
    return db.scalars(
        select(Revision).where(
            Revision.user_id == user_id, Revision.revision_no == revision_no
        )
    ).first()


def list_revisions(db: Session, user_id: str) -> list[Revision]:
    return list(
        db.scalars(
            select(Revision)
            .where(Revision.user_id == user_id)
            .order_by(Revision.revision_no.desc())
        ).all()
    )


def _diff_published(
    prev: Revision | None, new_sessions: list[dict]
) -> tuple[list, list]:
    """Diff the previously published set against the recomputed sessions.

    Returns ``(retractions, additions)`` and rewrites each newly published
    session's effective ``session_key`` in place.

    Migration rules for a pre-upgrade database:

    * a legacy published key whose stored ids cannot be split back to the
      recorded event count is flagged for review and retracted **once per
      key**; sessions that would have used that key under the old rule and
      are still eligible are re-published under new-style keys;
    * unchanged sessions keep their original (old-style) key so they are not
      retracted/re-published just because of the key-scheme change;
    * everything else gets the collision-free new-style key.
    """
    new_published = [s for s in new_sessions if s["status"] == "published"]

    if prev is None:
        return [], list(new_published)

    old_snaps = [s for s in prev.sessions if s.status == "published"]
    retractions: list[SessionSnapshot] = []
    additions: list[dict] = []
    retracted_keys: set[str] = set()
    consumed: set[int] = set()

    def _flagged(snap: SessionSnapshot) -> bool:
        _ids, need_review = decode_event_ids(
            snap.event_ids, snap.event_count, snap.id_encoding
        )
        return need_review

    # Pass 1: ambiguous legacy keys are always retracted, each only once.
    for snap in old_snaps:
        if _flagged(snap) and snap.session_key not in retracted_keys:
            retracted_keys.add(snap.session_key)
            retractions.append(snap)

    # Pass 2: intact old keys — keep them when the identical session still
    # publishes, otherwise retract (again, once per key).
    for snap in old_snaps:
        if _flagged(snap) or snap.session_key in retracted_keys:
            continue
        old_ids, _review = decode_event_ids(
            snap.event_ids, snap.event_count, snap.id_encoding
        )
        match_idx = next(
            (
                i
                for i, cand in enumerate(new_published)
                if i not in consumed
                and cand["event_ids"] == old_ids
                and snap.session_key
                in (cand["session_key"], cand["legacy_key"])
            ),
            None,
        )
        if match_idx is not None:
            # Identical content survives the upgrade: keep its original key.
            consumed.add(match_idx)
            new_published[match_idx]["session_key"] = snap.session_key
        elif snap.session_key not in retracted_keys:
            retracted_keys.add(snap.session_key)
            retractions.append(snap)

    # Pass 3: every published session not bound to an intact old row is new
    # (including sessions that used an ambiguous legacy key) and keeps the
    # new-style collision-free key it was computed with.
    for i, sess in enumerate(new_published):
        if i not in consumed:
            additions.append(sess)

    retractions.sort(key=lambda s: s.session_key)
    return retractions, additions


def _persist_revision(
    db: Session,
    user_id: str,
    reason: str,
    detail: str,
    watermark: datetime,
    idle_gap_seconds: int,
    new_sessions: list[dict],
    retractions: list,
    additions: list,
) -> Revision:
    next_no = (
        db.scalar(
            select(func.coalesce(func.max(Revision.revision_no), 0)).where(
                Revision.user_id == user_id
            )
        )
        or 0
    ) + 1

    rev = Revision(
        user_id=user_id,
        revision_no=next_no,
        reason=reason,
        detail=detail,
        watermark=watermark,
        idle_gap_seconds=idle_gap_seconds,
    )
    db.add(rev)
    db.flush()

    for sess in new_sessions:
        db.add(
            SessionSnapshot(
                revision_id=rev.id,
                session_key=sess["session_key"],
                start_time=sess["start"],
                end_time=sess["end"],
                event_count=len(sess["event_ids"]),
                event_ids=encode_event_ids(sess["event_ids"]),
                id_encoding=ENCODING_JSON,
                status=sess["status"],
            )
        )

    for snap in retractions:
        db.add(
            RevisionChange(
                revision_id=rev.id,
                change_type="retract",
                session_key=snap.session_key,
                start_time=snap.start_time,
                end_time=snap.end_time,
                event_count=snap.event_count,
                event_ids=snap.event_ids,
                id_encoding=snap.id_encoding,
            )
        )
    for sess in additions:
        db.add(
            RevisionChange(
                revision_id=rev.id,
                change_type="add",
                session_key=sess["session_key"],
                start_time=sess["start"],
                end_time=sess["end"],
                event_count=len(sess["event_ids"]),
                event_ids=encode_event_ids(sess["event_ids"]),
                id_encoding=ENCODING_JSON,
            )
        )

    db.flush()
    if FAIL_BEFORE_COMMIT:
        raise RuntimeError("simulated persistence failure (FAIL_BEFORE_COMMIT)")
    return rev


def _state_dicts(
    db: Session, user_id: str, idle_gap_seconds: int, watermark: datetime
) -> tuple[list, list, int | None]:
    """Current published/pending sessions and the latest revision number.

    When no revision exists yet (nothing was ever published/retracted), the
    state is derived on the fly from the raw events so the API still reports
    pending sessions before the first publish.
    """
    rev = latest_revision(db, user_id)
    if rev is not None:
        published = [
            _snap_to_dict(s) for s in rev.sessions if s.status == "published"
        ]
        pending = [
            _snap_to_dict(s) for s in rev.sessions if s.status == "pending"
        ]
        return published, pending, rev.revision_no

    sessions = classify_sessions(
        sessionize(_load_user_events(db, user_id), idle_gap_seconds),
        watermark,
        idle_gap_seconds,
    )
    return (
        [s for s in sessions if s["status"] == "published"],
        [s for s in sessions if s["status"] == "pending"],
        None,
    )


def _snap_to_dict(snap: SessionSnapshot) -> dict:
    ids, need_review = decode_event_ids(
        snap.event_ids, snap.event_count, snap.id_encoding
    )
    return {
        "session_key": snap.session_key,
        "start": snap.start_time,
        "end": snap.end_time,
        "event_ids": ids,
        "ids_need_review": need_review,
        "status": snap.status,
    }


def import_events(
    db: Session, user_id: str, events: list[dict]
) -> dict:
    """Idempotent batch import; corrective revision on late-event impact.

    ``events`` is a list of ``{"event_id", "event_time"}`` dicts whose
    times are timezone-aware datetimes (normalized to UTC here). A revision
    is only created when the *published* set changes; the first import while
    everything is still pending creates no revision.
    """
    # In-request duplicate detection.
    seen: set[str] = set()
    duplicates_in_batch: set[str] = set()
    for ev in events:
        if ev["event_id"] in seen:
            duplicates_in_batch.add(ev["event_id"])
        seen.add(ev["event_id"])
    if duplicates_in_batch:
        raise ApiError(
            422,
            "duplicate event_id within import batch: "
            + ", ".join(sorted(duplicates_in_batch)),
        )

    cfg = get_or_create_user(db, user_id)
    existing_rows = db.scalars(
        select(Event).where(
            Event.user_id == user_id,
            Event.event_id.in_([e["event_id"] for e in events]),
        )
    ).all()
    existing = {row.event_id: row for row in existing_rows}

    conflicts = [
        eid
        for eid, row in existing.items()
        if any(
            e["event_id"] == eid
            and to_utc(e["event_time"]) != row.event_time
            for e in events
        )
    ]
    if conflicts:
        raise ApiError(
            409,
            "event_id already imported with a different event_time: "
            + ", ".join(sorted(conflicts)),
        )

    inserted = 0
    duplicate = 0
    for ev in events:
        eid = ev["event_id"]
        if eid in existing:
            duplicate += 1
            continue
        db.add(
            Event(
                user_id=user_id,
                event_id=eid,
                event_time=to_utc(ev["event_time"]),
            )
        )
        inserted += 1

    if inserted == 0:
        # Fully idempotent replay: nothing changes, no revision is created.
        db.flush()
        published, pending, _rev_no = _state_dicts(
            db, user_id, cfg.idle_gap_seconds, cfg.watermark
        )
        return {
            "summary": {
                "user_id": user_id,
                "received": len(events),
                "inserted": 0,
                "duplicate": duplicate,
                "conflicts": [],
            },
            "new_revision_no": None,
            "retractions": [],
            "additions": [],
            "published_dicts": published,
            "pending_dicts": pending,
        }

    try:
        db.flush()
    except IntegrityError as exc:  # defensive: concurrent same-key import
        db.rollback()
        raise ApiError(409, "concurrent conflicting import") from exc

    all_events = _load_user_events(db, user_id)
    new_sessions = classify_sessions(
        sessionize(all_events, cfg.idle_gap_seconds),
        cfg.watermark,
        cfg.idle_gap_seconds,
    )

    prev = latest_revision(db, user_id)
    retractions, additions = _diff_published(prev, new_sessions)

    new_rev_no: int | None = None
    if retractions or additions:
        detail = (
            f"import of {inserted} new event(s); "
            f"{len(retractions)} retracted, {len(additions)} newly published"
        )
        rev = _persist_revision(
            db,
            user_id,
            reason="late_import",
            detail=detail,
            watermark=cfg.watermark,
            idle_gap_seconds=cfg.idle_gap_seconds,
            new_sessions=new_sessions,
            retractions=retractions,
            additions=additions,
        )
        new_rev_no = rev.revision_no

    published = [s for s in new_sessions if s["status"] == "published"]
    pending = [s for s in new_sessions if s["status"] == "pending"]
    return {
        "summary": {
            "user_id": user_id,
            "received": len(events),
            "inserted": inserted,
            "duplicate": duplicate,
            "conflicts": [],
        },
        "new_revision_no": new_rev_no,
        "retractions": retractions,
        "additions": additions,
        "published_dicts": published,
        "pending_dicts": pending,
    }


def advance_watermark(
    db: Session, user_id: str, new_watermark: datetime
) -> dict:
    """Move the watermark forward and publish newly eligible sessions.

    The watermark is monotonic: an equal or earlier value is rejected. No
    revision is created when the eligible published set does not change.
    """
    cfg = get_or_create_user(db, user_id)
    new_watermark = to_utc(new_watermark)
    if new_watermark < cfg.watermark:
        raise ApiError(
            409,
            "watermark can only move forward: "
            f"current={cfg.watermark.isoformat()} requested={new_watermark.isoformat()}",
        )
    if new_watermark == cfg.watermark:
        published, pending, rev_no = _state_dicts(
            db, user_id, cfg.idle_gap_seconds, cfg.watermark
        )
        return {
            "watermark": cfg.watermark,
            "new_revision_no": rev_no,
            "retractions": [],
            "additions": [],
            "published_dicts": published,
            "pending_dicts": pending,
        }

    cfg.watermark = new_watermark
    db.flush()

    all_events = _load_user_events(db, user_id)
    new_sessions = classify_sessions(
        sessionize(all_events, cfg.idle_gap_seconds),
        cfg.watermark,
        cfg.idle_gap_seconds,
    )

    prev = latest_revision(db, user_id)
    retractions, additions = _diff_published(prev, new_sessions)

    new_rev_no: int | None = None
    if retractions or additions:
        detail = (
            f"watermark advanced to {new_watermark.isoformat()}; "
            f"{len(additions)} session(s) published"
        )
        rev = _persist_revision(
            db,
            user_id,
            reason="publish",
            detail=detail,
            watermark=cfg.watermark,
            idle_gap_seconds=cfg.idle_gap_seconds,
            new_sessions=new_sessions,
            retractions=retractions,
            additions=additions,
        )
        new_rev_no = rev.revision_no

    published = [s for s in new_sessions if s["status"] == "published"]
    pending = [s for s in new_sessions if s["status"] == "pending"]
    return {
        "watermark": cfg.watermark,
        "new_revision_no": new_rev_no,
        "retractions": retractions,
        "additions": additions,
        "published_dicts": published,
        "pending_dicts": pending,
    }
