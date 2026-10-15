"""I11: never destroy unknown constraints/auditing on package-owned tables.

These tests fail if object ownership is inferred only from its name prefix.
They exercise public upgrades/rollback and real SQLite enforcement, including
an object introduced after preliminary inspection.
"""
from pathlib import Path
import sqlite3
from uuid import uuid4

import pytest
import cross_tool_awareness.migrations as migration

FIXTURES = Path(__file__).parent / 'fixtures'


def seed(tmp_path, version):
    path = tmp_path / 'bridge.db'
    schema = ({2: FIXTURES / 'v0_3_1_schema.sql',
               3: FIXTURES / 'v0_4_0_schema.sql'}.get(version)
              or Path(migration.__file__).with_name('schema.sql'))
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE messages (id TEXT PRIMARY KEY, payload TEXT NOT NULL)')
        db.executescript(schema.read_text())
        db.execute('INSERT INTO awareness_schema_version VALUES (?, 1, ?)',
                   (version, {2: '0.3.1', 3: '0.4.0', 4: '0.5.1'}[version]))
        db.execute('CREATE TABLE external_audit (event TEXT)')
    return path


def delivery(db, sender):
    mid = str(uuid4())
    db.execute('INSERT INTO messages VALUES (?, ?)', (mid, mid))
    db.execute("INSERT INTO targeted_deliveries (message_id,target_session_uuid,"
               "target_session_incarnation_id,sender_session_uuid,sender_session_incarnation_id,"
               "delivery_state,created_at,next_attempt_at,expires_at) "
               "VALUES (?, 'target', 'incarnation', ?, 'sender-incarnation', 'pending', 1, 1, 999)",
               (mid, sender))
    return mid


def add_foreign_object(db, kind):
    if kind == 'index':
        db.execute('CREATE UNIQUE INDEX user_unique_sender ON targeted_deliveries(sender_session_uuid)')
    else:
        db.execute('CREATE TRIGGER user_delivery_audit AFTER INSERT ON targeted_deliveries '
                   'BEGIN INSERT INTO external_audit VALUES (NEW.message_id); END')


def assert_guard_works(db, kind):
    sender = str(uuid4())
    mid = delivery(db, sender)
    if kind == 'index':
        with pytest.raises(sqlite3.IntegrityError):
            delivery(db, sender)
    else:
        assert db.execute('SELECT event FROM external_audit WHERE event=?', (mid,)).fetchall() == [(mid,)]


@pytest.mark.parametrize('version', [2, 3])
@pytest.mark.parametrize('kind', ['index', 'trigger'])
def test_upgrade_refuses_unknown_objects_without_losing_enforcement(tmp_path, version, kind):
    path = seed(tmp_path, version)
    with sqlite3.connect(path) as db:
        add_foreign_object(db, kind)
        assert_guard_works(db, kind)
    result = migration.apply(path, backup_dir=tmp_path / 'backups', now=100)
    assert result.status in ('refused', 'unknown_managed_state'), result
    assert result.backup is None and not (tmp_path / 'backups').exists()
    with sqlite3.connect(path) as db:
        assert db.execute('SELECT MAX(version) FROM awareness_schema_version').fetchone()[0] == version
        assert_guard_works(db, kind)


@pytest.mark.parametrize('version', [2, 3])
@pytest.mark.parametrize('kind', ['index', 'trigger'])
def test_object_added_before_write_lock_is_refused(tmp_path, monkeypatch, version, kind):
    path = seed(tmp_path, version)
    original = migration._validate_schema_exact
    inspected = False

    def add_after_outer_check(db, *, version):
        nonlocal inspected
        result = original(db, version=version)
        if not inspected:
            inspected = True
            with sqlite3.connect(path) as writer:
                add_foreign_object(writer, kind)
        return result

    monkeypatch.setattr(migration, '_validate_schema_exact', add_after_outer_check)
    result = migration.apply(path, backup_dir=tmp_path / 'backups', now=100)
    assert result.status == 'refused', result
    assert result.backup is None and not (tmp_path / 'backups').exists()
    with sqlite3.connect(path) as db:
        assert db.execute('SELECT MAX(version) FROM awareness_schema_version').fetchone()[0] == version
        assert_guard_works(db, kind)


@pytest.mark.parametrize('version', [2, 3, 4])
@pytest.mark.parametrize('kind', ['index', 'trigger'])
def test_rollback_refuses_unknown_objects_without_destroying_them(tmp_path, version, kind):
    path = seed(tmp_path, version)
    with sqlite3.connect(path) as db:
        add_foreign_object(db, kind)
    result = migration.rollback(path, backup_dir=tmp_path / 'backups', now=100)
    assert result.status == 'refused', result
    assert result.backup is None and not (tmp_path / 'backups').exists()
    with sqlite3.connect(path) as db:
        assert_guard_works(db, kind)


@pytest.mark.parametrize('version', [2, 3, 4])
def test_unrelated_bridge_objects_remain_allowed_and_functional(tmp_path, version):
    path = seed(tmp_path, version)
    with sqlite3.connect(path) as db:
        db.execute('CREATE UNIQUE INDEX user_bridge_payload ON messages(payload)')
        db.execute('CREATE TRIGGER user_bridge_audit AFTER INSERT ON messages '
                   'BEGIN INSERT INTO external_audit VALUES (NEW.id); END')
    if version < 4:
        result = migration.apply(path, backup_dir=tmp_path / 'backups', now=100)
        assert result.status == 'applied', result
    else:
        result = migration.rollback(path, backup_dir=tmp_path / 'backups', now=100)
        assert result.status == 'rolled_back', result
    with sqlite3.connect(path) as db:
        db.execute("INSERT INTO messages VALUES ('bridge-1', 'same-payload')")
        with pytest.raises(sqlite3.IntegrityError):
            db.execute("INSERT INTO messages VALUES ('bridge-2', 'same-payload')")
        assert db.execute("SELECT event FROM external_audit WHERE event='bridge-1'").fetchall() == [('bridge-1',)]
