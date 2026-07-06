# =============================================================
# UNIT TESTS — migration runner pure logic (Priority 2 item 1)
# =============================================================
# The version-discovery and pending-diff logic decides WHAT runs
# against the database, so it is tested with no database at all.
# =============================================================

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "db"))

import migrate  # noqa: E402


def _mk(tmp_path, names):
    for n in names:
        (tmp_path / n).write_text("SELECT 1;")
    return str(tmp_path)


class TestDiscover:
    def test_orders_by_version(self, tmp_path):
        d = _mk(tmp_path, ["V002__b.sql", "V001__a.sql", "V010__c.sql"])
        versions = [v for v, _, _ in migrate.discover_migrations(d)]
        assert versions == [1, 2, 10]

    def test_ignores_non_migration_files(self, tmp_path):
        d = _mk(tmp_path, ["V001__a.sql", "README.md", "notes.txt", "V1__bad.SQL"])
        names = [n for _, n, _ in migrate.discover_migrations(d)]
        assert names == ["V001__a.sql"]

    def test_duplicate_version_raises(self, tmp_path):
        d = _mk(tmp_path, ["V001__a.sql", "V001__b.sql"])
        with pytest.raises(ValueError):
            migrate.discover_migrations(d)


class TestPending:
    def test_excludes_applied(self):
        allm = [(1, "a", "/a"), (2, "b", "/b"), (3, "c", "/c")]
        assert migrate.pending(allm, {1, 2}) == [(3, "c", "/c")]

    def test_empty_when_all_applied(self):
        allm = [(1, "a", "/a"), (2, "b", "/b")]
        assert migrate.pending(allm, {1, 2}) == []

    def test_all_pending_on_fresh_db(self):
        allm = [(1, "a", "/a"), (2, "b", "/b")]
        assert migrate.pending(allm, set()) == allm


class TestRealMigrationsDir:
    """The actual repo migrations must be discoverable and well-formed."""
    def test_repo_migrations_discover_cleanly(self):
        found = migrate.discover_migrations()
        versions = [v for v, _, _ in found]
        assert versions == sorted(versions)
        assert len(versions) == len(set(versions))  # no dupes
        assert 1 in versions  # V001 exists
