"""
Unit tests for src/ingestion/discovery.py.

dataset_discovery validates that every required source CSV is present
and non-empty before ingestion begins. Tests use tmp_path exclusively
to avoid any dependency on /opt/airflow/data/ or the project's own
data/ directory.
"""

import pytest

from src.ingestion.discovery import REQUIRED_FILES, dataset_discovery


def _write_all_files(source_dir, content="id\n1\n"):
    """Write all required CSV files with minimal valid content."""
    for name in REQUIRED_FILES:
        (source_dir / name).write_text(content)


def test_dataset_discovery_returns_all_paths_when_all_files_present(tmp_path):
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    _write_all_files(source_dir)

    result = dataset_discovery(str(source_dir))

    assert len(result) == len(REQUIRED_FILES)
    for path_str in result:
        assert str(tmp_path) in path_str, (
            "returned path must be within tmp_path, not a default system path"
        )


def test_dataset_discovery_raises_on_missing_file(tmp_path):
    """Every required CSV must be present; a single missing file is
    enough to fail the whole discovery check."""
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    _write_all_files(source_dir)
    (source_dir / "orders.csv").unlink()

    with pytest.raises(FileNotFoundError, match="orders.csv"):
        dataset_discovery(str(source_dir))


def test_dataset_discovery_raises_on_empty_file(tmp_path):
    """A zero-byte file is not a valid source; it must be rejected
    before ingestion attempts to read it."""
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    _write_all_files(source_dir)
    (source_dir / "customers.csv").write_text("")  # zero bytes

    with pytest.raises(ValueError, match="customers.csv"):
        dataset_discovery(str(source_dir))
