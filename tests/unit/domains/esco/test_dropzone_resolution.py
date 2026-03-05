"""Unit tests for ESCO dropzone file resolution logic."""

from __future__ import annotations

from typing import TYPE_CHECKING

from skill_radar.cli.esco import (
    _format_missing_file_error,
    _get_dropzone_candidates,
    resolve_dropzone_file,
)

if TYPE_CHECKING:
    from pathlib import Path


class TestGetDropzoneCandidates:
    """Tests for _get_dropzone_candidates helper."""

    def test_returns_correct_candidate_paths(self, tmp_path: Path) -> None:
        """Should return 4 candidate paths in the correct order."""
        candidates = _get_dropzone_candidates(tmp_path, "v1.2.1", "fr")

        assert len(candidates) == 4
        assert candidates[0] == tmp_path / "esco" / "esco.zip"
        assert candidates[1] == tmp_path / "esco" / "esco_fr.zip"
        assert candidates[2] == tmp_path / "esco" / "v1.2.1" / "esco.zip"
        assert candidates[3] == tmp_path / "esco" / "v1.2.1" / "fr" / "esco.zip"

    def test_uses_provided_lang_in_filename(self, tmp_path: Path) -> None:
        """Should use the provided language code in the esco_{lang}.zip pattern."""
        candidates = _get_dropzone_candidates(tmp_path, "v1.0.0", "en")

        assert candidates[1] == tmp_path / "esco" / "esco_en.zip"

    def test_uses_provided_version_in_path(self, tmp_path: Path) -> None:
        """Should use the provided version in the version-specific paths."""
        candidates = _get_dropzone_candidates(tmp_path, "v2.0.0", "de")

        assert candidates[2] == tmp_path / "esco" / "v2.0.0" / "esco.zip"
        assert candidates[3] == tmp_path / "esco" / "v2.0.0" / "de" / "esco.zip"


class TestResolveDropzoneFile:
    """Tests for resolve_dropzone_file function."""

    def test_returns_none_when_no_files_exist(self, tmp_path: Path) -> None:
        """Should return None when dropzone is empty."""
        result = resolve_dropzone_file(tmp_path, "v1.2.1", "fr")
        assert result is None

    def test_returns_none_when_only_directory_exists(self, tmp_path: Path) -> None:
        """Should return None when only directories exist (no files)."""
        (tmp_path / "esco").mkdir(parents=True)
        result = resolve_dropzone_file(tmp_path, "v1.2.1", "fr")
        assert result is None

    def test_finds_file_at_first_candidate_esco_zip(self, tmp_path: Path) -> None:
        """Should find esco/esco.zip (first candidate)."""
        esco_dir = tmp_path / "esco"
        esco_dir.mkdir(parents=True)
        expected_file = esco_dir / "esco.zip"
        expected_file.write_bytes(b"dummy zip content")

        result = resolve_dropzone_file(tmp_path, "v1.2.1", "fr")

        assert result == expected_file

    def test_finds_file_at_second_candidate_esco_lang_zip(self, tmp_path: Path) -> None:
        """Should find esco/esco_{lang}.zip (second candidate)."""
        esco_dir = tmp_path / "esco"
        esco_dir.mkdir(parents=True)
        expected_file = esco_dir / "esco_fr.zip"
        expected_file.write_bytes(b"dummy zip content")

        result = resolve_dropzone_file(tmp_path, "v1.2.1", "fr")

        assert result == expected_file

    def test_finds_file_at_third_candidate_version_dir(self, tmp_path: Path) -> None:
        """Should find esco/{version}/esco.zip (third candidate)."""
        version_dir = tmp_path / "esco" / "v1.2.1"
        version_dir.mkdir(parents=True)
        expected_file = version_dir / "esco.zip"
        expected_file.write_bytes(b"dummy zip content")

        result = resolve_dropzone_file(tmp_path, "v1.2.1", "fr")

        assert result == expected_file

    def test_finds_file_at_fourth_candidate_version_lang_dir(self, tmp_path: Path) -> None:
        """Should find esco/{version}/{lang}/esco.zip (fourth candidate)."""
        lang_dir = tmp_path / "esco" / "v1.2.1" / "fr"
        lang_dir.mkdir(parents=True)
        expected_file = lang_dir / "esco.zip"
        expected_file.write_bytes(b"dummy zip content")

        result = resolve_dropzone_file(tmp_path, "v1.2.1", "fr")

        assert result == expected_file

    def test_prefers_first_candidate_when_multiple_exist(self, tmp_path: Path) -> None:
        """Should return first candidate when multiple files exist."""
        esco_dir = tmp_path / "esco"
        esco_dir.mkdir(parents=True)

        # Create multiple candidate files
        first_candidate = esco_dir / "esco.zip"
        first_candidate.write_bytes(b"first")

        second_candidate = esco_dir / "esco_fr.zip"
        second_candidate.write_bytes(b"second")

        result = resolve_dropzone_file(tmp_path, "v1.2.1", "fr")

        assert result == first_candidate

    def test_skips_to_next_candidate_when_first_missing(self, tmp_path: Path) -> None:
        """Should skip to second candidate when first is missing."""
        esco_dir = tmp_path / "esco"
        esco_dir.mkdir(parents=True)

        # Only create second candidate (skip first)
        second_candidate = esco_dir / "esco_fr.zip"
        second_candidate.write_bytes(b"second")

        result = resolve_dropzone_file(tmp_path, "v1.2.1", "fr")

        assert result == second_candidate

    def test_ignores_directories_with_matching_names(self, tmp_path: Path) -> None:
        """Should not match directories, only files."""
        esco_dir = tmp_path / "esco"
        esco_dir.mkdir(parents=True)

        # Create a directory named esco.zip (should be ignored)
        fake_dir = esco_dir / "esco.zip"
        fake_dir.mkdir()

        # Create actual file at second candidate
        second_candidate = esco_dir / "esco_fr.zip"
        second_candidate.write_bytes(b"actual file")

        result = resolve_dropzone_file(tmp_path, "v1.2.1", "fr")

        assert result == second_candidate


class TestFormatMissingFileError:
    """Tests for _format_missing_file_error helper."""

    def test_contains_helpful_guidance(self, tmp_path: Path) -> None:
        """Should contain actionable guidance for the user."""
        error_msg = _format_missing_file_error(tmp_path, "v1.2.1", "fr")

        # Check key elements are present
        assert "No ESCO artifact found" in error_msg
        assert "manual download" in error_msg.lower()
        assert "./data/incoming" in error_msg
        assert "make upload-esco" in error_msg
        assert "VERSION=v1.2.1" in error_msg
        assert "LANG=fr" in error_msg

    def test_lists_all_checked_paths(self, tmp_path: Path) -> None:
        """Should list all candidate paths that were checked."""
        error_msg = _format_missing_file_error(tmp_path, "v1.2.1", "fr")

        # All candidate patterns should appear
        assert "esco/esco.zip" in error_msg
        assert "esco/esco_fr.zip" in error_msg
        assert "esco/v1.2.1/esco.zip" in error_msg
        assert "esco/v1.2.1/fr/esco.zip" in error_msg

    def test_includes_docker_command_example(self, tmp_path: Path) -> None:
        """Should include example docker compose exec command."""
        error_msg = _format_missing_file_error(tmp_path, "v1.2.1", "fr")

        assert "docker compose exec" in error_msg
        assert "skill-radar esco upload" in error_msg

    def test_references_container_path(self, tmp_path: Path) -> None:
        """Should explain the container sees /opt/skillradar/incoming."""
        error_msg = _format_missing_file_error(tmp_path, "v1.2.1", "fr")

        assert "/opt/skillradar/incoming" in error_msg
