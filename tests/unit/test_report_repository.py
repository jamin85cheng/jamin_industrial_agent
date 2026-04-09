from pathlib import Path

from src.api.repositories.report_repository import ReportRepository


def _make_repo(case_dir: Path) -> ReportRepository:
    return ReportRepository(
        {
            "sqlite": {"path": str(case_dir / "metadata.db")},
            "postgres": {"enabled": False},
        }
    )


def test_report_repository_persists_report_metadata(tmp_path):
    case_dir = tmp_path / "report_repository"
    case_dir.mkdir(parents=True, exist_ok=True)
    repo = _make_repo(case_dir)
    repo.init_schema()

    report_file = case_dir / "diagnosis-report.html"
    report_file.write_text("<html><body>report</body></html>", encoding="utf-8")

    created = repo.create_report(
        report_id="RPT_TEST_001",
        task_id="TASK_TEST_001",
        diagnosis_id="DIAG_TEST_001",
        alert_id="ALT_TEST_001",
        tenant_id="default",
        export_format="html",
        file_path=str(report_file),
        filename=report_file.name,
        media_type="text/html; charset=utf-8",
        created_by="operator",
        metadata={"entrypoint": "alert"},
    )

    assert created["report_id"] == "RPT_TEST_001"
    assert created["download_url"] == "/reports/RPT_TEST_001/download"
    assert created["file_size_bytes"] == report_file.stat().st_size

    loaded = repo.get_report("RPT_TEST_001", tenant_id="default")
    assert loaded is not None
    assert loaded["task_id"] == "TASK_TEST_001"
    assert loaded["metadata"]["entrypoint"] == "alert"

    reports = repo.list_reports(tenant_id="default", alert_id="ALT_TEST_001")
    assert len(reports) == 1
    assert reports[0]["filename"] == report_file.name
