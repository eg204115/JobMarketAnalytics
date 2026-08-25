"""
Unit tests for the orchestration LOGIC only — the go/no-go gating and the
alerting decisions — with every Spark-dependent stage mocked. Bronze, Silver
and Gold correctness is Chapters 3-5's job and is already covered by their own
tests; re-testing it here would just make these tests slow and duplicate.

These cover the same three branches the master pipeline's If Condition has:
full success, every source failed, and partial failure.
"""

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from pipelines.local_orchestrator import LayerPaths, PipelineRunResult, run_pipeline
from utils.config_loader import ConfigError


def _fake_connector(name: str, countries=("us",)) -> MagicMock:
    """
    A connector stub with fetch() returning a two-record list. MagicMock's
    default auto-attribute would make source_config a Mock whose .get()
    returns a Mock, so the country loop needs a real dict here.
    """
    connector = MagicMock()
    connector.source_name = name
    connector.source_config = {"countries": list(countries)}
    connector.fetch.return_value = [MagicMock(), MagicMock()]
    return connector


def _fake_spark() -> MagicMock:
    spark = MagicMock()
    spark.read.format.return_value.load.return_value = MagicMock()
    return spark


# Applied to every test that reaches Silver/Gold. Patched as one stack because
# each of these would otherwise try to touch a real Delta table. Applied in
# reverse so the mock arguments arrive in the listed order, exactly as they
# would with the decorators stacked bottom-up on each test.
def _patch_stages(func):
    for decorator in reversed([
        patch("pipelines.local_orchestrator.send_teams_alert"),
        patch("pipelines.local_orchestrator.merge_bridge_table"),
        patch("pipelines.local_orchestrator.merge_fact_table"),
        patch(
            "pipelines.local_orchestrator.build_fact_and_bridge",
            return_value=(MagicMock(), MagicMock()),
        ),
        patch("pipelines.local_orchestrator.upsert_dim_company_scd2"),
        patch("pipelines.local_orchestrator.upsert_dim_location"),
        patch("pipelines.local_orchestrator.upsert_dim_skill"),
        patch("pipelines.local_orchestrator.DeltaTable"),
        patch("pipelines.local_orchestrator.run_silver_transformation"),
        patch("pipelines.local_orchestrator.run_bronze_ingestion"),
        patch("pipelines.local_orchestrator.write_postings_json"),
        patch("pipelines.local_orchestrator.build_enabled_connectors"),
        patch("pipelines.local_orchestrator.load_config"),
    ]):
        func = decorator(func)
    return func


@_patch_stages
def test_full_success_no_alert_sent(
    mock_load_config, mock_build_connectors, mock_write_json, mock_bronze,
    mock_silver, mock_delta, mock_dim_skill, mock_dim_location,
    mock_dim_company, mock_build_fact, mock_merge_fact, mock_merge_bridge,
    mock_alert,
):
    mock_build_connectors.return_value = [_fake_connector("adzuna")]

    result = run_pipeline(window_date=date(2026, 7, 15), spark=_fake_spark())

    assert result.is_full_success
    assert result.succeeded_sources == ["adzuna"]
    assert result.total_records == 2
    mock_alert.assert_not_called()


@patch("pipelines.local_orchestrator.send_teams_alert")
@patch("pipelines.local_orchestrator.build_enabled_connectors")
@patch("pipelines.local_orchestrator.load_config")
def test_all_sources_failing_halts_before_silver(
    mock_load_config, mock_build_connectors, mock_alert,
):
    failing = _fake_connector("adzuna")
    failing.fetch.side_effect = Exception("API down")
    mock_build_connectors.return_value = [failing]

    result = run_pipeline(window_date=date(2026, 7, 15), spark=MagicMock())

    assert not result.any_source_succeeded
    assert result.silver_ran is False
    assert result.gold_ran is False
    mock_alert.assert_called_once()
    assert "ALL sources failed" in mock_alert.call_args[0][1]


@_patch_stages
def test_partial_failure_still_runs_silver_and_gold_and_alerts(
    mock_load_config, mock_build_connectors, mock_write_json, mock_bronze,
    mock_silver, mock_delta, mock_dim_skill, mock_dim_location,
    mock_dim_company, mock_build_fact, mock_merge_fact, mock_merge_bridge,
    mock_alert,
):
    good = _fake_connector("adzuna")
    bad = _fake_connector("jooble")
    bad.fetch.side_effect = Exception("rate limited")
    mock_build_connectors.return_value = [good, bad]

    result = run_pipeline(window_date=date(2026, 7, 15), spark=_fake_spark())

    assert result.succeeded_sources == ["adzuna"]
    assert result.failed_sources == ["jooble"]
    assert result.silver_ran is True
    assert result.gold_ran is True
    # Partial data still loaded, so this is not a "full success" run even
    # though every shared stage completed — the distinction is the whole
    # reason the alert fires.
    assert not result.is_full_success
    mock_alert.assert_called_once()
    assert "Partial failure" in mock_alert.call_args[0][1]


@_patch_stages
def test_silver_failure_stops_before_gold_and_alerts(
    mock_load_config, mock_build_connectors, mock_write_json, mock_bronze,
    mock_silver, mock_delta, mock_dim_skill, mock_dim_location,
    mock_dim_company, mock_build_fact, mock_merge_fact, mock_merge_bridge,
    mock_alert,
):
    mock_build_connectors.return_value = [_fake_connector("adzuna")]
    mock_silver.side_effect = Exception("schema mismatch")

    result = run_pipeline(window_date=date(2026, 7, 15), spark=_fake_spark())

    assert result.silver_ran is False
    assert result.gold_ran is False
    assert "silver" in result.fatal_error
    mock_build_fact.assert_not_called()
    assert "Silver transformation failed" in mock_alert.call_args[0][1]


@_patch_stages
def test_gold_runs_dimensions_before_fact_build(
    mock_load_config, mock_build_connectors, mock_write_json, mock_bronze,
    mock_silver, mock_delta, mock_dim_skill, mock_dim_location,
    mock_dim_company, mock_build_fact, mock_merge_fact, mock_merge_bridge,
    mock_alert,
):
    """
    Ordering guard, not a formality: the fact build resolves surrogate keys by
    joining the dimensions, and building it against stale dimensions yields
    NULL foreign keys rather than an error.
    """
    mock_build_connectors.return_value = [_fake_connector("adzuna")]
    call_order = []
    mock_dim_skill.side_effect = lambda *a, **k: call_order.append("skill")
    mock_dim_location.side_effect = lambda *a, **k: call_order.append("location")
    mock_dim_company.side_effect = lambda *a, **k: call_order.append("company")
    mock_build_fact.side_effect = lambda *a, **k: (
        call_order.append("fact"), (MagicMock(), MagicMock())
    )[1]

    run_pipeline(window_date=date(2026, 7, 15), spark=_fake_spark())

    assert call_order == ["skill", "location", "company", "fact"]
    # The SCD2 effective dates must come from the window being processed, not
    # from today — otherwise a backfill stamps history with the wrong dates.
    assert mock_dim_company.call_args.kwargs["run_date"] == date(2026, 7, 15)


@patch("pipelines.local_orchestrator.send_teams_alert")
@patch("pipelines.local_orchestrator.build_enabled_connectors")
@patch("pipelines.local_orchestrator.load_config")
def test_no_enabled_sources_is_not_a_failure(
    mock_load_config, mock_build_connectors, mock_alert,
):
    """An all-disabled sources.yaml means "nothing to do", not "everything broke"."""
    mock_build_connectors.return_value = []

    result = run_pipeline(window_date=date(2026, 7, 15), spark=MagicMock())

    assert result.failed_sources == []
    assert result.fatal_error is None
    assert result.silver_ran is False
    mock_alert.assert_not_called()


@patch("pipelines.local_orchestrator.send_teams_alert")
@patch("pipelines.local_orchestrator.load_config", side_effect=ConfigError("no .env"))
def test_config_error_aborts_window_with_alert(mock_load_config, mock_alert):
    result = run_pipeline(window_date=date(2026, 7, 15), spark=MagicMock())

    assert result.fatal_error == "no .env"
    assert not result.any_source_succeeded
    assert "config error" in mock_alert.call_args[0][1]


def test_bare_result_is_not_a_success():
    """
    Guards is_full_success against the empty case: a result that never ran a
    stage has no failed sources either, so a naive "no failures" check would
    call a pipeline that did nothing a success.
    """
    assert not PipelineRunResult(window_date=date(2026, 7, 15)).is_full_success


@pytest.mark.parametrize(
    "prefix, expected_fact",
    [
        ("data/delta/", "data/delta/fact_job_postings"),
        ("Tables/", "Tables/fact_job_postings"),
    ],
)
def test_layer_paths_derive_from_prefix(prefix, expected_fact):
    """Local vs Fabric is one parameter, not nine paths kept in sync by hand."""
    paths = LayerPaths(prefix=prefix)

    assert paths.fact == expected_fact
    assert paths.silver == f"{prefix}silver_job_postings"
    assert paths.bridge == f"{prefix}bridge_job_skill"
