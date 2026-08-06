"""
Unit tests for mcp-server/server.py helper functions.
Tests: parse_databricks_result, airflow_api, databricks_sql,
       desk validation, and VALID_DESKS constant.
All external calls (requests, boto3) are mocked.
"""
import pytest
import sys
import os
from unittest.mock import patch, MagicMock

# Add mcp-server to path so we can import server module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "mcp-server"))

import server as srv


# ── parse_databricks_result ──────────────────────────────────────────────

class TestParseDatabricksResult:
    """Test the Databricks response parser."""

    def test_valid_response(self):
        resp = {
            "manifest": {
                "schema": {
                    "columns": [
                        {"name": "desk"},
                        {"name": "var_99_usd"},
                    ]
                }
            },
            "result": {
                "data_array": [
                    ["FX Desk", "1234567.89"],
                    ["Equity Desk", "987654.32"],
                ]
            },
        }
        rows = srv.parse_databricks_result(resp)
        assert len(rows) == 2
        assert rows[0] == {"desk": "FX Desk", "var_99_usd": "1234567.89"}
        assert rows[1] == {"desk": "Equity Desk", "var_99_usd": "987654.32"}

    def test_error_passthrough(self):
        resp = {"error": "Query failed: table not found"}
        rows = srv.parse_databricks_result(resp)
        assert len(rows) == 1
        assert "error" in rows[0]

    def test_empty_result(self):
        resp = {
            "manifest": {
                "schema": {
                    "columns": [{"name": "desk"}, {"name": "count"}]
                }
            },
            "result": {"data_array": []},
        }
        rows = srv.parse_databricks_result(resp)
        assert rows == []

    def test_malformed_response(self):
        resp = {"unexpected_key": "garbage"}
        rows = srv.parse_databricks_result(resp)
        assert len(rows) == 1
        assert "error" in rows[0]

    def test_missing_result_key(self):
        resp = {
            "manifest": {
                "schema": {"columns": [{"name": "col1"}]}
            },
            # no "result" key
        }
        rows = srv.parse_databricks_result(resp)
        assert rows == []


# ── airflow_api ──────────────────────────────────────────────────────────

class TestAirflowApi:
    """Test the Airflow REST API wrapper."""

    @patch("server.requests.request")
    def test_get_request(self, mock_req):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"dag_runs": []}
        mock_resp.raise_for_status = MagicMock()
        mock_req.return_value = mock_resp

        result = srv.airflow_api("dags/marketrisk_pipeline/dagRuns")
        assert result == {"dag_runs": []}
        mock_req.assert_called_once()
        assert mock_req.call_args[0][0] == "GET"

    @patch("server.requests.request")
    def test_post_request(self, mock_req):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"dag_run_id": "manual__2026"}
        mock_resp.raise_for_status = MagicMock()
        mock_req.return_value = mock_resp

        result = srv.airflow_api("dags/test/dagRuns", method="POST", data={"conf": {}})
        assert "dag_run_id" in result

    @patch("server.requests.request")
    def test_http_error_raises(self, mock_req):
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = Exception("401 Unauthorized")
        mock_req.return_value = mock_resp

        with pytest.raises(Exception, match="401"):
            srv.airflow_api("dags/test")


# ── VALID_DESKS ──────────────────────────────────────────────────────────

class TestValidDesks:
    """Test the VALID_DESKS constant."""

    def test_four_desks(self):
        assert len(srv.VALID_DESKS) == 4

    def test_uppercase(self):
        for desk in srv.VALID_DESKS:
            assert desk == desk.upper(), f"{desk} is not uppercase"

    def test_expected_desks(self):
        expected = {"FX DESK", "EQUITY DESK", "RATES DESK", "CREDIT DESK"}
        assert srv.VALID_DESKS == expected


# ── databricks_sql ───────────────────────────────────────────────────────

class TestDatabricksSql:
    """Test the Databricks SQL executor with mocked HTTP."""

    @patch("server.requests.post")
    def test_immediate_success(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "status": {"state": "SUCCEEDED"},
            "statement_id": "abc",
            "manifest": {"schema": {"columns": [{"name": "cnt"}]}},
            "result": {"data_array": [["42"]]},
        }
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp

        result = srv.databricks_sql("SELECT 1")
        assert result["status"]["state"] == "SUCCEEDED"

    @patch("server.time.sleep")
    @patch("server.requests.get")
    @patch("server.requests.post")
    def test_polls_pending_then_succeeds(self, mock_post, mock_get, mock_sleep):
        # Initial POST returns PENDING
        post_resp = MagicMock()
        post_resp.json.return_value = {
            "status": {"state": "PENDING"},
            "statement_id": "abc",
        }
        post_resp.raise_for_status = MagicMock()
        mock_post.return_value = post_resp

        # First poll: RUNNING, second poll: SUCCEEDED
        poll1 = MagicMock()
        poll1.json.return_value = {"status": {"state": "RUNNING"}, "statement_id": "abc"}
        poll1.raise_for_status = MagicMock()

        poll2 = MagicMock()
        poll2.json.return_value = {
            "status": {"state": "SUCCEEDED"},
            "statement_id": "abc",
            "manifest": {"schema": {"columns": [{"name": "cnt"}]}},
            "result": {"data_array": [["1"]]},
        }
        poll2.raise_for_status = MagicMock()

        mock_get.side_effect = [poll1, poll2]

        result = srv.databricks_sql("SELECT 1")
        assert result["status"]["state"] == "SUCCEEDED"
        assert mock_sleep.call_count == 2

    @patch("server.requests.post")
    def test_failed_query(self, mock_post):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "status": {
                "state": "FAILED",
                "error": {"message": "Table not found"},
            },
            "statement_id": "abc",
        }
        mock_resp.raise_for_status = MagicMock()
        mock_post.return_value = mock_resp

        result = srv.databricks_sql("SELECT * FROM nonexistent")
        assert "error" in result
        assert "Table not found" in result["error"]
