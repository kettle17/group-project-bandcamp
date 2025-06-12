# pylint: skip-file
"""Test file for the load pipeline."""

import os
from unittest.mock import patch, MagicMock
import pytest
import psycopg2
import pandas as pd
from psycopg2.extensions import connection as psycopg2_connection
from load import (
    get_db_connection,
    upload_to_db,
    export_to_csv,
    run_load
)

pytest.skip(allow_module_level=True)


@pytest.fixture
def mock_env_vars():
    """Fixture providing mock environment variables for database connection."""
    return {
        "DB_HOST": "testlocalhost",
        "DB_NAME": "testdb",
        "DB_USER": "testuser",
        "DB_PASSWORD": "veryrealpassword"
    }


class TestGetDbConnection:
    """Test class for get_db_connection function."""

    @patch("load.psycopg2.connect")
    def test_returns_connection(self, mock_connect) -> None:
        """Test that get_db_connection returns a valid psycopg2 connection object."""
        mock_conn = mock_connect.return_value
        conn = get_db_connection()
        assert conn is mock_conn

    @patch("load.psycopg2.connect")
    @patch.dict("os.environ")
    def test_uses_env_variables(self, mock_connect, mock_env_vars) -> None:
        """Test that get_db_connection uses environment variables for credentials."""
        os.environ.update(mock_env_vars)

        get_db_connection()
        mock_connect.assert_called_once_with(
            host="testlocalhost",
            dbname="testdb",
            user="testuser",
            password="veryrealpassword"
        )

    @patch("load.psycopg2.connect")
    def test_calls_psycopg2_connect(self, mock_connect) -> None:
        """Test that get_db_connection calls psycopg2.connect method."""
        conn = get_db_connection()
        mock_connect.assert_called_once()


class TestUploadToDb:
    """Test class for upload_to_db function."""

    @patch("load.psycopg2.connect")
    def test_handles_invalid_data_type_df(self, mock_connect) -> None:
        """Test that upload_to_db raises TypeError for invalid DataFrame input."""
        mock_conn = mock_connect.return_value
        with pytest.raises(TypeError):
            upload_to_db(12345, mock_conn)

    def test_handles_invalid_data_type_connection(self) -> None:
        """Test that upload_to_db raises TypeError for invalid connection input."""
        df = pd.DataFrame({"test_df": [1, 2, 3]})
        with pytest.raises(TypeError):
            upload_to_db(df, 12345)

    @patch("load.psycopg2.connect")
    def test_handles_empty_dataframe(self, mock_connect) -> None:
        """Test that upload_to_db handles empty DataFrame gracefully."""
        mock_conn = mock_connect.return_value
        df = pd.DataFrame()

        upload_to_db(df, mock_conn)

        mock_conn.cursor().execute.assert_not_called()

    @patch("load.psycopg2.connect")
    def test_handles_database_error(self, mock_connect) -> None:
        """Test that upload_to_db raises DatabaseError when database insertion fails."""
        df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})

        mock_conn = MagicMock()
        mock_cursor = mock_conn.cursor.return_value
        mock_cursor.execute.side_effect = psycopg2.DatabaseError(
            "DB insertion failed")

        mock_connect.return_value = mock_conn

        with pytest.raises(psycopg2.DatabaseError):
            upload_to_db(df, mock_conn)


class TestExportToCsv:
    """Test class for export_to_csv function."""

    @patch("load.pd.DataFrame.to_csv")
    def test_handles_empty_dataframe(self, mock_to_csv) -> None:
        """Test that export_to_csv handles empty DataFrames without calling to_csv."""
        df = pd.DataFrame()

        export_to_csv(df)

        mock_to_csv.assert_not_called()

    @patch("load.pd.DataFrame.to_csv")
    def test_calls_to_csv_method(self, mock_to_csv) -> None:
        """Test that export_to_csv calls DataFrame.to_csv method."""
        df = pd.DataFrame({"col1": [1, 2, 3]})

        export_to_csv(df)

        mock_to_csv.assert_called_once_with("output.csv", index=False)

    @patch("load.pd.DataFrame.to_csv")
    def test_uses_custom_output_path(self, mock_to_csv) -> None:
        """Test that export_to_csv uses custom path when provided."""
        df = pd.DataFrame({"col1": [1, 2, 3]})

        export_to_csv(df, "custom.csv")

        mock_to_csv.assert_called_once_with("custom.csv", index=False)


class TestRunLoad:
    """Test class for run_load function."""

    @patch("load.os.path.exists", return_value=False)
    def test_handles_missing_file(self, mock_exists) -> None:
        """Test that run_load handles missing transformed_data.csv file gracefully."""
        with pytest.raises(FileNotFoundError):
            run_load(csv_path="a_missing_file.csv")

    @patch("load.get_db_connection")
    @patch("load.upload_to_db")
    def test_handles_empty_dataframe(self, mock_upload, mock_get_conn) -> None:
        """Test that run_load handles empty dataframe input appropriately."""
        df = pd.DataFrame()
        run_load(dataframe=df)
        mock_upload.assert_not_called()
        mock_get_conn.assert_not_called()

    def test_handles_both_input_types_invalid(self) -> None:
        """Test that run_load handles case where both dataframe and CSV path are invalid."""
        with pytest.raises(ValueError):
            run_load()

    @patch("load.get_db_connection")
    @patch("load.upload_to_db")
    @patch("load.pd.read_csv")
    @patch("load.os.path.exists", return_value=True)
    def test_executes_required_functions(self, mock_exists, mock_read_csv, mock_upload, mock_get_conn) -> None:
        """Test that run_load calls all required functions in correct sequence."""
        mock_df = pd.DataFrame({"a": [1, 2]})
        mock_read_csv.return_value = mock_df
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn

        run_load(csv_path="valid.csv")

        mock_exists.assert_called_once_with("valid.csv")
        mock_read_csv.assert_called_once_with("valid.csv")
        mock_get_conn.assert_called_once()
        mock_upload.assert_called_once_with(mock_df, mock_conn)