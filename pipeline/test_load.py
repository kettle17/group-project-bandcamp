
"""Test file for the load pipeline."""

from unittest.mock import patch, MagicMock
import pytest
import pandas as pd
from psycopg2.extras import RealDictCursor
from load import (
    get_db_connection,
    run_load
)


@pytest.mark.usefixtures("setup_test_env")
class TestGetDbConnection:
    """Test class for get_db_connection function."""

    @patch("load.psycopg2.connect")
    def test_returns_connection(self, mock_connect) -> None:
        """Test that get_db_connection returns a valid psycopg2 connection object."""
        mock_conn = mock_connect.return_value
        config = {
            "DB_HOST": "localhost",
            "DB_PORT": "5432",
            "DB_USER": "test_user",
            "DB_PASSWORD": "test_pass",
            "DB_NAME": "test_db"
        }
        conn = get_db_connection(config)
        assert conn is mock_conn

    @patch("load.psycopg2.connect")
    def test_calls_psycopg2_connect(self, mock_connect) -> None:
        """Test that get_db_connection calls psycopg2.connect method."""
        config = {
            "DB_HOST": "localhost",
            "DB_PORT": "5432",
            "DB_USER": "test_user",
            "DB_PASSWORD": "test_pass",
            "DB_NAME": "test_db"
        }
        get_db_connection(config)
        mock_connect.assert_called_once_with(
            host="localhost",
            port="5432",
            user="test_user",
            password="test_pass",
            dbname="test_db",
            cursor_factory=RealDictCursor,
        )


class TestRunLoad:
    """Test class for run_load function."""

    @patch("load.os.path.exists", return_value=False)
    def test_handles_missing_file(self):
        """Test that run_load handles missing transformed_data.csv file gracefully."""
        with pytest.raises(FileNotFoundError):
            run_load(csv_path="a_missing_file.csv")

    @patch("load.upload_to_db")
    @patch("load.get_db_connection")
    def test_handles_empty_dataframe(self, mock_get_conn, mock_upload):
        """Test that run_load handles empty dataframe input appropriately."""
        mock_upload.return_value = None
        df = pd.DataFrame()
        run_load(df)
        mock_upload.assert_not_called()
        mock_get_conn.assert_not_called()

    def test_handles_both_input_types_invalid(self):
        """Test that run_load handles case where both dataframe and CSV path are invalid."""
        with pytest.raises(ValueError):
            run_load()

    @patch("load.pd.read_csv")
    @patch("load.os.path.exists", return_value=True)
    @patch("load.upload_to_db")
    @patch("load.get_db_connection")
    def test_executes_required_functions(self, mock_get_conn, mock_upload, mock_exists, mock_read_csv):
        """Test that run_load calls all required functions in correct sequence."""
        mock_df = pd.DataFrame({"a": [1, 2]})
        mock_read_csv.return_value = mock_df
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn

        run_load(csv_path="valid.csv")

        mock_exists.assert_called_once_with("valid.csv")
        mock_read_csv.assert_called_once_with(
            "valid.csv", parse_dates=['utc_date'])
        mock_get_conn.assert_called_once()
        mock_upload.assert_called_once_with(mock_df, mock_conn)
