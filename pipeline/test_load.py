
"""Test file for the load pipeline."""

from unittest.mock import patch, MagicMock
import pytest
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from load import (
    get_db_connection,
    run_load,
    copy_df,
    build_frames,
    insert_dimension_data
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


class TestCopyDataFrame:
    """Test to verify copy_df works."""

    def test_copy_df_query_results(self, empty_df_with_columns):
        """Tests that copy_df returns the correct table columns."""
        cur = MagicMock()
        copy_df(cur, empty_df_with_columns, "stage_sale", [
            "item_type", "item_description", "album_name"])
        assert cur.copy_expert.called


class TestBuildFrames:
    """Tests for the build_frames function."""

    def test_build_frames_with_sample_data(self, sample_df):
        """Tests that build_frames returns correct output with an accurate fixture."""
        sales_df, tags_df = build_frames(sample_df)
        assert len(sales_df) == 2
        assert "track_name" in sales_df.columns
        assert sales_df["track_name"].iloc[0] == "Thank Me Later"[:15]
        assert tags_df.shape[0] == 2
        assert set(tags_df["tag_name"]) == {"rock", "rnb"}

    def test_build_frames_with_bad_data(self, bad_df):
        """Tests that build_frames rteurns correct output with incorrect data."""
        sales_df, tags_df = build_frames(bad_df)
        assert sales_df.shape[0] == bad_df.shape[0]
        assert tags_df.empty or "tag_name" in tags_df.columns
        assert pd.isna(sales_df["utc_date"]).all()
        assert pd.isna(sales_df["release_date"]).all()


class TestRunLoad:
    """Test class for run_load function."""

    @patch("load.os.path.exists", return_value=False)
    def test_handles_missing_file(self, _mock_exists):
        """Test that run_load handles missing transformed_data.csv file gracefully."""
        with pytest.raises(FileNotFoundError):
            run_load(csv_path="a_missing_file.csv")

    @patch("load.run_load")
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

    @patch("load.copy_df")
    @patch("load.insert_dimension_data")
    @patch("load.get_db_connection")
    @patch("load.build_frames")
    @patch("load.pd.read_csv")
    @patch("load.os.path.exists", return_value=True)
    def test_run_load_csv_path(
        self,
        mock_exists,
        mock_read_csv,
        mock_build_frames,
        mock_get_db_conn,
        mock_insert_dimension_data,
        mock_copy_df,
        sample_df,
        bad_df
    ):
        mock_get_db_conn = MagicMock()
        mock_get_db_conn.cur.rowcount.return_value = 5
        tags_df = MagicMock()
        tags_df.empty = True
        mock_build_frames.return_value = (sample_df, tags_df)
        mock_read_csv.return_value = sample_df
        mock_exists.return_value = True
        mock_cursor = MagicMock()

        run_load(csv_path="dummy.csv")
        assert mock_insert_dimension_data.call_count >= 1
