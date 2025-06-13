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
    parse_tag_list,
    load_sales_csv,
    get_filtered,
    upload_to_db,
    export_to_csv,
    extract_tags,
    load_existing,
    remove_existing
)
# pytest.skip(allow_module_level=True)


@pytest.fixture(autouse=True)
def mock_env_vars():
    """Fixture providing mock environment variables for database connection."""
    env_vars = {
        "DB_HOST": "testlocalhost",
        "DB_NAME": "testdb",
        "DB_USER": "testuser",
        "DB_PASSWORD": "veryrealpassword",
        "DB_PORT": "5432"
    }
    with patch.dict("os.environ", env_vars):
        yield env_vars


@pytest.mark.usefixtures("mock_env_vars")
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
        psycopg2.connect(
            user='testuser',
            password='veryrealpassword',
            host='testlocalhost',
            port='5432',
            database='testdb'
        )

    @patch("load.psycopg2.connect")
    def test_calls_psycopg2_connect(self, mock_connect) -> None:
        """Test that get_db_connection calls psycopg2.connect method."""
        conn = get_db_connection()
        mock_connect.assert_called_once()


class TestParseTagList:
    def test_parse_valid_string(self):
        raw = "['jungle', 'dnb', 'raggatek']"
        assert parse_tag_list(raw) == ['jungle', 'dnb', 'raggatek']

    def test_empty_string(self):
        assert parse_tag_list('') == []

    def test_not_a_string(self):
        assert parse_tag_list(None) == []

    def test_malformed_list(self):
        assert parse_tag_list("[raggatek,jungle]") == ['raggatek', 'jungle']


class TestExtractTags:
    def test_basic_tags(self):
        df = pd.Series(["['jungle', 'dnb', 'raggatek']", "['korean dancehall']", None, "['jungle', 'Korean Dancehall']"])
        expected = ['dnb', 'jungle', 'korean dancehall', 'raggatek']
        assert extract_tags(df) == expected

    def test_empty_and_malformed(self):
        df = pd.Series([None, '', '[]', "[raggatek,jungle]", "['jungle', 'dnb']"])
        expected = ['dnb', 'jungle', 'raggatek']
        assert extract_tags(df) == expected

    def test_whitespace_and_case(self):
        df = pd.Series(["[' jungle ', 'DNB']", "['Korean Dancehall', ' dnb']"])
        expected = ['dnb', 'jungle', 'korean dancehall']
        assert extract_tags(df) == expected


@pytest.mark.usefixtures("mock_env_vars")
class TestLoadSalesCsv:
    """Test class for load_sales_csv function."""

    @patch("load.pd.read_csv")
    @patch("load.get_logger")
    def test_loads_csv_and_logs(self, mock_get_logger, mock_read_csv):
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        expected_df = pd.DataFrame({'a': [1, 2, 3]})
        mock_read_csv.return_value = expected_df

        df = load_sales_csv()

        mock_read_csv.assert_called_once_with('data/clean_sales3.csv', parse_dates=['utc_date'])
        mock_logger.info.assert_called_once_with("Loading sales CSV data...")
        assert df.equals(expected_df)


class TestGetFiltered:
    """Test class for get_filtered function."""

    def test_filters_correctly(self):
        data = {
            'slug_type': ['t', 'a', 'p', 't', 'a'],
            'item_type': ['t', 'a', 'p', 't', 'p'],
            'value': [10, 20, 30, 40, 50]
        }
        df = pd.DataFrame(data)
        filtered = get_filtered(df)

        assert set(filtered.keys()) == {'track', 'album', 'merchandise'}

        expected_track = df[(df['slug_type'] == 't') & (df['item_type'] == 't')]
        assert filtered['track'].equals(expected_track)

        expected_album = df[(df['slug_type'] == 'a') & (df['item_type'].isin(['a', 'p']))]
        assert filtered['album'].equals(expected_album)

        expected_merch = df[df['slug_type'] == 'p']
        assert filtered['merchandise'].equals(expected_merch)


class TestLoadExisting:
    """Tests for the load_existing function."""

    def test_load_existing_basic(self):
        """Test that load_existing returns correct dict keys and mappings."""
        sales = pd.DataFrame({'country_name': ['USA']})
        content_dfs = {
            'track': pd.DataFrame({'artist_name': ['A1'], 'tag_names': ["['rock']"], 'url': ['u1']}),
            'album': pd.DataFrame({'artist_name': ['A2'], 'tag_names': ["['pop']"], 'url': ['u2']}),
            'merchandise': pd.DataFrame({'artist_name': ['A3'], 'tag_names': [None], 'url': ['u3']})
        }
        cursor = MagicMock()
        cursor.fetchall.side_effect = [
            [{'country_id': 1, 'country_name': 'USA'}],
            [{'artist_id': 10, 'artist_name': 'A1'}, {'artist_id': 20, 'artist_name': 'A2'}, {'artist_id': 30, 'artist_name': 'A3'}],
            [{'tag_id': 100, 'tag_name': 'rock'}, {'tag_id': 200, 'tag_name': 'pop'}],
            [{'track_id': 1000, 'url': 'u1'}],
            [{'album_id': 2000, 'url': 'u2'}],
            [{'merchandise_id': 3000, 'url': 'u3'}],
        ]

        result = load_existing(cursor, sales, content_dfs)

        assert 'country' in result
        assert result['country']['USA'] == 1
        assert 'artist' in result
        assert result['artist']['A2'] == 20
        assert 'tag' in result
        assert 'rock' in result['tag']
        assert 'track' in result
        assert result['track']['u1'] == 1000


class TestRemoveExisting:
    """Tests for the remove_existing function."""

    def test_remove_existing_basic(self):
        """Test that remove_existing filters out URLs already in existing."""
        sales = pd.DataFrame({'url': ['u1', 'u2', 'u3']})
        existing = {
            'track': {'u1': 1},
            'album': {'u2': 2},
            'merchandise': {}
        }

        filtered = remove_existing(sales, existing)

        assert 'u3' in filtered['url'].values
        assert 'u1' not in filtered['url'].values
        assert 'u2' not in filtered['url'].values


# class TestUploadToDb:
#     """Test class for upload_to_db function."""

#     @patch("load.psycopg2.connect")
#     def test_handles_invalid_data_type_df(self, mock_connect) -> None:
#         """Test that upload_to_db raises TypeError for invalid DataFrame input."""
#         mock_conn = mock_connect.return_value
#         with pytest.raises(TypeError):
#             upload_to_db(12345, mock_conn)

#     def test_handles_invalid_data_type_connection(self) -> None:
#         """Test that upload_to_db raises TypeError for invalid connection input."""
#         df = pd.DataFrame({"test_df": [1, 2, 3]})
#         with pytest.raises(TypeError):
#             upload_to_db(df, 12345)

#     @patch("load.psycopg2.connect")
#     def test_handles_empty_dataframe(self, mock_connect) -> None:
#         """Test that upload_to_db handles empty DataFrame gracefully."""
#         mock_conn = mock_connect.return_value
#         df = pd.DataFrame()

#         upload_to_db(df, mock_conn)

#         mock_conn.cursor().execute.assert_not_called()

#     @patch("load.psycopg2.connect")
#     def test_handles_database_error(self, mock_connect) -> None:
#         """Test that upload_to_db raises DatabaseError when database insertion fails."""
#         df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})

#         mock_conn = MagicMock()
#         mock_cursor = mock_conn.cursor.return_value
#         mock_cursor.execute.side_effect = psycopg2.DatabaseError(
#             "DB insertion failed")

#         mock_connect.return_value = mock_conn

#         with pytest.raises(psycopg2.DatabaseError):
#             upload_to_db(df, mock_conn)


# class TestExportToCsv:
#     """Test class for export_to_csv function."""

#     @patch("load.pd.DataFrame.to_csv")
#     def test_handles_empty_dataframe(self, mock_to_csv) -> None:
#         """Test that export_to_csv handles empty DataFrames without calling to_csv."""
#         df = pd.DataFrame()

#         export_to_csv(df)

#         mock_to_csv.assert_not_called()

#     @patch("load.pd.DataFrame.to_csv")
#     def test_calls_to_csv_method(self, mock_to_csv) -> None:
#         """Test that export_to_csv calls DataFrame.to_csv method."""
#         df = pd.DataFrame({"col1": [1, 2, 3]})

#         export_to_csv(df)

#         mock_to_csv.assert_called_once_with("output.csv", index=False)

#     @patch("load.pd.DataFrame.to_csv")
#     def test_uses_custom_output_path(self, mock_to_csv) -> None:
#         """Test that export_to_csv uses custom path when provided."""
#         df = pd.DataFrame({"col1": [1, 2, 3]})

#         export_to_csv(df, "custom.csv")

#         mock_to_csv.assert_called_once_with("custom.csv", index=False)


# class TestRunLoad:
#     """Test class for run_load function."""

#     @patch("load.os.path.exists", return_value=False)
#     def test_handles_missing_file(self, mock_exists) -> None:
#         """Test that run_load handles missing transformed_data.csv file gracefully."""
#         with pytest.raises(FileNotFoundError):
#             run_load(csv_path="a_missing_file.csv")

#     @patch("load.get_db_connection")
#     @patch("load.upload_to_db")
#     def test_handles_empty_dataframe(self, mock_upload, mock_get_conn) -> None:
#         """Test that run_load handles empty dataframe input appropriately."""
#         df = pd.DataFrame()
#         run_load(dataframe=df)
#         mock_upload.assert_not_called()
#         mock_get_conn.assert_not_called()

#     def test_handles_both_input_types_invalid(self) -> None:
#         """Test that run_load handles case where both dataframe and CSV path are invalid."""
#         with pytest.raises(ValueError):
#             run_load()

#     @patch("load.get_db_connection")
#     @patch("load.upload_to_db")
#     @patch("load.pd.read_csv")
#     @patch("load.os.path.exists", return_value=True)
#     def test_executes_required_functions(self, mock_exists, mock_read_csv, mock_upload, mock_get_conn) -> None:
#         """Test that run_load calls all required functions in correct sequence."""
#         mock_df = pd.DataFrame({"a": [1, 2]})
#         mock_read_csv.return_value = mock_df
#         mock_conn = MagicMock()
#         mock_get_conn.return_value = mock_conn

#         run_load(csv_path="valid.csv")

#         mock_exists.assert_called_once_with("valid.csv")
#         mock_read_csv.assert_called_once_with("valid.csv")
#         mock_get_conn.assert_called_once()
#         mock_upload.assert_called_once_with(mock_df, mock_conn)
