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
    extract_tags,
    get_existing_entities,
    remove_existing_rows,
    insert_entities, 
    insert_tags, 
    insert_content,
    run_load
)


class TestGetDbConnection:
    """Test class for get_db_connection function."""

    @patch("load.psycopg2.connect")
    def test_returns_connection(self, mock_connect) -> None:
        """Test that get_db_connection returns a valid psycopg2 connection object."""
        mock_conn = mock_connect.return_value
        conn = get_db_connection()
        assert conn is mock_conn

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


class TestGetExistingEntities:
    """Tests for the load_existing function."""

    def test_load_existing_basic(self):
        """Test that load_existing returns correct dict keys and mappings."""
        sales = pd.DataFrame({'country_name': ['United Kingdom']})
        content_dfs = {
            'track': pd.DataFrame({'artist_name': ['Halogenix'], 'tag_names': ["['drum and bass']"], 'url': ['halogenix-track-1']}),
            'album': pd.DataFrame({'artist_name': ['Molecular'], 'tag_names': ["['liquid funk']"], 'url': ['molecular-album-1']}),
            'merchandise': pd.DataFrame({'artist_name': ['Trex'], 'tag_names': [None], 'url': ['trex-merch-1']})
        }
        cursor = MagicMock()
        cursor.fetchall.side_effect = [
            [{'country_id': 44, 'country_name': 'United Kingdom'}],
            [{'artist_id': 101, 'artist_name': 'Halogenix'}, {'artist_id': 202, 'artist_name': 'Molecular'}, {'artist_id': 303, 'artist_name': 'LTJ Bukem'}],
            [{'tag_id': 1001, 'tag_name': 'drum and bass'}, {'tag_id': 1002, 'tag_name': 'liquid funk'}],
            [{'track_id': 5555, 'url': 'halogenix-track-1'}],
            [{'album_id': 6666, 'url': 'molecular-album-1'}],
            [{'merchandise_id': 7777, 'url': 'trex-merch-1'}],
        ]

        result = get_existing_entities(cursor, sales, content_dfs)

        assert 'country' in result
        assert result['country']['United Kingdom'] == 44
        assert 'artist' in result
        assert result['artist']['Molecular'] == 202
        assert 'tag' in result
        assert 'drum and bass' in result['tag']
        assert 'track' in result
        assert result['track']['halogenix-track-1'] == 5555


class TestRemoveExistingRows:
    """Tests for the remove_existing function."""

    def test_remove_existing_basic(self):
        """Test that remove_existing filters out URLs already in existing."""
        sales = pd.DataFrame({'url': ['u1.com', 'u2.com', 'u3.com']})
        existing = {
            'track': {'u1.com': 1},
            'album': {'u2.com': 2},
            'merchandise': {}
        }

        filtered = remove_existing_rows(sales, existing)

        assert 'u3.com' in filtered['url'].values
        assert 'u1.com' not in filtered['url'].values
        assert 'u2.com' not in filtered['url'].values


class TestInsertEntities:
    """Tests for the insert_entities function."""
    @patch('load.execute_values')
    def test_inserts_entities_and_returns_mapping(self, mock_execute_values):
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            {'country_name': 'UK', 'country_id': 1},
            {'country_name': 'FR', 'country_id': 2}
        ]
        df = pd.DataFrame({'country_name': ['UK', 'FR', 'UK']})

        result = insert_entities(df, 'country', mock_cursor)

        mock_execute_values.assert_called_once()
        assert result == {'UK': 1, 'FR': 2}

    def test_returns_empty_dict_when_no_values(self):
        mock_cursor = MagicMock()
        df = pd.DataFrame({'country_name': []})

        result = insert_entities(df, 'country', mock_cursor)

        assert result == {}


class TestInsertTags:
    """Tests for the insert_tags function."""
    @patch('load.execute_values')
    def test_inserts_tags_and_returns_mapping(self, mock_execute_values):
        mock_cursor = MagicMock()
        df = pd.DataFrame({'tag_names': ["['jungle', 'drum and bass']", "['liquid']"]})
        mock_cursor.fetchall.return_value = [
            {'tag_name': 'jungle', 'tag_id': 99},
            {'tag_name': 'drum and bass', 'tag_id': 111},
            {'tag_name': 'liquid', 'tag_id': 42},
        ]

        result = insert_tags(df, mock_cursor)

        mock_execute_values.assert_called_once()
        assert result == {'jungle': 99, 'drum and bass': 111, 'liquid': 42}

    def test_returns_empty_dict_when_no_tags(self):
        mock_cursor = MagicMock()
        df = pd.DataFrame({'tag_names': []})

        result = insert_tags(df, mock_cursor)

        assert result == {}


class TestInsertContent:
    """Tests for the insert_content"""
    @patch('load.execute_values')
    def test_inserts_tracks_and_returns_url_id_mapping(self, mock_execute_values):
        mock_cursor = MagicMock()
        df = pd.DataFrame([
            {'item_description': 'Track 1', 'url': 'url1', 'art_url': None, 'sold_for': 5.0, 'release_date': '2023-01-01'},
            {'item_description': 'Track 2', 'url': 'url2', 'art_url': None, 'sold_for': None, 'release_date': None},
        ])
        mock_cursor.fetchall.return_value = [
            {'url': 'url1', 'track_id': 100},
            {'url': 'url2', 'track_id': 101},
        ]

        result = insert_content(df, 'track', mock_cursor)

        mock_execute_values.assert_called_once()
        assert result == {'url1': 100, 'url2': 101}


    def test_returns_empty_dict_with_unknown_content_type(self):
        mock_cursor = MagicMock()
        df = pd.DataFrame([
            {'item_description': 'Item', 'url': 'url', 'art_url': None, 'sold_for': 10.0}
        ])

        result = insert_content(df, 'unknown', mock_cursor)

        assert result == {}


    def test_returns_empty_dict_with_empty_df(self):
        mock_cursor = MagicMock()
        df = pd.DataFrame([])

        result = insert_content(df, 'track', mock_cursor)

        assert result == {}


class TestUploadToDb:
    """Test class for upload_to_db function."""

    def test_handles_invalid_data_type_df(self):
        """Test that upload_to_db raises TypeError for invalid DataFrame input."""
        with pytest.raises(TypeError):
            upload_to_db(12345, MagicMock())

    def test_handles_invalid_data_type_connection(self):
        """Test that upload_to_db raises TypeError for invalid connection input."""
        df = pd.DataFrame({"test_df": [1, 2, 3]})
        with pytest.raises(TypeError):
            upload_to_db(df, "not_a_connection")

    @patch("load.get_logger")
    def test_handles_empty_dataframe(self, mock_get_logger):
        """Test that upload_to_db handles empty DataFrame gracefully."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger
        mock_conn = MagicMock()
        df = pd.DataFrame()

        upload_to_db(df, mock_conn)

        mock_logger.info.assert_called_with("Empty dataframe received, nothing to upload.")
        mock_conn.cursor().execute.assert_not_called()

    @patch("load.get_logger")
    def test_handles_database_error(self, mock_get_logger):
        """Test that upload_to_db raises DatabaseError when database insertion fails."""
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        df = pd.DataFrame({
            "col1": [1, 2],
            "col2": ["a", "b"],
            "slug_type": ["t", "a"],
            "item_type": ["t", "a"],
            "url": ["url1", "url2"],
            "country_name": ["USA", "UK"],
            "utc_date": pd.to_datetime(["2024-01-01", "2024-01-02"])
        })
        mock_conn = MagicMock()
        mock_cursor = mock_conn.cursor.return_value
        mock_cursor.execute.side_effect = psycopg2.DatabaseError("DB insertion failed")

        with pytest.raises(psycopg2.DatabaseError):
            upload_to_db(df, mock_conn)

        mock_conn.rollback.assert_called_once()
        mock_logger.error.assert_called()

class TestRunLoad:
    """Test class for run_load function."""

    @patch("load.os.path.exists", return_value=False)
    def test_handles_missing_file(self, mock_exists):
        """Test that run_load handles missing transformed_data.csv file gracefully."""
        with pytest.raises(FileNotFoundError):
            run_load(csv_path="a_missing_file.csv")

    @patch("load.upload_to_db")
    @patch("load.get_db_connection")
    def test_handles_empty_dataframe(self, mock_get_conn, mock_upload):
        """Test that run_load handles empty dataframe input appropriately."""
        mock_upload.return_value = None
        df = pd.DataFrame()
        run_load(dataframe=df)
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
        mock_read_csv.assert_called_once_with("valid.csv", parse_dates=['utc_date'])
        mock_get_conn.assert_called_once()
        mock_upload.assert_called_once_with(mock_df, mock_conn)
    