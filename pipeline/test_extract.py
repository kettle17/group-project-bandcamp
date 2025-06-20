# pylint: skip-file

"""Test file for the extract pipeline."""

import pytest
import os
import json
from unittest.mock import patch, mock_open
from extract import (
    get_api_request,
    fetch_api_data,
    validate_api_data,
    collect_api_rows_and_columns,
    save_to_csv,
    run_extract,
    get_time_offset
)


class TestGetAPIRequest:
    """Tests for get_api_request."""

    def test_get_api_request_start_date_invalid_type(self):
        """Test that checks if start_date is a valid int."""
        with pytest.raises(TypeError):
            get_api_request('I am not an int')

    def test_get_api_request_start_date_should_deny_negative(self):
        """Test that checks if start_date accepts negative values."""
        with pytest.raises(ValueError):
            get_api_request(-58492573)

    @patch('requests.get')
    def test_get_api_request_should_deny_404(self, fake_requests):
        """Test that checks if function halts if it can't connect."""
        fake_requests.return_value.status_code = 404
        with pytest.raises(ConnectionError):
            get_api_request(24234344)

    @patch('requests.get')
    def test_get_api_request_correct(self, fake_requests):
        """Test that checks if function correctly functions."""
        fake_requests.return_value.status_code = 200
        fake_requests.return_value.response = json.dumps({'cool': 'cool'})
        assert get_api_request(24234344)


class TestFetchAPIData:
    """Tests for fetch_api_data."""

    @patch('extract.get_api_request')
    def test_fetch_api_data_output_is_correct_format(self, fake_get_request, example_api_call):
        """Test that correctly executes the function."""
        fake_get_request.return_value = example_api_call
        assert type(fetch_api_data(1749583860)) == dict

    def test_fetch_api_data_start_date_invalid_type(self):
        """Test that checks if script halts on incorrect type parameter."""
        with pytest.raises(TypeError):
            fetch_api_data('I am not an int')

    def test_fetch_api_data_start_date_should_deny_negative(self):
        """Test that checks if start_date accepts negative values."""
        with pytest.raises(ValueError):
            fetch_api_data(-58492573)

    @patch('extract.get_api_request')
    def test_fetch_api_data_incorrect_data_returned(self, fake_get_request, incorrect_api_call):
        """Test that checks if script halts if the format of dictionary data is not as expected."""
        fake_get_request.return_value = incorrect_api_call
        with pytest.raises(ValueError):
            fetch_api_data(1749583860)


class TestValidateAPIData:
    """Tests for validate_api_data."""

    def test_validate_api_data_wrong_type_for_data(self):
        """Test that checks if script halts if the data type is not a dict."""
        with pytest.raises(TypeError):
            validate_api_data('I am not a dict', 'filepath')

    def test_validate_api_data_wrong_type_for_filepath(self):
        """Test that checks if script halts if the file path isn't a string."""
        with pytest.raises(TypeError):
            validate_api_data({'dict': 'dict'}, 2424)

    def test_validate_api_data_folder_path_doesnt_exist(self):
        """Test that checks if script halts if the folder path doesn't exist."""
        with pytest.raises(OSError):
            validate_api_data(
                {'dict': 'dict'}, 'data/data/data/data/data/output.csv')

    def test_validate_api_data_wrong_value_for_filepath(self):
        """Test that checks if script halts if the file path doesn't end in csv"""
        with pytest.raises(ValueError):
            validate_api_data({'dict': 'dict'}, 'data/output')

    @patch('extract.get_api_request')
    def test_validate_api_data_incorrect_data_returned_no_start_date(self, fake_get_request, incorrect_api_call):
        """Test that checks that csv isn't saved if the data is incorrectly formatted (empty request)."""
        fake_get_request.return_value = incorrect_api_call
        with pytest.raises(ValueError):
            validate_api_data(incorrect_api_call, 'data/output.csv')

    @patch('extract.get_api_request')
    def test_validate_api_data_incorrect_data_returned_no_events(self, fake_get_request):
        """Test that checks that csv isn't saved if the data is incorrectly formatted (no events)."""
        fake_get_request.return_value = {
            "start_date": 1749583860,
            "end_date": 1749584460,
            "data_delay_sec": 120,
            "server_time": 1749584469
        }
        api_data = fake_get_request()
        with pytest.raises(ValueError):
            validate_api_data(api_data, 'data/output.csv')

    @patch('extract.get_api_request')
    def test_validate_api_data_incorrect_data_returned_empty_events(self, fake_get_request):
        """Test that checks that csv isn't saved if the data is incorrectly formatted (events is empty)."""
        fake_return = {
            "start_date": 1749583860,
            "end_date": 1749584460,
            "data_delay_sec": 120,
            "events": [{}],
            "server_time": 1749584469
        }
        fake_get_request.return_value = fake_return
        api_data = fake_get_request()
        with pytest.raises(ValueError):
            validate_api_data(api_data, 'data/output.csv')

    @patch('extract.get_api_request')
    def test_validate_api_data_correct_data_returned(self, fake_get_request, example_api_call):
        """Test that checks that csv isn't saved if the data is incorrectly formatted (events is empty)."""
        fake_get_request.return_value = example_api_call
        file_path = 'data/output.csv'
        altered_file_path = os.path.dirname(__file__) + '/' + file_path
        api_data = fake_get_request()
        assert validate_api_data(
            api_data, 'data/output.csv') == altered_file_path


class TestCollectAPIRowsAndColumns:
    """Tests for collect_api_rows_and_columns. """

    @patch('extract.get_release_date_and_genres')
    def test_collect_api_rows_and_columns_correct_data_returned(self, fake_release_date_and_columns, example_api_call):
        """Test that checks that csv is saved if the data is correctly formatted."""
        example_api_call
        fake_release_date_and_columns.return_value = {'genres': [
            'jazz', 'chamber jazz', 'cool jazz', 'guitar', 'modal jazz'], 'release_date': 'released October 11, 2024'}

        assert collect_api_rows_and_columns(example_api_call) == (
            [
                {'addl_count': None, 'utc_date': 1749638295.4456537, 'artist_name': 'Ella Zirina',
                 'item_type': 'a', 'item_description': 'Boundless Blue, Sunset Hue',
                 'album_title': None, 'slug_type': 'a', 'track_album_slug_text': None,
                 'genres': ['jazz', 'chamber jazz', 'cool jazz', 'guitar', 'modal jazz'],
                 'currency': 'EUR', 'amount_paid': 7, 'item_price': 7, 'amount_paid_usd': 8,
                 'country': 'Japan', 'art_id': 3672109546, 'releases': None,
                 'package_image_id': None, 'release_date': 'released October 11, 2024',
                 'url': 'https://ellazirina.bandcamp.com/album/boundless-blue-sunset-hue',
                 'country_code': 'jp', 'amount_paid_fmt': '€7',
                 'art_url': 'https://f4.bcbits.com/img/a3672109546_7.jpg'
                 }],
            ['addl_count', 'album_title', 'amount_paid', 'amount_paid_fmt', 'amount_paid_usd',
             'art_id', 'art_url', 'artist_name', 'country', 'country_code', 'currency', 'genres',
             'item_description', 'item_price', 'item_type', 'package_image_id', 'release_date', 'releases',
             'slug_type', 'track_album_slug_text', 'url', 'utc_date'])


class TestRunExtract:
    """Tests for run_extract."""
    @patch('requests.get')
    def test_run_extract_wrong_type_for_filepath(self, fake_requests):
        """Test that checks if script halts if the file path isn't a string."""
        fake_requests.return_value.status_code = 200
        fake_requests.return_value.response = json.dumps({'cool': 'cool'})
        with pytest.raises(TypeError):
            run_extract({"filepath": "i am not a filepath"})

    @patch('requests.get')
    def test_run_extract_wrong_value_folder_path_doesnt_exist(self, fake_requests, example_api_call):
        """Test that checks if script halts if the folder path doesn't exist."""
        fake_requests.return_value.status_code = 200
        fake_requests.return_value.json.return_value = example_api_call
        with pytest.raises(OSError):
            run_extract('data/data/data/data/data/output.csv')

    def test_run_extract_wrong_type_for_curr_time(self):
        """Test that checks if script halts if the curr_time isn't an int."""
        with pytest.raises(TypeError):
            run_extract('data/output.csv', 'i am not an int')

    def test_run_extract_wrong_value_for_curr_time(self):
        """Test that checks if script halts if the curr_time is negative."""
        with pytest.raises(ValueError):
            run_extract('data/output.csv', -49234739284723)

    @patch('extract.get_api_request')
    def test_run_extract_incorrect_data_returned(self, fake_get_request, incorrect_api_call):
        """Test that checks that csv isn't saved if the data is incorrectly formatted."""
        fake_get_request.return_value = incorrect_api_call
        with pytest.raises(ValueError):
            run_extract('data/output.csv', 1749583860242424)

    @patch('extract.get_release_date_and_genres')
    @patch('extract.get_api_request')
    @patch('extract.save_to_csv')
    def test_run_extract_correct_data_returned(self, fake_save_to_csv, fake_get_request,
                                               fake_release_date_and_columns,
                                               example_api_call):
        """Test that checks that csv is saved if the data is correctly formatted."""
        fake_get_request.return_value = example_api_call
        fake_release_date_and_columns.return_value == {'genres': [
            'jazz', 'chamber jazz', 'cool jazz', 'guitar', 'modal jazz'], 'release_date': 'released October 11, 2024'}
        fake_save_to_csv.return_value = True
        assert run_extract('data/output.csv', 2380921482190481)


class TestGetTimeOffset:
    """Tests for get_time_offset."""

    def tests_get_time_offset_passing_5000_should_return_4800(self):
        """Test that checks if the offset is working."""
        assert get_time_offset(5000) == 4800

    def tests_get_time_offset_raises_errors(self):
        """Test that checks if the function errors as expected."""
        with pytest.raises(TypeError):
            get_time_offset('I am not an int')
