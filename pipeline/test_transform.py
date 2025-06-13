"""Test file for the transform pipeline."""

from datetime import datetime, UTC
from unittest.mock import patch
import pytest
import pandas as pd

from transform import (rename_columns,
                       get_required_columns,
                       handle_missing_values,
                       correct_album_name,
                       standardize_dates,
                       standardize_release_date,
                       sort_by_date,
                       clean_dataframe,
                       export_dataframe)


class TestCleaningFunctions:
    """Tests for cleaning the DataFrame"""

    def test_rename_all_columns(self, sample_df):
        """Tests that rename_columns correctly renames specific columns."""
        renamed_df = rename_columns(sample_df)
        assert list(renamed_df.columns) == ['utc_date', 'artist_name', 'item_type',
                                            'item_description', 'album_name', 'currency',
                                            'amount_paid', 'sold_for', 'country_name',
                                            'country_code', 'url', 'art_url', 'event_type',
                                            'slug_type', 'tag_names', 'release_date']

    def test_rename_no_columns(self, bad_df):
        """Tests that if no columns match, DataFrame remains unchanged."""
        renamed_df = rename_columns(bad_df)
        expected_cols = [
            "utc_date", "artist_name", "item_type", "item_description",
            "album_name", "currency", "amount_paid", "sold_for",
            "country_name", "country_code", "url", "art_url",
            "event_type", "slug_type"
        ]
        assert list(renamed_df.columns) == expected_cols

    def test_completely_empty_dataframe(self, empty_df):
        """Tests renaming on an empty DataFrame."""
        renamed_df = rename_columns(empty_df)
        assert renamed_df.empty
        assert not list(renamed_df.columns)

    def test_get_required_columns_returns_the_required_columns(self, sample_df):
        """Tests get_required_columns returns the required columns."""
        result = get_required_columns(sample_df)
        expected_cols = [
            "utc_date", "item_type", "album_name", "artist_name", "item_description",
            "tag_names", "sold_for", "release_date", "country_name", "slug_type", "url", "art_url"
        ]
        assert list(result.columns) == expected_cols
        assert result.shape[1] == len(expected_cols)

    def test_get_required_columns_raises_error_when_column_is_not_in_df(self, empty_df):
        """Tests get_required_columns raises an error when the required columns aren't in the df."""
        with pytest.raises(KeyError):
            get_required_columns(empty_df)

    def test_clean_dataframe_returns_not_empty_sample_df(self, sample_df):
        """Tests clean_dataframe does not return an empty df."""
        result = clean_dataframe(sample_df)
        assert not result.empty
        assert "utc_date" in result.columns

    def test_clean_dataframe_raises_error_when_df_is_empty(self, empty_df):
        """Checks that clean_dataframe raises an error when the given df is empty."""
        with pytest.raises(KeyError):
            clean_dataframe(empty_df)


class TestMissingValues:
    """Tests for functionalities that doeal with missing values."""

    def test_handle_missing_values(self, bad_df):
        """
        Tests that handle_missing_values returns correct number 
        of rows with no missing values.
        """
        result = handle_missing_values(bad_df)
        assert len(result) == 1
        assert result["artist_name"].isnull().sum() == 0

    def test_handle_missing_values_raises_a_key_error(self, empty_df_with_columns):
        """Tests hanle_missing_values raises a KeyError if artist_name is not in columns."""
        with pytest.raises(KeyError):
            handle_missing_values(empty_df_with_columns)

    def test_correct_album_name_corrects_album_names(self, df_with_albums):
        """Tests that correct_album_name returns the correct names. """
        corrected = correct_album_name(df_with_albums)
        assert corrected.loc[0, "album_name"] == "Album One"
        assert corrected.loc[1, "album_name"] == "Track One Album"
        assert corrected.loc[2, "album_name"] == "Album Two"

    def test_correct_album_name_does_nothing_if_no_albums(self, df_no_albums):
        """Tests that correct_album_name returns nothing if there are no albums."""
        corrected = correct_album_name(df_no_albums)
        assert corrected.equals(df_no_albums)

    def test_correct_album_name_with_empty_dataframe(self, empty_df_with_columns):
        """Tests that correct_album_name returns empty name."""
        corrected = correct_album_name(empty_df_with_columns)
        assert corrected.empty
        assert list(corrected.columns) == [
            "item_type", "item_description", "album_name"]

    def test_correct_album_name_raises_a_key_error(self, empty_df):
        """Tests that correct_album_name raises a KeyError when required columns are not there."""
        with pytest.raises(KeyError):
            correct_album_name(empty_df)


class TestSortingFunctions:
    """Tests for all of the sorting functionalities."""

    def test_sort_by_date_returns_correct_order(self, sample_df):
        """Checks that sort_by_date returns the correct order of dates."""
        sorted_df = sort_by_date(sample_df.copy())
        utc_dates = sorted_df["utc_date"].tolist()
        assert utc_dates == sorted(
            utc_dates), "Dates are not sorted in ascending order"

    def test_sort_by_date_raises_a_value_error_if_date_is_null(self, bad_df):
        """Tests that sort_by_date raises a ValueError if utc_date contains nulls."""
        with pytest.raises(ValueError, match="utc_date column contains null values."):
            sort_by_date(bad_df)


class TestStandardizationFunctions:
    """Tests for all of the standardization functions."""

    def test_standardize_dates_returns_datetime_for_valid_input(self):
        """Tests that standardize_dates returns a datetime object for valid float input."""
        timestamp = 1749542761.50579
        result = standardize_dates(timestamp)
        assert isinstance(result, datetime)

    def test_standardize_dates_raises_type_error_on_invalid_input(self):
        """Tests that standardize_dates raises TypeError for non-numeric input."""
        invalid_input = "not_a_timestamp"
        with pytest.raises(TypeError):
            standardize_dates(invalid_input)

    def test_standardize_release_dates_returns_a_datetime(self, sample_df):
        """Tests that standardize_release_date returns a datetime instance."""
        date_str = sample_df.loc[0, "release_date"]
        result = standardize_release_date(date_str)
        assert isinstance(result, datetime)

    def test_standardize_release_dates_raises_an_error_if_date_is_not_a_string(self):
        """Tests that standardize_release_date raises ValueError if input is not a valid string."""
        bad_value = 12345
        with pytest.raises(TypeError):
            standardize_release_date(bad_value)


class TestPipeline:
    """Tests for the combinining of functionalities and the exporting to csv."""
    @patch("transform.handle_missing_values")
    @patch("transform.correct_album_name")
    @patch("transform.standardize_release_date", side_effect=lambda x: x)
    @patch("transform.standardize_dates", side_effect=lambda x: datetime.fromtimestamp(float(x),
                                                                                       tz=UTC))
    @patch("transform.sort_by_date")
    @patch("transform.get_required_columns")
    @patch("transform.rename_columns")
    def test_clean_dataframe_pipeline(self,
                                      mock_rename, mock_get_required, mock_handle_missing,
                                      mock_correct_album, mock_standardize_dates,
                                      mock_standardize_release, mock_sort, sample_df
                                      ):
        """Tests that clean_dataframe applies all transformation steps."""
        mock_rename.return_value = sample_df
        mock_get_required.return_value = sample_df
        mock_handle_missing.return_value = sample_df
        mock_correct_album.return_value = sample_df
        mock_sort.return_value = sample_df

        result = clean_dataframe(sample_df)

        assert isinstance(result, pd.DataFrame)
        mock_rename.assert_called_once()
        mock_get_required.assert_called_once()
        mock_handle_missing.assert_called_once()
        mock_correct_album.assert_called_once()
        mock_standardize_dates.assert_called()
        mock_standardize_release.assert_called()
        mock_sort.assert_called_once()

    @patch("pandas.DataFrame.to_csv")
    def test_export_dataframe_call(self, mock_to_csv, sample_df):
        """Test that export_dataframe creates a csv of the clean data."""
        export_dataframe(sample_df, output_path="mock_path.csv")
        mock_to_csv.assert_called_once_with("mock_path.csv", index=False)
