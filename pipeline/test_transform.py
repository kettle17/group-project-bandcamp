"""Test file for the transform pipeline."""

# pylint: skip-file
import pytest
from unittest.mock import patch
import pandas as pd

from transform import (rename_columns,
                       get_required_columns,
                       handle_missing_values,
                       standardize_dates,
                       sort_by_date,
                       standardize_currency,
                       clean_dataframe,
                       export_dataframe)


class TestCleaningFunctions:
    """Tests for cleaning the DataFrame"""

    def test_rename_all_columns(self, sample_df):
        """Tests that rename_columns can rename all of the columns."""
        renamed_df = rename_columns(sample_df)
        assert list(renamed_df.columns) == [
            "country_name", "album_name", "track_name", "release_date"]

    def test_rename_no_columns(self, bad_df):
        renamed_df = rename_columns(bad_df)
        assert list(renamed_df.columns) == ["artist", "price", "genre"]

    def test_completely_empty_dataframe(self, empty_df):
        renamed_df = rename_columns(empty_df)
        assert renamed_df.empty
        assert list(renamed_df.columns) == []

    def test_get_required_columns_returns_the_required_columns(self, sample_df):
        """Tests get_required_columns returns the required columns."""
        result = get_required_columns(sample_df)
        expected_cols = [
            "utc_date", "artist_name", "item_type", "item_description", "album_title",
            "currency", "amount_paid", "country",
            "url", "art_url"
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
        with pytest.raises(ValueError):
            clean_dataframe(empty_df)

    def test_handle_missing_values(self, bad_df):
        """Tests that handle_missing_values returns correct number of rows with no missing values."""
        result = handle_missing_values(bad_df)
        assert len(result) == 1
        assert result["artist_name"].isnull().sum() == 0


class TestFilteringFunctions:
    """Tests for all of the filtering functionalities."""

    def test_filter_by_currency_default(self, sample_df):
        """Tests filter_by_currency returns one of the 3 required currencies only."""
        result = filter_by_currency(sample_df)
        assert result["currency"].isin(["USD", "GBP", "EUR"]).all()
        assert result.shape[0] == 2

    def test_filter_by_currency_custom(self, sample_df):
        """Tests filter_by_currency will return an empty df if the only accepted currency is EUR."""
        result = filter_by_currency(sample_df, accepted=["EUR"])
        assert result.empty

    def test_sort_by_date_returns_correct_order(self, sample_df):
        """Checks that sort_by_date returns the correct order of dates."""
        pass

    def test_sort_by_date_raises_a_value_error_if_date_is_null(self, bad_df):
        """Tests that sort_by_date raises a ValueError if date is null."""
        pass


class TestStandardizationFunctions:
    """Tests for all of the standardization functions."""

    def test_standardize_dates_returns_a_datetime(self, sample_df):
        """Tests that standardize_dates returns a datetime instance."""
        pass

    def test_standaize_dates_raises_an_error_if_date_is_not_a_string(self, bad_df):
        """Tests that standardize_dates raises an ValueError if the argument is not a string."""
        pass

    def test_standardize_currency_returns_correct_currency(self, sample_df):
        """Tests that standardize_currency returns the correct value."""
        pass

    def test_standardize_currency_raises_a_type_error_if_input_is_not_a_float(self, bad_df):
        """Test that standardize_currency raises a TypeError if value is not a float."""
        pass


class TestPipeline:
    """Tests for the combinining of functionalities and the exporting to csv."""

    @patch("transform.add_foreign_key_column")
    @patch("transform.validate_foreign_keys", return_value=True)
    @patch("transform.filter_by_currency")
    @patch("transform.group_sales_by")
    def test_clean_dataframe_pipeline(self, mock_group, mock_filter, mock_validate, mock_fk, sample_df):
        """Tests the clean_dataframe pipeline to see if it returns a df and that all of the functions are called at least once."""
        mock_group.return_value = sample_df
        mock_filter.return_value = sample_df
        mock_fk.return_value = sample_df

        result = clean_dataframe(sample_df)

        assert isinstance(result, pd.DataFrame)
        mock_filter.assert_called_once()
        mock_group.assert_called_once()
        mock_fk.assert_called_once()
        mock_validate.assert_called_once()

    @patch("pandas.DataFrame.to_csv")
    def test_export_dataframe_call(self, mock_to_csv, sample_df):
        """Test that export_dataframe creates a csv of the clean data."""
        export_dataframe(sample_df, output_path="mock_path.csv")
        mock_to_csv.assert_called_once_with("mock_path.csv", index=False)
