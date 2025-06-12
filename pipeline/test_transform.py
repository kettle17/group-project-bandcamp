"""Test file for the transform pipeline."""

# pylint: skip-file
import pytest
from unittest.mock import patch
import pandas as pd

from transform import (get_required_columns,
                       check_valid_sale,
                       clean_urls,
                       handle_missing_values,
                       validate_foreign_keys,
                       get_id_mapping,
                       filter_by_currency,
                       add_foreign_key_column,
                       clean_dataframe,
                       export_dataframe)

pytest.skip(allow_module_level=True)


class TestCleaningFunctions:
    """Tests for cleaning the DataFrame"""

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

    def test_check_valid_sale_is_greater_than_zero(self, sample_df):
        """Tests the check_valid_sale function returns true when a sale's value is greater than zero."""
        result = check_valid_sale(sample_df)
        assert result

    def test_check_valid_sale_raises_error_when_invalid_df_given(self, bad_df):
        """Tests the check_valid_sale function raises a ValueError when given an invalid pandas dataframe."""
        with pytest.raises(ValueError):
            check_valid_sale(bad_df)

    def test_clean_urls_returns_correct_format(self, sample_df):
        """Tests clean_urls to check it returns the correct beginning for the url."""
        result = clean_urls(sample_df)
        assert result["url"].iloc[0].startswith("https://")
        assert result["art_url"].iloc[1].startswith("https://")

    def test_clean_urls_raises_error_when_url_is_invalid(self, bad_df):
        """Tests clean_urls raises a ValueError when the url doesn't start with //."""
        with pytest.raises(ValueError):
            clean_urls(bad_df)

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


class TestsForeignKeyMapping:
    """Tests for all of the functions that go into adding foreign keys columns."""

    def test_validate_foreign_keys_structure(self, sample_df):
        """Tests validate_foreign_keys returns a bool."""
        result = validate_foreign_keys(sample_df)
        assert isinstance(result, bool)

    @patch("transform.get_id_mapping")
    def test_get_id_mapping_valid(self, mock_query_func, sample_df):
        """Tests get_id_mapping returns a dict and that it returns the correct id."""
        mock_query_func.return_value = pd.DataFrame({
            "artist_name": ["Alex Lynch", "Blackchild (ITA)"],
            "artist_id": [100, 101]
        })
        result = get_id_mapping(sample_df, table_name="artists")
        assert isinstance(result, dict)
        assert result["Alex Lynch"] == 100

    @patch("transform.get_id_mapping")
    def test_add_foreign_key_column_works(self, mock_get_mapping, sample_df):
        """Tests to check add_foreign_key_column adds the correct column and its values."""
        mock_get_mapping.return_value = {
            "Alex Lynch": 100,
            "Blackchild (ITA)": 101
        }
        sample_df = sample_df.copy()
        result = add_foreign_key_column(sample_df)
        assert "foreign_key" in result.columns
        assert result["foreign_key"].tolist() == [100, 101]


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
