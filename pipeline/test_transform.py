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


def test_get_required_columns_returns_the_required_columns(sample_df):
    result = get_required_columns(sample_df)
    expected_cols = [
        "utc_date", "artist_name", "item_type", "item_description", "album_title",
        "currency", "amount_paid", "country",
        "url", "art_url"
    ]
    assert list(result.columns) == expected_cols
    assert result.shape[1] == len(expected_cols)


def test_get_required_columns_raises_error(empty_df):
    with pytest.raises(KeyError):
        get_required_columns(empty_df)


def test_check_valid_sale_is_greater_than_zero(sample_df):
    result = check_valid_sale(sample_df)
    assert result


def test_check_valid_sale_raises_error(bad_df):
    with pytest.raises(ValueError):
        check_valid_sale(bad_df)


def test_clean_urls_returns_correct_format(sample_df):
    result = clean_urls(sample_df)
    assert result["url"].iloc[0].startswith("https://")
    assert result["art_url"].iloc[1].startswith("https://")


def test_clean_urls_raises_error(bad_df):
    with pytest.raises(ValueError):
        clean_urls(bad_df)


def test_clean_dataframe_returns_not_empty_sample_df(sample_df):
    result = clean_dataframe(sample_df)
    assert not result.empty
    assert "utc_date" in result.columns


def test_clean_dataframe_raises_error(empty_df):
    with pytest.raises(ValueError):
        clean_dataframe(empty_df)


def test_handle_missing_values(bad_df):
    result = handle_missing_values(bad_df)
    assert len(result) == 1
    assert result["artist_name"].isnull().sum() == 0


def test_filter_by_currency_default(sample_df):
    result = filter_by_currency(sample_df)
    assert result["currency"].isin(["USD", "GBP", "EUR"]).all()
    assert result.shape[0] == 2


def test_filter_by_currency_custom(sample_df):
    result = filter_by_currency(sample_df, accepted=["EUR"])
    assert result.empty


def test_validate_foreign_keys_structure(sample_df):
    result = validate_foreign_keys(sample_df)
    assert isinstance(result, bool)


@patch("transform.get_id_mapping")
def test_get_id_mapping_valid(mock_query_func, sample_df):
    mock_query_func.return_value = pd.DataFrame({
        "artist_name": ["Alex Lynch", "Blackchild (ITA)"],
        "artist_id": [100, 101]
    })
    result = get_id_mapping(sample_df, table_name="artists")
    assert isinstance(result, dict)
    assert result["Alex Lynch"] == 100


@patch("transform.get_id_mapping")
def test_add_foreign_key_column_works(mock_get_mapping, sample_df):
    mock_get_mapping.return_value = {
        "Alex Lynch": 100,
        "Blackchild (ITA)": 101
    }
    sample_df = sample_df.copy()
    result = add_foreign_key_column(sample_df)
    assert "foreign_key" in result.columns
    assert result["foreign_key"].tolist() == [100, 101]


@patch("transform.add_foreign_key_column")
@patch("transform.validate_foreign_keys", return_value=True)
@patch("transform.filter_by_currency")
@patch("transform.group_sales_by")
def test_clean_dataframe_pipeline(mock_group, mock_filter, mock_validate, mock_fk, sample_df):
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
def test_export_dataframe_call(mock_to_csv, sample_df):
    export_dataframe(sample_df, output_path="mock_path.csv")
    mock_to_csv.assert_called_once_with("mock_path.csv", index=False)
