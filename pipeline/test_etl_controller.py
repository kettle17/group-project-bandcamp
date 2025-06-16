"""Test file for the etl_controller script."""

# pylint: skip-file

from unittest.mock import patch, MagicMock
from etl_controller import (run_pipeline, etl_lambda_handler)


class TestPipeline:
    """Tests for the ETL pipeline."""

    @patch("etl_controller.run_load")
    @patch("etl_controller.clean_dataframe")
    @patch("etl_controller.pd.read_csv")
    @patch("etl_controller.run_extract")
    def test_run_pipeline(self, mock_extract, mock_read_csv, mock_clean, mock_load):
        """Tests that each part of the etl pipeline is called at least once."""
        mock_df = MagicMock()
        mock_read_csv.return_value = mock_df
        mock_clean.return_value = mock_df

        run_pipeline()

        mock_extract.assert_called_once_with('data/output.csv')
        mock_read_csv.assert_called_once_with('data/output.csv')
        mock_clean.assert_called_once_with(mock_df)
        mock_load.assert_called_once_with(mock_df)


class TestLambdaHandler:
    """Tests for the Lambda Handler."""

    @patch("etl_controller.run_pipeline")
    def test_lambda_handler_success(self, mock_run_pipeline):
        """Tests that lambda_handler returns the correct output."""
        response = etl_lambda_handler({}, {})
        assert response["statusCode"] == 200
        assert "executed successfully" in response["body"]

    @patch("etl_controller.run_pipeline", side_effect=Exception("Boom"))
    def test_lambda_handler_failure(self, mock_run_pipeline):
        """Tests that lambda_handler handles the wrong output."""
        response = etl_lambda_handler({}, {})
        assert response["statusCode"] == 500
        assert "ETL pipeline failed" in response["body"]
        assert "Boom" in response["body"]
