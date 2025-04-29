import pytest
import requests
from unittest.mock import patch
from dags import api_consume

@pytest.mark.parametrize("budget_str,expected", [
    ("US$ 2 million [ 4 ]", 2000000),
    ("USD$ 1.5 million", 1500000),
    ("$1.2 million", 1200000),
    ("£1.3 million", int(1.3 * 1_000_000 * 1.56)),
    ("₤1.1 million", int(1.1 * 1_000_000 * 1.56)),
    ("€1.3 million", int(1.3 * 1_000_000 * 1.3)),
    ("US$ 2", 2),  # no "million" keyword
    ("US$ 2.5", 2),  # will floor float to int
    ("0.0", 0),
    ("0", 0),
    ("Error: something failed", 0),
    (None, 0),
    ("US$ 1.5–2 million", 1500000),
    ("$1.2 million [approx]", 1200000),
    ("USD$ 1,200,000", 1200000),
    ("$1,200,000–$2,275,000 [1]", 1200000),
])
# Test the parse bugget function with various inputs
def test_parse_budget_to_usd(budget_str, expected):
    assert api_consume.parse_budget_to_usd(budget_str) == expected


def test_fetch_budget_success():
    # Sample response data
    mock_response = {
        "Budget": "US$ 100 million"
    }

    # Mock the requests.get method to return a predefined response
    with patch('requests.get') as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response
        
        url = "http://example.com/movie-details"
        result = api_consume.fetch_budget(url)
        
        # Assert that the correct budget was returned
        assert result == "US$ 100 million"
        mock_get.assert_called_once_with(url, timeout=10)


# Test when the "Budget" field is missing in the response
def test_fetch_budget_missing_budget():
    # Sample response with no "Budget" field
    mock_response = {}

    with patch('requests.get') as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response
        
        url = "http://example.com/movie-details"
        result = api_consume.fetch_budget(url)
        
        # Assert that "0.0" is returned when the Budget field is missing
        assert result == "0.0"
        mock_get.assert_called_once_with(url, timeout=10)


# Test when there's a request error (e.g., network issues)
def test_fetch_budget_request_error():
    with patch('requests.get') as mock_get:
        # Simulate a request error (e.g., timeout or connection error)
        mock_get.side_effect = requests.RequestException("Connection error")
        
        url = "http://example.com/movie-details"
        result = api_consume.fetch_budget(url)
        
        # Assert that the error message is returned
        assert result == "Error: Connection error"
        mock_get.assert_called_once_with(url, timeout=10)


# Test when there's an invalid JSON response
def test_fetch_budget_invalid_json():
    with patch('requests.get') as mock_get:
        # Simulate an invalid JSON response
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.side_effect = ValueError("Invalid JSON")
        
        url = "http://example.com/movie-details"
        result = api_consume.fetch_budget(url)
        
        # Assert that the error message for invalid JSON is returned
        assert result == "Error: Invalid JSON response"
        mock_get.assert_called_once_with(url, timeout=10)
        