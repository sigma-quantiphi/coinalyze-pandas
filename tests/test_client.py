import os

import pytest
import pandas as pd
from dotenv import load_dotenv
from src import CoinalyzePandasClient


@pytest.fixture
def coinalyze_client():
    """Fixture to initialize CoinalyzePandasClient with a mocked API key."""
    load_dotenv()
    api_key = os.environ.get("COINALYZE_API_KEY")
    return CoinalyzePandasClient(api_key=api_key)


@pytest.fixture
def date_range():
    """Fixture to provide date range for tests."""
    now = pd.Timestamp.utcnow()
    return now.floor("1d"), now.ceil("1d")


@pytest.mark.parametrize(
    "symbols", [["BTCUSDT_PERP.A", "BTCUSDC_PERP.0", "BTCUSD_PERP.0"]]
)
def test_exchanges(coinalyze_client, symbols):
    exchanges = coinalyze_client.exchanges()
    assert exchanges is not None


def test_futures(coinalyze_client):
    futures = coinalyze_client.future_markets()
    assert futures is not None


def test_spot_markets(coinalyze_client):
    spot = coinalyze_client.spot_markets()
    assert spot is not None


@pytest.mark.parametrize(
    "symbols", [["BTCUSDT_PERP.A", "BTCUSDC_PERP.0", "BTCUSD_PERP.0"]]
)
def test_open_interest(coinalyze_client, symbols):
    open_interest = coinalyze_client.open_interest(symbols=symbols)
    assert open_interest is not None


@pytest.mark.parametrize(
    "symbols", [["BTCUSDT_PERP.A", "BTCUSDC_PERP.0", "BTCUSD_PERP.0"]]
)
def test_funding_rate(coinalyze_client, symbols):
    funding_rate = coinalyze_client.funding_rate(symbols=symbols)
    assert funding_rate is not None


@pytest.mark.parametrize(
    "symbols", [["BTCUSDT_PERP.A", "BTCUSDC_PERP.0", "BTCUSD_PERP.0"]]
)
def test_predicted_funding_rate(coinalyze_client, symbols):
    predicted_funding_rate = coinalyze_client.predicted_funding_rate(symbols=symbols)
    assert predicted_funding_rate is not None


@pytest.mark.parametrize(
    "symbols", [["BTCUSDT_PERP.A", "BTCUSDC_PERP.0", "BTCUSD_PERP.0"]]
)
def test_open_interest_history(coinalyze_client, date_range, symbols):
    time_from, time_to = date_range
    open_interest_history = coinalyze_client.open_interest_history(
        symbols=symbols, time_from=time_from, time_to=time_to, interval="1min"
    )
    assert open_interest_history is not None


@pytest.mark.parametrize(
    "symbols", [["BTCUSDT_PERP.A", "BTCUSDC_PERP.0", "BTCUSD_PERP.0"]]
)
def test_funding_rate_history(coinalyze_client, date_range, symbols):
    time_from, time_to = date_range
    funding_rate_history = coinalyze_client.funding_rate_history(
        symbols=symbols, time_from=time_from, time_to=time_to
    )
    assert funding_rate_history is not None


@pytest.mark.parametrize(
    "symbols", [["BTCUSDT_PERP.A", "BTCUSDC_PERP.0", "BTCUSD_PERP.0"]]
)
def test_predicted_funding_rate_history(coinalyze_client, date_range, symbols):
    time_from, time_to = date_range
    predicted_funding_rate_history = coinalyze_client.predicted_funding_rate_history(
        symbols=symbols, time_from=time_from, time_to=time_to
    )
    assert predicted_funding_rate_history is not None


@pytest.mark.parametrize(
    "symbols", [["BTCUSDT_PERP.A", "BTCUSDC_PERP.0", "BTCUSD_PERP.0"]]
)
def test_liquidation_history(coinalyze_client, date_range, symbols):
    time_from, time_to = date_range
    liquidations = coinalyze_client.liquidation_history(
        symbols=symbols, time_from=time_from, time_to=time_to, interval="1min"
    )
    assert liquidations is not None


@pytest.mark.parametrize(
    "symbols", [["BTCUSDT_PERP.A", "BTCUSDC_PERP.0", "BTCUSD_PERP.0"]]
)
def test_long_short_ratio_history(coinalyze_client, date_range, symbols):
    time_from, time_to = date_range
    long_short_ratio_history = coinalyze_client.long_short_ratio_history(
        symbols=symbols, time_from=time_from, time_to=time_to, interval="1min"
    )
    assert long_short_ratio_history is not None


@pytest.mark.parametrize(
    "symbols", [["BTCUSDT_PERP.A", "BTCUSDC_PERP.0", "BTCUSD_PERP.0"]]
)
def test_ohlcv_history(coinalyze_client, date_range, symbols):
    time_from, time_to = date_range
    ohlcv = coinalyze_client.ohlcv_history(
        symbols=symbols, time_from=time_from, time_to=time_to
    )
    assert ohlcv is not None
