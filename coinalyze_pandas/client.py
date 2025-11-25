import httpx
import pandas as pd
from dataclasses import dataclass, field
from cachetools.func import ttl_cache


def preprocess_df(
    df: pd.DataFrame,
) -> pd.DataFrame:
    for column, unit in {"t": "s", "expire_at": "ms", "update": "ms"}.items():
        if column in df.columns:
            df[column] = pd.to_datetime(df[column], utc=True, unit=unit)
    return df.rename(
        columns={
            "exchange": "exchange_code",
            "t": "timestamp",
        }
    )


@dataclass(unsafe_hash=True)
class CoinalyzePandasClient:
    api_key: str = field(repr=False)
    base_url: str = "https://api.coinalyze.net/v1"
    timeout: float = 10
    client: httpx.Client = field(init=False, repr=False)

    def __post_init__(self):
        self.client = httpx.Client(
            base_url=self.base_url,
            timeout=self.timeout,
            headers={"api_key": self.api_key},
        )

    def _get(self, endpoint: str, params: dict | None = None) -> dict | list:
        new_params = {}
        if params:
            for key, value in params.items():
                if value is not None:
                    if isinstance(value, pd.Timestamp):
                        value = value.timestamp()
                    elif key == "symbols" and isinstance(value, list):
                        value = ",".join(value)
                    new_params[key] = value
        r = self.client.get(endpoint, params=new_params)
        r.raise_for_status()
        return r.json()

    @ttl_cache(ttl=3600)  # 1 hour
    def exchanges(self) -> pd.DataFrame:
        raw = self._get(endpoint="/exchanges")
        df = pd.DataFrame(raw)
        df.columns = [f"exchange_{x}" for x in df.columns]
        return df

    def spot_markets(self) -> pd.DataFrame:
        raw = self._get(endpoint="/spot-markets")
        return preprocess_df(pd.DataFrame(raw)).merge(self.exchanges(), how="left")

    def future_markets(self) -> pd.DataFrame:
        raw = self._get(endpoint="/future-markets")
        return preprocess_df(pd.DataFrame(raw)).merge(self.exchanges(), how="left")

    def open_interest(self, symbols: list[str]) -> pd.DataFrame:
        raw = self._get(
            endpoint="/open-interest",
            params={"symbols": symbols},
        )
        return preprocess_df(pd.DataFrame(raw))

    def funding_rate(self, symbols: list[str]) -> pd.DataFrame:
        raw = self._get(
            endpoint="/funding-rate",
            params={"symbols": symbols},
        )
        return preprocess_df(pd.DataFrame(raw))

    def predicted_funding_rate(self, symbols: list[str]) -> pd.DataFrame:
        raw = self._get(
            endpoint="/predicted-funding-rate",
            params={"symbols": symbols},
        )
        return preprocess_df(pd.DataFrame(raw))

    def open_interest_history(
        self,
        symbols: list[str],
        interval: str = "1min",
        time_from: pd.Timestamp | None = None,
        time_to: pd.Timestamp | None = None,
        convert_to_usd: bool = False,
    ) -> pd.DataFrame:
        raw = self._get(
            endpoint="/open-interest-history",
            params={
                "symbols": symbols,
                "interval": interval,
                "from": time_from,
                "to": time_to,
                "convert_to_usd": convert_to_usd,
            },
        )
        df = pd.json_normalize(raw, record_path="history", meta="symbol")
        return preprocess_df(
            df,
        ).rename(
            columns={
                "o": "open",
                "h": "high",
                "l": "low",
                "c": "close",
            }
        )

    def funding_rate_history(
        self,
        symbols: list[str],
        interval: str = "1min",
        time_from: pd.Timestamp | None = None,
        time_to: pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        raw = self._get(
            endpoint="/funding-rate-history",
            params={
                "symbols": symbols,
                "interval": interval,
                "from": time_from,
                "to": time_to,
            },
        )
        df = pd.json_normalize(raw, record_path="history", meta="symbol")
        return preprocess_df(
            df,
        ).rename(
            columns={
                "o": "open",
                "h": "high",
                "l": "low",
                "c": "close",
            }
        )

    def predicted_funding_rate_history(
        self,
        symbols: list[str],
        interval: str = "1min",
        time_from: pd.Timestamp | None = None,
        time_to: pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        raw = self._get(
            endpoint="/predicted-funding-rate-history",
            params={
                "symbols": symbols,
                "interval": interval,
                "from": time_from,
                "to": time_to,
            },
        )
        df = pd.json_normalize(raw, record_path="history", meta="symbol")
        return preprocess_df(
            df,
        ).rename(
            columns={
                "o": "open",
                "h": "high",
                "l": "low",
                "c": "close",
            }
        )

    def liquidation_history(
        self,
        symbols: list[str],
        interval: str = "1min",
        time_from: pd.Timestamp | None = None,
        time_to: pd.Timestamp | None = None,
        convert_to_usd: bool = False,
    ) -> pd.DataFrame:
        raw = self._get(
            endpoint="/liquidation-history",
            params={
                "symbols": symbols,
                "interval": interval,
                "from": time_from,
                "to": time_to,
                "convert_to_usd": convert_to_usd,
            },
        )
        df = pd.json_normalize(raw, record_path="history", meta="symbol")
        return preprocess_df(
            df,
        ).rename(
            columns={
                "l": "longs",
                "s": "shorts",
            }
        )

    def long_short_ratio_history(
        self,
        symbols: list[str],
        interval: str = "1min",
        time_from: pd.Timestamp | None = None,
        time_to: pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        raw = self._get(
            endpoint="/long-short-ratio-history",
            params={
                "symbols": symbols,
                "interval": interval,
                "from": time_from,
                "to": time_to,
            },
        )
        df = pd.json_normalize(raw, record_path="history", meta="symbol")
        return preprocess_df(
            df,
        ).rename(
            columns={
                "r": "ratio",
                "l": "longs",
                "s": "shorts",
            }
        )

    def ohlcv_history(
        self,
        symbols: list[str],
        interval: str = "1min",
        time_from: pd.Timestamp | None = None,
        time_to: pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        raw = self._get(
            endpoint="/ohlcv-history",
            params={
                "symbols": symbols,
                "interval": interval,
                "from": time_from,
                "to": time_to,
            },
        )
        df = pd.json_normalize(raw, record_path="history", meta="symbol")
        return preprocess_df(
            df,
        ).rename(
            columns={
                "o": "open",
                "h": "high",
                "l": "low",
                "c": "close",
                "v": "volume",
                "bv": "buy_volume",
                "tx": "total_trades",
                "btx": "buy_trades",
            }
        )

    def close(self):
        self.client.close()
