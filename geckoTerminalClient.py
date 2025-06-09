import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import ccxt
import requests
import time
from typing import Dict, List, Optional

class GeckoTerminalClient:
    def __init__(self):
        self.base_url = "https://api.geckoterminal.com/api/v2"
        self.session = requests.Session()
        self.rate_limit = 30  # calls per minute
        self.calls = []

    def _rate_limit_check(self):
        """Enforce rate limiting: 30 calls per minute."""
        current_time = time.time()
        self.calls = [call for call in self.calls if current_time - call < 60]
        if len(self.calls) >= self.rate_limit:
            sleep_time = 60 - (current_time - self.calls[0])
            if sleep_time > 0:
                time.sleep(sleep_time)
        self.calls.append(current_time)

    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """Make an API request with rate limiting."""
        self._rate_limit_check()
        try:
            response = self.session.get(f"{self.base_url}{endpoint}", params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error making request to {endpoint}: {e}")
            return {}

    def fetch_pool_metrics(self, network: str, pool_address: str) -> Optional[Dict]:
        """Fetch TVL and 24h volume for a specific pool."""
        endpoint = f"/networks/{network}/pools/{pool_address}"
        response = self._make_request(endpoint)
        if not response or 'data' not in response or 'attributes' not in response['data']:
            print(f"No data found for pool {pool_address} on {network}")
            return None

        attributes = response['data']['attributes']
        try:
            tvl = float(attributes.get('reserve_in_usd', 0))
            volume = float(attributes.get('volume_usd', {}).get('h24', 0))
            return {
                'network': network,
                'pool_address': pool_address,
                'tvl_usd': tvl,
                'volume_24h_usd': volume,
                'fetch_timestamp': int(time.time())
            }
        except (ValueError, TypeError) as e:
            print(f"Error processing metrics for pool {pool_address} on {network}: {e}")
            return None

    def fetch_multi_pool_metrics(self, network: str, pool_addresses: List[str]) -> List[Dict]:
        """Fetch TVL and 24h volume for multiple pools."""
        if not pool_addresses:
            return []

        pool_addresses_str = ",".join(pool_addresses)
        endpoint = f"/networks/{network}/pools/multi/{pool_addresses_str}"
        response = self._make_request(endpoint)
        results = []
        if not response or 'data' not in response:
            print(f"No data found for pools on {network}")
            return results

        for pool_data in response['data']:
            try:
                attributes = pool_data.get('attributes', {})
                pool_address = attributes.get('address', '')
                tvl = float(attributes.get('reserve_in_usd', 0))
                volume = float(attributes.get('volume_usd', {}).get('h24', 0))
                results.append({
                    'network': network,
                    'pool_address': pool_address,
                    'tvl_usd': tvl,
                    'volume_24h_usd': volume,
                    'fetch_timestamp': int(time.time())
                })
            except (ValueError, TypeError) as e:
                print(f"Error processing metrics for pool {pool_address} on {network}: {e}")
                continue
        return results

    def fetch_pool_ohlcv(
        self, network: str, pool_address: str, timeframe: str = "day",
        aggregate: int = 1, before_timestamp: Optional[int] = None,
        limit: int = 100, currency: str = "usd", token: str = "base"
    ) -> Optional[List[Dict]]:
        """Fetch OHLCV data for a specific pool."""
        if network == "ethereum":
            network = "eth"
        endpoint = f"/networks/{network}/pools/{pool_address}/ohlcv/{timeframe}"
        params = {
            "aggregate": aggregate,
            "limit": min(limit, 1000),
            "currency": currency,
            "token": token
        }
        if before_timestamp:
            params["before_timestamp"] = before_timestamp

        response = self._make_request(endpoint, params=params)
        if not response or 'data' not in response or 'attributes' not in response['data']:
            print(f"No OHLCV data found for pool {pool_address} on {network}")
            return None

        try:
            ohlcv_data = response['data']['attributes']['ohlcv_list']
            results = [
                {
                    'timestamp': int(data[0]),
                    'datetime': pd.to_datetime(int(data[0]), unit='s'),
                    'open': float(data[1]),
                    'high': float(data[2]),
                    'low': float(data[3]),
                    'close': float(data[4]),
                    'volume': float(data[5])
                }
                for data in ohlcv_data
            ]
            return results
        except (KeyError, ValueError, TypeError) as e:
            print(f"Error processing OHLCV data for pool {pool_address} on {network}: {e}")
            return None

class YieldSamuraiClient:
    def __init__(self, api_key: Optional[str] = None):
        self.base_url = "https://api.yieldsamurai.com/v1"
        self.session = requests.Session()
        self.rate_limit = 10  # calls per minute (demo limit)
        self.calls = []
        self.api_key = api_key
        # Demo headers
        self.session.headers.update({
            "accept": "application/json",
            "Authorization": "demo" if not api_key else f"Bearer {api_key}"
        })

    def _rate_limit_check(self):
        """Enforce rate limiting: 10 calls per minute."""
        current_time = time.time()
        self.calls = [call for call in self.calls if current_time - call < 60]
        if len(self.calls) >= self.rate_limit:
            sleep_time = 60 - (current_time - self.calls[0])
            if sleep_time > 0:
                time.sleep(sleep_time)
        self.calls.append(current_time)

    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """Make an API request with rate limiting."""
        self._rate_limit_check()
        try:
            url = f"{self.base_url}{endpoint}"
            print(f"YieldSamurai request: {url} with params {params}")
            response = self.session.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error making request to {endpoint}: {e}")
            return {}

    def fetch_tvl(
        self, chain: str, pool_address: str, days: int = 7, interval: str = "hourly"
    ) -> Optional[List[Dict]]:
        """Fetch historical TVL for a pool (7 days max for demo)."""
        endpoint = f"/pools/{chain}/{pool_address}/historical"
        params = {
            "days": min(days, 7),  # Enforce demo limit
            "interval": interval
        }
        response = self._make_request(endpoint, params)
        if not response or 'records' not in response:
            print(f"No TVL data found for pool {pool_address} on {chain}")
            return None

        try:
            results = [
                {
                    'timestamp': int(record['timestamp']),
                    'datetime': pd.to_datetime(int(record['timestamp']), unit='s'),
                    'tvl_usd': float(record['tvl']['totalUsd'])
                }
                for record in response['records']
            ]
            # Save to CSV
            if results:
                tvl_df = pd.DataFrame(results)
                tvl_csv = f"yieldsamurai_tvl_{chain}_{pool_address[:6]}.csv"
                tvl_df.to_csv(tvl_csv, index=False)
                print(f"YieldSamurai TVL saved to {tvl_csv}")
            return results
        except (KeyError, ValueError, TypeError) as e:
            print(f"Error processing TVL data for pool {pool_address} on {chain}: {e}")
            return None