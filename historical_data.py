"""
================================================================================
HISTORICAL DATA SEEDING UTILITIES
================================================================================

Handles initialization pipeline querying, fetching structural backlogs from
Fyers Intraday API servers for data priming.
"""

# ============================================================
# Imports
# ============================================================
from fyers_apiv3 import fyersModel
import pandas as pd
import datetime as dt
import pytz


# ============================================================
# Configuration
# ============================================================
symbol = 'NSE:RELIANCE-EQ'
timeZone = 'Asia/Kolkata'
resolution = "1"


# ============================================================
# Historical Data Fetching
# ============================================================
def fetch_historical_data(fyers: fyersModel.FyersModel) -> pd.DataFrame:
    """
    Fetch today's historical OHLCV candles from the Fyers REST API.

    Before connecting to the live WebSocket stream, we need to pre-populate
    our dataframe with candles from earlier in the trading day. Without this,
    the pattern detection engine would have no historical context to work with
    when the bot starts — for example, if launched at 11:00 AM, we'd miss
    all the candles from 9:15 AM to 10:59 AM.

    This function queries Fyers for all candles from midnight to now,
    converts the UNIX timestamps to IST, and returns a properly formatted
    OHLCV DataFrame ready for use with TA-Lib.

    Parameters
    ----------
    fyers : fyersModel.FyersModel
        An authenticated Fyers REST API client instance.

    Returns
    -------
    pandas.DataFrame
        OHLCV dataframe indexed by timezone-aware IST timestamps.
        Columns: ['open', 'high', 'low', 'close', 'volume']
    """

    # Get the current IST time to define our query window
    now = dt.datetime.now(pytz.timezone(timeZone))

    # Set the start of the query window to midnight today (not 9:15 AM,
    # since Fyers will automatically return only valid market-hours candles)
    start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)

    # Convert both boundaries to UNIX timestamps (integers), as required by Fyers API
    previous_time = start_of_day.timestamp()
    current_time = now.timestamp()

    # Build the Fyers historical data request payload
    nifty_data = {
        "symbol": symbol,           # Which stock/instrument to fetch
        "resolution": resolution,   # Candle size: "1" = 1-minute candles
        "date_format": "0",         # Use UNIX epoch timestamps (not human-readable strings)
        "range_from": int(previous_time),
        "range_to": int(current_time),
        "cont_flag": "1"            # For continuous data (relevant for futures)
    }

    # Send the REST API request and extract the candles array from the response
    response = fyers.history(data=nifty_data)
    historical_data = response['candles']   # List of [timestamp, open, high, low, close, volume]

    # Convert the raw list into a labeled DataFrame
    df = pd.DataFrame(historical_data, columns=['date', 'open', 'high', 'low', 'close', 'volume'])

    # Fyers returns timestamps as UNIX epoch integers in UTC.
    # Step 1: Parse integers into pandas Timestamps (UTC)
    # Step 2: Convert from UTC to Indian Standard Time (IST = UTC+5:30)
    df['date'] = pd.to_datetime(df['date'], unit='s')
    df['date'] = df['date'].dt.tz_localize('UTC').dt.tz_convert(pytz.timezone(timeZone))

    # Use the timestamp column as the DataFrame index for easy time-based lookup
    df.set_index('date', inplace=True)

    return df