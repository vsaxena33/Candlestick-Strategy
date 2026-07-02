# ============================================================
# Imports
# ============================================================
import pandas as pd


# ============================================================
# Configuration
# ============================================================
timeZone = 'Asia/Kolkata'


# ============================================================
# Update Logic for Live Data
# ============================================================
def update_live_data(data, message, last_total_volume):
    """
    Update the OHLCV dataframe using incoming websocket tick data.

    The Fyers WebSocket does not stream pre-built candles. Instead, it sends
    raw tick messages with the latest traded price (LTP) and the total
    cumulative volume traded since market open (9:15 AM).

    This function is responsible for converting those raw ticks into a
    proper OHLCV candle structure — updating an existing candle if the tick
    belongs to the current minute, or creating a new candle if a new minute
    has started.

    Volume Handling:
        The websocket provides CUMULATIVE volume for the entire session.
        To get the volume for just the current candle, we compute:

            Incremental Volume = Current Total Volume - Previous Total Volume

        This gives us only the shares traded since the last tick.

    Parameters
    ----------
    data : pandas.DataFrame
        The existing OHLCV dataframe, indexed by timezone-aware timestamps.
        Each row represents one completed or in-progress minute candle.

    message : dict
        A single incoming tick message from the Fyers WebSocket. Expected keys:
        - 'symbol'          : The trading symbol (used to validate message type).
        - 'ltp'             : Last Traded Price — the current market price.
        - 'vol_traded_today': Cumulative shares traded since session open.

    last_total_volume : int or None
        The cumulative volume value received from the previous tick.
        Used to calculate how much volume was traded in just this tick.
        None on the very first tick of the session.

    Returns
    -------
    tuple : (pandas.DataFrame, int)
        - Updated OHLCV dataframe with the latest candle reflected.
        - The latest cumulative volume, to be stored and passed in on the next tick.
    """

    # Ignore malformed or non-market messages (e.g., heartbeat or error frames)
    # that don't contain a 'symbol' field
    if "symbol" not in message:
        return data, last_total_volume

    # Extract the Last Traded Price — the core price data point for this tick
    ltp = message.get('ltp')

    # If for some reason there's no price in this message, skip it entirely
    if ltp is None:
        return data, last_total_volume

    # Extract cumulative session volume. This is the TOTAL shares traded
    # from 9:15 AM up to this exact tick — not just this candle's volume.
    total_vol = message.get('vol_traded_today')

    # Round the current wall-clock time down to the nearest minute.
    # This gives us the "candle timestamp" — e.g., 10:05:42 → 10:05:00.
    # All ticks within the same minute share this same candle timestamp.
    timestamp = pd.Timestamp.now(tz=timeZone).floor('1min')

    # If the websocket didn't send volume data in this tick, fall back to
    # the last known volume so we don't accidentally reset our baseline
    if total_vol is None:
        total_vol = last_total_volume if last_total_volume is not None else 0

    # Compute how many shares were traded ONLY in this tick interval.
    # On the very first tick, we have no baseline to compare against,
    # so we conservatively set incremental volume to 0.
    if last_total_volume is None:
        incremental_vol = 0
    else:
        incremental_vol = total_vol - last_total_volume

    # Safeguard against negative incremental volume, which can occasionally
    # occur during data anomalies or reconnections mid-session
    if incremental_vol < 0:
        incremental_vol = 0

    # ── Candle Update or Creation ──────────────────────────────────────────
    # Check if the latest candle in our dataframe belongs to the current minute.
    # If yes → update it in place. If no → start a brand new candle row.

    if len(data) > 0 and data.index[-1] == timestamp:
        # ── Same minute: UPDATE the existing candle ──
        # The Close is always the latest traded price
        data.loc[data.index[-1], 'close'] = ltp

        # High is the maximum price seen so far within this minute
        data.loc[data.index[-1], 'high'] = max(data.iloc[-1]['high'], ltp)

        # Low is the minimum price seen so far within this minute
        data.loc[data.index[-1], 'low'] = min(data.iloc[-1]['low'], ltp)

        # Accumulate volume tick by tick within the same candle
        data.loc[data.index[-1], 'volume'] += incremental_vol

    else:
        # ── New minute: CREATE a fresh candle ──
        # All four OHLC prices start at LTP (the first tick of this candle)
        new_candle = pd.DataFrame(
            [{'open': ltp, 'high': ltp, 'low': ltp, 'close': ltp, 'volume': incremental_vol}],
            index=[timestamp]
        )
        # Append the new candle row to our running dataframe
        data = pd.concat([data, new_candle])

    return data, total_vol


