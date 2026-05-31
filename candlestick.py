"""
================================================================================
REAL-TIME CANDLESTICK PATTERN DETECTION & ALGORITHMIC EXECUTION ENGINE
================================================================================

This program creates an autonomous, real-time algorithmic trading system
using live market data from the FYERS WebSocket API. It dynamically detects
candlestick patterns using the `TA-Lib` library and executes a rule-based
trading strategy with live risk management.

The system continuously streams live tick-by-tick market data to:

1. Build Real-Time OHLCV Candles:
   Processes cumulative session volume into incremental, minute-by-minute
   candle volume while dynamically updating Open, High, Low, and Close prices
   on every incoming tick.

2. Detect Candlestick Patterns:
   Evaluates finalized candles against a comprehensive library of 60+
   TA-Lib candlestick pattern functions to identify high-probability
   bullish or bearish reversal signals.

3. Execute Algorithmic Trade Signals:
   Evaluates setups exactly once per candle close to eliminate mid-candle
   noise and avoid signal duplication:
   - LONG Entry:  Triggered when a bullish candlestick pattern is confirmed
                  on the last closed candle.
   - SHORT Entry: Triggered when a bearish candlestick pattern is confirmed
                  on the last closed candle.

4. Manage Active Risk & Live Exits:
   - Queries live Market Depth (Bid/Ask) to simulate realistic execution fills.
   - Implements a dynamic, structure-based Trailing Stop Loss (trailing to the
     low/high of the last closed candle) and automatically recalculates the
     Take Profit target to maintain a fixed Risk-to-Reward ratio.
   - Exits immediately if a counter-trend pattern appears on a new candle.
   - Persists all execution events chronologically to 'trades_log.csv'.

The project demonstrates production-grade architecture used in:
- Quantitative Trading & Strategy Automation
- Event-Driven WebSocket Streaming Data Engineering
- Live State Management & WebSocket Callbacks
- Algorithmic Risk Containment & Trailing Stop Allocations

Libraries Used:
- FYERS API v3   (fyersModel, FyersWebsocket)
- TA-Lib         (60+ candlestick pattern recognition functions)
- pandas & numpy (OHLCV DataFrame management)
- pytz           (timezone-aware timestamp handling)

Author: Vaibhav Saxena
================================================================================
"""


# ============================================================
# Imports
# ============================================================
from fyers_apiv3.FyersWebsocket import data_ws  # Fyers WebSocket client for live tick streaming
from fyers_apiv3 import fyersModel              # Fyers REST API client for quotes and history
from credentials import client_id               # Your Fyers App ID (kept in a separate file for security)
import datetime as dt                           # Standard date/time utilities
import pandas as pd                             # DataFrame for managing OHLCV candle data
import pytz                                     # Timezone conversion (UTC → IST)
import talib as ta                              # TA-Lib: technical analysis and candlestick pattern library


# ============================================================
# Configuration
# ============================================================

# The trading symbol in Fyers format: Exchange:Ticker-Type
symbol = 'NSE:RELIANCE-EQ'

# All timestamps are converted to and stored in Indian Standard Time
timeZone = 'Asia/Kolkata'

# Candle resolution in minutes. "1" means 1-minute candles.
resolution = "1"


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


# ============================================================
# Trade Logger
# ============================================================
def log_trade(action, symbol, price):
    """
    Append a single trade event to the persistent CSV trade log.

    Each row in the log represents one entry or exit event, including
    a timestamp, direction, symbol, and the simulated execution price.
    The log is opened in append mode so records accumulate across sessions.

    Parameters
    ----------
    action : str
        Direction of the trade: "Buy" for long entries/short exits,
        "Sell" for short entries/long exits.

    symbol : str
        The trading symbol (e.g., 'NSE:RELIANCE-EQ').

    price : float
        The simulated execution price (ask for buys, bid for sells),
        sourced from live market depth at the moment of signal.
    """
    with open("trades_log.csv", "a") as file:
        # Write one CSV row: timestamp, action, symbol, price
        file.write(f"{dt.datetime.now(pytz.timezone(timeZone))},{action},{symbol},{price}\n")


# ============================================================
# Candlestick Pattern Detection
# ============================================================
def candle_signal(data):
    """
    Scan the most recent candles for a recognized bullish or bearish pattern.

    This function iterates through a curated list of 60+ TA-Lib candlestick
    pattern recognition functions and returns the first pattern detected,
    along with an 'index' value indicating how many candles back the pattern's
    anchor candle (used for Stop Loss placement) lies.

    Pattern Evaluation Rules:
        - Most patterns are evaluated at iloc[-2] — the last CLOSED candle.
          (iloc[-1] is the live, still-forming candle and is intentionally excluded.)
        - Patterns that require a confirmation candle after the signal candle
          (e.g., Doji, Hammer, Spinning Top) are evaluated at iloc[-3], because
          we check whether the NEXT candle (iloc[-2]) has already confirmed the signal
          by closing above the pattern's high (bullish) or below its low (bearish).
        - Patterns requiring a 'penetration' parameter (e.g., Morning/Evening Star,
          Dark Cloud Cover) are called with penetration=0 to disable the threshold filter.

    Stop Loss Index:
        The 'index' value paired with each pattern indicates the candle position
        used for Stop Loss calculation:
        - index=1 → SL based on the signal candle itself (iloc[-2])
        - index=2 → SL based on the candle before the signal (iloc[-3])
        - index=3 → SL based on two candles before the signal (iloc[-4])
        - index=5 → SL based on four candles before the signal (iloc[-6])
        This is used by the caller as: self.data.iloc[-1 - index]

    Special Cases:
        - CDL3INSIDE: The SL index is determined dynamically based on which
          candle's low/high is more extreme.
        - CDLDOJI / CDLHAMMER / CDLLONGLEGGEDDOJI / CDLRICKSHAWMAN / CDLSPINNINGTOP:
          These are context-dependent patterns. A raw TA-Lib signal is only acted
          upon if the NEXT candle has already closed beyond the pattern's range
          (i.e., confirmed breakout above high or breakdown below low).
        - CDLHAMMER: Only treated as bullish if the next candle confirms by
          closing above the hammer's high.

    Parameters
    ----------
    data : pandas.DataFrame
        A slice of the OHLCV dataframe (typically the last 10 candles).
        Must contain columns: 'open', 'high', 'low', 'close'.
        Must have at least 10 rows for reliable TA-Lib lookback computation.

    Returns
    -------
    tuple : (int, int)
        - candle : Signal value. +100 = bullish, -100 = bearish, 0 = no signal.
        - index  : Number of candles back to use for Stop Loss placement.
                   0 if no signal detected.
    """

    # ── Pattern Registry ──────────────────────────────────────────────────
    # Each entry is a tuple of (TA-Lib function name, SL index).
    # Patterns are checked in order — the FIRST match is returned.
    # Patterns at the top of the list are given higher priority.
    patterns = [
        ("CDL2CROWS", 2),           ("CDL3BLACKCROWS", 3),      ("CDL3INSIDE", 2),          ("CDL3LINESTRIKE", 1),      ("CDL3OUTSIDE", 2),      ("CDL3STARSINSOUTH", 3),
        ("CDL3WHITESOLDIERS", 3),   ("CDLABANDONEDBABY", 2),    ("CDLADVANCEBLOCK", 1),     ("CDLBELTHOLD", 1),         ("CDLBREAKAWAY", 2),
        ("CDLCLOSINGMARUBOZU", 1),  ("CDLCONCEALBABYSWALL", 1), ("CDLCOUNTERATTACK", 1),    ("CDLDARKCLOUDCOVER", 1),   ("CDLDOJI", 1),
        ("CDLDOJISTAR", 2),         ("CDLDRAGONFLYDOJI", 1),    ("CDLENGULFING", 1),        ("CDLEVENINGDOJISTAR", 2),  ("CDLEVENINGSTAR", 2),
        ("CDLGAPSIDESIDEWHITE", 3), ("CDLGRAVESTONEDOJI", 1),   ("CDLHAMMER", 1),           ("CDLHANGINGMAN", 1),       ("CDLHARAMI", 2),
        ("CDLHARAMICROSS", 2),      ("CDLHIGHWAVE", 1),         ("CDLHIKKAKE", 2),          ("CDLHIKKAKEMOD", 2),       ("CDLHOMINGPIGEON", 2),
        ("CDLIDENTICAL3CROWS", 3),  ("CDLINNECK", 2),           ("CDLINVERTEDHAMMER", 1),   ("CDLKICKING", 2),          ("CDLKICKINGBYLENGTH", 2),
        ("CDLLADDERBOTTOM", 2),     ("CDLLONGLEGGEDDOJI", 1),   ("CDLLONGLINE", 1),         ("CDLMARUBOZU", 1),         ("CDLMATCHINGLOW", 1),
        ("CDLMATHOLD", 5),          ("CDLMORNINGDOJISTAR", 2),  ("CDLMORNINGSTAR", 2),      ("CDLONNECK", 2),           ("CDLPIERCING", 1),
        ("CDLRICKSHAWMAN", 1),      ("CDLRISEFALL3METHODS", 5), ("CDLSEPARATINGLINES", 2),  ("CDLSHOOTINGSTAR", 1),     ("CDLSHORTLINE", 1),
        ("CDLSPINNINGTOP", 1),      ("CDLSTALLEDPATTERN", 1),   ("CDLSTICKSANDWICH", 1),    ("CDLTAKURI", 1),           ("CDLTASUKIGAP", 3),
        ("CDLTHRUSTING", 1),        ("CDLTRISTAR", 2),          ("CDLUNIQUE3RIVER", 2),     ("CDLUPSIDEGAP2CROWS", 1),  ("CDLXSIDEGAP3METHODS", 2),
    ]

    # Unpack OHLC columns as Series for direct use with TA-Lib functions
    open_, high, low, close = data["open"], data["high"], data["low"], data["close"]

    for pattern_name, index in patterns:

        # Dynamically fetch the TA-Lib function by name (e.g., ta.CDLENGULFING)
        func = getattr(ta, pattern_name)

        # ── Evaluate the Pattern ───────────────────────────────────────────
        if pattern_name in {
            # These patterns require a 'penetration' parameter to control
            # how deeply the second candle must close into the first candle's body.
            # Setting penetration=0 disables the filter and accepts all occurrences.
            "CDLABANDONEDBABY",
            "CDLDARKCLOUDCOVER",
            "CDLEVENINGDOJISTAR",
            "CDLEVENINGSTAR",
            "CDLMATHOLD",
            "CDLMORNINGDOJISTAR",
            "CDLMORNINGSTAR",
        }:
            # Read the signal at iloc[-2]: the last fully closed candle
            candle = func(open_, high, low, close, penetration=0).iloc[-2]

        elif pattern_name in {
            # These single-candle patterns need next-candle confirmation.
            # We evaluate the pattern at iloc[-3] (the signal candle)
            # and later check if iloc[-2] (the next candle) confirms direction.
            "CDLDOJI", "CDLHAMMER", "CDLLONGLEGGEDDOJI", "CDLRICKSHAWMAN", "CDLSPINNINGTOP"
        }:
            candle = func(open_, high, low, close).iloc[-3]

        else:
            # Standard case: evaluate at the last closed candle
            candle = func(open_, high, low, close).iloc[-2]

        # ── Non-Zero Signal Detected ───────────────────────────────────────
        if candle != 0:

            # Special case: CDL3INSIDE (Harami + confirmation candle)
            # The SL index is dynamic — it points to whichever candle
            # has the more extreme high/low relevant to trade direction.
            if pattern_name == "CDL3INSIDE":
                index = (2 if low.iloc[-3] < low.iloc[-4] else 3) if candle > 0 else (2 if high.iloc[-3] > high.iloc[-4] else 3)

            # Special case: Doji-family patterns and Spinning Tops
            # These are neutral by themselves — we require the NEXT candle
            # to have already broken above the pattern high (bullish confirmation)
            # or below the pattern low (bearish confirmation) before acting.
            elif pattern_name in {"CDLDOJI", "CDLLONGLEGGEDDOJI", "CDLRICKSHAWMAN", "CDLSPINNINGTOP"}:
                if close.iloc[-2] > high.iloc[-3]:
                    # Next candle closed above the pattern's high → confirmed bullish breakout
                    candle, index = 100, 2
                elif close.iloc[-2] < low.iloc[-3]:
                    # Next candle closed below the pattern's low → confirmed bearish breakdown
                    candle, index = -100, 2
                # If neither condition is met, candle retains its raw TA-Lib value,
                # which will be returned as-is (could be bullish or bearish based on context)

            # Special case: Hammer
            # A Hammer is only treated as a bullish signal if the next candle
            # confirms by closing above the hammer's high. Otherwise we ignore it.
            elif pattern_name == "CDLHAMMER":
                if close.iloc[-2] > high.iloc[-3]:
                    candle, index = 100, 2
                # If not confirmed, we fall through and still return the raw signal

            # Return the first pattern match found — direction + SL index
            return candle, index

    # No pattern detected across all 60+ checks
    return 0, 0


# ============================================================
# Candlestick Class — Real-Time State Manager & Strategy Engine
# ============================================================
class Candlestick:
    """
    Core engine that manages live market state and executes the trading strategy.

    This class serves as the central hub of the trading system. It owns the
    live OHLCV dataframe, all active position state, and all four WebSocket
    callback methods that Fyers calls on each market event.

    Responsibilities:
    ─────────────────
    1. Candle Management:
       On every incoming tick, delegates to update_live_data() to keep
       the OHLCV dataframe current and accurate.

    2. Entry Logic (once per candle close, no position open):
       Calls candle_signal() exactly once per new closed candle.
       Opens a LONG position on a bullish signal, SHORT on bearish.
       Sets Stop Loss based on the pattern's anchor candle.
       Sets Take Profit at 2x the initial risk (1:2 RR ratio).
       Sets a trigger level for trailing stop activation.

    3. Exit Logic (evaluated every tick when in a position):
       - Immediate exit if LTP hits the Take Profit or Stop Loss level.
       - Once per new candle close: trails the Stop Loss if the candle
         closed beyond the trigger level, or exits if a counter-trend
         pattern is detected.

    4. WebSocket Event Handling:
       onopen()    → Subscribes to live data on connection.
       onmessage() → Main strategy loop, called on every market tick.
       onerror()   → Logs WebSocket errors.
       onclose()   → Logs WebSocket disconnections.

    Attributes
    ----------
    data : pandas.DataFrame
        Live OHLCV candle dataframe, updated on every tick.

    last_total_volume : int or None
        Tracks the previous tick's cumulative volume for incremental calculation.

    position : str or None
        Current open position: 'LONG', 'SHORT', or None.

    sl : float or None
        Current Stop Loss price level.

    tp : float or None
        Current Take Profit price level.

    trigger : float or None
        Price level that activates trailing stop logic.
        For LONG: the high of the entry candle (trail when close > trigger).
        For SHORT: the low of the entry candle (trail when close < trigger).

    last_evaluated_candle : pandas.Timestamp or None
        Timestamp of the last candle on which strategy logic was evaluated.
        Prevents duplicate signals within the same candle.

    fyers : fyersModel.FyersModel
        Authenticated Fyers REST API client for fetching live bid/ask quotes.
    """

    # ============================================================
    # Initialization
    # ============================================================
    def __init__(self, data, fyers):
        """
        Initialize the Candlestick engine with historical data and API client.

        Parameters
        ----------
        data : pandas.DataFrame
            Pre-loaded OHLCV dataframe from fetch_historical_data().
            Provides the pattern engine with candle history from earlier today.

        fyers : fyersModel.FyersModel
            Authenticated Fyers REST API client used to fetch live bid/ask prices
            at the moment of signal, simulating realistic execution fills.
        """
        self.data = data                        # OHLCV dataframe; grows with each new candle
        self.last_total_volume = None           # Baseline for incremental volume calculation
        self.position = None                    # No open position at startup
        self.sl = None                          # Stop Loss level (set on entry)
        self.tp = None                          # Take Profit level (set on entry)
        self.fyers = fyers                      # REST API client for live quotes
        self.trigger = None                     # Trailing stop activation level
        self.last_evaluated_candle = None       # Guards against re-evaluating the same candle


    # ============================================================
    # Position State Cleanup
    # ============================================================
    def _clear_position(self):
        """
        Reset all position-related state after a trade is closed.

        Called after every exit event (TP hit, SL hit, or reversal signal)
        to ensure the engine starts clean before evaluating the next entry.
        """
        self.position = None
        self.sl = None
        self.tp = None
        self.trigger = None


    # ============================================================
    # WebSocket Callback — Main Strategy Loop
    # ============================================================
    def onmessage(self, message):
        """
        Primary callback invoked by the Fyers WebSocket on every market tick.

        This is the heart of the trading engine. It is called hundreds of times
        per minute and must execute quickly and deterministically.

        Execution Flow (per tick):
        ──────────────────────────
        1. Update the live OHLCV candle with the new tick data.
        2. Identify the last CLOSED candle (iloc[-2]) and its timestamp.
        3. If no position is open and a new candle has just closed:
               → Run pattern detection on the last 10 candles.
               → If bullish signal: enter LONG, set SL/TP/trigger.
               → If bearish signal: enter SHORT, set SL/TP/trigger.
        4. If in a LONG position:
               → If LTP hits TP or SL: exit immediately.
               → Else if new candle closed:
                     → Trail SL if close broke above trigger.
                     → Else if bearish reversal pattern: exit.
        5. If in a SHORT position:
               → If LTP hits SL or TP: exit immediately.
               → Else if new candle closed:
                     → Trail SL if close broke below trigger.
                     → Else if bullish reversal pattern: exit.

        Design Note on closed_candle_time gating:
            All once-per-candle logic (pattern detection, trailing, reversals)
            is gated behind `closed_candle_time != self.last_evaluated_candle`.
            This ensures that even though onmessage() fires ~100+ times per minute,
            heavy operations like candle_signal() run only once per candle close.

        Parameters
        ----------
        message : dict
            Raw tick message from the Fyers WebSocket. Key fields:
            - 'ltp'             : Last Traded Price
            - 'vol_traded_today': Cumulative session volume
            - 'symbol'          : Instrument identifier
        """

        # ── Step 1: Update the live candle with incoming tick ──────────────
        self.data, self.last_total_volume = update_live_data(
            data=self.data,
            message=message,
            last_total_volume=self.last_total_volume
        )

        # Print the last few rows so we can monitor the live feed in the terminal
        print(self.data.tail())

        # ── Step 2: Identify the last CLOSED candle ────────────────────────
        # iloc[-1] = the live, still-forming candle (incomplete — never trade on this)
        # iloc[-2] = the last FULLY closed candle (safe to evaluate patterns on)
        closed_candle = self.data.iloc[-2]
        closed_candle_time = self.data.index[-2]


        # ── Step 3: Entry Logic ────────────────────────────────────────────
        # Only evaluate when: (a) no position is currently open, AND
        #                     (b) this candle has not already been evaluated
        if not self.position and closed_candle_time != self.last_evaluated_candle:

            # Mark this candle as evaluated to prevent duplicate signals
            # from the many ticks that will arrive during the same minute
            self.last_evaluated_candle = closed_candle_time

            # Run pattern detection on the last 10 candles.
            # Note: index returned here refers to positions within self.data (not the tail slice),
            # since both count from the end. Safe as long as index <= 9 and tail size is 10.
            # Falls back to (0, 0) if fewer than 10 candles are available (e.g., early morning).
            candle, index = candle_signal(data=self.data.tail(10)) if len(self.data) >= 10 else (0, 0)

            # ── LONG Entry ─────────────────────────────────────────────────
            if candle > 0:
                print(f"\n[EXECUTION] 🔥 Last closed candle is bullish!")

                # Fetch the live ASK price (best price at which we can BUY)
                ask = self.fyers.quotes(data={"symbols": symbol})['d'][0]['v']['ask']
                log_trade("Buy", symbol, ask)

                # Stop Loss = the LOW of the pattern's anchor candle
                # (index candles back from the live candle)
                self.sl = self.data.iloc[-1 - index]['low']

                # Take Profit = Entry + 2x the initial risk (1:2 Risk-Reward ratio)
                self.tp = ask + (ask - self.sl) * 2

                # Trigger = the HIGH of the signal candle.
                # Once the next candle closes ABOVE this, we start trailing the SL.
                self.trigger = closed_candle['high']

                self.position = 'LONG'

            # ── SHORT Entry ────────────────────────────────────────────────
            elif candle < 0:
                print(f"\n[EXECUTION] 🔥 Last closed candle is bearish!")

                # Fetch the live BID price (best price at which we can SELL)
                bid = self.fyers.quotes(data={"symbols": symbol})['d'][0]['v']['bid']
                log_trade("Sell", symbol, bid)

                # Stop Loss = the HIGH of the pattern's anchor candle
                self.sl = self.data.iloc[-1 - index]['high']

                # Take Profit = Entry - 2x the initial risk (1:2 Risk-Reward ratio)
                self.tp = bid - (self.sl - bid) * 2

                # Trigger = the LOW of the signal candle.
                # Once the next candle closes BELOW this, we start trailing the SL.
                self.trigger = closed_candle['low']

                self.position = 'SHORT'


        # ── Step 4: LONG Exit Logic ────────────────────────────────────────
        if self.position == 'LONG':
            ltp = message.get('ltp')

            # ── Immediate Exit: TP or SL hit on this tick ─────────────────
            if ltp >= self.tp or ltp <= self.sl:
                print(f"\n[EXIT] LONG position exited at LTP: {ltp}. TP was {self.tp}, SL was {self.sl}.")
                bid = self.fyers.quotes(data={"symbols": symbol})['d'][0]['v']['bid']
                log_trade("Sell", symbol, bid)
                self._clear_position()

            # ── Once-per-candle: Trailing or Reversal check ────────────────
            elif closed_candle_time != self.last_evaluated_candle:
                # Mark this candle as evaluated for the exit logic as well
                self.last_evaluated_candle = closed_candle_time

                if closed_candle['close'] > self.trigger:
                    # The candle closed above our trigger level → momentum is continuing.
                    # Trail the Stop Loss up to the LOW of this candle,
                    # and shift TP by the same amount to maintain the 1:2 RR ratio.
                    new_sl = closed_candle['low']
                    if new_sl > self.sl:                    # Only trail UP, never back down
                        diff = new_sl - self.sl
                        self.tp += diff                     # Shift TP up by the same amount
                        self.sl = new_sl                    # New SL = low of the latest candle
                        self.trigger = closed_candle['high'] # New trigger = high of this candle

                elif (candle := candle_signal(data=self.data.tail(10))[0]) < 0:
                    # A fresh bearish pattern appeared — trend may be reversing.
                    # Exit the LONG position immediately.
                    print(f"\n[EXECUTION] 🔥 New bearish candle detected while in LONG position. Time to exit!")
                    bid = self.fyers.quotes(data={"symbols": symbol})['d'][0]['v']['bid']
                    log_trade("Sell", symbol, bid)
                    self._clear_position()

        # ── Step 5: SHORT Exit Logic ───────────────────────────────────────
        elif self.position == 'SHORT':
            ltp = message.get('ltp')

            # ── Immediate Exit: SL or TP hit on this tick ─────────────────
            if ltp >= self.sl or ltp <= self.tp:
                print(f"\n[EXIT] SHORT position exited at LTP: {ltp}. TP was {self.tp}, SL was {self.sl}.")
                ask = self.fyers.quotes(data={"symbols": symbol})['d'][0]['v']['ask']
                log_trade("Buy", symbol, ask)
                self._clear_position()

            # ── Once-per-candle: Trailing or Reversal check ────────────────
            elif closed_candle_time != self.last_evaluated_candle:
                self.last_evaluated_candle = closed_candle_time

                if closed_candle['close'] < self.trigger:
                    # The candle closed below our trigger level → bearish momentum continues.
                    # Trail the Stop Loss down to the HIGH of this candle,
                    # and shift TP down by the same amount to maintain the 1:2 RR ratio.
                    new_sl = closed_candle['high']
                    if new_sl < self.sl:                    # Only trail DOWN, never back up
                        diff = self.sl - new_sl
                        self.tp -= diff                     # Shift TP down by the same amount
                        self.sl = new_sl                    # New SL = high of the latest candle
                        self.trigger = closed_candle['low'] # New trigger = low of this candle

                elif (candle := candle_signal(data=self.data.tail(10))[0]) > 0:
                    # A fresh bullish pattern appeared — trend may be reversing.
                    # Exit the SHORT position immediately.
                    print(f"\n[EXECUTION] 🔥 New bullish candle detected while in SHORT position. Time to exit!")
                    ask = self.fyers.quotes(data={"symbols": symbol})['d'][0]['v']['ask']
                    log_trade("Buy", symbol, ask)
                    self._clear_position()


    # ============================================================
    # WebSocket Callback — Error Handler
    # ============================================================
    def onerror(self, message):
        """
        Callback invoked by the Fyers WebSocket when a connection error occurs.

        Errors are printed to the console for monitoring. In a production system,
        this would also trigger alerts or automatic reconnection logic.

        Parameters
        ----------
        message : dict
            The error payload received from the WebSocket client.
        """
        print("Error:", message)


    # ============================================================
    # WebSocket Callback — Connection Close Handler
    # ============================================================
    def onclose(self, message):
        """
        Callback invoked by the Fyers WebSocket when the connection is closed.

        The WebSocket is configured with reconnect=True, so Fyers will
        automatically attempt to reconnect after a drop. This callback
        simply logs the event for awareness.

        Parameters
        ----------
        message : dict
            The close event payload from the WebSocket client.
        """
        print("Connection closed:", message)


    # ============================================================
    # WebSocket Callback — Connection Open / Subscription Handler
    # ============================================================
    def onopen(self):
        """
        Callback invoked by the Fyers WebSocket immediately after connection.

        This is where we subscribe to the live data feed for our target symbol.
        'SymbolUpdate' is the Fyers data type that provides real-time tick data
        including LTP, bid/ask, and cumulative volume.

        After subscribing, keep_running() is called to maintain the WebSocket
        connection in a persistent listening loop.
        """
        # Fyers data type for real-time tick updates (LTP, volume, bid/ask, etc.)
        data_type = "SymbolUpdate"

        # Subscribe to the configured symbol's live feed
        symbols = [symbol]
        fyersSocket.subscribe(symbols=symbols, data_type=data_type)

        # Keep the WebSocket alive and continuously receiving messages
        fyersSocket.keep_running()


# ============================================================
# Program Entry Point
# ============================================================
if __name__ == "__main__":
    """
    ============================================================
    HOW THE SYSTEM WORKS
    ============================================================

    WebSocket Tick
           ↓
    Update Current Candle (update_live_data)
           ↓
    Candle Closes
           ↓
    Detect Candlestick Patterns (candle_signal)
           ↓
    Execute Entry / Manage Exit (Candlestick.onmessage)

    ============================================================
    """

    # ── Step 1: Load Access Token ──────────────────────────────────────────
    # The access token authenticates all API calls to Fyers.
    # It is stored in a flat text file and must be refreshed daily
    # (Fyers tokens expire at the end of each trading session).
    try:
        with open('access_token.txt', 'r') as file:
            access_token = file.read()
    except FileNotFoundError:
        print("Error: access_token.txt not found! Please login first.")
        exit()

    # ── Step 2: Fetch Today's Historical Candles ───────────────────────────
    # Pre-populate the OHLCV dataframe with candles from earlier today
    # so the pattern engine has the historical context it needs.
    print("Fetching morning data...")
    fyers_connection = fyersModel.FyersModel(
        client_id=client_id,
        token=access_token,
        is_async=False,     # Synchronous mode: wait for each REST response before continuing
        log_path=''         # Empty string: suppress Fyers internal API logs
    )
    historical_df = fetch_historical_data(fyers=fyers_connection)

    # ── Step 3: Initialize the Strategy Engine ─────────────────────────────
    # The Candlestick object holds all position state and handles
    # all incoming WebSocket events through its callback methods.
    candlestick = Candlestick(historical_df, fyers_connection)

    # ── Step 4: Configure the WebSocket Connection ─────────────────────────
    # Wire up all four event callbacks to the Candlestick engine.
    # The access token is passed in "appid:token" format as required by Fyers.
    fyersSocket = data_ws.FyersDataSocket(
        access_token=access_token,              # Auth token for WebSocket handshake
        log_path="",                            # Suppress internal WebSocket logs
        litemode=False,                         # Full mode: receive all tick fields (bid, ask, volume, etc.)
        write_to_file=False,                    # Print to console, not to a log file
        reconnect=True,                         # Auto-reconnect if the connection drops
        on_connect=candlestick.onopen,          # Called once on successful connection → subscribes
        on_close=candlestick.onclose,           # Called on disconnection → logs the event
        on_error=candlestick.onerror,           # Called on WebSocket errors → logs the error
        on_message=candlestick.onmessage        # Called on every tick → runs the full strategy loop
    )

    # ── Step 5: Start Streaming ────────────────────────────────────────────
    # Initiates the WebSocket connection. Once connected, the onopen callback
    # fires, subscribes to live data, and the system runs continuously
    # until manually stopped or the market closes.
    print("Connecting to live stream...")
    fyersSocket.connect()
