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
from fyers_apiv3.FyersWebsocket import data_ws      # Fyers WebSocket client for live tick streaming
from fyers_apiv3 import fyersModel                  # Fyers REST API client for quotes and history
from credentials import client_id                   # Your Fyers App ID (kept in a separate file for security)
from historical_data import fetch_historical_data   # Pre-populate today's candles from Fyers REST API
from market_engine import Candlestick               # Core strategy engine for pattern detection and trade execution

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

    candlestick.fyersSocket = fyersSocket       # Pass the WebSocket instance to the Candlestick engine

    # ── Step 5: Start Streaming ────────────────────────────────────────────
    # Initiates the WebSocket connection. Once connected, the onopen callback
    # fires, subscribes to live data, and the system runs continuously
    # until manually stopped or the market closes.
    print("Connecting to live stream...")
    fyersSocket.connect()