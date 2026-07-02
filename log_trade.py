"""
================================================================================
TRADE LOGGING COMPONENT
================================================================================

Provides automated IO appending tools to output standardized execution sheets.
"""

# ============================================================
# Import
# ============================================================
import datetime as dt
import pytz

# ============================================================
# Configuration
# ============================================================
timeZone   = 'Asia/Kolkata'

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
        file.write(f"{dt.datetime.now(pytz.timezone(timeZone))},{action},'candlesticks',{symbol},{price}\n")