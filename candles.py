# ============================================================
# Candlestick Pattern Detection
# ============================================================
import talib as ta


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