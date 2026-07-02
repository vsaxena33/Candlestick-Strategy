# ============================================================
# Import
# ============================================================
from fyers_apiv3.FyersWebsocket import data_ws
from candle_engine import update_live_data
from log_trade import log_trade
from candles import candle_signal

# ============================================================
# Configuration
# ============================================================
symbol     = 'NSE:RELIANCE-EQ'

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
        self.cooldown_candles = 0               # Number of candles to skip after an exit signal
        self.fyersSocket: data_ws = None        # WebSocket client for live tick streaming (initialized later)


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

            if self.cooldown_candles > 0:
                self.cooldown_candles -= 1

            if self.cooldown_candles > 0:
                return

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


        print(self.position, self.sl, self.tp, self.trigger)
        # ── Step 4: LONG Exit Logic ────────────────────────────────────────
        if self.position == 'LONG':
            ltp = message.get('ltp')

            # ── Immediate Exit: TP or SL hit on this tick ─────────────────
            if ltp >= self.tp or ltp < self.sl:
                print(f"\n[EXIT] LONG position exited at LTP: {ltp}. TP was {self.tp}, SL was {self.sl}.")
                bid = self.fyers.quotes(data={"symbols": symbol})['d'][0]['v']['bid']
                log_trade("Sell", symbol, bid)
                self._clear_position()
                self.cooldown_candles = 3



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
            if ltp > self.sl or ltp <= self.tp:
                print(f"\n[EXIT] SHORT position exited at LTP: {ltp}. TP was {self.tp}, SL was {self.sl}.")
                ask = self.fyers.quotes(data={"symbols": symbol})['d'][0]['v']['ask']
                log_trade("Buy", symbol, ask)
                self._clear_position()
                self.cooldown_candles = 3

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
        self.fyersSocket.subscribe(symbols=symbols, data_type=data_type)

        # Keep the WebSocket alive and continuously receiving messages
        self.fyersSocket.keep_running()