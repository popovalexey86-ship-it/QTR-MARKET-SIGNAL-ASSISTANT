from market_signal_assistant.models import AssetClass, Instrument

MARKET_INSTRUMENTS = (
    Instrument("BTCUSDT", AssetClass.CRYPTO),
    Instrument("ETHUSDT", AssetClass.CRYPTO),
    Instrument("SOLUSDT", AssetClass.CRYPTO),
    Instrument("AAPL", AssetClass.STOCK),
    Instrument("SPY", AssetClass.FUND),
    Instrument("EURUSD=X", AssetClass.FOREX),
)

CRYPTO_PRESET = MARKET_INSTRUMENTS[:3]
MARKETS_PRESET = (MARKET_INSTRUMENTS[0], *MARKET_INSTRUMENTS[3:])
