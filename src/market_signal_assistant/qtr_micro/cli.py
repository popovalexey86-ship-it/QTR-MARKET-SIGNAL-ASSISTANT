from __future__ import annotations

import os
from collections.abc import Sequence

from market_signal_assistant.localized_argparse import RussianArgumentParser


def main(argv: Sequence[str] | None = None) -> None:
    parser = RussianArgumentParser(
        description="Fail-closed preflight для QTR Micro Bybit Demo."
    )
    parser.add_argument("command", choices=("preflight",))
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument(
        "--json",
        action="store_true",
        help="вывести безопасный machine-readable JSON",
    )
    args = parser.parse_args(argv)

    from market_signal_assistant.qtr_micro.client import (
        BybitDemoTradingClient,
        UrllibBybitDemoTransport,
    )
    from market_signal_assistant.qtr_micro.preflight import (
        QtrMicroPreflight,
        configuration_failure,
        format_preflight,
        preflight_json,
    )
    from market_signal_assistant.qtr_micro.settings import QtrMicroSettings
    from market_signal_assistant.qtr_micro.state import JsonQtrMicroStateStore

    symbol = str(args.symbol).upper()
    try:
        settings = QtrMicroSettings.from_environment()
        client = None
        if settings.credentials_present:
            client = BybitDemoTradingClient(
                UrllibBybitDemoTransport(
                    settings.api_key,
                    settings.api_secret,
                    base_url=settings.base_url,
                )
            )
        result = QtrMicroPreflight(
            settings,
            client,
            state_store=JsonQtrMicroStateStore(),
        ).run(symbol)
    except (RuntimeError, ValueError) as error:
        result = configuration_failure(
            str(error),
            base_url=os.getenv("BYBIT_DEMO_BASE_URL", ""),
            symbol=symbol,
        )
    print(preflight_json(result) if args.json else format_preflight(result))
    if not result.ready:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
