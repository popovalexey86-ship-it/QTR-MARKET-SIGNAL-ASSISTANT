from __future__ import annotations

import argparse


class RussianArgumentParser(argparse.ArgumentParser):
    """Argument parser with localized framework-generated help labels."""

    def format_usage(self) -> str:
        return _translate_help(super().format_usage())

    def format_help(self) -> str:
        return _translate_help(super().format_help())


def _translate_help(text: str) -> str:
    return (
        text.replace("usage:", "использование:")
        .replace("options:", "параметры:")
        .replace("show this help message and exit", "показать справку и выйти")
    )
