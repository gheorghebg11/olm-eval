#!/usr/bin/env python3
"""
Unified parser runner for olmOCR.

This script provides a common interface to run different parsing providers
(llamaparse, reducto, etc.) with their respective configurations.
"""

import argparse
import asyncio
import sys
from pathlib import Path


def get_available_parsers() -> list[str]:
    """Get list of available parsers by checking subdirectories."""
    parsers_dir = Path(__file__).parent
    parsers = []

    for item in parsers_dir.iterdir():
        if item.is_dir() and not item.name.startswith('_') and not item.name.startswith('.'):
            parse_file = item / "parse.py"
            if parse_file.exists():
                parsers.append(item.name)

    return sorted(parsers)


def get_available_configs(parser_name: str) -> list[str]:
    """Get list of available configs for a parser."""
    configs_dir = Path(__file__).parent / parser_name / "configs"

    if not configs_dir.exists():
        return []

    configs = []
    for config_file in configs_dir.glob("*.json"):
        configs.append(config_file.stem)  # Just the name without .json

    return sorted(configs)


async def run_parser_async(parser_name: str, config_name: str | None, input_path: str, extra_args: list[str]):
    """Run the parser asynchronously."""
    parser_dir = Path(__file__).parent / parser_name
    parse_script = parser_dir / "parse.py"

    if not parse_script.exists():
        print(f"Error: Parser script not found: {parse_script}")
        return 1

    # Build command
    cmd = [sys.executable, "-m", f"parsers.{parser_name}.parse", input_path, parser_name]

    # Add config if specified
    if config_name:
        config_path = parser_dir / "configs" / f"{config_name}.json"
        if not config_path.exists():
            print(f"Error: Config file not found: {config_path}")
            print(f"Available configs for {parser_name}: {', '.join(get_available_configs(parser_name))}")
            return 1
        cmd.extend(["--config", str(config_path)])

    # Add extra arguments
    cmd.extend(extra_args)

    print(f"Running: {' '.join(cmd)}")
    print("=" * 60)

    # Run the command
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    # Stream output in real-time
    async def stream_output(stream, prefix=""):
        while True:
            line = await stream.readline()
            if not line:
                break
            print(f"{prefix}{line.decode().rstrip()}")

    # Run both stdout and stderr streaming concurrently
    await asyncio.gather(
        stream_output(process.stdout),
        stream_output(process.stderr, "STDERR: ")
    )

    await process.wait()
    return process.returncode


async def main():
    available_parsers = get_available_parsers()

    parser = argparse.ArgumentParser(
        description="Unified parser runner for olmOCR",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Examples:
  # Use llamaparse with a specific config
  python -m parsers.run_parser input_dir llamaparse --config agent_openai_gpt41mini

  # Use llamaparse with fast_mode config and output suffix
  python -m parsers.run_parser input_dir llamaparse --config fast_mode --output-suffix my_run

  # Use reducto parser
  python -m parsers.run_parser input_dir reducto

Available parsers: {', '.join(available_parsers)}
"""
    )

    parser.add_argument(
        "input",
        type=str,
        help="Input file or directory to parse"
    )

    parser.add_argument(
        "parser",
        type=str,
        choices=available_parsers,
        help=f"Parser to use: {', '.join(available_parsers)}"
    )

    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Config name (without .json extension). The script will look in parsers/<parser>/configs/<config>.json"
    )

    parser.add_argument(
        "--list-configs",
        action="store_true",
        help="List available configs for the specified parser"
    )

    # Parse known args, pass the rest to the parser script
    args, extra_args = parser.parse_known_args()

    # If listing configs, show them and exit
    if args.list_configs:
        configs = get_available_configs(args.parser)
        if configs:
            print(f"Available configs for {args.parser}:")
            for config in configs:
                print(f"  - {config}")
        else:
            print(f"No configs found for {args.parser}")
        return 0

    # Run the parser
    return await run_parser_async(args.parser, args.config, args.input, extra_args)


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
