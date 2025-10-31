"""
Method 2: High Parallelization with asyncio.gather()

This script demonstrates parsing multiple files in parallel using asyncio.gather().
Good for: Batch processing multiple files with maximum parallelization.
"""

import argparse
import asyncio
import os
import time
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from llama_cloud_services import LlamaParse


async def parse_file_async(parser: LlamaParse, file_path: str, index: int, output_dir: str | None = None, input_base_dir: str | None = None):
    """Parse a single file asynchronously and save the output, preserving directory structure."""
    print(f"[{index}] Starting parse: {file_path}")
    start_time = time.time()

    try:
        # Parse the file
        result = await parser.aparse(file_path)

        # Get markdown nodes
        markdown_nodes = await result.aget_markdown_nodes(split_by_page=True)

        # Determine output path
        if output_dir:
            # If input_base_dir is provided, preserve the relative directory structure
            if input_base_dir:
                # Get relative path from input_base_dir
                rel_path = os.path.relpath(file_path, input_base_dir)
                rel_dir = os.path.dirname(rel_path)
                filename = Path(file_path).stem + ".md"

                # Create the same subdirectory structure in output_dir
                if rel_dir and rel_dir != '.':
                    output_subdir = os.path.join(output_dir, rel_dir)
                    os.makedirs(output_subdir, exist_ok=True)
                    output_path = os.path.join(output_subdir, filename)
                else:
                    os.makedirs(output_dir, exist_ok=True)
                    output_path = os.path.join(output_dir, filename)
            else:
                os.makedirs(output_dir, exist_ok=True)
                output_path = os.path.join(output_dir, Path(file_path).stem + ".md")
        else:
            # Save in the same directory as the input file
            output_path = str(Path(file_path).with_suffix(".md"))

        # Save markdown to file
        with open(output_path, 'w', encoding='utf-8') as f:
            for node in markdown_nodes:
                f.write(node.text + "\n\n")

        elapsed = time.time() - start_time
        print(f"[{index}] Completed: {file_path} ({elapsed:.2f}s, {len(markdown_nodes)} nodes) -> {output_path}")

        return {
            "success": True,
            "file_path": file_path,
            "output_path": output_path,
            "job_id": result.job_id,
            "markdown_nodes": markdown_nodes,
            "elapsed_time": elapsed,
        }

    except Exception as e:
        elapsed = time.time() - start_time
        print(f"[{index}] Failed: {file_path} - {str(e)} ({elapsed:.2f}s)")
        return {
            "success": False,
            "file_path": file_path,
            "error": str(e),
            "elapsed_time": elapsed,
        }


async def parse_multiple_files_parallel(file_paths: list[str], parser_config: dict = None, output_dir: str | None = None, input_base_dir: str | None = None):
    """
    Parse multiple files in parallel using asyncio.gather().

    Args:
        file_paths: List of file paths to parse
        parser_config: Optional parser configuration dict
        output_dir: Optional output directory for results
        input_base_dir: Optional base directory to preserve relative structure
    """
    # Initialize parser with config
    config = parser_config or {
        "parse_mode": "parse_page_with_agent",
        "model": "openai-gpt-4-1-mini",
        "high_res_ocr": True,
        "adaptive_long_table": True,
        "outlined_table_extraction": True,
        "output_tables_as_HTML": True,
    }

    parser = LlamaParse(**config)

    print(f"Starting parallel parsing of {len(file_paths)} files...")
    start_time = time.time()

    # Create tasks for all files
    tasks = [
        parse_file_async(parser, file_path, i, output_dir, input_base_dir)
        for i, file_path in enumerate(file_paths, 1)
    ]

    # Run all tasks in parallel
    results = await asyncio.gather(*tasks, return_exceptions=True)

    total_elapsed = time.time() - start_time

    # Process results
    successful = [r for r in results if isinstance(r, dict) and r.get("success")]
    failed = [r for r in results if isinstance(r, dict) and not r.get("success")]
    exceptions = [r for r in results if isinstance(r, Exception)]

    print(f"\n{'=' * 60}")
    print(f"Parallel Parsing Complete!")
    print(f"{'=' * 60}")
    print(f"Total time: {total_elapsed:.2f}s")
    print(f"Successful: {len(successful)}/{len(file_paths)}")
    print(f"Failed: {len(failed)}/{len(file_paths)}")
    print(f"Exceptions: {len(exceptions)}/{len(file_paths)}")

    if successful:
        avg_time = sum(r["elapsed_time"] for r in successful) / len(successful)
        total_nodes = sum(len(r["markdown_nodes"]) for r in successful)
        print(f"Average parse time: {avg_time:.2f}s")
        print(f"Total nodes extracted: {total_nodes}")

    return results


def load_file_paths(file_list_path: str, extensions: list[str]) -> list[str]:
    """
    Load file paths from a text file and filter by extensions.

    Args:
        file_list_path: Path to a text file containing one file path per line
        extensions: List of allowed extensions (e.g., ['.pdf', '.docx'])

    Returns:
        List of filtered file paths
    """
    if not os.path.exists(file_list_path):
        raise FileNotFoundError(f"File list not found: {file_list_path}")

    file_paths = []
    with open(file_list_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):  # Skip empty lines and comments
                file_paths.append(line)

    # Filter by extensions
    filtered_paths = [
        fp for fp in file_paths
        if any(fp.lower().endswith(ext.lower()) for ext in extensions)
    ]

    print(f"Loaded {len(file_paths)} file paths, {len(filtered_paths)} match extensions {extensions}")
    return filtered_paths


def find_files_in_dir(input_dir: str, extensions: list[str], recursive: bool = False) -> list[str]:
    """
    Find all files in a directory matching the given extensions.

    Args:
        input_dir: Directory to search
        extensions: List of allowed extensions (e.g., ['.pdf', '.docx'])
        recursive: If True, search subdirectories recursively

    Returns:
        List of file paths found
    """
    if not os.path.exists(input_dir):
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    if not os.path.isdir(input_dir):
        raise ValueError(f"Not a directory: {input_dir}")

    file_paths = []

    if recursive:
        # Recursive search
        for root, _, files in os.walk(input_dir):
            for file in files:
                if any(file.lower().endswith(ext.lower()) for ext in extensions):
                    file_paths.append(os.path.join(root, file))
    else:
        # Non-recursive search (only top-level directory)
        for file in os.listdir(input_dir):
            file_path = os.path.join(input_dir, file)
            if os.path.isfile(file_path) and any(file.lower().endswith(ext.lower()) for ext in extensions):
                file_paths.append(file_path)

    search_type = "recursively" if recursive else "in"
    print(f"Found {len(file_paths)} files matching extensions {extensions} {search_type} {input_dir}")
    return file_paths


async def main():
    parser = argparse.ArgumentParser(
        description="Parse multiple files in parallel using LlamaParse"
    )
    parser.add_argument(
        "input",
        type=str,
        help="Path to a text file containing file paths (one per line) OR a directory to search recursively"
    )
    parser.add_argument(
        "--extensions",
        type=str,
        nargs="+",
        default=[".pdf"],
        help="Allowed file extensions (default: .pdf). Example: --extensions .pdf .docx .txt"
    )
    parser.add_argument(
        "--output-suffix",
        type=str,
        default=None,
        help="Optional suffix for output directory. Output will be saved to runs/{YYYYMMDD_HHMMSS}_{suffix}/"
    )
    parser.add_argument(
        "-r",
        "--recursive",
        action="store_true",
        help="Recursively search for files in subdirectories (only applies when input is a directory)"
    )

    args = parser.parse_args()

    load_dotenv()

    os.environ["LLAMA_CLOUD_API_KEY"] = os.environ["STAGING_API_KEY"]
    os.environ["LLAMA_CLOUD_BASE_URL"] = os.environ["STAGING_BASE_URL"]

    # Determine if input is a file list or directory
    input_base_dir = None
    if os.path.isfile(args.input):
        # Load file paths from text file
        file_paths = load_file_paths(args.input, args.extensions)
    elif os.path.isdir(args.input):
        # Find files in directory (recursive if -r flag is set)
        file_paths = find_files_in_dir(args.input, args.extensions, args.recursive)
        if args.recursive:
            input_base_dir = args.input  # Preserve directory structure for recursive mode
    else:
        print(f"Error: Input '{args.input}' is neither a file nor a directory")
        return

    if not file_paths:
        print("No files to process after filtering.")
        return

    # Create timestamped output directory in runs/
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if args.output_suffix:
        output_dir = f"runs/{timestamp}_{args.output_suffix}"
    else:
        output_dir = f"runs/{timestamp}"

    print(f"Output directory: {output_dir}")

    results = await parse_multiple_files_parallel(file_paths, output_dir=output_dir, input_base_dir=input_base_dir)

    # Process successful results
    successful_results = [r for r in results if isinstance(r, dict) and r.get("success")]

    for result in successful_results:
        print(f"\nFile: {result['file_path']}")
        print(f"  Output: {result.get('output_path', 'N/A')}")
        print(f"  Job ID: {result['job_id']}")
        print(f"  Nodes: {len(result['markdown_nodes'])}")
        print(f"  Time: {result['elapsed_time']:.2f}s")


if __name__ == "__main__":
    asyncio.run(main())
