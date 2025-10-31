"""
Method 2: High Parallelization with asyncio.gather()

This script demonstrates parsing multiple files in parallel using asyncio.gather().
Good for: Batch processing multiple files with maximum parallelization.
"""

import argparse
import asyncio
import json
import os
import re
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


async def parse_multiple_files_parallel(file_paths: list[str], parser_config: dict | None = None, output_dir: str | None = None, input_base_dir: str | None = None):
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


# Mapping from camelCase to snake_case with defaults based on LlamaParse parameters
# From the platform repo in llamaparse/worker/src/prepareJobParameters.tx
CAMEL_TO_SNAKE_MAPPING = {
    "adaptiveLongTable": ("adaptive_long_table", False),
    "annotateLinks": ("annotate_links", None),
    "autoMode": ("auto_mode", False),
    "autoModeConfigurationJson": ("auto_mode_configuration_json", None),
    "autoModeTriggerOnImageInPage": ("auto_mode_trigger_on_image_in_page", False),
    "autoModeTriggerOnRegexpInPage": ("auto_mode_trigger_on_regexp_in_page", None),
    "autoModeTriggerOnTableInPage": ("auto_mode_trigger_on_table_in_page", False),
    "autoModeTriggerOnTextInPage": ("auto_mode_trigger_on_text_in_page", None),
    "azureOpenAiApiVersion": ("azure_openai_api_version", None),
    "azureOpenAiDeploymentName": ("azure_openai_deployment_name", None),
    "azureOpenAiEndpoint": ("azure_openai_endpoint", None),
    "azureOpenAiKey": ("azure_openai_key", None),
    "bboxBottom": ("bbox_bottom", None),
    "bboxLeft": ("bbox_left", None),
    "bboxRight": ("bbox_right", None),
    "bboxTop": ("bbox_top", None),
    "boundingBox": ("bounding_box", None),
    "compactMarkdownTable": ("compact_markdown_table", False),
    "complementalFormattingInstruction": ("complemental_formatting_instruction", None),
    "contentGuidelineInstruction": ("content_guideline_instruction", None),
    "disableImageExtraction": ("disable_image_extraction", False),
    "disableOcr": ("disable_ocr", False),
    "doNotCache": ("do_not_cache", False),
    "doNotUnrollColumns": ("do_not_unroll_columns", False),
    "extractCharts": ("extract_charts", False),
    "extractLayout": ("extract_layout", False),
    "fileId": ("file_id", None),
    "fileName": ("file_name", "input.pdf"),
    "formattingInstruction": ("formatting_instruction", None),
    "guessXLSXSheetName": ("guess_xlsx_sheet_name", False),
    "hideFooters": ("hide_footers", False),
    "hideHeaders": ("hide_headers", False),
    "highResOcr": ("high_res_ocr", False),
    "htmlMakeAllElementsVisible": ("html_make_all_elements_visible", False),
    "htmlRemoveFixedElements": ("html_remove_fixed_elements", False),
    "htmlRemoveNavigationElements": ("html_remove_navigation_elements", False),
    "httpProxy": ("http_proxy", None),
    "ignoreDocumentElementsForLayoutDetection": ("ignore_document_elements_for_layout_detection", False),
    "inputS3Path": ("input_s3_path", None),
    "inputS3Region": ("input_s3_region", None),
    "inputUrl": ("input_url", None),
    "invalidateCache": ("invalidate_cache", False),
    "isFormattingInstruction": ("is_formatting_instruction", True),
    "jobTimeoutExtraTimePerPageInSeconds": ("job_timeout_extra_time_per_page_in_seconds", 2),
    "jobTimeoutInSeconds": ("job_timeout_in_seconds", None),
    "keepPageSeparatorWhenMergingTables": ("keep_page_separator_when_merging_tables", False),
    "language": ("lang", "en"),
    "layoutAware": ("layout_aware", False),
    "preciseBoundingBox": ("precise_bounding_box", False),
    "specializedChartParsingAgentic": ("specialized_chart_parsing_agentic", False),
    "specializedChartParsingOneShot": ("specialized_chart_parsing_one_shot", False),
    "specializedChartParsingEfficient": ("specialized_chart_parsing_efficient", False),
    "specializedImageParsing": ("specialized_image_parsing", False),
    "logFiles": ("log_files", False),
    "markdownTableMultilineHeaderSeparator": ("markdown_table_multiline_header_separator", "<br/>"),
    "maxPages": ("max_pages", 0),
    "mergeTablesAcrossPagesInMarkdown": ("merge_tables_across_pages_in_markdown", False),
    "model": ("model", None),
    "multimodalPipeline": ("multimodal_pipeline", False),
    "outlinedTableExtraction": ("outlined_table_extraction", False),
    "aggressiveTableExtraction": ("aggressive_table_extraction", False),
    "outputPDFOfDocument": ("output_pdf_of_document", False),
    "outputS3PathPrefix": ("output_s3_path_prefix", None),
    "outputS3Region": ("output_s3_region", None),
    "outputTablesAsHTML": ("output_tables_as_HTML", False),
    "pageErrorTolerance": ("page_error_tolerance", 0),
    "pageFooterPrefix": ("page_footer_prefix", None),
    "pageFooterSuffix": ("page_footer_suffix", None),
    "pageHeaderPrefix": ("page_header_prefix", None),
    "pageHeaderSuffix": ("page_header_suffix", None),
    "pagePrefix": ("page_prefix", None),
    "pageSeparator": ("page_separator", "\n---\n"),
    "pageSuffix": ("page_suffix", None),
    "parseMode": ("parse_mode", "parse_page_with_llm"),
    "preserveLayoutAlignmentAcrossPages": ("preserve_layout_alignment_across_pages", False),
    "preserveVerySmallText": ("preserve_very_small_text", False),
    "replaceFailedPageMode": ("replace_failed_page_mode", "RAW_TEXT"),
    "replaceFailedPageWithErrorMessagePrefix": ("replace_failed_page_with_error_message_prefix", None),
    "replaceFailedPageWithErrorMessageSuffix": ("replace_failed_page_with_error_message_suffix", None),
    "saveImages": ("save_images", False),
    "skipDiagonalText": ("skip_diagonal_text", False),
    "spreadSheetExtractSubTables": ("spreadsheet_extract_sub_tables", False),
    "spreadSheetForceFormulaComputation": ("spreadsheet_force_formula_computation", False),
    "inlineImagesInMarkdown": ("inline_images_in_markdown", False),
    "strictModeBuggyFont": ("strict_mode_buggy_font", False),
    "strictModeImageExtraction": ("strict_mode_image_extraction", False),
    "strictModeImageOCR": ("strict_mode_image_ocr", False),
    "strictModeReconstruction": ("strict_mode_reconstruction", False),
    "structuredOutput": ("structured_output", False),
    "structuredOutputJSONSchema": ("structured_output_json_schema", None),
    "structuredOutputJSONSchemaName": ("structured_output_json_schema_name", None),
    "structuredOutputSchema": ("structured_output_schema", None),
    "takeScreenshot": ("take_screenshot", False),
    "targetPages": ("target_pages", None),
    "template": ("template", None),
    "vendorAPIKey": ("vendor_api_key", None),
    "webhookUrl": ("webhook_url", None),
    "premiumMode": ("premium_mode", False),
    "removeHiddenText": ("remove_hidden_text", False),
    # # Additional common aliases -- not in the SDK
    # "llmMD": ("llm_md", None),
    # "ignoreFonts": ("ignore_fonts", None),
    # "debugAllFonts": ("debug_all_fonts", None),
}


def camel_to_snake(name: str) -> str:
    """
    Convert camelCase to snake_case.

    Args:
        name: String in camelCase format

    Returns:
        String in snake_case format
    """
    # Check if we have a direct mapping first
    if name in CAMEL_TO_SNAKE_MAPPING:
        return CAMEL_TO_SNAKE_MAPPING[name][0]

    # Otherwise, use regex conversion
    # Insert underscore before uppercase letters (except at the start)
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    # Insert underscore before uppercase letters preceded by lowercase letters or numbers
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def convert_config_keys_to_snake_case(config: dict) -> dict:
    """
    Convert all config keys from camelCase to snake_case.
    Uses defaults from CAMEL_TO_SNAKE_MAPPING when values are not provided.

    Args:
        config: Dictionary with camelCase keys

    Returns:
        Dictionary with snake_case keys
    """
    result = {}
    for key, value in config.items():
        snake_key = camel_to_snake(key)
        # Use provided value, or fall back to default if available
        if key in CAMEL_TO_SNAKE_MAPPING:
            _, default_value = CAMEL_TO_SNAKE_MAPPING[key]
            result[snake_key] = value if value is not None else default_value
        else:
            result[snake_key] = value
    return result


def load_config(config_path: str) -> dict:
    """
    Load parser configuration from a JSON file and convert keys to snake_case.

    Args:
        config_path: Path to the JSON config file

    Returns:
        Dictionary containing parser configuration with snake_case keys
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, 'r') as f:
        config = json.load(f)

    # Convert camelCase keys to snake_case
    config = convert_config_keys_to_snake_case(config)

    print(f"Loaded config from: {config_path}")
    print(f"Config keys: {', '.join(config.keys())}")
    return config


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
        "--parser-name",
        type=str,
        default="llamaparse",
        help="Name of the parser being used (default: llamaparse). Options: llamaparse, reducto, etc."
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to JSON config file (e.g., configs/agent_openai_gpt41mini.json). If not provided, uses default config."
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

    # Load config if provided
    parser_config = None
    if args.config:
        parser_config = load_config(args.config)

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

    # Save parse config to _parse_config.json in the output directory
    os.makedirs(output_dir, exist_ok=True)
    config_save_path = os.path.join(output_dir, "_parse_config.json")

    # Prepare config to save (use parser_config if provided, otherwise use defaults)
    actual_config = parser_config or {
        "parse_mode": "parse_page_with_agent",
        "model": "openai-gpt-4-1-mini",
        "high_res_ocr": True,
        "adaptive_long_table": True,
        "outlined_table_extraction": True,
        "output_tables_as_html": True,
    }

    # Use the parser_name from args (e.g., "llamaparse", "reducto")
    parser_name = args.parser_name

    # Structure: {"parser": "parser_name", "config": {full_config}}
    config_to_save = {
        "parser": parser_name,
        "config": actual_config
    }

    with open(config_save_path, 'w') as f:
        json.dump(config_to_save, f, indent=2)
    print(f"Saved parse config to: {config_save_path}")
    print(f"  Parser: {parser_name}")
    if "parse_mode" in actual_config:
        print(f"  Parse mode: {actual_config['parse_mode']}")

    results = await parse_multiple_files_parallel(file_paths, parser_config=parser_config, output_dir=output_dir, input_base_dir=input_base_dir)

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
