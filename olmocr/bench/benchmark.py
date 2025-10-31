#!/usr/bin/env python3
"""
This script runs olmocr bench.
It will take as an argument a folder, and scan it for .jsonl files which contain the various rules and properties that we will check.
It will then validate the JSON files to make sure they are all valid.
Then, each other folder in there (besides /pdfs) represents a pipeline tool that we will evaluate.
We will validate that each one of those contains at least one .md file (or repeated generations, e.g. _pg{page}_repeat{repeat}.md)
corresponding to its parse for every .pdf in the /pdfs folder.
Then, we will read each one, and check if they pass against all the rules.
If a rule fails on some of the repeats, a short explanation is printed.
The final score is the average of per-JSONL file scores, where each JSONL file's score is the proportion of tests from that file that pass.
Statistical analysis including bootstrap confidence intervals are provided for the results.
Pairwise permutation tests are conducted between specific candidate pairs.
"""

import argparse
import glob
import os
import random
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple

from pypdf import PdfReader
from tqdm import tqdm

from .report_better import generate_html_report
from .tests import BaselineTest, BasePDFTest, load_tests, save_tests
from .utils import calculate_bootstrap_ci


def evaluate_candidate(
    candidate_folder: str, all_tests: List[BasePDFTest], pdf_basenames: List[str], force: bool = False, md_folder: str | None = None
) -> Tuple[float, int, List[str], List[str], Dict[str, List[float]], List[float], Dict[str, Dict[int, List[Tuple[BasePDFTest, bool, str]]]]]:
    """
    For the candidate folder (pipeline tool output), validate that it contains at least one .md file
    (i.e. repeated generations like _pg{page}_repeat{repeat}.md) for every PDF in the pdf folder.
    Then, run each rule against all corresponding .md files concurrently and average the results.

    Returns a tuple:
      (overall_score, total_tests, candidate_errors, test_failures, test_type_breakdown, all_test_scores, test_results)

      - overall_score: Average fraction of tests passed (averaged over repeats and tests).
        Note: This is now updated at reporting time to be the average of per-JSONL file scores.
      - total_tests: Total number of tests evaluated.
      - candidate_errors: List of candidate errors (e.g. missing files).
      - test_failures: List of failure messages for tests not passing on all repeats.
      - test_type_breakdown: Dictionary mapping test type to list of average pass ratios for tests of that type.
      - all_test_scores: List of all individual test scores (used for bootstrapping).
      - test_results: Dictionary mapping PDF name to dictionary mapping page number to list of (test, passed, explanation) tuples.
    """
    candidate_errors = []
    test_failures = []
    test_type_breakdown = {}  # key: test type, value: list of average pass ratios
    all_test_scores = []  # Store all individual test scores for bootstrapping
    test_results = {}  # Store detailed test results for reporting
    candidate_name = os.path.basename(candidate_folder)

    # Map each PDF to its corresponding MD repeats (e.g., doc1_pg1_repeat1.md, doc1_pg2_repeat2.md, etc.)
    pdf_to_md_files = {}
    # Use md_folder if provided, otherwise use candidate_folder
    search_folder = md_folder if md_folder else candidate_folder
    all_files = list(glob.glob(os.path.join(search_folder, "**/*.md"), recursive=True))

    for pdf_name in pdf_basenames:
        md_base = os.path.splitext(pdf_name)[0]
        # Try pattern 1: {pdf_base}_pg{page}_repeat{repeat}.md
        md_regex = re.compile(rf"^{re.escape(md_base)}_pg\d+_repeat\d+\.md$")
        md_files = [f for f in all_files if md_regex.match(os.path.relpath(f, search_folder))]

        # Try pattern 2: {pdf_base}.md (exact match to PDF name)
        if not md_files:
            md_exact = os.path.join(search_folder, md_base + ".md")
            if os.path.exists(md_exact):
                md_files = [md_exact]

        if not md_files and not force:
            candidate_errors.append(
                f"Candidate '{candidate_name}' is missing MD repeats for {pdf_name} "
                f"(expected files matching {md_base}_pg{{page}}_repeat*.md or {md_base}.md)."
            )
        else:
            pdf_to_md_files[pdf_name] = md_files

    if candidate_errors:
        return (0.0, len(all_tests), candidate_errors, test_failures, test_type_breakdown, all_test_scores, test_results)

    # Define an inner function to evaluate a single test
    def process_test(test: BasePDFTest) -> Tuple[float | None, str | None, str, List[str], Tuple[bool, str]]:
        local_errors = []
        test_failure = None
        pdf_name = test.pdf

        # Initialize the test_results structure if needed
        if pdf_name not in test_results:
            test_results[pdf_name] = {}
        if test.page not in test_results[pdf_name]:
            test_results[pdf_name][test.page] = []

        md_base = os.path.splitext(pdf_name)[0]
        md_files = pdf_to_md_files.get(pdf_name, [])

        # Filter MD files for the specific page corresponding to the test
        page_md_files = [f for f in md_files if re.search(rf"_pg{test.page}_", os.path.basename(f))]

        # If no page-specific files found, check if we have a single MD file matching the PDF name
        # In this case, use it for all pages
        if not page_md_files and md_files:
            # Check if any of the MD files match the exact PDF name pattern (no page/repeat suffix)
            exact_match_files = [f for f in md_files if os.path.basename(f) == os.path.basename(md_base + ".md")]
            if exact_match_files:
                page_md_files = exact_match_files

        if not page_md_files:
            if not force:
                local_errors.append(
                    f"Candidate '{candidate_name}' is missing MD repeats for {pdf_name} page {test.page} "
                    f"(expected files matching {md_base}_pg{test.page}_repeat*.md or {md_base}.md)."
                )
            # Return None as test_avg to indicate this test should be excluded from statistics
            test_results[pdf_name][test.page].append((test, False, "Missing MD files"))
            return (None, None, test.type, local_errors, (False, "Missing MD files"))

        repeat_passes = 0
        num_repeats = 0
        explanations = []
        for md_path in page_md_files:
            num_repeats += 1
            try:
                with open(md_path, "r", encoding="utf-8") as f:
                    md_content = f.read()
            except Exception as e:
                local_errors.append(f"Error reading {md_path}: {e}")
                continue

            try:
                passed, explanation = test.run(md_content)
                if passed:
                    repeat_passes += 1
                else:
                    explanations.append(explanation)
            except Exception as e:
                local_errors.append(f"Error running test {test.id} on {md_path}: {e}")
                explanations.append(str(e))

        test_avg = repeat_passes / num_repeats if num_repeats > 0 else 0.0
        final_passed = test_avg > 0.5  # Consider test passed if majority of repeats pass
        final_explanation = explanations[0] if explanations else "All repeats passed"

        # Store the test result for reporting
        test_results[pdf_name][test.page].append((test, final_passed, final_explanation))

        if test_avg < 1.0:
            test_failure = (
                f"Test {test.id} on {md_base} page {test.page} average pass ratio: {test_avg:.3f} "
                f"({repeat_passes}/{num_repeats} repeats passed). Ex: {explanations[0] if explanations else 'No explanation'}"
            )
        return (test_avg, test_failure, test.type, local_errors, (final_passed, final_explanation))

    total_test_score = 0.0
    evaluated_test_count = 0
    futures = []
    # Use a thread pool to evaluate each test concurrently.
    with ThreadPoolExecutor(max_workers=min(os.cpu_count() or 1, 64)) as executor:
        futures = [executor.submit(process_test, test) for test in all_tests]
        # tqdm progress bar for this candidate's tests
        for future in tqdm(as_completed(futures), total=len(futures), desc=f"Evaluating tests for {candidate_name}", unit="test"):
            test_avg, test_failure, test_type, errors, _ = future.result()

            # Skip tests that were not evaluated (missing files in --force mode)
            if test_avg is not None:
                all_test_scores.append(test_avg)
                total_test_score += test_avg
                evaluated_test_count += 1
                if test_type not in test_type_breakdown:
                    test_type_breakdown[test_type] = []
                test_type_breakdown[test_type].append(test_avg)

            if test_failure:
                test_failures.append(test_failure)
            local_errors = errors
            if local_errors:
                candidate_errors.extend(local_errors)

    overall_score = total_test_score / evaluated_test_count if evaluated_test_count > 0 else 0.0
    return (overall_score, evaluated_test_count, candidate_errors, test_failures, test_type_breakdown, all_test_scores, test_results)


def main():
    parser = argparse.ArgumentParser(description="Run OLMOCR Bench.")
    parser.add_argument(
        "--dir",
        default=os.path.join(os.path.dirname(__file__), "sample_data"),
        help="Path to the folder containing .jsonl files, /pdfs folder, and pipeline tool subfolders.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Run benchmark even if some files are missing",
    )
    parser.add_argument("--candidate", type=str, default=None, help="Run test only for a single candidate")
    parser.add_argument("--skip_baseline", action="store_true", help="Skip running baseline tests (ex. that check that basic content is present on each page)")
    parser.add_argument("--skip_math", action="store_true", help="Skip JSONL files containing 'math' in their filename")
    parser.add_argument(
        "--bootstrap_samples",
        type=int,
        default=1000,
        help="Number of bootstrap samples for confidence interval calculation (default: 1000).",
    )
    parser.add_argument(
        "--confidence_level",
        type=float,
        default=0.95,
        help="Confidence level for interval calculation (default: 0.95 for 95% CI).",
    )
    # New arguments
    parser.add_argument("--sample", type=int, default=None, help="Randomly sample N tests to run instead of all tests.")
    parser.add_argument(
        "--test_report",
        type=str,
        nargs="?",
        const="auto",
        default=None,
        help="Generate an HTML report. Provide a filename or leave empty for auto-generated timestamped filename (_eval_report_YYYYMMDD_HHMMSS.html)"
    )
    parser.add_argument(
        "--output_failed", type=str, default=None, help="Output a JSONL file containing tests that failed across all candidates. Provide a filename."
    )
    parser.add_argument(
        "--md_folder", type=str, default=None, help="Read markdown files from this specific folder instead of candidate subfolders."
    )
    args = parser.parse_args()

    input_folder = args.dir if os.path.isdir(args.dir) else os.path.dirname(args.dir)
    n_bootstrap = args.bootstrap_samples
    ci_level = args.confidence_level
    pdf_folder = os.path.join(input_folder, "pdfs")

    if not os.path.exists(pdf_folder):
        print("Error: /pdfs folder must exist in your data directory.", file=sys.stderr)
        sys.exit(1)

    all_pdf_files = list(glob.glob(os.path.join(pdf_folder, "**/*.pdf"), recursive=True))

    if not all_pdf_files:
        print(f"Error: No PDF files found in {pdf_folder}", file=sys.stderr)
        sys.exit(1)

    pdf_basenames = [os.path.relpath(p, pdf_folder) for p in all_pdf_files]

    if os.path.isfile(args.dir):
        jsonl_files = [args.dir]
    else:
        all_jsonl_files = glob.glob(os.path.join(input_folder, "*.jsonl"))
        # Filter out backup files with datetime pattern (name_YYYYMMDD_HHMMSS.jsonl)
        jsonl_files = [f for f in all_jsonl_files if not re.search(r'_\d{8}_\d{6}\.jsonl$', f)]

    # Filter out math files if requested
    if args.skip_math:
        jsonl_files = [f for f in jsonl_files if 'math' not in os.path.basename(f).lower()]
        print(f"Skipping math-related JSONL files (--skip_math enabled)")

    if not jsonl_files:
        print(f"Error: No .jsonl files found in {input_folder}.", file=sys.stderr)
        sys.exit(1)

    all_tests = []
    test_to_jsonl = {}  # Map test IDs to their source jsonl files
    for jsonl_path in jsonl_files:
        jsonl_basename = os.path.basename(jsonl_path)
        tests = load_tests(jsonl_path)
        for test in tests:
            test_to_jsonl[test.id] = jsonl_basename
        all_tests.extend(tests)

    if not all_tests:
        print("No valid tests found. Exiting.", file=sys.stderr)
        sys.exit(1)

    for pdf in pdf_basenames:
        if not any(t.type == "baseline" for t in all_tests if t.pdf == pdf):
            all_tests.append(BaselineTest(id=f"{pdf}_baseline", pdf=pdf, page=1, type="baseline"))
            test_to_jsonl[all_tests[-1].id] = "baseline"

    for pdf in pdf_basenames:
        pdf_doc = PdfReader(os.path.join(pdf_folder, pdf))
        for page in range(1, len(pdf_doc.pages) + 1):
            if not any(test for test in all_tests if test.pdf == pdf and test.page == page) and not args.force:
                print(f"No dataset entry found for pdf {pdf} page {page}")
                sys.exit(1)

    if args.skip_baseline:
        all_tests = [test for test in all_tests if test.type != "baseline"]

    candidate_folders = []

    # If md_folder is specified, use a dummy candidate folder (just for naming)
    if args.md_folder:
        # Use the md_folder name as the candidate name
        candidate_name = os.path.basename(args.md_folder.rstrip('/'))
        candidate_folders.append(args.md_folder)
    else:
        for entry in os.listdir(input_folder):
            full_path = os.path.join(input_folder, entry)
            if args.candidate is not None:
                if entry == args.candidate:
                    candidate_folders.append(full_path)
            else:
                if os.path.isdir(full_path) and entry != "pdfs":
                    candidate_folders.append(full_path)

    # Sample tests if requested
    if args.sample is not None and args.sample > 0:
        if args.sample >= len(all_tests):
            print(f"Sample size {args.sample} is greater than or equal to the total number of tests ({len(all_tests)}). Using all tests.")
        else:
            print(f"Randomly sampling {args.sample} tests out of {len(all_tests)} total tests.")
            all_tests = random.sample(all_tests, args.sample)

    if not candidate_folders:
        print("Error: No candidate pipeline folders found (subdirectories besides 'pdfs').", file=sys.stderr)
        sys.exit(1)

    candidate_folders.sort()

    summary = []
    test_results_by_candidate = {}
    print("\nRunning tests for each candidate:")
    # Process candidates sequentially so that each candidate's progress bar is distinct.
    for candidate in candidate_folders:
        candidate_name = os.path.basename(candidate)
        print(f"\nEvaluating candidate: {candidate_name}")
        overall_score, total_tests, candidate_errors, test_failures, test_type_breakdown, all_test_scores, test_results = evaluate_candidate(
            candidate, all_tests, pdf_basenames, args.force, args.md_folder
        )

        # Always store test results for displaying jsonl file groupings
        test_results_by_candidate[candidate_name] = test_results

        # Group results by jsonl file for more accurate CI calculation
        jsonl_results = {}
        jsonl_scores = []  # List to store scores by jsonl file for CI calculation
        jsonl_file_sizes = []  # List to store the number of tests per jsonl file

        for test in all_tests:
            # Get the jsonl file this test came from
            jsonl_file = test_to_jsonl.get(test.id, "unknown")

            if jsonl_file not in jsonl_results:
                jsonl_results[jsonl_file] = {"total": 0, "passed": 0, "scores": []}

            # Get the test result for this candidate if it exists
            if not candidate_errors and hasattr(test, "pdf") and hasattr(test, "page"):
                pdf_name = test.pdf
                page = test.page
                if pdf_name in test_results and page in test_results.get(pdf_name, {}):
                    for t, passed, explanation in test_results[pdf_name][page]:
                        if t.id == test.id:
                            # Only count tests that were actually evaluated (not skipped due to missing files)
                            if explanation != "Missing MD files":
                                jsonl_results[jsonl_file]["total"] += 1
                                # Store the test score in its jsonl group
                                result_score = 1.0 if passed else 0.0
                                jsonl_results[jsonl_file]["scores"].append(result_score)
                                if passed:
                                    jsonl_results[jsonl_file]["passed"] += 1
                            break

        # Gather all the scores by jsonl file for CI calculation
        for jsonl_file, results in jsonl_results.items():
            if results["scores"]:
                jsonl_file_sizes.append(len(results["scores"]))
                jsonl_scores.extend(results["scores"])

        # Calculate CI using the updated function with splits
        if jsonl_scores:
            ci = calculate_bootstrap_ci(jsonl_scores, n_bootstrap=n_bootstrap, ci_level=ci_level, splits=jsonl_file_sizes)
        else:
            ci = (0.0, 0.0)
        summary.append((candidate_name, overall_score, total_tests, candidate_errors, test_failures, test_type_breakdown, ci, all_test_scores))
        print(f"\nCandidate: {candidate_name}")
        if candidate_errors:
            for err in candidate_errors:
                print(f"  [ERROR] {err}")
        else:
            if test_failures:
                for fail in test_failures:
                    print(f"  [FAIL] {fail}")
            # Calculate and show the per-category average score
            jsonl_pass_rates = []
            for _, results in jsonl_results.items():
                if results["total"] > 0:
                    pass_rate = results["passed"] / results["total"]
                    jsonl_pass_rates.append(pass_rate)

            per_category_score = sum(jsonl_pass_rates) / len(jsonl_pass_rates) if jsonl_pass_rates else 0.0
            print(f"  Average Score: {per_category_score * 100:.1f}% (95% CI: [{ci[0] * 100:.1f}%, {ci[1] * 100:.1f}%]) over {total_tests} tests.")

    print("\n" + "=" * 60)
    print("Final Summary with 95% Confidence Intervals:")

    # Collect summary stats for HTML report
    summary_stats_for_report = []

    for idx, (candidate_name, _, total_tests, candidate_errors, _, test_type_breakdown, ci, _) in enumerate(summary):
        # Group results by jsonl file
        jsonl_results = {}
        for test in all_tests:
            # Get the jsonl file this test came from
            jsonl_file = test_to_jsonl.get(test.id, "unknown")

            if jsonl_file not in jsonl_results:
                jsonl_results[jsonl_file] = {"total": 0, "passed": 0}

            # Get the test result for this candidate if it exists
            test_result = None
            if not candidate_errors and hasattr(test, "pdf") and hasattr(test, "page"):
                pdf_name = test.pdf
                page = test.page
                if pdf_name in test_results_by_candidate.get(candidate_name, {}) and page in test_results_by_candidate[candidate_name].get(pdf_name, {}):
                    for t, passed, explanation in test_results_by_candidate[candidate_name][pdf_name][page]:
                        if t.id == test.id:
                            # Only count tests that were actually evaluated (not skipped due to missing files)
                            if explanation != "Missing MD files":
                                test_result = passed
                            break

            # Only increment counters if test was evaluated
            if test_result is not None:
                jsonl_results[jsonl_file]["total"] += 1
                if test_result:
                    jsonl_results[jsonl_file]["passed"] += 1

        # Calculate new overall score as average of per-JSONL pass rates
        jsonl_pass_rates = []
        for jsonl_file, results in jsonl_results.items():
            if results["total"] > 0:
                pass_rate = results["passed"] / results["total"]
                jsonl_pass_rates.append(pass_rate)

        # New overall score is average of per-JSONL pass rates
        new_overall_score = sum(jsonl_pass_rates) / len(jsonl_pass_rates) if jsonl_pass_rates else 0.0

        # Update the overall_score in the summary list for later use (e.g., in permutation tests)
        summary[idx] = (candidate_name, new_overall_score, total_tests, candidate_errors, summary[idx][4], test_type_breakdown, ci, summary[idx][7])

        # Store data for HTML report
        summary_stats_for_report.append({
            'name': candidate_name,
            'overall_score': new_overall_score,
            'ci': ci,
            'test_type_breakdown': test_type_breakdown,
            'jsonl_results': jsonl_results,
            'has_errors': bool(candidate_errors)
        })

        if candidate_errors:
            status = "FAILED (errors)"
            ciw_str = ""
        else:
            status = f"{new_overall_score * 100:0.1f}%"
            # Use the CI that was calculated with proper category-based bootstrap
            half_width = ((ci[1] - ci[0]) / 2) * 100
            ciw_str = f"± {half_width:0.1f}%"
        print(f"{candidate_name:20s} : Average Score: {status} {ciw_str} (average of per-JSONL scores)")

        # Sort the test types alphabetically
        for ttype in sorted(test_type_breakdown.keys()):
            scores = test_type_breakdown[ttype]
            avg = sum(scores) / len(scores) * 100 if scores else 0.0
            print(f"    {ttype:8s}: {avg:0.1f}% average pass rate over {len(scores)} tests")

        print("\n    Results by JSONL file:")
        for jsonl_file, results in sorted(jsonl_results.items()):
            if results["total"] > 0:
                pass_rate = (results["passed"] / results["total"]) * 100
                print(f"        {jsonl_file:30s}: {pass_rate:0.1f}% ({results['passed']}/{results['total']} tests)")
        print("")

    # Generate HTML report if requested
    if args.test_report:
        from datetime import datetime

        # Determine the report filename
        if args.test_report == "auto":
            # Auto-generate timestamped filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_filename = f"_eval_report_{timestamp}.html"

            # Save report in the MD folder (first candidate folder or md_folder if specified)
            if args.md_folder:
                report_path = os.path.join(args.md_folder, report_filename)
            elif candidate_folders:
                report_path = os.path.join(candidate_folders[0], report_filename)
            else:
                report_path = report_filename
        else:
            # Use the user-provided filename
            report_path = args.test_report

        # Determine MD folder for the report
        md_folder_for_report = args.md_folder if args.md_folder else (candidate_folders[0] if candidate_folders else None)
        generate_html_report(test_results_by_candidate, pdf_folder, report_path, md_folder_for_report, summary_stats_for_report)
        print(f"\nHTML report saved to: {report_path}")

    # Output tests that failed across all candidates if requested
    if args.output_failed:
        # Identify tests that failed across all candidates
        all_failed_tests = []
        valid_candidates = [c for c in summary if not c[3]]  # Skip candidates with errors

        for test in all_tests:
            # Track whether this test has any results
            has_results = False
            any_passed = False

            for candidate_name, _, _, _, _, _, _, _ in valid_candidates:
                # Get the test result for this candidate
                test_result = None
                if hasattr(test, "pdf") and hasattr(test, "page"):
                    pdf_name = test.pdf
                    page = test.page
                    if pdf_name in test_results_by_candidate.get(candidate_name, {}) and page in test_results_by_candidate[candidate_name].get(pdf_name, {}):
                        for t, passed, explanation in test_results_by_candidate[candidate_name][pdf_name][page]:
                            if t.id == test.id:
                                has_results = True
                                test_result = passed
                                if passed:
                                    any_passed = True
                                break

            # If we have results for this test and it never passed for any candidate, add it to the failed list
            if has_results and not any_passed:
                # Add to the list
                all_failed_tests.append(test)

        # If we have any failed tests, write them to the specified JSONL file
        output_path = os.path.join(input_folder, args.output_failed) if not os.path.isabs(args.output_failed) else args.output_failed

        if all_failed_tests:
            save_tests(all_failed_tests, output_path)

            print(f"\nOutput {len(all_failed_tests)} tests that failed across all candidates to {output_path}")
        else:
            print("\nNo tests failed across all candidates. No output file created.")


if __name__ == "__main__":
    main()
