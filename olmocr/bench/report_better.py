import glob
import json
import os
from pathlib import Path
from typing import Dict, List, Tuple

from tqdm import tqdm

from olmocr.data.renderpdf import render_pdf_to_base64webp

from .tests import BasePDFTest


def generate_html_report(
    test_results_by_candidate: Dict[str, Dict[str, Dict[int, List[Tuple[BasePDFTest, bool, str]]]]],
    pdf_folder: str,
    output_file: str,
    md_folder: str | None = None,
    summary_stats: List[Dict] | None = None,
    parse_mode: str | None = None,
    parse_config: Dict | None = None,
    jsonl_folder: str | None = None
) -> None:
    """
    Generate a simple static HTML report of test results.

    Args:
        test_results_by_candidate: Dictionary mapping candidate name to dictionary mapping PDF name to dictionary
                                  mapping page number to list of (test, passed, explanation) tuples.
        pdf_folder: Path to the folder containing PDF files.
        output_file: Path to the output HTML file.
        md_folder: Path to the folder containing Markdown files (optional).
        summary_stats: List of dictionaries containing summary statistics to display at the top (optional).
        parse_mode: The parsing mode used (optional).
        parse_config: The configuration used for parsing (optional).
        jsonl_folder: Path to the folder containing JSONL test files (optional).
    """
    candidates = list(test_results_by_candidate.keys())

    title = "LlamaIndex MD evaluation Report"

    # Create HTML report
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <style>"""

    html += """
        body {
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 20px;
            color: #333;
            line-height: 1.6;
        }

        h1, h2, h3, h4 {
            margin-top: 20px;
            margin-bottom: 10px;
        }

        .test-block {
            border: 1px solid #ddd;
            margin-bottom: 30px;
            padding: 15px;
            border-radius: 5px;
        }

        .test-block.pass {
            border-left: 5px solid #4CAF50;
        }

        .test-block.fail {
            border-left: 5px solid #F44336;
        }

        .pdf-image {
            max-width: 100%;
            border: 1px solid #ddd;
            margin: 10px 0;
        }

        .markdown-content {
            background: #f5f5f5;
            padding: 15px;
            border: 1px solid #ddd;
            border-radius: 5px;
            font-family: monospace;
            white-space: pre-wrap;
            overflow-x: auto;
            margin: 10px 0;
        }

        .status {
            display: inline-block;
            padding: 3px 8px;
            border-radius: 3px;
            font-weight: bold;
            margin-left: 10px;
        }

        .pass-status {
            background-color: #4CAF50;
            color: white;
        }

        .fail-status {
            background-color: #F44336;
            color: white;
        }

        .test-details {
            margin: 10px 0;
        }

        .test-explanation {
            margin-top: 10px;
            padding: 10px;
            background: #fff9c4;
            border-radius: 3px;
        }

        hr {
            border: 0;
            border-top: 1px solid #ddd;
            margin: 30px 0;
        }

        .load-pdf-btn {
            background-color: #2196F3;
            color: white;
            border: none;
            padding: 10px 20px;
            text-align: center;
            text-decoration: none;
            display: none;  /* Hidden by default since PDF loads automatically */
            font-size: 14px;
            margin: 10px 0;
            cursor: pointer;
            border-radius: 5px;
        }

        .load-pdf-btn:hover {
            background-color: #0b7dda;
        }

        .pdf-viewer-container {
            display: block;  /* Show by default */
            margin: 10px 0;
            border: 1px solid #ddd;
            border-radius: 5px;
            overflow: hidden;
        }

        .pdf-iframe {
            width: 100%;
            height: calc(100vh - 100px);  /* Use full viewport height minus some padding */
            border: none;
        }

        .content-columns {
            display: flex;
            gap: 0;
            margin: 20px 0;
            position: relative;
        }

        .left-column {
            flex: 0 0 50%;
            min-width: 200px;
            padding-right: 10px;
            overflow: auto;
        }

        .resizer {
            flex: 0 0 10px;
            background: #ddd;
            cursor: col-resize;
            position: relative;
            transition: background 0.2s;
        }

        .resizer:hover {
            background: #999;
        }

        .resizer::before {
            content: '⋮';
            position: absolute;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            color: #666;
            font-size: 16px;
        }

        .right-column {
            flex: 1;
            min-width: 200px;
            padding-left: 10px;
            overflow: auto;
        }

        @media (max-width: 1200px) {
            .content-columns {
                flex-direction: column;
            }

            .left-column {
                flex: 1;
                padding-right: 0;
                border-bottom: 1px solid #ddd;
                padding-bottom: 20px;
            }

            .resizer {
                display: none;
            }

            .right-column {
                flex: 1;
                padding-left: 0;
                padding-top: 20px;
            }
        }
    </style>
    <script>"""

    html += """
        // Auto-load all PDFs on page load
        document.addEventListener('DOMContentLoaded', function() {
            const iframes = document.querySelectorAll('.pdf-iframe');
            iframes.forEach(function(iframe) {
                const pdfPath = iframe.getAttribute('data-pdf-path');
                const page = iframe.getAttribute('data-page');
                if (pdfPath && page) {
                    // Use #page=N&zoom=FitH to open at specific page and fit width
                    iframe.src = pdfPath + '#page=' + page + '&zoom=FitH';
                }
            });

            // Setup resizable dividers
            const resizers = document.querySelectorAll('.resizer');
            resizers.forEach(function(resizer) {
                let leftColumn = null;
                let container = null;

                const onMouseDown = function(e) {
                    container = resizer.parentElement;
                    leftColumn = resizer.previousElementSibling;

                    document.body.style.cursor = 'col-resize';
                    document.body.style.userSelect = 'none';

                    // Add overlay to prevent iframe from capturing mouse events
                    const overlay = document.createElement('div');
                    overlay.id = 'resize-overlay';
                    overlay.style.cssText = 'position: fixed; top: 0; left: 0; right: 0; bottom: 0; z-index: 9999; cursor: col-resize;';
                    document.body.appendChild(overlay);

                    document.addEventListener('mousemove', onMouseMove);
                    document.addEventListener('mouseup', onMouseUp);

                    e.preventDefault();
                };

                const onMouseMove = function(e) {
                    if (!leftColumn || !container) return;

                    const containerRect = container.getBoundingClientRect();
                    const containerWidth = containerRect.width;
                    const mouseX = e.clientX - containerRect.left;
                    const newLeftPercent = (mouseX / containerWidth) * 100;

                    // Constrain between 20% and 80%
                    if (newLeftPercent >= 20 && newLeftPercent <= 80) {
                        leftColumn.style.flex = `0 0 ${newLeftPercent}%`;
                    }
                };

                const onMouseUp = function() {
                    document.body.style.cursor = '';
                    document.body.style.userSelect = '';

                    // Remove overlay
                    const overlay = document.getElementById('resize-overlay');
                    if (overlay) {
                        overlay.remove();
                    }

                    document.removeEventListener('mousemove', onMouseMove);
                    document.removeEventListener('mouseup', onMouseUp);

                    leftColumn = null;
                    container = null;
                };

                resizer.addEventListener('mousedown', onMouseDown);
            });
        });

        function loadPDF(testId, pdfPath, page) {
            const container = document.getElementById('pdf-viewer-' + testId);
            const btn = document.getElementById('load-btn-' + testId);
            const iframe = document.getElementById('pdf-iframe-' + testId);

            // Always show and load on first click
            container.style.display = 'block';
            btn.style.display = 'none';  // Hide button after click

            // Set the iframe source (lazy load)
            if (!iframe.src) {
                // Use #page=N&zoom=FitH to open at specific page and fit width
                iframe.src = pdfPath + '#page=' + page + '&zoom=FitH';
            }
        }
    </script>
</head>
<body>
"""

    html += f'<h1 style="text-align: center;">{title}</h1>'

    # Two-column grid layout for configuration and summary
    html += """
    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin: 20px 0;">
"""

    # Left column: Configuration
    html += """
        <div style="background: #f9f9f9; border: 1px solid #ddd; border-radius: 8px; padding: 20px;">
            <h2 style="margin-top: 0; color: #555;">Configuration</h2>
"""

    # Add paths
    html += f"""
            <ul style="font-size: 14px; color: #666; margin: 10px 0; list-style-type: none; padding-left: 0;">
                <li style="margin: 5px 0;"><strong>PDFs path:</strong> {Path(pdf_folder).resolve()}</li>
                <li style="margin: 5px 0;"><strong>MDs path:</strong> {md_folder}</li>
                <li style="margin: 5px 0;"><strong>GTs path:</strong> {Path(jsonl_folder).resolve() if jsonl_folder else 'N/A'}</li>
            </ul>
"""

    # Add parse mode and config if provided
    if parse_mode or parse_config:
        html += """            <hr style="margin: 15px 0; border: 0; border-top: 1px solid #ddd;">\n"""

        if parse_mode:
            html += f"""            <p style="margin: 10px 0;"><strong>Parser:</strong> <code style="background: #e8e8e8; padding: 2px 6px; border-radius: 3px;">{parse_mode}</code></p>\n"""

        if parse_config:
            # Format the config as pretty-printed JSON
            config_json = json.dumps(parse_config, indent=2)
            html += f"""
            <details style="margin-top: 10px;">
                <summary style="cursor: pointer; font-weight: bold; margin: 10px 0;">Show Parse Config (Click to expand)</summary>
                <pre style="background: #f5f5f5; padding: 15px; border: 1px solid #ddd; border-radius: 5px; overflow-x: auto; margin-top: 10px;"><code>{config_json}</code></pre>
            </details>
"""

    html += """        </div>
"""

    # Right column: Summary statistics
    if summary_stats:
        html += """
        <div style="background: #f0f8ff; border: 2px solid #2196F3; border-radius: 8px; padding: 20px;">
            <h2 style="margin-top: 0; color: #2196F3;">Summary</h2>
"""
        for candidate_data in summary_stats:
            candidate_name = candidate_data['name']
            overall_score = candidate_data['overall_score']
            # ci = candidate_data['ci']
            test_type_breakdown = candidate_data['test_type_breakdown']
            jsonl_results = candidate_data['jsonl_results']
            has_errors = candidate_data.get('has_errors', False)

            if has_errors:
                html += f"""
            <div style="margin-bottom: 20px;">
                <h3 style="color: #F44336;">{candidate_name}: FAILED (errors)</h3>
            </div>
"""
            else:
                # half_width = ((ci[1] - ci[0]) / 2) * 100
                html += f"""
                <h3>Scores per eval type:</h3>
                <ul style="list-style-type: none; padding-left: 20px;">
"""
                # Test type breakdown
                for ttype in sorted(test_type_breakdown.keys()):
                    scores = test_type_breakdown[ttype]
                    avg = sum(scores) / len(scores) * 100 if scores else 0.0
                    html += f"""                    <li><strong>{ttype}:</strong> {avg:.1f}% average pass rate over {len(scores)} tests</li>\n"""

                html += """                </ul>
                <h3>Scores per document type:</h3>
                <ul style="list-style-type: none; padding-left: 20px;">
"""
                for jsonl_file, results in sorted(jsonl_results.items()):
                    if results["total"] > 0:
                        pass_rate = (results["passed"] / results["total"]) * 100
                        html += f"""                    <li><strong>{jsonl_file}:</strong> {pass_rate:.1f}% ({results['passed']}/{results['total']} tests)</li>\n"""

                html += """                </ul>
"""

        html += """        </div>
"""
    else:
        # If no summary stats, add empty placeholder to maintain grid
        html += """
        <div style="background: #f9f9f9; border: 1px solid #ddd; border-radius: 8px; padding: 20px;">
            <h2 style="margin-top: 0; color: #555;">Summary</h2>
            <p style="color: #666;">No summary statistics available.</p>
        </div>
"""

    # Close the grid container
    html += """    </div>
"""

    # Collect all unique test types and document types for filters (before generating HTML)
    all_test_types_temp = set()
    all_doc_types_temp = set()
    for candidate in test_results_by_candidate:
        for pdf_name in test_results_by_candidate[candidate]:
            # Extract document type from PDF path (parent directory)
            doc_type = str(Path(pdf_name).parent) if Path(pdf_name).parent != Path('.') else 'root'
            all_doc_types_temp.add(doc_type)
            for page in test_results_by_candidate[candidate][pdf_name]:
                for test, passed, explanation in test_results_by_candidate[candidate][pdf_name][page]:
                    all_test_types_temp.add(test.type)

    # Add filters and distribution charts aligned side-by-side
    html += """
    <div style="margin: 20px auto; max-width: 1400px; padding: 0 20px;">
        <!-- Filters and Charts Grid -->
        <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 20px; align-items: start;">
            <!-- Status Filter & Chart -->
            <div>
                <div style="margin-bottom: 15px; text-align: center;">
                    <label for="filter-status" style="font-weight: bold; font-size: 20px; display: block; margin-bottom: 8px;">Status</label>
                    <select id="filter-status" onchange="applyFilters()" style="width: 100%; padding: 8px 12px; border: 1px solid #ddd; border-radius: 5px; font-size: 14px; cursor: pointer;">
                        <option value="all">All</option>
                        <option value="errors">Errors Only</option>
                        <option value="correct">Correct Only</option>
                    </select>
                </div>
                <div style="background: #f9f9f9; border: 1px solid #ddd; border-radius: 8px; padding: 15px;">
                    <h3 style="margin: 0 0 10px 0; font-size: 16px; color: #555; text-align: center;">Test Distribution</h3>
                    <div id="chart-status" style="min-height: 80px;"></div>
                </div>
            </div>

            <!-- Test Type Filter & Chart -->
            <div>
                <div style="margin-bottom: 15px; text-align: center;">
                    <label for="filter-test-type" style="font-weight: bold; font-size: 20px; display: block; margin-bottom: 8px;">Eval type</label>
                    <select id="filter-test-type" onchange="applyFilters()" style="width: 100%; padding: 8px 12px; border: 1px solid #ddd; border-radius: 5px; font-size: 14px; cursor: pointer;">
                        <option value="all">All</option>
"""

    # Add test type options
    for test_type in sorted(all_test_types_temp):
        html += f"""                        <option value="{test_type}">{test_type}</option>\n"""

    html += """                    </select>
                </div>
                <div style="background: #f9f9f9; border: 1px solid #ddd; border-radius: 8px; padding: 15px;">
                    <h3 style="margin: 0 0 10px 0; font-size: 16px; color: #555; text-align: center;">Test Distribution</h3>
                    <div id="chart-test-type" style="min-height: 80px;"></div>
                </div>
            </div>

            <!-- Doc Type Filter & Chart -->
            <div>
                <div style="margin-bottom: 15px; text-align: center;">
                    <label for="filter-doc-type" style="font-weight: bold; font-size: 20px; display: block; margin-bottom: 8px;">Doc type</label>
                    <select id="filter-doc-type" onchange="applyFilters()" style="width: 100%; padding: 8px 12px; border: 1px solid #ddd; border-radius: 5px; font-size: 14px; cursor: pointer;">
                        <option value="all">All</option>
"""

    # Add document type options
    for doc_type in sorted(all_doc_types_temp):
        html += f"""                        <option value="{doc_type}">{doc_type}</option>\n"""

    html += """                    </select>
                </div>
                <div style="background: #f9f9f9; border: 1px solid #ddd; border-radius: 8px; padding: 15px;">
                    <h3 style="margin: 0 0 10px 0; font-size: 16px; color: #555; text-align: center;">Test Distribution</h3>
                    <div id="chart-doc-type" style="min-height: 80px;"></div>
                </div>
            </div>
        </div>
    </div>
"""

    # Process all candidates
    print("Generating test report...")
    for candidate in candidates:
        # Count total evaluated tests (excluding missing MD files) for progress bar
        total_evaluated_tests = 0
        for pdf_name in test_results_by_candidate[candidate].keys():
            for page in test_results_by_candidate[candidate][pdf_name].keys():
                for test, passed, explanation in test_results_by_candidate[candidate][pdf_name][page]:
                    if explanation != "Missing MD files":
                        total_evaluated_tests += 1

        # Get all PDFs for this candidate
        all_pdfs = sorted(test_results_by_candidate[candidate].keys())

        # Process tests with accurate progress bar
        processed_tests = 0
        pbar = tqdm(total=total_evaluated_tests, desc=f"Processing {candidate}")

        for pdf_name in all_pdfs:
            pages = sorted(test_results_by_candidate[candidate][pdf_name].keys())

            for page in pages:
                # Get tests for this PDF page
                tests = test_results_by_candidate[candidate][pdf_name][page]

                # Filter out tests that were not evaluated due to missing MD files
                evaluated_tests = [(test, passed, explanation) for test, passed, explanation in tests
                                   if explanation != "Missing MD files"]

                # Skip this page entirely if no evaluated tests
                if not evaluated_tests:
                    continue

                for test, passed, explanation in evaluated_tests:
                    processed_tests += 1
                    pbar.update(1)

                    result_class = "pass" if passed else "fail"
                    status_text = "PASSED" if passed else "FAILED"
                    status_class = "pass-status" if passed else "fail-status"

                    # Begin test block
                    html += f"""
    <div class="test-block {result_class}" data-status="{result_class}" data-test-type="{test.type}" data-doc-type="{str(Path(pdf_name).parent) if Path(pdf_name).parent != Path('.') else 'root'}">
        <h3>Test ID: {test.id} <span class="status {status_class}">{status_text}</span></h3>
        <p><strong>PDF:</strong> {pdf_name} | <strong>Page:</strong> {page} | <strong>Type:</strong> {test.type}</p>

        <div class="test-details">
"""

                    # Add test details based on type
                    test_type = getattr(test, "type", "").lower()
                    if test_type == "present" and hasattr(test, "text"):
                        text = getattr(test, "text", "")
                        html += f"""            <p><strong>Text to find:</strong> "{text}"</p>\n"""
                    elif test_type == "absent" and hasattr(test, "text"):
                        text = getattr(test, "text", "")
                        html += f"""            <p><strong>Text should not appear:</strong> "{text}"</p>\n"""
                    elif test_type == "order" and hasattr(test, "before") and hasattr(test, "after"):
                        before = getattr(test, "before", "")
                        after = getattr(test, "after", "")
                        html += f"""            <p><strong>Text order:</strong> "{before}" should appear before "{after}"</p>\n"""
                    elif test_type == "table":
                        if hasattr(test, "cell"):
                            cell = getattr(test, "cell", "")
                            html += f"""            <p><strong>Table cell:</strong> "{cell}"</p>\n"""
                        if hasattr(test, "up") and getattr(test, "up", None):
                            up = getattr(test, "up")
                            html += f"""            <p><strong>Above:</strong> "{up}"</p>\n"""
                        if hasattr(test, "down") and getattr(test, "down", None):
                            down = getattr(test, "down")
                            html += f"""            <p><strong>Below:</strong> "{down}"</p>\n"""
                        if hasattr(test, "left") and getattr(test, "left", None):
                            left = getattr(test, "left")
                            html += f"""            <p><strong>Left:</strong> "{left}"</p>\n"""
                        if hasattr(test, "right") and getattr(test, "right", None):
                            right = getattr(test, "right")
                            html += f"""            <p><strong>Right:</strong> "{right}"</p>\n"""
                    elif test_type == "math" and hasattr(test, "math"):
                        math = getattr(test, "math", "")
                        html += f"""            <p><strong>Math equation:</strong> {math}</p>\n"""

                    html += """        </div>\n"""

                    # Add explanation for failed tests
                    if not passed:
                        html += f"""        <div class="test-explanation">
            <strong>Explanation:</strong> {explanation}
        </div>\n"""

                    # Two-column layout: PDF on left, Markdown on right
                    html += """        <div class="content-columns">\n"""

                    # Left column: PDF viewer
                    html += """            <div class="left-column">\n"""
                    pdf_path = os.path.join(pdf_folder, pdf_name)
                    test_id_safe = test.id.replace("/", "_").replace(".", "_").replace("-", "_")
                    try:
                        html += """                <h4>PDF Render:</h4>\n"""
                        # Use file:// protocol or relative path to PDF
                        pdf_url = f"file://{os.path.abspath(pdf_path)}"
                        html += f"""                <button class="load-pdf-btn" id="load-btn-{test_id_safe}" onclick="loadPDF('{test_id_safe}', '{pdf_url}', {page})">View PDF</button>\n"""
                        html += f"""                <div class="pdf-viewer-container" id="pdf-viewer-{test_id_safe}">
                    <iframe class="pdf-iframe" id="pdf-iframe-{test_id_safe}" title="PDF Page {page}" data-pdf-path="{pdf_url}" data-page="{page}"></iframe>
                </div>\n"""
                    except Exception as e:
                        html += f"""                <p>Error setting up PDF viewer: {str(e)}</p>\n"""
                    html += """            </div>\n"""

                    # Resizer divider
                    html += """            <div class="resizer"></div>\n"""

                    # Right column: Markdown content
                    html += """            <div class="right-column">\n"""
                    md_content = None
                    try:
                        md_base = os.path.splitext(pdf_name)[0]
                        # Look for MD files in the md_folder if provided, otherwise try relative to candidate
                        if md_folder:
                            # Try pattern 1: {md_base}_pg{page}_repeat*.md
                            md_pattern1 = os.path.join(md_folder, f"{md_base}_pg{page}_repeat*.md")
                            md_files = list(glob.glob(md_pattern1))
                            # Try pattern 2: {md_base}.md (exact match)
                            if not md_files:
                                md_pattern2 = os.path.join(md_folder, f"{md_base}.md")
                                if os.path.exists(md_pattern2):
                                    md_files = [md_pattern2]
                        else:
                            md_files = list(glob.glob(os.path.join(os.path.dirname(pdf_folder), candidate, f"{md_base}_pg{page}_repeat*.md")))

                        if md_files:
                            md_file_path = md_files[0]  # Use the first repeat as an example
                            with open(md_file_path, "r", encoding="utf-8") as f:
                                md_content = f.read()
                    except Exception as e:
                        md_content = f"Error loading Markdown content: {str(e)}"

                    if md_content:
                        html += """                <h4>Markdown Content:</h4>\n"""
                        html += f"""                <div class="markdown-content">{md_content}</div>\n"""
                    else:
                        html += """                <p>No Markdown content available</p>\n"""
                    html += """            </div>\n"""

                    html += """        </div>\n"""

                    # End test block
                    html += """    </div>\n"""

                # Add separator after the last test on this page
                html += """    <hr>\n"""

        # Close progress bar for this candidate
        pbar.close()

    # Add JavaScript for filtering and charts
    html += """
    <script>
        function updateCharts() {
            const testBlocks = document.querySelectorAll('.test-block');
            const statusFilter = document.getElementById('filter-status').value;
            const testTypeFilter = document.getElementById('filter-test-type').value;
            const docTypeFilter = document.getElementById('filter-doc-type').value;

            // Count distributions for visible tests
            const statusCounts = { pass: 0, fail: 0 };
            const testTypeCounts = {};
            const docTypeCounts = {};

            testBlocks.forEach(block => {
                const status = block.getAttribute('data-status');
                const testType = block.getAttribute('data-test-type');
                const docType = block.getAttribute('data-doc-type');

                let includeInStats = true;

                // Apply filters to determine which blocks to include in stats
                if (statusFilter === 'errors' && status !== 'fail') {
                    includeInStats = false;
                } else if (statusFilter === 'correct' && status !== 'pass') {
                    includeInStats = false;
                }

                if (includeInStats && testTypeFilter !== 'all' && testType !== testTypeFilter) {
                    includeInStats = false;
                }

                if (includeInStats && docTypeFilter !== 'all' && docType !== docTypeFilter) {
                    includeInStats = false;
                }

                if (includeInStats) {
                    // Count status
                    statusCounts[status] = (statusCounts[status] || 0) + 1;

                    // Count test type
                    testTypeCounts[testType] = (testTypeCounts[testType] || 0) + 1;

                    // Count doc type
                    docTypeCounts[docType] = (docTypeCounts[docType] || 0) + 1;
                }
            });

            // Render charts
            renderBarChart('chart-status', statusCounts, { pass: '#4CAF50', fail: '#F44336' });
            renderBarChart('chart-test-type', testTypeCounts);
            renderBarChart('chart-doc-type', docTypeCounts);
        }

        function renderBarChart(containerId, data, colorMap = {}) {
            const container = document.getElementById(containerId);
            // Filter out entries with 0 count and sort by count descending
            const entries = Object.entries(data).filter(([_, count]) => count > 0).sort((a, b) => b[1] - a[1]);
            const total = entries.reduce((sum, [_, count]) => sum + count, 0);

            if (total === 0) {
                container.innerHTML = '<p style="color: #999; font-size: 14px; margin: 0;">No data</p>';
                return;
            }

            const maxCount = Math.max(...entries.map(([_, count]) => count));

            let html = '<div style="display: flex; flex-direction: column; gap: 8px;">';
            entries.forEach(([label, count]) => {
                const percentage = ((count / total) * 100).toFixed(1);
                const barWidth = (count / maxCount) * 100;
                const color = colorMap[label] || '#2196F3';

                html += `
                    <div style="display: flex; align-items: center; gap: 8px; font-size: 13px;">
                        <div style="min-width: 80px; text-align: right; font-weight: 500;">${label}:</div>
                        <div style="flex: 1; background: #e0e0e0; border-radius: 3px; height: 20px; position: relative; overflow: hidden;">
                            <div style="background: ${color}; height: 100%; width: ${barWidth}%; transition: width 0.3s;"></div>
                        </div>
                        <div style="min-width: 60px; text-align: right; color: #666;">${count} (${percentage}%)</div>
                    </div>
                `;
            });
            html += '</div>';

            container.innerHTML = html;
        }

        function applyFilters() {
            const testBlocks = document.querySelectorAll('.test-block');
            const statusFilter = document.getElementById('filter-status').value;
            const testTypeFilter = document.getElementById('filter-test-type').value;
            const docTypeFilter = document.getElementById('filter-doc-type').value;

            testBlocks.forEach(block => {
                const status = block.getAttribute('data-status');
                const testType = block.getAttribute('data-test-type');
                const docType = block.getAttribute('data-doc-type');

                let showBlock = true;

                // Apply status filter
                if (statusFilter === 'errors' && status !== 'fail') {
                    showBlock = false;
                } else if (statusFilter === 'correct' && status !== 'pass') {
                    showBlock = false;
                }

                // Apply test type filter
                if (showBlock && testTypeFilter !== 'all' && testType !== testTypeFilter) {
                    showBlock = false;
                }

                // Apply document type filter
                if (showBlock && docTypeFilter !== 'all' && docType !== docTypeFilter) {
                    showBlock = false;
                }

                block.style.display = showBlock ? 'block' : 'none';
            });

            // Update charts after filtering
            updateCharts();
        }

        // Initialize charts on page load
        window.addEventListener('DOMContentLoaded', function() {
            updateCharts();
        });
    </script>
</body>
</html>
"""

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Simple HTML report generated: {output_file}")
