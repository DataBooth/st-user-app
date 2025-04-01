"""
Import Analyser - A tool to analyse imports in Python files and Jupyter notebooks.
"""

import os
import argparse
import re
import json
import time
import random
import httpx
import asyncio
import sys
import csv
from pathlib import Path
from typing import List, Dict, Tuple, Optional, Any, Set
from collections import defaultdict
from bs4 import BeautifulSoup
import toml
import duckdb
from loguru import logger
from tqdm import tqdm

# Configure Loguru logger
logger.add("import_analyser.log", rotation="5 MB", level="INFO")

# Database path

DB_PATH = Path.home() / "code" / "data" / "local" / "import_analyser" / "snyk.duckdb"

def connect_to_db() -> duckdb.DuckDBPyConnection:
    """Connect to DuckDB database, creating the directory if needed."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(DB_PATH))

def load_config(config_path: str) -> Dict[str, Any]:
    """
    Load configuration from a TOML file.
    """
    try:
        config_path_obj = Path(config_path).resolve()
        logger.info(f"Loading configuration from: {config_path_obj}")  # Log config Path
        with open(str(config_path_obj), 'r', encoding='utf-8') as config_file:
            config = toml.load(config_file)
            logger.debug(f"Configuration loaded from {config_path_obj}")
            return config
    except FileNotFoundError:
        logger.error(f"Configuration file not found at {config_path}")
        return {}
    except toml.TomlDecodeError as e:
        logger.error(f"Error decoding TOML file: {e}")
        return {}

async def collect_python_files(directory_path: str, exclude_dirs: Optional[List[str]] = None) -> Tuple[List[Path], List[Path]]:
    """
    Recursively collect all .py and .ipynb files in the given directory.
    """
    py_files = []
    ipynb_files = []
    excluded_venv_directories = []  # Added this

    root_path = Path(directory_path).resolve()  # Added this

    if not root_path.exists() or not root_path.is_dir():
        logger.error(f"The provided path '{directory_path}' is not a valid directory.")
        raise ValueError(f"The provided path '{directory_path}' is not a valid directory.")

    logger.info(f"Resolved head directory: {root_path}")  # Added this

    exclude_dirs = exclude_dirs or ['.venv']  # Default exclude .venv

    for current_path, dirs, files in os.walk(str(root_path)):
        # Identify and log excluded .venv directories
        venv_directories = [d for d in dirs if d in exclude_dirs]
        excluded_venv_directories.extend([Path(current_path) / venv for venv in venv_directories])

        # Exclude directories specified in exclude_dirs
        dirs[:] = [d for d in dirs if d not in exclude_dirs]

        # Process each file in the current directory
        for file in files:
            file_path = Path(current_path) / file

            if file.endswith('.py'):
                py_files.append(file_path)
            elif file.endswith('.ipynb'):
                ipynb_files.append(file_path)

    for venv_dir in excluded_venv_directories:  # Added this loop
        logger.info(f"Excluding .venv directory: {venv_dir}")

    logger.info(f"Found {len(py_files)} Python files and {len(ipynb_files)} Jupyter notebooks")
    return py_files, ipynb_files


def extract_top_level_package(import_statement: str) -> Optional[str]:
    """
    Extract the top-level package name from an import statement.
    """
    import_pattern = r'^import\s+([a-zA-Z0-9_]+)(?:\.|$)'
    from_pattern = r'^from\s+([a-zA-Z0-9_]+)(?:\.|$)'

    import_match = re.match(import_pattern, import_statement.strip())
    if import_match:
        return import_match.group(1)

    from_match = re.match(from_pattern, import_statement.strip())
    if from_match:
        return from_match.group(1)

    return None


def process_python_file(file_path: Path) -> List[Tuple[str, str]]:
    """
    Process a Python file and extract import statements.
    """
    import_statements = []

    import_patterns = [
        r'^import\s+[a-zA-Z0-9_\.]+(?:\s+as\s+[a-zA-Z0-9_]+)?(?:\s*,\s*[a-zA-Z0-9_\.]+(?:\s+as\s+[a-zA-Z0-9_]+)?)*',
        r'^from\s+[a-zA-Z0-9_\.]+\s+import\s+(?:[a-zA-Z0-9_\*]+(?:\s+as\s+[a-zA-Z0-9_]+)?(?:\s*,\s*[a-zA-Z0-9_\*]+(?:\s+as\s+[a-zA-Z0-9_]+)?)*|\((?:[a-zA-Z0-9_\*]+(?:\s+as\s+[a-zA-Z0-9_]+)?(?:\s*,\s*[a-zA-Z0-9_\*]+(?:\s+as\s+[a-zA-Z0-9_]+)?)*)\))'
    ]

    try:
        with open(str(file_path), 'r', encoding='utf-8') as file:
            in_multiline_import = False
            current_import = ""

            for line in file:
                line = line.strip()

                if not line or line.startswith('#'):
                    continue

                if in_multiline_import:
                    current_import += " " + line
                    if ")" in line:
                        in_multiline_import = False
                        top_package = extract_top_level_package(current_import)
                        if top_package:
                            import_statements.append((current_import, top_package))
                        current_import = ""
                    continue

                is_import_line = False
                for pattern in import_patterns:
                    if re.match(pattern, line):
                        is_import_line = True
                        if "(" in line and ")" not in line:
                            in_multiline_import = True
                            current_import = line
                        else:
                            top_package = extract_top_level_package(line)
                            if top_package:
                                import_statements.append((line, top_package))
                        break
    except Exception as e:
        logger.error(f"Error processing {file_path}: {e}")

    return import_statements


def process_jupyter_notebook(file_path: Path) -> List[Tuple[str, str]]:
    """
    Process a Jupyter notebook file and extract import statements from code cells.
    """
    import_statements = []

    try:
        with open(str(file_path), 'r', encoding='utf-8') as file:
            notebook = json.load(file)

            for cell in notebook.get('cells', []):
                if cell.get('cell_type') != 'code':
                    continue

                if isinstance(cell.get('source'), list):
                    code = ''.join(cell.get('source', []))
                else:
                    code = cell.get('source', '')

                lines = code.split('\n')
                in_multiline_import = False
                current_import = ""

                for line in lines:
                    line = line.strip()

                    if not line or line.startswith('#'):
                        continue

                    if in_multiline_import:
                        current_import += " " + line
                        if ")" in line:
                            in_multiline_import = False
                            top_package = extract_top_level_package(current_import)
                            if top_package:
                                import_statements.append((current_import, top_package))
                            current_import = ""
                        continue

                    import_patterns = [
                        r'^import\s+[a-zA-Z0-9_\.]+(?:\s+as\s+[a-zA-Z0-9_]+)?(?:\s*,\s*[a-zA-Z0-9_\.]+(?:\s+as\s+[a-zA-Z0-9_]+)?)*',
                        r'^from\s+[a-zA-Z0-9_\.]+\s+import\s+(?:[a-zA-Z0-9_\*]+(?:\s+as\s+[a-zA-Z0-9_]+)?(?:\s*,\s*[a-zA-Z0-9_\*]+(?:\s+as\s+[a-zA-Z0-9_]+)?)*|\((?:[a-zA-Z0-9_\*]+(?:\s+as\s+[a-zA-Z0-9_]+)?(?:\s*,\s*[a-zA-Z0-9_\*]+(?:\s+as\s+[a-zA-Z0-9_]+)?)*)\))'
    ]

                    for pattern in import_patterns:
                        if re.match(pattern, line):
                            if "(" in line and ")" not in line:
                                in_multiline_import = True
                                current_import = line
                            else:
                                top_package = extract_top_level_package(line)
                                if top_package:
                                    import_statements.append((line, top_package))
                            break
    except Exception as e:
        logger.error(f"Error processing notebook {file_path}: {e}")

    return import_statements


def is_stdlib(package_name: str, python_version: str) -> bool:
    """
    Check if a package is part of the Python standard library.
    """
    import stdlib_list
    try:
        s = stdlib_list.stdlib_list(python_version)
        return package_name in s
    except stdlib_list.exceptions.VersionNotFound:
        logger.warning(f"No stdlib list found for Python version {python_version}. Assuming it is not stdlib")
        return False

async def fetch_snyk_data(package_name: str, python_version: str, config: Dict[str, Any], snyk_data_cache: Dict[str, Any], snyk_not_found_packages: List[Dict[str, str]]) -> Optional[Dict[str, str]]:
    """
    Fetch the Snyk package health score from the Snyk Advisor.
    """
    local_package_rule = config.get("local_package_rule", "src.")

    # Connect to the database
    conn = connect_to_db()
    # load from DB
    conn.execute("CREATE TABLE IF NOT EXISTS snyk_name_overrides (package_name VARCHAR, snyk_name VARCHAR)") #create a table

    if package_name.startswith(local_package_rule):
        return {'package_name': package_name, 'version': 'N/A', 'health_score': 'local'}

    if is_stdlib(package_name, python_version):
        return {'package_name': package_name, 'version': 'N/A', 'health_score': 'stdlib'}

    # Use overridden name if it exists - load from DB
    result = conn.execute("SELECT snyk_name FROM snyk_name_overrides WHERE package_name = ?", (package_name,)).fetchone()
    snyk_package_name = result[0] if result else package_name #load from database or package_name

    # Check cache
    if snyk_package_name in snyk_data_cache:
        return snyk_data_cache[snyk_package_name]

    # First Snyk URL
    url = f"https://snyk.io/advisor/python/{snyk_package_name}"
    # Second Swapped Snyk URL

    swapped_package_name = package_name.replace('_', '-') if '_' in package_name else package_name.replace('-', '_')
    swapped_url = f"https://snyk.io/advisor/python/{swapped_package_name}"

    delay = random.uniform(0.2, 0.6)
    time.sleep(delay)

    try:
        async with httpx.AsyncClient() as client:
            # First try with overridden Snyk Package Name
            try:
                response = await client.get(url, timeout=10)
                response.raise_for_status()
                effective_url = url  # Store effective URL
                check_extract = snyk_package_name
                used_swapped = False
            except httpx.HTTPStatusError as e:
                # If that fails, try swapping back to original name
                logger.debug(f"Snyk lookup failed for {snyk_package_name} (HTTP {e.response.status_code}). Trying {swapped_package_name}")
                response = await client.get(swapped_url, timeout=10)
                response.raise_for_status()
                effective_url = swapped_url  # Store effective URL
                check_extract = swapped_package_name
                used_swapped = True

            html_content = response.text

        soup = BeautifulSoup(html_content, 'html.parser')

        name_element = soup.find('h1', {'data-v-c3c1b2fe': True})
        version_element = soup.find('span', {'data-v-c3c1b2fe': True})

        if not name_element or not version_element:
            logger.warning(f"Could not find package name or version for {snyk_package_name} or {swapped_package_name}")
            snyk_not_found_packages.append({"name": package_name, "snyk_url": url})
            return None

        extracted_package_name = name_element.text.strip()
        extracted_version = version_element.text.strip().replace('v', '')

        health_score_div = soup.find('div', {'class': 'number', 'data-v-3f4fee08': True, 'data-v-77223d2e': True})
        if health_score_div:
            health_score_span = health_score_div.find('span', {'data-v-3f4fee08': True, 'data-v-77223d2e': True})
            if health_score_span:
                health_score_text = health_score_span.text.strip()
                health_score = health_score_text.split('/')[0].strip()
            else:
                health_score = "N/A"
        else:
            health_score = "N/A"

        snyk_data = {
            'package_name': extracted_package_name,
            'version': extracted_version,
            'health_score': health_score
        }

        snyk_data_cache[check_extract] = snyk_data #check extract
        #Update the snyk overrides table
        if used_swapped == True and snyk_package_name != swapped_package_name:

          conn.execute("INSERT INTO snyk_name_overrides (package_name, snyk_name) VALUES (?, ?)", (package_name, swapped_package_name))
          logger.info(f"Updating snyk_name_overrides: Added {package_name} = {swapped_package_name}")

        return snyk_data

    except httpx.HTTPStatusError as e:
        logger.warning(f"HTTP error {e.response.status_code} while fetching Snyk data for {snyk_package_name} and {swapped_package_name}: {e}")
        snyk_not_found_packages.append({"name": package_name, "snyk_url": url})
        return None
    except httpx.RequestError as e:
        logger.warning(f"Request error while fetching Snyk data for {snyk_package_name} and {swapped_package_name}: {e}")
        snyk_not_found_packages.append({"name": package_name, "snyk_url": url})
        return None
    except Exception as e:
        logger.exception(f"Error while fetching Snyk data for {snyk_package_name} and {swapped_package_name}: {e}")
        return None


async def analyse_imports(py_files: List[Path], ipynb_files: List[Path], python_version: str, config: Dict[str, Any], snyk_data_cache: Dict[str, Any], snyk_not_found_packages: List[Dict[str, str]]) -> Dict[str, Any]:
    """
    Analyse import statements in Python files and Jupyter notebooks.
    """
    imports_by_file = {}
    package_counts = defaultdict(int)
    package_snyk_data = {}

    all_files = py_files + ipynb_files
    with tqdm(total=len(all_files), desc="Analysing files") as pbar:
        # Process Python files
        for py_file in py_files:
            import_statements = process_python_file(py_file)
            if import_statements:
                imports_by_file[str(py_file)] = import_statements

                for _, package in import_statements:
                    package_counts[package] += 1
            pbar.update(1)

        # Process Jupyter notebook files
        for ipynb_file in ipynb_files:
            import_statements = process_jupyter_notebook(ipynb_file)
            if import_statements:
                imports_by_file[str(ipynb_file)] = import_statements

                for _, package in import_statements:
                    package_counts[package] += 1
            pbar.update(1)

    # Fetch Snyk data for each unique package
    unique_packages = list(package_counts.keys())
    for package in unique_packages:
        snyk_data = await fetch_snyk_data(package, python_version, config, snyk_data_cache, snyk_not_found_packages)
        if snyk_data:
            package_snyk_data[package] = snyk_data

    results = {
        'imports_by_file': imports_by_file,
        'package_counts': dict(sorted(package_counts.items(), key=lambda x: x[1], reverse=True)),
        'package_snyk_data': package_snyk_data
    }

    return results


def generate_reports(analysis_results: Dict[str, Any], snyk_not_found_packages: List[Dict[str, str]], output_dir: Optional[str] = None) -> Tuple[Path, Path, Path]:
    """
    Generate detailed log and summary CSV from analysis results.
    """
    output_dir = Path(output_dir) if output_dir else Path.cwd()
    log_path = output_dir / "import_log.txt"
    csv_path = output_dir / "import_summary.csv"
    snyk_not_found_path = output_dir / "snyk_not_found_packages.csv"

    output_dir.mkdir(parents=True, exist_ok=True)

    #Log the output paths
    logger.info(f"Detailed log will be written to: {log_path.resolve()}")
    logger.info(f"Summary CSV will be written to: {csv_path.resolve()}")
    logger.info(f"Snyk Not Found CSV will be written to: {snyk_not_found_path.resolve()}")

    # Generate detailed log file
    with open(str(log_path), 'w', encoding='utf-8') as log_file:
        log_file.write("IMPORT ANALYSIS DETAILED LOG\n")
        log_file.write("=========================\n\n")

        for file_path, imports in analysis_results['imports_by_file'].items():
            log_file.write(f"File: {file_path}\n")
            log_file.write("-" * (len(file_path) + 6) + "\n")

            for import_stmt, package in imports:
                log_file.write(f"  Import: {import_stmt}\n")
                log_file.write(f"  Package: {package}\n")

                # Add Snyk data to the log if available
                snyk_data = analysis_results['package_snyk_data'].get(package)
                if snyk_data:
                    log_file.write(f"  Snyk Package Name: {snyk_data['package_name']}\n")
                    log_file.write(f"  Snyk Version: {snyk_data['version']}\n")
                    log_file.write(f"  Snyk Health Score: {snyk_data['health_score']}\n")
                else:
                    log_file.write("  Snyk Data: Not Available\n")

                log_file.write("\n")

            log_file.write("\n")
        logger.info(f"Detailed log written to: {log_path}")

    # Generate summary CSV file
    with open(str(csv_path), 'w', encoding='utf-8') as csv_file:
        csv_file.write("package,count,snyk_package_name,snyk_version,snyk_health_score\n")

        for package, count in analysis_results['package_counts'].items():
            snyk_data = analysis_results['package_snyk_data'].get(package)
            if snyk_data:
                csv_file.write(
                    f"{package},{count},{snyk_data['package_name']},{snyk_data['version']},{snyk_data['health_score']}\n")
            else:
                csv_file.write(f"{package},{count},N/A,N/A,N/A\n")
        logger.info(f"Summary CSV written to: {csv_path}")

    # Generate snyk not found csv file
    with open(str(snyk_not_found_path), 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['name', 'snyk_url']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for row in snyk_not_found_packages:
            writer.writerow(row)
        logger.info(f"Snyk Not Found CSV written to: {snyk_not_found_path}")

    return log_path, csv_path, snyk_not_found_path


async def main() -> None:
    """
    Main function to run the import analyser.
    """
    parser = argparse.ArgumentParser(description='analyse Python imports in a directory')
    parser.add_argument('directory', type=str, help='Directory to analyse')
    parser.add_argument('--output', '-o', type=str, default=None,
                        help='Output directory for reports (default: current directory)')
    parser.add_argument('--exclude', '-e', type=str, default=None,
                        help='Comma-separated list of directories to exclude (default: .venv)')
    parser.add_argument('--config', '-c', type=str, default='config.toml',
                        help='Path to the configuration file (default: config.toml)')
    args = parser.parse_args()

    # Load configuration from TOML file
    config = load_config(args.config)

    # Get excluded directories from command line arguments
    exclude_dirs = args.exclude.split(',') if args.exclude else ['.venv']

    try:
        py_files, ipynb_files = await collect_python_files(args.directory, exclude_dirs)

        # Extract the Python version
        python_version = f"{sys.version_info.major}.{sys.version_info.minor}"

        snyk_data_cache = {}  # Initialise cache
        snyk_not_found_packages: List[Dict[str, str]] = []

        # analyse imports
        analysis_results = await analyse_imports(py_files, ipynb_files, python_version, config, snyk_data_cache, snyk_not_found_packages)

        log_path, csv_path, snyk_not_found_path = generate_reports(analysis_results, snyk_not_found_packages, args.output)

        logger.info(f"Detailed log written to: {log_path}")
        logger.info(f"Summary CSV written to: {csv_path}")
        logger.info(f"Snyk Not Found CSV written to: {snyk_not_found_path}")

    except Exception as e:
        logger.exception(f"Error: {e}")
        return 1

    return 0


if __name__ == "__main__":
    import asyncio
    import stdlib_list
    asyncio.run(main())
