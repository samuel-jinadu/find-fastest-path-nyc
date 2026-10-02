#!/bin/bash

# === 1. Find the project root ===
# This script looks for pyproject.toml (uv/Poetry), requirements.txt, or .git
# to determine the project root. It works even if you run the script from
# inside a subdirectory.
get_project_root() {
    local dir="$1"
    while [ "$dir" != "/" ]; do
        if [ -f "$dir/pyproject.toml" ] || [ -f "$dir/requirements.txt" ] || [ -d "$dir/.git" ] || [ -f "$dir/main.py" ]; then
            echo "$dir"
            return 0
        fi
        dir="$(dirname "$dir")"
    done
    echo "ERROR: Could not find project root (no pyproject.toml, requirements.txt, or .git found)" >&2
    exit 1
}

# Determine script location and find project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(get_project_root "$SCRIPT_DIR")"

echo "Project root: $PROJECT_ROOT"
cd "$PROJECT_ROOT" || exit 1

# === 2. Run the find command ===
# Output file name
mkdir -p "$PROJECT_ROOT/scripts"
OUTPUT_FILE="$PROJECT_ROOT/scripts/project_snapshot.txt"

# Clear or create the output file
> "$OUTPUT_FILE"

# Find and process files, pruning cache/build/data directories.
#
# We prune:
#   - VCS/IDE folders: .git, .idea, .vscode, .direnv, .ipynb_checkpoints
#   - Python env/caches: .venv, venv, env, __pycache__, .pytest_cache,
#                        .mypy_cache, .ruff_cache, .tox, .eggs, *.egg-info, htmlcov
#   - Build artifacts:   build, dist
#   - Data/output:       data, output  (contains the large IoT JSON + Delta tables)
#
# We exclude (even inside included dirs):
#   - Compiled/binary files, raw data files, lockfiles, logs, notebooks
#   - The snapshot itself (prevents self-inclusion)
find . \
    \( \
        -name .git -o \
        -name .idea -o \
        -name .vscode -o \
        -name .direnv -o \
        -name .ipynb_checkpoints -o \
        -name .venv -o \
        -name venv -o \
        -name env -o \
        -name __pycache__ -o \
        -name .pytest_cache -o \
        -name .mypy_cache -o \
        -name .ruff_cache -o \
        -name .tox -o \
        -name .eggs -o \
        -name "*.egg-info" -o \
        -name htmlcov -o \
        -name build -o \
        -name dist -o \
        -name data -o \
        -name output \
    \) -prune -o \
    \( -type d -printf "[DIR]  %p\n" \) -o \
    \( -type f \
        ! -name "*.pyc" \
        ! -name "*.pyo" \
        ! -name "*.pyd" \
        ! -name "*.so" \
        ! -name "*.dylib" \
        ! -name "*.dll" \
        ! -name "*.class" \
        ! -name "*.jar" \
        ! -name "*.whl" \
        ! -name "*.db" \
        ! -name "*.db-shm" \
        ! -name "*.db-wal" \
        ! -name "*.csv" \
        ! -name "*.tsv" \
        ! -name "*.parquet" \
        ! -name "*.snappy.parquet" \
        ! -name "*.avro" \
        ! -name "*.orc" \
        ! -name "*.log" \
        ! -name "*.ipynb" \
        ! -name ".coverage" \
        ! -name ".DS_Store" \
        ! -name "poetry.lock" \
        ! -name "Pipfile.lock" \
        ! -name "package-lock.json" \
        ! -name "*.tmp" \
        ! -name "*.swp" \
        ! -name ".env" \
        ! -name ".env.*" \
        ! -name "project_snapshot.txt" \
        ! -name "pack-proj.sh" \
        ! -name "push.sh" \
        ! -name "uv.lock" \
        ! -name "pytest.ini" \
        ! -name "*.cfg" \
        -exec sh -c 'echo "--- $1 ---"; cat "$1"' _ {} \; \
    \) \
    >> "$OUTPUT_FILE"

echo "Snapshot written to: $OUTPUT_FILE"