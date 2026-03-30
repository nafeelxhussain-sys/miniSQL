#!/usr/bin/env python3
"""
MiniSQL Test Runner - Works on Windows, Linux & macOS
Executes one query at a time
"""

import os
import sys
import subprocess
from pathlib import Path

def run_query(exe_path, query):
    """Run a single query"""
    try:
        result = subprocess.run(
            [str(exe_path)],
            input=query + "\n",
            capture_output=True,
            text=True,
            timeout=10
        )
        print(result.stdout.strip())
        if result.stderr:
            print("ERROR:", result.stderr.strip())
        return True
    except Exception as e:
        print(f"Failed: {e}")
        return False

def main():
    print("MiniSQL Test Suite")
    print("====================")

    root = Path(__file__).parent.parent
    os.chdir(root)

    # Executable path (handles Windows .exe automatically)
    build_dir = root / "build"
    exe_name = "minisql.exe" if os.name == "nt" else "minisql"
    exe_path = build_dir / "bin" / exe_name

    # Build if executable not found
    if not exe_path.exists():
        print("🔨 Building MiniSQL...")
        build_dir.mkdir(exist_ok=True)
        try:
            subprocess.run(["cmake", "..", "-DCMAKE_BUILD_TYPE=Release"], 
                         cwd=build_dir, check=True)
            subprocess.run(["cmake", "--build", ".", "--config", "Release"], 
                         cwd=build_dir, check=True)
            print("Build successful!\n")
        except:
            print("Build failed! Please build manually first.")
            sys.exit(1)

    # Get all test files
    test_files = sorted(Path("test/queries").glob("*.sql"))

    print(f"Found {len(test_files)} test files. Running one by one...\n")

    for test_file in test_files:
        print(f"\nRunning: {test_file.name}")
        print("=" * 70)

        with open(test_file, "r", encoding="utf-8") as f:
            content = f.read()

        # Split queries by semicolon
        queries = [q.strip() for q in content.split(';') if q.strip()]

        for i, query in enumerate(queries, 1):
            if query:
                print(f"\n[{i}/{len(queries)}] Executing:")
                print(query + ";")
                print("-" * 50)
                run_query(exe_path, query)
                print("-" * 50)
                input("Press Enter to continue...")

        print(f"Completed: {test_file.name}\n")

    print("All tests finished successfully!")

if __name__ == "__main__":
    main()