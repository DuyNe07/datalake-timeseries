#!/usr/bin/env python3
"""
Command line script to show Nessie commit graph.
Run this script directly from the command line.
"""
import sys
import os

# Find the absolute path to the modules directory
# This handles both Windows and Linux paths
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)
modules_dir = os.path.join(parent_dir, "modules")

# Add modules directory to system path
if modules_dir not in sys.path:
    sys.path.insert(0, modules_dir)

# Import main function from show_commit_graph
try:
    # Try importing as if we're in the installed package
    from modules.show_commit_graph import main
except ImportError:
    # Try a direct import if the package path doesn't work
    try:
        from show_commit_graph import main
    except ImportError:
        # Last resort: try with just the modules directory
        sys.path.insert(0, os.path.join(parent_dir, "modules"))
        from show_commit_graph import main

if __name__ == "__main__":
    print(f"Using modules from: {modules_dir}")
    sys.exit(main())
