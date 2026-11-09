"""
Strip CSV header rows from already-generated LEAN trade archives.

LEAN data files must not contain a header row: the first line of the CSV inside
``{symbol}.zip`` has to be a valid data row.  Archives produced by older builds
of the pipeline / API started with::

    Time,Open,High,Low,Close,Volume

which makes LEAN reject every bar (``failed-data-requests-percentage: 100%``).

The generators (``utils.create_zip_file`` and ``achest.service.to_lean_zip``)
are fixed, but files already on disk keep the header.  This script rewrites
them in place, preserving the CSV filename inside each zip::

    soxx.zip
    └── soxx_daily_trade.csv   ← first line is now a data row

Usage::

    python fix_trade_headers.py                     # ../data (project data root)
    python fix_trade_headers.py ../data --dry-run
    python fix_trade_headers.py /path/to/Lean/Data/equity/usa/daily
"""

import argparse
import os
import sys
import zipfile

DATA_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data')

# First column of a valid LEAN row is a timestamp: "YYYYMMDD HH:MM" for daily
# data or milliseconds since midnight for intraday data.  Both start with a digit.
def _is_data_row(line: str) -> bool:
    return line.strip()[:1].isdigit()


def _strip_header(csv_text: str) -> str:
    """Remove leading non-data lines from LEAN CSV text."""
    lines = csv_text.splitlines()
    while lines and not _is_data_row(lines[0]):
        lines.pop(0)
    return ''.join(line + '\n' for line in lines)


def fix_zip(filepath: str, dry_run: bool = False) -> bool:
    """Rewrite *filepath* without CSV header rows.  Returns True if changed."""
    try:
        with zipfile.ZipFile(filepath, 'r') as z_in:
            entries = [(name, z_in.read(name)) for name in z_in.namelist()]
    except (zipfile.BadZipFile, OSError) as error:
        print(f"  SKIP {os.path.basename(filepath)}: {error}")
        return False

    csv_entries = [(name, data) for name, data in entries if name.endswith('.csv')]
    if not csv_entries:
        return False

    changed = False
    new_entries = []
    for name, data in entries:
        if not name.endswith('.csv'):
            new_entries.append((name, data))
            continue
        text = data.decode('utf-8-sig')
        fixed = _strip_header(text)
        if fixed != text:
            changed = True
        new_entries.append((name, fixed.encode('utf-8')))

    if not changed:
        return False

    print(f"  {'WOULD FIX' if dry_run else 'FIXED'} {os.path.basename(filepath)}")
    if dry_run:
        return True

    temp_path = filepath + '.tmp'
    with zipfile.ZipFile(temp_path, 'w', zipfile.ZIP_DEFLATED) as z_out:
        for name, data in new_entries:
            z_out.writestr(name, data)
    os.replace(temp_path, filepath)
    return True


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1] if __doc__ else None)
    parser.add_argument(
        'roots',
        nargs='*',
        default=[os.path.normpath(DATA_ROOT)],
        help='Directories to scan recursively for .zip LEAN archives '
             '(default: the project data/ directory)',
    )
    parser.add_argument('--dry-run', action='store_true', help='Report changes without writing files')
    args = parser.parse_args(argv)

    scanned = changed = 0
    for root in args.roots:
        if not os.path.isdir(root):
            print(f"ERROR: not a directory: {root}")
            return 2
        for dirpath, _dirnames, filenames in os.walk(root):
            for filename in sorted(filenames):
                if not filename.endswith('.zip'):
                    continue
                scanned += 1
                if fix_zip(os.path.join(dirpath, filename), dry_run=args.dry_run):
                    changed += 1

    verb = 'would be updated' if args.dry_run else 'updated'
    print(f"\nScanned {scanned} zip archive(s); {changed} {verb}.")
    return 0


if __name__ == '__main__':
    sys.exit(main())
