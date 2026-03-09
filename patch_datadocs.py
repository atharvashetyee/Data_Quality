"""
GE DataDocs HTML Patcher
=========================
Patches Great Expectations generated HTML files to use local assets
instead of CDN links.

- NO CDN URLs hardcoded anywhere in this script
- Safe to keep on server
- Run after every build_data_docs() call

Usage:
    python patch_datadocs.py

Pipeline integration (in datadocs_generator.py):
    context.build_data_docs()
    from patch_datadocs import patch_datadocs_html
    patch_datadocs_html()
"""

import os
import re
import logging
from pathlib import Path

logger = logging.getLogger("DQPipeline.patch_datadocs")

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION — update GE root path if needed
# ─────────────────────────────────────────────────────────────────────────────

GE_ROOT = Path("outputs/great_expectations")
DATADOCS_ROOT = GE_ROOT / "uncommitted" / "data_docs" / "local_site"
ASSETS_DIR = DATADOCS_ROOT / "local_assets"


# ─────────────────────────────────────────────────────────────────────────────
# CORE PATCH LOGIC — no URLs hardcoded
# ─────────────────────────────────────────────────────────────────────────────

def _extract_local_filename(url: str) -> str:
    """
    Extracts a clean filename from any CDN URL.

    Examples:
        https://unpkg.com/bootstrap-table@1.19.1/dist/bootstrap-table.min.css
            → bootstrap-table.min.css

        https://cdn.jsdelivr.net/npm/vega@5
            → vega@5  (kept as-is, matched against local_assets/)

        https://great-expectations-web-assets.s3.amazonaws.com/logo-long.png?d=123
            → logo-long.png  (query string stripped)
    """
    # Strip query string
    clean_url = url.split("?")[0]
    # Get last path segment
    filename = clean_url.rstrip("/").split("/")[-1]
    return filename


def _find_local_asset(url: str, assets_dir: Path) -> str | None:
    """
    Given a CDN URL, finds the matching file in local_assets/ folder.
    Matches by checking if any local filename contains the extracted URL filename.
    Returns relative path string if found, None otherwise.
    """
    url_filename = _extract_local_filename(url).lower()

    # Remove version tags like @5, @4, @6 for matching
    url_filename_clean = re.sub(r'@[\d.]+', '', url_filename).lower()

    for local_file in assets_dir.iterdir():
        local_name = local_file.name.lower()
        # Try exact match first
        if url_filename_clean and url_filename_clean in local_name:
            return local_file.name
        # Try matching core name (strip version numbers from local file too)
        local_name_clean = re.sub(r'[-_][\d.]+', '', local_name)
        if url_filename_clean and url_filename_clean in local_name_clean:
            return local_file.name

    return None


def patch_datadocs_html() -> None:
    """
    Scans all GE-generated HTML files and replaces every CDN URL
    (href or src) with a relative path to local_assets/.

    - No CDN URLs hardcoded — dynamically matches whatever GE puts in
    - Safe to run multiple times (idempotent)
    - Skips URLs that don't have a matching file in local_assets/
    """
    # Validate paths
    if not DATADOCS_ROOT.exists():
        logger.error(f"DataDocs folder not found: {DATADOCS_ROOT}")
        print(f"✗ DataDocs folder not found:\n  {DATADOCS_ROOT}")
        return

    if not ASSETS_DIR.exists():
        logger.error(f"local_assets/ folder not found: {ASSETS_DIR}")
        print(f"✗ local_assets/ folder not found:\n  {ASSETS_DIR}")
        print(f"  Please copy local_assets/ folder to:\n  {DATADOCS_ROOT}")
        return

    # Count available local assets
    local_files = list(ASSETS_DIR.iterdir())
    if not local_files:
        print(f"✗ local_assets/ folder is empty: {ASSETS_DIR}")
        return

    html_files = list(DATADOCS_ROOT.rglob("*.html"))
    if not html_files:
        print("✗ No HTML files found in DataDocs folder.")
        return

    print(f"\n🔧 Patching {len(html_files)} HTML file(s)")
    print(f"   Using {len(local_files)} files from local_assets/\n")

    total_replaced = 0
    total_skipped_urls = 0

    for html_file in html_files:
        content = html_file.read_text(encoding="utf-8")
        original = content
        replaced_in_file = 0
        skipped_in_file = 0

        # Relative path from this HTML file to local_assets/
        rel_assets = os.path.relpath(ASSETS_DIR, html_file.parent).replace("\\", "/")

        # Find all CDN URLs in href and src attributes
        cdn_urls = re.findall(r'(?:href|src)="(https?://[^"]+)"', content)
        unique_cdn_urls = list(dict.fromkeys(cdn_urls))  # deduplicate, preserve order

        for url in unique_cdn_urls:
            local_filename = _find_local_asset(url, ASSETS_DIR)

            if local_filename:
                local_path = f"{rel_assets}/{local_filename}"
                # Replace all occurrences of this URL (including with query strings)
                base_url = url.split("?")[0]
                pattern = re.escape(base_url) + r'(?:\?[^"\']*)?'
                new_content, n = re.subn(pattern, local_path, content)
                if n:
                    content = new_content
                    replaced_in_file += n
            else:
                skipped_in_file += 1
                logger.debug(f"No local match for: {url}")

        if content != original:
            html_file.write_text(content, encoding="utf-8")
            print(f"  ✓ Patched ({replaced_in_file:2d} URLs replaced): "
                  f"{html_file.relative_to(DATADOCS_ROOT)}")
            total_replaced += replaced_in_file
        else:
            print(f"  ─ No changes needed:          "
                  f"{html_file.relative_to(DATADOCS_ROOT)}")

        total_skipped_urls += skipped_in_file

    print(f"\n{'─'*55}")
    print(f"  Total URLs replaced : {total_replaced}")
    if total_skipped_urls > 0:
        print(f"  URLs not matched    : {total_skipped_urls} "
              f"(no local file found — check local_assets/)")
    print()


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE INTEGRATION
# ─────────────────────────────────────────────────────────────────────────────

def localize_after_build() -> None:
    """
    Drop-in replacement for localize_after_build() from the old script.
    Call this immediately after context.build_data_docs() in your pipeline.

    In datadocs_generator.py:

        context.build_data_docs()
        from patch_datadocs import localize_after_build
        localize_after_build()
    """
    patch_datadocs_html()


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("GE DataDocs HTML Patcher")
    print("=" * 55)
    patch_datadocs_html()
    print("✅ Done. Open index.html — no internet required.\n")
