"""
fetch_clinvar.py — One-time enrichment script
Queries ClinVar for each variant in variant_table and saves results to clinvar_cache.json.

Run:  python3 fetch_clinvar.py
Output: clinvar_cache.json  (keyed by variant_id)

After running, copy clinvar_cache.json to SCC alongside app.py.
Re-run any time you want to refresh classifications.
"""

import json
import time
import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
import mariadb

# ── Config ────────────────────────────────────────────────────────────────────

DB = dict(host="bioed-new.bu.edu", database="Team8",
          user="zona7721", password="zona7721", port=4253)

# NCBI asks that you identify yourself via email.
# Optionally get a free API key at https://www.ncbi.nlm.nih.gov/account/
# With no key: 3 requests/sec limit.  With key: 10 requests/sec.
NCBI_EMAIL   = "zona890721@gmail.com"
NCBI_API_KEY = ""          # paste your key here if you have one, otherwise leave blank

CACHE_FILE   = "clinvar_cache.json"
DELAY        = 0.4         # seconds between requests (safe for no-key limit)

# ── NCBI helpers ──────────────────────────────────────────────────────────────

def _get(url):
    with urllib.request.urlopen(url, timeout=20) as r:
        return json.loads(r.read())

def _build(endpoint, **kwargs):
    kwargs["email"] = NCBI_EMAIL
    kwargs["retmode"] = "json"
    if NCBI_API_KEY:
        kwargs["api_key"] = NCBI_API_KEY
    return f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/{endpoint}?{urllib.parse.urlencode(kwargs)}"

def search_clinvar(gene, hgvsc):
    """Return list of ClinVar variation IDs matching gene + HGVS c. change."""
    # Search strategy: gene name AND the c. notation as a variant name
    # e.g.  ABCA4[gene] AND "c.1804C>T"[variant name]
    term = f'{gene}[gene] AND "{hgvsc}"[variant name]'
    url  = _build("esearch.fcgi", db="clinvar", term=term, retmax=5)
    try:
        data = _get(url)
        return data.get("esearchresult", {}).get("idlist", [])
    except Exception as e:
        print(f"    esearch error: {e}")
        return []

def fetch_summary(var_id):
    """Fetch ClinVar esummary for a single variation ID — significance, review status, GRCh38 position."""
    url = _build("esummary.fcgi", db="clinvar", id=var_id)
    try:
        data   = _get(url)
        result = data.get("result", {})
        entry  = result.get(str(var_id))
        if not entry:
            return None

        # ── Clinical significance & review status ─────────────────────────
        # Newer ClinVar API uses 'germline_classification'; older used 'clinical_significance'
        clf = entry.get("germline_classification") or entry.get("clinical_significance") or {}
        significance  = clf.get("description", "")
        review_status = clf.get("review_status", "")

        # ── GRCh38 chromosome position (ref/alt come from a separate efetch call) ──
        grch38_loc = ""
        for vset in entry.get("variation_set", []):
            for loc in vset.get("variation_loc", []):
                if loc.get("assembly_name") == "GRCh38":
                    chrom = loc.get("chr", "")
                    start = loc.get("start", "")
                    grch38_loc = f"{chrom}:{start}" if chrom and start else ""
                    break
            if grch38_loc:
                break

        title = entry.get("title", "")

        return {
            "clinvar_id":    var_id,
            "title":         title,
            "significance":  significance,
            "review_status": review_status,
            "grch38_loc":    grch38_loc,
            "grch38_change": "",   # filled in by fetch_alleles()
        }
    except Exception as e:
        print(f"    esummary error: {e}")
        return None

def fetch_alleles(var_id):
    """
    Fetch ref/alt alleles from ClinVar VCV XML (efetch).
    The esummary JSON does not include ref/alt in variation_loc — they only appear
    in the full VCV XML as <SequenceLocation referenceAllele="G" alternateAllele="A"/>.
    Returns a string like "G>A", or "" if not found.
    """
    key_parts = {"email": NCBI_EMAIL}
    if NCBI_API_KEY:
        key_parts["api_key"] = NCBI_API_KEY
    params = urllib.parse.urlencode({**key_parts,
                                     "db": "clinvar", "id": var_id,
                                     "rettype": "vcv", "retmode": "xml"})
    url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?{params}"
    try:
        with urllib.request.urlopen(url, timeout=20) as r:
            xml_bytes = r.read()
        root = ET.fromstring(xml_bytes)
        # Walk every <SequenceLocation> element; pick the GRCh38 one
        for loc in root.iter("SequenceLocation"):
            if loc.get("Assembly") == "GRCh38":
                ref = loc.get("referenceAllele", "")
                alt = loc.get("alternateAllele", "")
                if ref and alt:
                    return f"{ref}>{alt}"
        return ""
    except Exception as e:
        print(f"    efetch/allele error: {e}")
        return ""

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    # Load any existing cache so we can resume if interrupted
    try:
        with open(CACHE_FILE) as f:
            cache = json.load(f)
        print(f"Loaded existing cache with {len(cache)} entries.")
    except FileNotFoundError:
        cache = {}

    # Fetch all variants from the database
    conn = mariadb.connect(**DB)
    cur  = conn.cursor()
    cur.execute("""
        SELECT variant_id, gene, hgvsc
        FROM variant_table
        WHERE gene IS NOT NULL AND gene != ''
          AND hgvsc IS NOT NULL AND hgvsc != ''
        ORDER BY gene, variant_id
    """)
    variants = cur.fetchall()
    conn.close()

    total   = len(variants)
    matched = 0
    skipped = 0

    print(f"\nProcessing {total} variants...\n")

    for i, (variant_id, gene, hgvsc) in enumerate(variants, 1):
        key = str(variant_id)

        # Skip if already cached
        if key in cache:
            skipped += 1
            print(f"[{i}/{total}] {gene} {hgvsc}  →  (cached, skipping)")
            continue

        print(f"[{i}/{total}] {gene} {hgvsc}", end="  →  ", flush=True)

        # Step 1: search for ClinVar IDs
        ids = search_clinvar(gene, hgvsc)
        time.sleep(DELAY)

        if not ids:
            print("no ClinVar match")
            cache[key] = None
            continue

        # Step 2: fetch summary (significance, review status, GRCh38 position)
        result = fetch_summary(ids[0])
        time.sleep(DELAY)

        if not result:
            print("summary fetch failed")
            cache[key] = None
            continue

        # Step 3: fetch ref/alt alleles from VCV XML (separate call needed)
        result["grch38_change"] = fetch_alleles(ids[0])
        time.sleep(DELAY)

        matched += 1
        print(f"{result['significance']}  |  {result['review_status']}  |  {result['grch38_loc']} {result['grch38_change']}")

        cache[key] = result

        # Save incrementally every 10 variants so progress isn't lost on interruption
        if i % 10 == 0:
            with open(CACHE_FILE, "w") as f:
                json.dump(cache, f, indent=2)
            print(f"  ── progress saved ({i}/{total}) ──")

    # Final save
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2)

    print(f"\nDone.")
    print(f"  Matched : {matched}/{total - skipped} queried")
    print(f"  Skipped : {skipped} (already cached)")
    print(f"  Saved to: {CACHE_FILE}")

if __name__ == "__main__":
    main()
