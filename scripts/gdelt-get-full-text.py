"""
HWC Extraction Pilot — Jina AI + Ollama (local LLM)
=====================================================
Tests the full extraction pipeline on a sample of articles:
  1. Fetch article text via Jina AI (r.jina.ai/<url>) — free, no setup
  2. Fall back to trafilatura if Jina fails
  3. Send text to local Ollama LLM for structured HWC event extraction
  4. Geocode primary_location via Google Maps Geocoding API
  5. Fall back to GDELT district coords when geocoding fails
  6. Save results + a quality report

Requirements:
    pip install trafilatura requests pandas tqdm
    pip install selenium   # optional — for --retry-failed-from

  Chrome / chromedriver (Selenium retry):
    - Default matches archive-search fetch_linked.py: plain Options(), non-headless, then
      WebDriverWait(body). For headless or Linux/Docker, see env vars below.
    - If Selenium Manager cannot download drivers (firewall/offline), set:
          export CHROMEDRIVER_PATH=/path/to/chromedriver
    - CHROME_BIN / GOOGLE_CHROME_BIN — non-default Chrome/Chromium binary
    - CHROME_HEADLESS=1 — headless + typical server flags (default is 0: visible window)
    - CHROME_NO_SANDBOX=1 — add --no-sandbox --disable-dev-shm-usage (Linux/WSL/Docker)
    - Selenium proxy: use CLI ``--proxy-server URL`` (not environment variables).

API keys needed:
    GOOGLE_MAPS_API_KEY   — https://console.cloud.google.com (Geocoding API)
                            (optional — falls back to GDELT coords if absent)

Two-run workflow
----------------
  1) First run (no --retry-failed-from): reads --input (default data/hwc_urls_geocoded.csv)
     — URL, title, GDELT fields; no fetch_method column. Writes data/hwc_final_report.csv.
  2) Second run: pass --retry-failed-from (default file: data/hwc_final_report.csv if you use
     the flag alone). Loads the previous run CSV (has fetch_method), retries only failed
     URLs with Selenium, leaves successful rows unchanged, writes merged CSV.

Usage:
    python scripts/gdelt-get-full-text.py --sample 50
    python scripts/gdelt-get-full-text.py --input data/hwc_urls_geocoded.csv --sample 15 -v
    python scripts/gdelt-get-full-text.py --retry-failed-from
    python scripts/gdelt-get-full-text.py --retry-failed-from data/hwc_final_report.csv --output data/hwc_final_report_updated.csv
    python scripts/gdelt-get-full-text.py --retry-failed-from --proxy-server http://proxy.example.com:8080
"""

import argparse
import json
import logging
import os
import random
import re
import sys
import time
from pathlib import Path

import pandas as pd
import requests
import trafilatura
from tqdm import tqdm

# ── Config ────────────────────────────────────────────────────────────────────

OLLAMA_BASE_URL     = "http://10.237.20.197:11434"
DEFAULT_MODEL       = "qwen2.5:14b"
GOOGLE_MAPS_API_KEY = ""

JINA_BASE         = "https://r.jina.ai/"
GEOCODING_URL     = "https://maps.googleapis.com/maps/api/geocode/json"
MAX_ARTICLE_CHARS = 6000   # truncate very long articles before sending to LLM
SLEEP_JINA        = 1.5    # seconds between Jina requests (be polite)
SLEEP_LLM         = 0.2    # seconds between Ollama requests (local, so minimal)
SLEEP_GEOCODE     = 0.2    # seconds between geocoding requests
SLEEP_SELENIUM    = 2.0    # pause after page load for JS-rendered article body
OLLAMA_TIMEOUT    = 120    # seconds — 14b model may be slow under load
MIN_ARTICLE_CHARS = 200    # same threshold as Jina / trafilatura fetch
SELENIUM_PAGE_LOAD_TIMEOUT = 60  # seconds for driver.get()

log = logging.getLogger(__name__)


def setup_logging(verbose: bool) -> None:
    """DEBUG on this module's logger when --verbose; does not change root logging."""
    for h in list(log.handlers):
        log.removeHandler(h)
    if verbose:
        log.setLevel(logging.DEBUG)
        h = logging.StreamHandler(sys.stderr)
        h.setFormatter(logging.Formatter("%(levelname)s [hwc-pilot] %(message)s"))
        log.addHandler(h)
    else:
        log.setLevel(logging.WARNING)
    log.propagate = False


def _location_usable_for_geocode(loc) -> bool:
    """Reject null / empty / literal 'null' strings from LLM JSON."""
    if loc is None or (isinstance(loc, float) and pd.isna(loc)):
        return False
    s = str(loc).strip()
    if not s or s.lower() in ("null", "none", "n/a", ""):
        return False
    return True


def _pilot_scalar_for_csv(v):
    """
    Coerce values for pilot result cells. With dtype=str + pandas StringDtype, only str
    is accepted; numpy scalars and pd.NA are normalized to str for CSV-safe output.
    """
    if v is None:
        return ""
    if isinstance(v, float) and pd.isna(v):
        return ""
    try:
        if v is pd.NA:
            return ""
    except TypeError:
        pass
    try:
        if pd.isna(v) and not isinstance(v, bool):
            return ""
    except (TypeError, ValueError):
        pass
    if isinstance(v, bool):
        return str(v)
    if isinstance(v, (int, float)):
        return str(v)
    return str(v)


# ── LLM extraction prompt ─────────────────────────────────────────────────────
# Adapted from the Groundsource paper's Appendix A prompt structure,
# modified for human-wildlife conflict events in India.

SYSTEM_PROMPT = """You are a meticulous wildlife conflict event analyst specialising in India.
Your task is to analyse Indian news articles and extract structured data about
human-wildlife conflict (HWC) events. You must respond ONLY with a single clean
JSON object — no markdown, no explanations, nothing outside the JSON."""

EXTRACTION_PROMPT = """Analyse this news article and extract human-wildlife conflict event data.

Publication date: {pub_date}
URL: {url}
GDELT candidate locations: {gdelt_locations}

Article text:
{article_text}

---

Follow these steps carefully:

STEP 1 — Is this an actual HWC event?
An HWC event is a specific, real incident that has already occurred involving:
  - A wild animal attacking, injuring, or killing a human
  - A human killing or injuring a wild animal (poaching counts)
  - Wild animals destroying crops, livestock, or property
  - Human deaths/injuries while trying to drive animals away
  - Conflicts over wildlife corridors or forest land

NOT an HWC event:
  - Policy discussions, budget allocations, conservation plans
  - Warnings about potential future conflicts
  - General statistics without a specific incident
  - Multiple unrelated incidents summarised together
  - Court cases or legal discussions without a specific triggering event

If NOT an HWC event → output the "no event" JSON (see format below) and stop.

STEP 2 — Extract event details (only if Step 1 = true)
  a) species: the primary animal involved. Use lowercase common name.
     Options: elephant, tiger, leopard, bear, wild boar, crocodile, snake,
              wolf, nilgai, gaur, rhino, monkey, other (specify)
  b) event_type: primary type of incident.
     Options: attack_on_human, human_killed, human_injured, crop_raid,
              livestock_kill, animal_killed_by_human, animal_injured,
              road_collision, corridor_conflict, other (specify)
  c) victims: numbers mentioned. Use null if not stated.
     - humans_killed, humans_injured, animals_killed, animals_injured
  d) event_date: the date the event occurred (not publication date).
     Format YYYY-MM-DD. Use publication date if only "recently" or similar.
     Use null if completely unknown.
  e) primary_location: the single most specific place name explicitly stated
     as where the incident occurred. Prefer village > forest range/division >
     taluka > district. Write as "Place, District, State, India".
     Example: "Nilambur, Malappuram, Kerala, India"
     Use null if no specific location is mentioned.
  f) location_type: how specific is primary_location?
     Options: village, town, forest_range, forest_division,
              taluka, district, state, unknown
  g) location_notes: any additional location context from the article
     (e.g. nearby landmarks, forest division name, river name). Max 100 chars.
  h) confidence: your confidence in the extraction overall.
     Options: high, medium, low

STEP 3 — Location reconciliation
  Compare primary_location against the GDELT candidate locations provided above.
  If one matches (same place, same or coarser specificity), record it.
  If none match, record null.

Output format (always return exactly this JSON structure):

{{
  "is_hwc_event": true or false,
  "species": "...",
  "event_type": "...",
  "victims": {{
    "humans_killed": null,
    "humans_injured": null,
    "animals_killed": null,
    "animals_injured": null
  }},
  "event_date": "YYYY-MM-DD or null",
  "primary_location": "Place, District, State, India or null",
  "location_type": "...",
  "location_notes": "...",
  "gdelt_location_match": "matched GDELT location or null",
  "confidence": "high/medium/low",
  "extraction_notes": "any caveats or uncertainties, max 150 chars"
}}
"""

# ── Article fetching ──────────────────────────────────────────────────────────

def fetch_via_jina(url: str, timeout: int = 20) -> str | None:
    """Fetch article text via Jina AI reader (r.jina.ai). Returns clean text or None."""
    try:
        jina_url = JINA_BASE + url
        headers = {
            "Accept": "text/plain",
            "X-Return-Format": "text",       # plain text, no markdown
            "X-Timeout": str(timeout),
        }
        resp = requests.get(jina_url, headers=headers, timeout=timeout + 5)
        if resp.status_code == 200 and len(resp.text.strip()) > 200:
            return resp.text.strip()
        return None
    except Exception as e:
        print(f"Error fetching {url} via Jina: {e}")
        return None


def fetch_via_trafilatura(url: str) -> str | None:
    """Fall back to trafilatura for article extraction."""
    try:
        downloaded = trafilatura.fetch_url(url)
        if downloaded:
            text = trafilatura.extract(downloaded, include_comments=False,
                                       include_tables=False)
            if text and len(text.strip()) > 200:
                return text.strip()
        return None
    except Exception as e:
        print(f"Error fetching {url} via trafilatura: {e}")
        return None


def fetch_article(url: str) -> tuple[str | None, str]:
    """Try Jina first, fall back to trafilatura. Returns (text, method)."""
    text = fetch_via_jina(url)
    if text:
        return text, "jina"
    time.sleep(0.5)
    text = fetch_via_trafilatura(url)
    if text:
        return text, "trafilatura"
    return None, "failed"


def _is_failed_fetch_method(val) -> bool:
    """Pilot CSV uses fetch_method='failed' when Jina + trafilatura both failed."""
    if val is None:
        return False
    if isinstance(val, float) and pd.isna(val):
        return False
    s = str(val).strip().lower()
    return s in ("failed", "false", "0", "no")


def make_chrome_driver(proxy_server: str | None = None):
    """
    Chrome for newspaper pages that block simple HTTP clients.

    Defaults follow archive-search ``fetch_linked.py``: plain ``Options()`` (no headless),
    then ``webdriver.Chrome(options=...)``. Many sites behave better with a real window.

    Env:
      CHROMEDRIVER_PATH / CHROMEDRIVER — explicit chromedriver if Selenium Manager fails.
      CHROME_BIN / GOOGLE_CHROME_BIN — browser binary.
      CHROME_HEADLESS=1 — use --headless=new plus server-oriented flags.
      CHROME_NO_SANDBOX=1 — add --no-sandbox and --disable-dev-shm-usage (Linux/Docker).

    proxy_server — if set, passed as Chrome ``--proxy-server=...`` (see CLI ``--proxy-server``).
    """
    try:
        from selenium import webdriver
        from selenium.common.exceptions import WebDriverException
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
    except ImportError as e:
        raise SystemExit(
            "Selenium is required for --retry-failed-from. Install with: pip install selenium"
        ) from e

    opts = Options()
    # Match fetch_linked.py: non-headless by default (headless commented there).
    headless = os.environ.get("CHROME_HEADLESS", "0").strip().lower() in (
        "1", "true", "yes",
    )
    if headless:
        opts.add_argument("--headless=new")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--window-size=1920,1080")
        opts.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

    if os.environ.get("CHROME_NO_SANDBOX", "").strip().lower() in ("1", "true", "yes"):
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        log.debug("Selenium: CHROME_NO_SANDBOX enabled")

    if proxy_server and str(proxy_server).strip():
        p = str(proxy_server).strip()
        opts.add_argument(f"--proxy-server={p}")
        log.debug("Selenium: --proxy-server=%s", p)

    chrome_bin = os.environ.get("CHROME_BIN") or os.environ.get("GOOGLE_CHROME_BIN")
    if chrome_bin:
        opts.binary_location = chrome_bin
        log.debug("Selenium: CHROME_BIN=%s", chrome_bin)

    driver_exe = os.environ.get("CHROMEDRIVER_PATH") or os.environ.get("CHROMEDRIVER")
    if driver_exe:
        service = Service(executable_path=driver_exe)
        log.debug("Selenium: using explicit CHROMEDRIVER_PATH=%s", driver_exe)
    else:
        service = Service()
        log.debug(
            "Selenium: no CHROMEDRIVER_PATH — Selenium Manager / cache "
            "(set CHROMEDRIVER_PATH if startup fails)"
        )

    log.debug("Selenium: creating Chrome (headless=%s, minimal_options=%s)", headless, not headless)
    try:
        driver = webdriver.Chrome(service=service, options=opts)
    except WebDriverException as e:
        raise SystemExit(
            "Could not start Chrome/Chromedriver for Selenium retry.\n"
            "  • Install Chrome/Chromium and a matching chromedriver (same major version).\n"
            "  • If drivers cannot be downloaded (offline/firewall):\n"
            "      export CHROMEDRIVER_PATH=/full/path/to/chromedriver\n"
            "  • Non-default browser:\n"
            "      export CHROME_BIN=/full/path/to/google-chrome\n"
            "  • Headless (servers): export CHROME_HEADLESS=1\n"
            "  • Linux/Docker: export CHROME_NO_SANDBOX=1\n"
            "  • HTTP(S) proxy: --proxy-server http://host:8080\n"
            f"  Underlying error: {e}"
        ) from e
    log.debug("Selenium: webdriver started (session id prefix=%s)", (driver.session_id or "")[:12])
    return driver


def fetch_html_selenium(driver, url: str) -> str | None:
    """
    Load URL in browser and return rendered HTML.

    Same flow as archive-search ``fetch_linked.py``: ``get`` → wait for ``body`` →
    optional short sleep for late JS → ``page_source``.
    """
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait

    log.debug("Selenium fetch: GET %r", url[:500] + ("…" if len(url) > 500 else ""))
    try:
        driver.set_page_load_timeout(SELENIUM_PAGE_LOAD_TIMEOUT)
        log.debug(
            "Selenium fetch: page_load_timeout=%ss, WebDriverWait(body) + sleep=%ss",
            SELENIUM_PAGE_LOAD_TIMEOUT,
            SLEEP_SELENIUM,
        )
        driver.get(url)
        WebDriverWait(driver, SELENIUM_PAGE_LOAD_TIMEOUT).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        time.sleep(SLEEP_SELENIUM)
        html = driver.page_source
        try:
            title = driver.title
        except Exception:
            title = ""
        cur = ""
        try:
            cur = driver.current_url
        except Exception:
            pass
        log.debug(
            "Selenium fetch: page_source len=%s title=%r current_url=%r",
            len(html) if html else 0,
            (title[:120] + "…") if len(str(title)) > 120 else title,
            (cur[:200] + "…") if len(str(cur)) > 200 else cur,
        )
        if cur and cur.rstrip("/") != url.rstrip("/"):
            log.debug("Selenium fetch: URL redirected/changed from request URL")
        return html
    except Exception as e:
        log.debug("Selenium fetch: exception type=%s", type(e).__name__, exc_info=True)
        msg = str(e)
        log.warning("Selenium could not load %s: %s", url[:100], e)
        if "ERR_TUNNEL_CONNECTION_FAILED" in msg or "ERR_PROXY" in msg:
            log.warning(
                "  Hint: tunnel/proxy error — try --proxy-server URL or fix network/VPN/firewall."
            )
        return None


def extract_text_from_html(html: str, url: str) -> str | None:
    """Parse article body from full page HTML with trafilatura."""
    if not html:
        log.debug("extract_text_from_html: empty html for %r", url[:120])
        return None
    if len(html) < 500:
        log.debug(
            "extract_text_from_html: html too short (%s chars < 500), skip trafilatura",
            len(html),
        )
        return None
    log.debug("extract_text_from_html: html len=%s, running trafilatura.extract", len(html))
    try:
        text = trafilatura.extract(
            html,
            url=url,
            include_comments=False,
            include_tables=False,
        )
        n = len(text.strip()) if text else 0
        log.debug(
            "extract_text_from_html: trafilatura raw len=%s (min_article=%s)",
            n,
            MIN_ARTICLE_CHARS,
        )
        if text and n > MIN_ARTICLE_CHARS:
            log.debug("extract_text_from_html: OK — returning %s chars", n)
            return text.strip()
        log.debug(
            "extract_text_from_html: rejected — text too short or empty after extract",
        )
    except Exception as e:
        log.debug("extract_text_from_html: trafilatura exception", exc_info=True)
        log.warning("trafilatura extract failed for %s: %s", url[:80], e)
    return None


# ── Ollama extraction ─────────────────────────────────────────────────────────

def check_ollama(model: str) -> bool:
    """Verify Ollama server is reachable and the model is available."""
    try:
        resp = requests.get(f"{OLLAMA_BASE_URL}/api/tags", timeout=5)
        if resp.status_code != 200:
            return False
        available = [m["name"] for m in resp.json().get("models", [])]
        if model not in available:
            print(f"  ⚠ Model '{model}' not found on server.")
            print(f"    Available: {available}")
            print(f"    Pull it with: ollama pull {model}")
            return False
        return True
    except Exception as e:
        print(f"  ✗ Cannot reach Ollama at {OLLAMA_BASE_URL}: {e}")
        return False


def extract_hwc_event(model: str, url: str, article_text: str,
                      pub_date: str, gdelt_locations: str) -> dict:
    """Send article to local Ollama LLM for structured HWC extraction."""
    truncated = article_text[:MAX_ARTICLE_CHARS]
    if len(article_text) > MAX_ARTICLE_CHARS:
        truncated += "\n\n[article truncated]"

    prompt = EXTRACTION_PROMPT.format(
        pub_date=pub_date or "unknown",
        url=url,
        gdelt_locations=gdelt_locations or "none available",
        article_text=truncated,
    )

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": prompt},
        ],
        "stream": False,
        "options": {
            "temperature": 0.0,    # deterministic — critical for structured extraction
            "num_predict": 1024,
        },
        "format": "json",          # Ollama native JSON mode — enforces valid JSON output
    }

    raw = ""
    try:
        resp = requests.post(
            f"{OLLAMA_BASE_URL}/api/chat",
            json=payload,
            timeout=OLLAMA_TIMEOUT,
        )
        resp.raise_for_status()
        raw = resp.json()["message"]["content"].strip()

        # Strip accidental markdown fences (some models ignore format:json)
        raw = re.sub(r"^```json\s*", "", raw, flags=re.MULTILINE)
        raw = re.sub(r"^```\s*",     "", raw, flags=re.MULTILINE)
        raw = re.sub(r"\s*```$",     "", raw)

        result = json.loads(raw)
        result["_llm_raw"] = raw
        result["_error"]   = None
        return result

    except json.JSONDecodeError as e:
        return {"is_hwc_event": None, "_error": f"JSON parse error: {e}",
                "_llm_raw": raw}
    except requests.exceptions.Timeout:
        return {"is_hwc_event": None,
                "_error": f"Ollama timeout after {OLLAMA_TIMEOUT}s — "
                           "try reducing --sample or increasing OLLAMA_TIMEOUT",
                "_llm_raw": ""}
    except Exception as e:
        return {"is_hwc_event": None, "_error": str(e), "_llm_raw": ""}


# ── Geocoding ─────────────────────────────────────────────────────────────────

def geocode_location(location_str: str) -> tuple[float | None, float | None, str]:
    """
    Geocode a location string using Google Maps API.
    Returns (lat, lon, resolved_address) or (None, None, "") on failure.
    """
    if not GOOGLE_MAPS_API_KEY or not location_str:
        log.debug(
            "geocode_location: skipped (api_key_set=%s, location_nonempty=%s)",
            bool(GOOGLE_MAPS_API_KEY),
            bool(location_str and str(location_str).strip()),
        )
        return None, None, ""
    try:
        params = {
            "address": location_str,
            "key": GOOGLE_MAPS_API_KEY,
            "region": "in",          # bias results toward India
            "components": "country:IN",
        }
        log.debug("Google Geocoding request address=%r", location_str[:500])
        resp = requests.get(GEOCODING_URL, params=params, timeout=10)
        data = resp.json()
        status = data.get("status", "?")
        err_msg = data.get("error_message")
        n_results = len(data.get("results") or [])
        log.debug(
            "Google Geocoding HTTP=%s status=%s error_message=%r n_results=%s",
            resp.status_code,
            status,
            err_msg,
            n_results,
        )
        if status == "OK" and data.get("results"):
            loc = data["results"][0]["geometry"]["location"]
            address = data["results"][0].get("formatted_address", "")
            log.debug(
                "Google Geocoding OK → lat=%s lon=%s formatted_address=%r",
                loc.get("lat"),
                loc.get("lng"),
                (address or "")[:200],
            )
            return loc["lat"], loc["lng"], address
        log.debug(
            "Google Geocoding no coordinates (status=%s, first_result_types would be unused)",
            status,
        )
    except Exception as e:
        log.warning("Google Geocoding exception for %r: %s", location_str[:120], e)
    return None, None, ""


def process_fetched_article(
    url: str,
    title: str,
    pub_date: str,
    gdelt_lat,
    gdelt_lon,
    gdelt_loc: str,
    article_text: str,
    fetch_method: str,
    model: str,
    no_geocode: bool,
) -> dict:
    """
    LLM extraction + geocode for one article after text is available.
    Returns a full result row dict aligned with data/hwc_final_report.csv columns.
    """
    result_row: dict = {
        "url": url,
        "title": title,
        "pub_date": pub_date,
        "gdelt_lat": gdelt_lat,
        "gdelt_lon": gdelt_lon,
        "fetch_method": fetch_method,
        "article_chars": len(article_text),
    }

    extracted = extract_hwc_event(
        model, url, article_text, pub_date, str(gdelt_loc or "")
    )
    time.sleep(SLEEP_LLM)

    result_row.update({
        "is_hwc_event":        extracted.get("is_hwc_event"),
        "species":             extracted.get("species"),
        "event_type":          extracted.get("event_type"),
        "humans_killed":       (extracted.get("victims") or {}).get("humans_killed"),
        "humans_injured":      (extracted.get("victims") or {}).get("humans_injured"),
        "animals_killed":      (extracted.get("victims") or {}).get("animals_killed"),
        "animals_injured":     (extracted.get("victims") or {}).get("animals_injured"),
        "event_date":          extracted.get("event_date"),
        "primary_location":    extracted.get("primary_location"),
        "location_type":       extracted.get("location_type"),
        "location_notes":      extracted.get("location_notes"),
        "gdelt_location_match": extracted.get("gdelt_location_match"),
        "confidence":          extracted.get("confidence"),
        "extraction_notes":    extracted.get("extraction_notes"),
        "_error":              extracted.get("_error"),
    })

    ploc = extracted.get("primary_location")
    ploc_ok = _location_usable_for_geocode(ploc)
    final_lat, final_lon, geocode_source = None, None, "none"

    if not no_geocode and extracted.get("is_hwc_event") and ploc_ok and GOOGLE_MAPS_API_KEY:
        lat, lon, addr = geocode_location(str(ploc).strip())
        time.sleep(SLEEP_GEOCODE)
        if lat is not None and lon is not None:
            final_lat, final_lon = lat, lon
            geocode_source = "google_maps"
            result_row["geocoded_address"] = addr
        else:
            result_row.pop("geocoded_address", None)
    else:
        result_row.pop("geocoded_address", None)

    if final_lat is None and gdelt_lat and str(gdelt_lat) not in ("", "nan", "None"):
        try:
            final_lat = float(gdelt_lat)
            final_lon = float(gdelt_lon)
            geocode_source = "gdelt_fallback"
        except (ValueError, TypeError):
            pass

    result_row["final_lat"] = final_lat
    result_row["final_lon"] = final_lon
    result_row["geocode_source"] = geocode_source
    return result_row


def accumulate_stats_from_result(stats: dict, result_row: dict) -> None:
    """Mirror the pilot run counters from a row produced by process_fetched_article."""
    if result_row.get("_error"):
        stats["extract_error"] += 1
    elif result_row.get("is_hwc_event"):
        stats["is_hwc"] += 1
    else:
        stats["not_hwc"] += 1

    gs = result_row.get("geocode_source") or "none"
    if gs == "google_maps":
        stats["geocoded_claude"] += 1
    elif gs == "gdelt_fallback":
        stats["geocoded_gdelt_fallback"] += 1
    elif gs == "gdelt_fallback_no_text":
        pass
    else:
        fl = result_row.get("final_lat")
        if fl is None or (isinstance(fl, float) and pd.isna(fl)):
            stats["no_geocode"] += 1
        else:
            s = str(fl).strip().lower()
            if s in ("", "nan", "none"):
                stats["no_geocode"] += 1


def retry_failed_pilot_results(
    pilot_csv: str,
    output_csv: str,
    model: str,
    no_geocode: bool,
    verbose: bool,
    max_retries: int | None,
    proxy_server: str | None = None,
):
    """
    Read data/hwc_final_report.csv; for rows with fetch_method=failed, load page with
    Selenium, extract text with trafilatura, re-run LLM + geocode; write merged CSV.
    """
    setup_logging(verbose)
    log.debug(
        "retry_failed_pilot_results: pilot_csv=%r output_csv=%r max_retries=%r proxy_server=%r",
        pilot_csv,
        output_csv,
        max_retries,
        proxy_server,
    )
    print(f"\nChecking Ollama at {OLLAMA_BASE_URL} with model '{model}'...")
    if not check_ollama(model):
        raise SystemExit("ERROR: Ollama server unreachable or model not available.")
    print("  ✓ Ollama ready")

    print(f"\nLoading pilot results (previous run — must include fetch_method): {pilot_csv}")
    # object dtype: pandas StringDtype (from dtype=str) rejects int/float/bool on df.at[..]=...
    df = pd.read_csv(pilot_csv, dtype=object)
    geo_path = Path(pilot_csv).resolve().parent / "hwc_urls_geocoded.csv"
    if geo_path.exists():
        try:
            g = pd.read_csv(geo_path, dtype=object)
            if "url" in g.columns and "all_india_locations" in g.columns:
                g = g[["url", "all_india_locations"]].drop_duplicates(subset=["url"])
                g = g.rename(columns={"all_india_locations": "gdelt_all_india_locations_hint"})
                df = df.merge(g, on="url", how="left")
                print(f"  → Merged GDELT location hints from {geo_path.name}")
                log.debug(
                    "Merged %s url→location rows from geocoded file",
                    len(g),
                )
        except Exception as e:
            print(f"  ⚠ Could not merge {geo_path.name}: {e}")
            log.debug("Geocoded merge failed", exc_info=True)
    if "fetch_method" not in df.columns:
        raise SystemExit(
            "ERROR: This CSV has no 'fetch_method' column.\n"
            "  --retry-failed-from expects output from a previous run (e.g. data/hwc_final_report.csv),\n"
            "  not hwc_urls_geocoded.csv. For a first-time run, omit --retry-failed-from and use --input."
        )

    failed_mask = df["fetch_method"].apply(_is_failed_fetch_method)
    n_failed = int(failed_mask.sum())
    n_ok_already = len(df) - n_failed
    print(f"  → {len(df)} rows total; {n_ok_already} already fetched OK (unchanged); "
          f"{n_failed} to retry (fetch_method=failed)")

    if n_failed == 0:
        Path(output_csv).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_csv, index=False)
        print(f"No failed rows — copied unchanged → {output_csv}")
        return

    to_process = df.index[failed_mask].tolist()
    if max_retries is not None and max_retries >= 0:
        to_process = to_process[:max_retries]
        print(f"  → Processing first {len(to_process)} failed rows (--max-selenium-retries)")

    log.debug("Starting Chrome driver for %s failed-row retries", len(to_process))
    driver = make_chrome_driver(proxy_server=proxy_server)
    n_ok = n_still_bad = 0
    try:
        for i, idx in enumerate(tqdm(to_process, unit="article", disable=verbose), start=1):
            row = df.loc[idx]
            url = str(row.get("url", "") or "").strip()
            title = row.get("title", "")
            pub_date = str(row.get("pub_date", "") or "")
            gdelt_lat = row.get("gdelt_lat", "")
            gdelt_lon = row.get("gdelt_lon", "")
            gdelt_loc = (
                row.get("gdelt_location_match")
                or row.get("gdelt_all_india_locations_hint")
                or row.get("all_india_locations")
                or ""
            )
            gdelt_loc = str(gdelt_loc) if gdelt_loc and str(gdelt_loc) != "nan" else ""

            log.debug("=" * 72)
            log.debug(
                "[%s/%s] Selenium retry df_index=%s url=%s",
                i,
                len(to_process),
                idx,
                url,
            )
            log.debug(
                "  row: title=%r pub_date=%r gdelt_loc_hint_len=%s",
                (str(title)[:100] + "…") if len(str(title)) > 100 else title,
                pub_date,
                len(gdelt_loc),
            )

            if not url:
                log.debug("  skip: empty url")
                df.at[idx, "fetch_method"] = "failed_selenium"
                df.at[idx, "_error"] = "empty url"
                n_still_bad += 1
                time.sleep(SLEEP_JINA)
                continue

            html = fetch_html_selenium(driver, url)
            if not html:
                log.debug("  Selenium returned no html → failed_selenium")
                df.at[idx, "fetch_method"] = "failed_selenium"
                df.at[idx, "_error"] = "selenium could not load page"
                n_still_bad += 1
                time.sleep(SLEEP_JINA)
                continue

            article_text = extract_text_from_html(html, url)
            if not article_text:
                log.debug(
                    "  trafilatura returned no usable text (see extract_text_from_html logs above)",
                )
                df.at[idx, "fetch_method"] = "failed_selenium"
                df.at[idx, "_error"] = "selenium/trafilatura could not extract article text"
                n_still_bad += 1
                time.sleep(SLEEP_JINA)
                continue

            log.debug("  pipeline: process_fetched_article (chars=%s)", len(article_text))
            out = process_fetched_article(
                url=url,
                title=title,
                pub_date=pub_date,
                gdelt_lat=gdelt_lat,
                gdelt_lon=gdelt_lon,
                gdelt_loc=str(gdelt_loc),
                article_text=article_text,
                fetch_method="selenium",
                model=model,
                no_geocode=no_geocode,
            )
            for k, v in out.items():
                if k not in df.columns:
                    df[k] = ""
                df.at[idx, k] = _pilot_scalar_for_csv(v)
            n_ok += 1
            log.debug(
                "  Selenium retry OK: fetch_method=%s is_hwc_event=%r geocode_source=%r",
                out.get("fetch_method"),
                out.get("is_hwc_event"),
                out.get("geocode_source"),
            )
            time.sleep(SLEEP_JINA)
    finally:
        try:
            log.debug("Selenium: quitting webdriver")
            driver.quit()
        except Exception as e:
            log.debug("Selenium: driver.quit() raised %s", e)

    Path(output_csv).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_csv, index=False)
    print(f"\n✓ Updated results saved → {output_csv}")
    print(f"  Selenium succeeded: {n_ok}  Still failed: {n_still_bad}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main(input_csv: str, sample_size: int, output_csv: str,
         output_report: str, no_geocode: bool, seed: int, model: str,
         verbose: bool = False):

    setup_logging(verbose)
    if verbose:
        log.debug(
            "Config: GOOGLE_MAPS_API_KEY is %s",
            "set" if GOOGLE_MAPS_API_KEY else "EMPTY (Google geocoding disabled)",
        )
        log.debug("Config: GEOCODING_URL=%s", GEOCODING_URL)

    # ── Verify Ollama is reachable ─────────────────────────────────────────
    print(f"\nChecking Ollama at {OLLAMA_BASE_URL} with model '{model}'...")
    if not check_ollama(model):
        raise SystemExit("ERROR: Ollama server unreachable or model not available.")
    print(f"  ✓ Ollama ready")

    # ── Load & sample ──────────────────────────────────────────────────────
    print(f"\nLoading: {input_csv}")
    df = pd.read_csv(input_csv, dtype=str)
    print(f"  → {len(df)} total articles (first run: input has no fetch_method; it is added in output)")

    if sample_size == -1:
        sample = df
        print(f"  → Sampled {len(sample)} articles (all)")
    else:
        # Prefer high-confidence/geocoded rows if columns exist
        if "theme_score" in df.columns:
            df["theme_score"] = pd.to_numeric(df["theme_score"], errors="coerce")
            hc = df[df["theme_score"] >= 3]
            rest = df[df["theme_score"] < 3].fillna(0)
            n_hc = min(len(hc), int(sample_size * 0.7))
            n_rest = sample_size - n_hc
            random.seed(seed)
            sample = pd.concat([
                hc.sample(n=n_hc, random_state=seed) if n_hc > 0 else pd.DataFrame(),
                rest.sample(n=min(n_rest, len(rest)), random_state=seed),
            ], ignore_index=True)
            print(f"  → Sampled {len(sample)} articles "
                f"({n_hc} high-confidence + {len(sample)-n_hc} others)")
        else:
            sample = df.sample(n=min(sample_size, len(df)), random_state=seed)
            print(f"  → Sampled {len(sample)} articles (random)")

    results = []
    stats = {
        "fetch_jina": 0, "fetch_trafilatura": 0, "fetch_failed": 0,
        "is_hwc": 0, "not_hwc": 0, "extract_error": 0,
        "geocoded_claude": 0, "geocoded_gdelt_fallback": 0, "no_geocode": 0,
    }

    print(f"\nProcessing {len(sample)} articles...\n")
    use_tqdm = not verbose
    iterator = tqdm(
        sample.iterrows(),
        total=len(sample),
        unit="article",
        disable=not use_tqdm,
    )
    for idx, (_, row) in enumerate(iterator, start=1):
        url       = str(row.get("url", ""))
        pub_date  = str(row.get("seendate", ""))
        gdelt_lat = row.get("best_lat", "")
        gdelt_lon = row.get("best_lon", "")
        gdelt_loc = row.get("all_india_locations", "") or row.get("best_location_name", "")
        title     = row.get("title", "")

        log.debug("=" * 72)
        log.debug("[%s/%s] URL: %s", idx, len(sample), url)
        log.debug("  title: %s", (str(title)[:120] + "…") if len(str(title)) > 120 else title)
        log.debug(
            "  GDELT hints: best_lat=%r best_lon=%r all_india_locations/best_name=%r",
            gdelt_lat,
            gdelt_lon,
            (str(gdelt_loc)[:200] + "…") if len(str(gdelt_loc)) > 200 else gdelt_loc,
        )

        result_row = {
            "url": url,
            "title": title,
            "pub_date": pub_date,
            "gdelt_lat": gdelt_lat,
            "gdelt_lon": gdelt_lon,
        }

        # ── 1. Fetch article text ──────────────────────────────────────────
        log.debug("  [fetch] trying Jina then trafilatura…")
        article_text, fetch_method = fetch_article(url)
        result_row["fetch_method"] = fetch_method
        stats[f"fetch_{fetch_method}"] += 1
        log.debug(
            "  [fetch] result=%s chars=%s",
            fetch_method,
            len(article_text) if article_text else 0,
        )
        time.sleep(SLEEP_JINA)

        if not article_text:
            result_row.update({
                "is_hwc_event": None, "species": None, "event_type": None,
                "event_date": None, "primary_location": None,
                "location_type": None, "confidence": None,
                "final_lat": gdelt_lat, "final_lon": gdelt_lon,
                "geocode_source": "gdelt_fallback_no_text",
                "_error": "article fetch failed",
            })
            log.debug("  [fetch] FAILED → using gdelt_fallback_no_text if coords exist")
            results.append(result_row)
            continue

        # ── 2–3. Ollama extraction + geocode (shared with Selenium retry path)
        log.debug("  [llm] calling Ollama extract_hwc_event…")
        result_row = process_fetched_article(
            url=url,
            title=title,
            pub_date=pub_date,
            gdelt_lat=gdelt_lat,
            gdelt_lon=gdelt_lon,
            gdelt_loc=str(gdelt_loc),
            article_text=article_text,
            fetch_method=fetch_method,
            model=model,
            no_geocode=no_geocode,
        )
        accumulate_stats_from_result(stats, result_row)

        results.append(result_row)

    # ── Save results ───────────────────────────────────────────────────────
    out_df = pd.DataFrame(results)
    Path(output_csv).parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(output_csv, index=False)
    print(f"\n✓ Results saved → {output_csv}")

    # ── Quality report ─────────────────────────────────────────────────────
    hwc_df    = out_df[out_df["is_hwc_event"] == True]
    n_total   = len(out_df)
    n_fetched = stats["fetch_jina"] + stats["fetch_trafilatura"]
    n_hwc     = len(hwc_df)

    # Location precision breakdown
    if n_hwc > 0:
        loc_types = hwc_df["location_type"].value_counts().to_dict()
        conf      = hwc_df["confidence"].value_counts().to_dict()
        species   = hwc_df["species"].value_counts().to_dict()
        ev_types  = hwc_df["event_type"].value_counts().to_dict()
        geocoded  = hwc_df[hwc_df["final_lat"].notna()]
        n_maps    = len(geocoded)
        n_village = sum(1 for t in hwc_df["location_type"] if t in
                        ("village", "town", "forest_range"))
    else:
        loc_types = conf = species = ev_types = {}
        n_maps = n_village = 0

    report = f"""
HWC Extraction Pilot — Quality Report
======================================
Sample size          : {n_total}
Successfully fetched : {n_fetched} ({n_fetched/n_total*100:.0f}%)
  via Jina AI        : {stats['fetch_jina']}
  via trafilatura    : {stats['fetch_trafilatura']}
  failed             : {stats['fetch_failed']}

Claude extraction
  HWC events found   : {n_hwc} ({n_hwc/max(n_fetched,1)*100:.0f}% of fetched)
  Not HWC            : {stats['not_hwc']}
  Extraction errors  : {stats['extract_error']}

Of {n_hwc} HWC events:
  Species breakdown  : {json.dumps(species, indent=4)}
  Event types        : {json.dumps(ev_types, indent=4)}
  Confidence         : {json.dumps(conf, indent=4)}
  Location precision : {json.dumps(loc_types, indent=4)}
  Subnational locs   : {n_village} ({n_village/max(n_hwc,1)*100:.0f}% at village/town/range)
  Mappable (lat/lon) : {n_maps} ({n_maps/max(n_hwc,1)*100:.0f}%)
    via Google Maps  : {stats['geocoded_claude']}
    via GDELT fallbk : {stats['geocoded_gdelt_fallback']}
    no coords        : {stats['no_geocode']}

Key feasibility signals
-----------------------
Precision proxy: {n_hwc/max(n_fetched,1)*100:.0f}% of fetched articles are actual HWC events
  (Groundsource flood baseline: ~66% after LLM filtering)

Location upgrade: {n_village}/{n_hwc} events have sub-district precision
  (vs GDELT which only gives district level)

Mappability: {n_maps}/{n_hwc} HWC events have usable coordinates

Sample failure modes to review manually:
  - Articles where is_hwc_event=None  (fetch/parse errors)
  - Articles where primary_location=null (no location in text)
  - Articles where confidence=low
"""

    print(report)
    rp = Path(output_report)
    rp.parent.mkdir(parents=True, exist_ok=True)
    rp.write_text(report)
    print(f"✓ Report saved → {output_report}")

    # ── Spot-check table ───────────────────────────────────────────────────
    if n_hwc > 0:
        print("\nSample of extracted HWC events (first 10):")
        cols = ["title", "species", "event_type", "event_date",
                "primary_location", "location_type", "confidence",
                "final_lat", "final_lon"]
        cols = [c for c in cols if c in hwc_df.columns]
        print(hwc_df[cols].head(10).to_string(index=False, max_colwidth=40))


if __name__ == "__main__":
    _root = Path(__file__).resolve().parent.parent
    _data = _root / "data"
    _out = _root / "outputs"
    p = argparse.ArgumentParser(
        description="HWC extraction pilot: Jina AI + Ollama local LLM + Google geocoding"
    )
    p.add_argument(
        "--input",
        default=str(_data / "hwc_urls_geocoded.csv"),
        help="First run only: article CSV with url, seendate, GDELT columns (no fetch_method). "
        "Ignored when --retry-failed-from is used.",
    )
    p.add_argument("--sample",  type=int, default=-1,
                   help="Number of articles to process (default: -1 for all articles)")
    p.add_argument(
        "--output",
        default=None,
        help="Output CSV (default: data/hwc_final_report.csv, or data/hwc_final_report_updated.csv "
        "when --retry-failed-from is set)",
    )
    p.add_argument("--report",  default=str(_out / "hwc_final_report.txt"))
    p.add_argument(
        "--retry-failed-from",
        "--fetch-failed-from",
        nargs="?",
        const=str(_data / "hwc_final_report.csv"),
        default=None,
        metavar="PILOT_CSV",
        help="Second run: load a previous run CSV (must have fetch_method). "
        "If you pass this flag with no path, uses data/hwc_final_report.csv. "
        "Only failed rows are re-fetched; other rows are kept from the previous run. "
        "Output default: data/hwc_final_report_updated.csv. --input is ignored.",
    )
    p.add_argument(
        "--max-selenium-retries",
        type=int,
        default=-1,
        help="With --retry-failed-from: max rows to retry (-1 = all failed rows)",
    )
    p.add_argument(
        "--proxy-server",
        default=None,
        metavar="URL",
        help="With --retry-failed-from: Chrome proxy, e.g. http://host:8080 or socks5://...",
    )
    p.add_argument("--model",   default=DEFAULT_MODEL,
                   help=f"Ollama model name (default: {DEFAULT_MODEL})")
    p.add_argument("--no-geocode", action="store_true",
                   help="Skip Google Maps geocoding (use GDELT coords only)")
    p.add_argument("--seed",    type=int, default=42,
                   help="Random seed for sampling")
    p.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Log each step per URL (fetch, LLM, geocode, Selenium retry) to stderr; disables tqdm bar",
    )
    args = p.parse_args()

    # nargs='?' + const: flag alone → pilot CSV path; omitted → first-run mode
    use_retry = args.retry_failed_from is not None
    output_csv = args.output or (
        str(_data / "hwc_final_report_updated.csv")
        if use_retry
        else str(_data / "hwc_final_report.csv")
    )

    if use_retry:
        max_r = args.max_selenium_retries
        retry_failed_pilot_results(
            pilot_csv=args.retry_failed_from,
            output_csv=output_csv,
            model=args.model,
            no_geocode=args.no_geocode,
            verbose=args.verbose,
            max_retries=max_r,
            proxy_server=args.proxy_server,
        )
    else:
        main(
            input_csv     = args.input,
            sample_size   = args.sample,
            output_csv    = output_csv,
            output_report = args.report,
            no_geocode    = args.no_geocode,
            seed          = args.seed,
            model         = args.model,
            verbose       = args.verbose,
        )