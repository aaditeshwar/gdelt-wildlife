# Generating `meta/*.json` files for new event domains

This README explains:

1. **How the GKG theme sets in `hwc_india_conflict_meta.json` were chosen** — so you can reason about them for new domains.
2. **The prompt to use with Claude** to generate a complete meta JSON for a new event type.
3. **How to verify and refine** the output before committing it.

---

## Part 1 — How the HWC GKG theme sets were chosen

### What GKG themes are

Every article GDELT ingests gets tagged with themes from several overlapping taxonomies. The main ones relevant here are:

| Prefix | Source | Example |
|---|---|---|
| `ENV_*` | GDELT's own environment taxonomy | `ENV_WILDLIFE`, `ENV_DEFORESTATION` |
| `WB_*` | World Bank topical taxonomy (~2,200 topics) | `WB_2069_WILDLIFE_MANAGEMENT` |
| `CRISISLEX_*` | CrisisLex crisis/disaster lexicon | `CRISISLEX_T03_DEAD`, `CRISISLEX_T04_INJURED` |
| `TAX_*` | GDELT's own actor/action taxonomy | `TAX_FNCACT_ATTACK` |
| Plain | GDELT's own general themes | `KILL`, `WOUND`, `AFFECT` |

The full theme list has over 2,500 entries (around 2,200 from the World Bank taxonomy alone).
There is no single static canonical file — the theme set grows over time. The best references are:

- **Community CSV**: `https://github.com/CatoMinor/GDELT-GKG-Themes` — maintained CSV of all known themes
- **GDELT blog lookups**: `https://blog.gdeltproject.org/new-november-2021-gkg-2-0-themes-lookup/` — periodically updated by GDELT
- **BigQuery enumeration** (most authoritative — reflects actual current data): run the full-corpus query from Part 3 without a date filter

For domain-specific discovery, add `AND V2Themes LIKE '%KEYWORD%'` to the query in Part 3.

You can also discover themes empirically with a domain-filtered BigQuery query:
```sql
WITH nested AS (
  SELECT SPLIT(RTRIM(REGEXP_REPLACE(V2Themes, r',\d+;', ';'), ';'), ';') themes
  FROM `gdelt-bq.gdeltv2.gkg_partitioned`
  WHERE _PARTITIONTIME >= TIMESTAMP("2023-01-01")
    AND V2Themes LIKE '%WILDLIFE%'
)
SELECT theme, COUNT(1) cnt FROM nested, UNNEST(themes) AS theme
GROUP BY theme ORDER BY cnt DESC LIMIT 100
```

### The scoring logic and why two sets are needed

The pipeline scores each article with `theme_score`:

- **1** = only `primary_themes` matched → article matches the domain topic but not the harm/incident signal
- **2** = only `secondary_themes` matched → article matches harm/incident tags but not the domain-specific topic
- **3** = both sets matched → **high confidence** this is an HWC article

This two-set design is intentional. An article about a tiger reserve management policy would score 1 (primary only). An article about a road accident would score 2 (secondary only). Only articles mentioning *both* domain and harm signals together score 3, which is what you want to prioritise for full-text extraction.

### How the HWC themes were specifically chosen

**`primary_themes`** (domain topic) — selected by scanning the `ENV_*` and `WB_*` namespaces for anything related to animals, forests, and ecosystems:

| Theme | Why included |
|---|---|
| `ENV_WILDLIFE` | Direct match — GDELT's primary wildlife tag |
| `ENV_POACHING` | Captures human-initiated animal harm |
| `ENV_SPECIESENDANGERED` / `ENV_SPECIESEXTINCT` | Co-occurs with tiger/elephant articles |
| `ENV_FORESTS` / `ENV_DEFORESTATION` | Corridor and habitat conflict articles |
| `WB_2069_WILDLIFE_MANAGEMENT` | World Bank tag for wildlife governance |
| `WB_678_FORESTS` | World Bank forests tag, co-occurs with range conflicts |
| `NATURAL_DISASTER` | Included because elephants entering villages is sometimes tagged as a disaster event |

**`secondary_themes`** — selected from the plain/CRISISLEX tags that indicate physical harm to people or animals:

| Theme | Why included |
|---|---|
| `KILL` | Explicit killing — humans or animals |
| `WOUND` | Injury events |
| `AFFECT` | Broad harm tag, catches injuries/impacts |
| `CRISISLEX_T03_DEAD` | CrisisLex crisis tweet lexicon: death |
| `CRISISLEX_T04_INJURED` | CrisisLex: injury |
| `TAX_FNCACT_ATTACK` | GDELT action taxonomy: physical attack event |
| `CRISISLEX_CRISISLEXREC` | General crisis recommendation tag |
| `SECURITY_SERVICES` | Sometimes applied to forest guard incidents |

The key insight: **`primary_themes` identifies the topic domain; `secondary_themes` identifies that something harmful actually happened**. Articles scoring high on both are genuine incidents, not policy or conservation news.

---

## Part 2 — Prompt to generate a new meta JSON

Use the following prompt with Claude. Paste it in full, then fill in the `[BRACKETED]` sections.

---

### The prompt

```
I am building a GDELT-based event mapping pipeline using this repository:
https://github.com/aaditeshwar/gdelt-wildlife

The pipeline uses a meta JSON file (see the example at
https://github.com/aaditeshwar/gdelt-wildlife/blob/main/meta/hwc_india_conflict_meta.json)
to configure all domain-specific settings: GDELT DOC API keywords,
GKG theme sets, geography filters, LLM extraction prompts, and map taxonomy.

Please generate a complete meta JSON file for the following event domain:

**Domain:** [SHORT LABEL, e.g. "crop_damage_india" or "heat_wave_india"]
**Title:** [HUMAN READABLE, e.g. "Crop damage from unseasonal rainfall (India)"]
**Description:** [1-2 sentences describing what events you want to map]
**Geography:** India (country code IN), English-language articles
**Fetch date range:** `gdelt_doc_fetch.fetch_start_date` (YYYY-MM-DD, **required**) and optional `fetch_end_date` (omit or empty string for **today** in the runner’s local date — see `scripts/domain_meta.py::get_gdelt_doc_fetch_date_range`). Do **not** use legacy `years_back`; the fetch script requires explicit dates.
**Window months:** 3 (chunk size for DOC API windows; `gdelt_doc_fetch.window_months`)

The JSON must follow exactly the same schema as hwc_india_conflict_meta.json
with these top-level keys:
  schema_version, domain, data_binding, taxonomy, map_style,
  generalization, gdelt_doc_fetch, gkg_theme_sets, gkg_geography, llm_extraction

Specific requirements:

1. **gdelt_doc_fetch**: Set **`fetch_start_date`** (required) and optional **`fetch_end_date`**
   (YYYY-MM-DD; omit or `""` for “through today”) and **`window_months`** (chunk size).
   Reference: `meta/hwc_india_conflict_meta.json` and `meta/event_domain_template.json`.
   **gdelt_doc_fetch.keywords**: Generate 8-12 specific search phrases
   that would appear in Indian news articles about this type of event.
   Include both common English terms and India-specific terminology
   (district names, Indian government agency names, crop names, etc.)
   where relevant. Avoid overly generic terms that would match unrelated articles.

2. **gkg_theme_sets**: This is the most important section.
   - **primary_themes** (domain / topic area — e.g. wildlife & forests in HWC, agriculture in crop damage):
     Choose 6-10 GKG theme codes from ENV_*, WB_*, or domain-specific
     themes that identify this topic area. Reference the GDELT theme taxonomy:
     http://data.gdeltproject.org/documentation/GDELT-Global_Knowledge_Graph_CategoryList.xlsx
     Explain briefly why each theme is included.
   - **secondary_themes** (harm / incident signal — e.g. conflict/harm tags in HWC):
     Choose 4-8 GKG theme codes indicating actual harm occurred
     (deaths, injury, economic loss, displacement, health impact).
     Use CRISISLEX_*, plain themes (KILL, WOUND, AFFECT, ECON_*), and
     TAX_* codes where appropriate.
   - Set **high_confidence_theme_score_min** to 3 (both sets must match).
   - Include **theme_score_guide** explaining scores 0-3 for this domain.

3. **taxonomy.event_type.allowed_values**: Define 8-12 event types
   specific to this domain (e.g. for crop damage: "crop_loss_flood",
   "crop_loss_hail", "crop_loss_drought", "livestock_death", etc.)

4. **taxonomy** should also include any domain-specific fields beyond
   the HWC baseline (species → e.g. crop_type, severity_class, etc.)

5. **map_style**: Define meaningful merge_groups (2-3 high-level
   categories for the map legend) and assign visually distinct hex colors.

6. **llm_extraction**: Write a complete system_prompt_lines and
   extraction_prompt_lines array.
   - The extraction prompt must include the four required placeholders:
     {pub_date}, {url}, {gdelt_locations}, {article_text}
   - STEP 1 must define clearly what IS and IS NOT a valid event of
     this type (the "gate" check — adapt from HWC's list of exclusions).
   - STEP 2 must extract all domain-specific fields defined in taxonomy.
   - The output JSON schema must match the taxonomy exactly.
   - Literal { and } in the embedded JSON example must use {{ and }}.
   - Use system_prompt_lines and extraction_prompt_lines as arrays of strings
     (one string per line), NOT as a single multiline string.

7. **data_binding**: Keep the same structure as HWC but change
   filter_hwc_events.column to match the primary boolean field name
   your LLM prompt will output (e.g. "is_crop_damage_event").

8. **Optional `bigquery_gkg_fetch`**: If you use `gdelt-fetch-urls.py --source bigquery`, add this object (see `meta/cropdamage_india_meta.json`). Set `partition_time_start` / `partition_time_end` for the GKG scan window; they may differ slightly from `gdelt_doc_fetch` dates (e.g. later start to reduce BigQuery cost)—document any intentional gap in `_comment`.

9. At the end, add a **design_notes** section (not consumed by scripts)
   explaining:
   - Why you chose these specific GKG themes over alternatives
   - Any known false-positive risks (articles that will score high
     but are not genuine events)
   - Suggested BigQuery query to empirically validate theme coverage
     before running the full pipeline

Output only the JSON. Do not include any explanation outside the JSON
(put all reasoning inside design_notes).

The filename should be: [DOMAIN_ID]_meta.json
```

---

## Part 3 — Suggested domains to generate next

Here are the domains you mentioned, with the key disambiguation challenges to watch for:

| Domain | Suggested `domain.id` | Key false-positive risk | Most useful primary themes |
|---|---|---|---|
| Crop damage (rainfall/hail) | `cropdamage_india` (`meta/cropdamage_india_meta.json`) | Policy/insurance articles, weather forecasts | `ENV_AGRICULTURE`, `WB_*` crops / risk, `NATURAL_DISASTER_*` in secondary |
| Drought / water stress | `drought_india` | Future outlook articles, budget allocation | `ENV_DROUGHT`, `WB_*_DROUGHT`, `WB_*_IRRIGATION` |
| Pest / locust attack | `pest_attack_india` | Research articles, preventive spraying news | `ENV_PESTICIDES`, `WB_*_PEST*`, `ENV_AGRICULTURE` |
| Heat wave health impacts | `heat_wave_india` | Weather forecasts, historical retrospectives | `ENV_CLIMATECHANGE`, `CRISISLEX_T03_DEAD`, `HEALTH_*` |
| Water safety / contamination | `water_safety_india` | Policy/infrastructure articles | `ENV_WATER`, `WB_*_WATER*`, `HEALTH_DISEASE` |
| Arsenic / slow poisoning | `arsenic_india` | Academic/research articles | `WB_*_WATER_QUALITY`, `HEALTH_DISEASE_POISONING` |

### Useful BigQuery query to discover themes for any domain

Before writing a meta JSON, run this to see what themes actually co-occur with your keywords in Indian news articles:

```sql
WITH articles AS (
  SELECT V2Themes
  FROM `gdelt-bq.gdeltv2.gkg_partitioned`
  WHERE _PARTITIONTIME >= TIMESTAMP("2021-01-01")
    AND (
      V2Themes LIKE '%DROUGHT%'          -- replace with your domain keyword
      OR DocumentIdentifier LIKE '%india%'
    )
    AND V2Locations LIKE '%India%'
),
nested AS (
  SELECT SPLIT(RTRIM(REGEXP_REPLACE(V2Themes, r',\d+;', ';'), ';'), ';') themes
  FROM articles
)
SELECT theme, COUNT(1) cnt
FROM nested, UNNEST(themes) AS theme
WHERE theme NOT IN ('', 'TAX_ETHNICITY_GENERIC')  -- filter noise
GROUP BY theme
ORDER BY cnt DESC
LIMIT 100
```

Run this first, scan the top 100 themes, and paste the output into your prompt to Claude — it will produce much more accurate theme selections than reasoning from the taxonomy alone.

---

## Part 4 — After generating the JSON

1. **Validate JSON syntax:** `python -m json.tool meta/your_new_meta.json`

2. **Confirm `gdelt_doc_fetch.fetch_start_date` is set** (and `fetch_end_date` if you need a fixed end). The fetch pipeline rejects meta files that only specify deprecated `years_back`.

3. **Check the filter column name** in `data_binding.filter_hwc_events.column` matches exactly what the LLM extraction prompt outputs as the top-level boolean field.

4. **Check placeholder names** in `llm_extraction.extraction_prompt_lines` match `extraction_prompt_placeholders` — all four of `pub_date`, `url`, `gdelt_locations`, `article_text` must be present.

5. **Test a dry run:**
   ```bash
   python scripts/gdelt-fetch-urls.py --meta meta/your_new_meta.json --dry-run
   ```

6. **Run on a small sample first** before committing to the full pipeline:
   ```bash
   python scripts/gdelt-fetch-urls.py --meta meta/your_new_meta.json
   python scripts/gdelt-enrich-urls-bigquery.py --meta meta/your_new_meta.json --project YOUR_PROJECT
   python scripts/gdelt-get-full-text.py --meta meta/your_new_meta.json --sample 30
   ```

7. **Review `design_notes`** in the generated JSON — if Claude flagged false-positive risks, adjust `gdelt_doc_fetch.keywords` to be more specific before scaling up.
