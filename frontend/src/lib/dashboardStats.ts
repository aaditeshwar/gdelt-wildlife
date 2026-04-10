import type { Feature, FeatureCollection } from "geojson";
import { deriveStateFromLocation } from "./indiaStates";

/** Reject bogus years from mis-parsed numeric dates (e.g. 8019 from 8-digit strings). */
function isPlausibleNewsYear(y: number): boolean {
  return y >= 1990 && y <= 2100;
}

export function parseYearFromProps(props: Record<string, unknown>): number | null {
  const ed = props.event_date ?? props.pub_date;
  const s = ed === null || ed === undefined ? "" : String(ed).trim();
  if (!s) return null;
  const iso = s.match(/^(\d{4})-\d{2}-\d{2}/);
  if (iso) {
    const y = parseInt(iso[1], 10);
    if (isPlausibleNewsYear(y)) return y;
  }
  const ymd = s.match(/^(\d{4})\d{4}T/);
  if (ymd) {
    const y = parseInt(ymd[1], 10);
    if (isPlausibleNewsYear(y)) return y;
  }
  const four = s.match(/\b(19|20)\d{2}\b/);
  if (four) return parseInt(four[0], 10);
  return null;
}

/**
 * Calendar year and month from ``event_date`` or ``pub_date`` (ISO, GDELT 8-digit date, or year-only).
 * Month is 1–12 when parseable; ``null`` when only the year could be inferred.
 */
export function parseYearMonthFromProps(
  props: Record<string, unknown>,
): { year: number; month: number | null } | null {
  const ed = props.event_date ?? props.pub_date;
  const s = ed === null || ed === undefined ? "" : String(ed).trim();
  if (!s) return null;
  const iso = s.match(/^(\d{4})-(\d{2})-\d{2}/);
  if (iso) {
    const y = parseInt(iso[1], 10);
    const mo = parseInt(iso[2], 10);
    if (mo >= 1 && mo <= 12 && isPlausibleNewsYear(y)) return { year: y, month: mo };
  }
  const gdelt8 = s.match(/^(\d{4})(\d{2})(\d{2})$/);
  if (gdelt8) {
    const y = parseInt(gdelt8[1], 10);
    const mo = parseInt(gdelt8[2], 10);
    if (mo >= 1 && mo <= 12 && isPlausibleNewsYear(y)) return { year: y, month: mo };
  }
  if (/^\d{14}$/.test(s)) {
    const y = parseInt(s.slice(0, 4), 10);
    const mo = parseInt(s.slice(4, 6), 10);
    if (mo >= 1 && mo <= 12 && isPlausibleNewsYear(y)) return { year: y, month: mo };
  }
  const yOnly = parseYearFromProps(props);
  if (yOnly !== null) return { year: yOnly, month: null };
  return null;
}

function normCategory(
  props: Record<string, unknown>,
  catField: string,
  fallback: string,
): string {
  const raw = props[catField];
  const t = String(raw ?? "").trim();
  if (!t || t.toLowerCase() === "nan" || t.toLowerCase() === "none") return fallback;
  return t;
}

/**
 * Single habitat fragment → canonical key. Merges wetland_* / wetland_named → wetland;
 * sanctuary_* / sanctuary_named → sanctuary; fixes santuary_* typo.
 */
export function normalizeAvianHabitatKey(snake: string): string {
  let t = snake.trim().toLowerCase().replace(/\s+/g, "_");
  if (!t || t === "nan" || t === "none") return "unknown";
  if (t.startsWith("santuary_")) {
    t = `sanctuary_${t.slice("santuary_".length)}`;
  }
  if (t.startsWith("wetland_") || t === "wetland_named" || t === "wetland") {
    return "wetland";
  }
  if (t.startsWith("sanctuary_") || t === "sanctuary_named" || t === "sanctuary") {
    return "sanctuary";
  }
  return t;
}

/**
 * Split compound habitat fields (e.g. "poultry farm, urban") and normalize each fragment.
 */
export function splitAvianHabitatTokens(raw: unknown): string[] {
  const s0 = String(raw ?? "").trim();
  if (!s0 || s0.toLowerCase() === "nan" || s0.toLowerCase() === "none") {
    return ["unknown"];
  }
  const fragments = s0.split(/[,;/]+/).map((p) => p.trim()).filter(Boolean);
  const use = fragments.length > 0 ? fragments : [s0];
  const out: string[] = [];
  for (const frag of use) {
    const key = normalizeAvianHabitatKey(frag.replace(/\s+/g, "_"));
    out.push(key === "unknown" ? "unknown" : key);
  }
  const uniq = [...new Set(out)];
  return uniq.length > 0 ? uniq : ["unknown"];
}

export function formatHabitatLabel(key: string): string {
  if (key === "unknown") return "Unknown";
  if (key === "wetland") return "Wetland (merged types)";
  if (key === "sanctuary") return "Sanctuary (merged types)";
  return key
    .split("_")
    .filter(Boolean)
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase())
    .join(" ");
}

/** Species tokens excluded from avian mortality bird-focused charts (mammals / human). */
export const AVIAN_SPECIES_CHART_EXCLUDE = new Set([
  "human",
  "tiger",
  "leopard",
  "cat",
  "tigress",
  "lion",
]);

/** Legacy avian map_category values merged into ``unclassified`` in charts (matches meta). */
function normalizeAvianMapCategory(cat: string): string {
  const t = cat.trim().toLowerCase();
  if (t === "unknown" || t === "other" || t === "not_applicable") return "unclassified";
  return cat;
}

/**
 * Stacked counts: ``counts[category][habitat]`` (chart: x = habitats, stacks = categories).
 * Avian habitat strings are split on commas/semicolons; wetland_* and sanctuary_* merged.
 */
export function aggregateCategoryHabitatStack(
  fc: FeatureCollection,
  categoryField: string,
  fallbackCategory: string,
  orderedCategories: string[],
  maxHabitats = 14,
): {
  categories: string[];
  habitats: string[];
  counts: Record<string, Record<string, number>>;
} {
  const counts: Record<string, Record<string, number>> = {};
  const habitatTotals: Record<string, number> = {};

  for (const f of fc.features || []) {
    const props = (f as Feature).properties as Record<string, unknown> | null;
    if (!props) continue;
    const cat = normalizeAvianMapCategory(normCategory(props, categoryField, fallbackCategory));
    const habs = splitAvianHabitatTokens(props.habitat_type);
    if (!counts[cat]) counts[cat] = {};
    for (const hab of habs) {
      counts[cat][hab] = (counts[cat][hab] || 0) + 1;
      habitatTotals[hab] = (habitatTotals[hab] || 0) + 1;
    }
  }

  const habitats = Object.entries(habitatTotals)
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .slice(0, maxHabitats)
    .map(([h]) => h);

  const present = new Set(Object.keys(counts));
  const categories = orderedCategories.filter((c) => present.has(c));
  for (const c of present) {
    if (!categories.includes(c)) categories.push(c);
  }

  return { categories, habitats, counts };
}

/**
 * Heatmap: species tokens (split) × habitat_type; optional species exclusion (e.g. mammals).
 */
export function aggregateAvianSpeciesHabitatHeatmap(
  fc: FeatureCollection,
  maxSpecies = 18,
  maxHabitats = 16,
  excludeSpecies?: Set<string>,
): {
  species: string[];
  habitats: string[];
  data: [number, number, number][];
  maxVal: number;
} {
  const exclude = excludeSpecies ?? new Set<string>();
  const pairCounts = new Map<string, number>();
  const speciesTotals: Record<string, number> = {};
  const habitatTotals: Record<string, number> = {};

  for (const f of fc.features || []) {
    const props = (f as Feature).properties as Record<string, unknown> | null;
    if (!props) continue;
    const tokens = splitSpeciesTokens(props.species).filter((t) => !exclude.has(t));
    if (!tokens.length) continue;
    const habs = splitAvianHabitatTokens(props.habitat_type);
    for (const sp of tokens) {
      for (const hab of habs) {
        if (hab === "environmental_stress") continue;
        speciesTotals[sp] = (speciesTotals[sp] || 0) + 1;
        habitatTotals[hab] = (habitatTotals[hab] || 0) + 1;
        const key = `${sp}\0${hab}`;
        pairCounts.set(key, (pairCounts.get(key) || 0) + 1);
      }
    }
  }

  const species = Object.entries(speciesTotals)
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .slice(0, maxSpecies)
    .map(([s]) => s);
  const habitats = Object.entries(habitatTotals)
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .slice(0, maxHabitats)
    .map(([h]) => h);

  const spIdx = new Map(species.map((s, i) => [s, i] as const));
  const habIdx = new Map(habitats.map((h, i) => [h, i] as const));

  const data: [number, number, number][] = [];
  for (const [key, v] of pairCounts) {
    const sep = key.indexOf("\0");
    if (sep < 0) continue;
    const s = key.slice(0, sep);
    const h = key.slice(sep + 1);
    const i = spIdx.get(s);
    const j = habIdx.get(h);
    if (i === undefined || j === undefined || v <= 0) continue;
    data.push([i, j, v]);
  }

  const maxVal = data.length ? Math.max(...data.map((x) => x[2]), 1) : 1;
  return { species, habitats, data, maxVal };
}

export type YearCategoryCount = Record<number, Record<string, number>>;
export type StateCategoryCount = Record<string, Record<string, number>>;
export type StateSpeciesCount = Record<string, Record<string, number>>;

/** Strip list/JSON-ish wrappers and quotes from one species fragment. */
function cleanSpeciesFragment(p: string): string {
  let t = p.trim();
  if (!t) return "";
  t = t.replace(/^\[+/, "").replace(/\]+$/, "");
  t = t.replace(/^['"]+|['"]+$/g, "").trim();
  t = t.replace(/^\[+|\]+$/g, "").trim();
  t = t.replace(/^['"]+|['"]+$/g, "").trim();
  t = t.toLowerCase().replace(/\s+/g, " ");
  return t;
}

/**
 * Split compound species labels ("elephant, tiger", "elephant and wild boar")
 * into single-species tokens for heatmap rows.
 * Handles list-like strings: `['tiger', 'bear']`, `['tiger'` / `'bear']`, etc.
 */
export function splitSpeciesTokens(raw: unknown): string[] {
  let s0 = String(raw ?? "").trim();
  if (!s0 || s0.toLowerCase() === "nan" || s0.toLowerCase() === "none") return [];
  if (s0.startsWith("[") && s0.endsWith("]")) {
    s0 = s0.slice(1, -1).trim();
  }
  let s = s0.toLowerCase().replace(/\s+and\s+/gi, ",").replace(/\s*&\s*/g, ",");
  const parts = s
    .split(/[,;/]/)
    .map((p) => cleanSpeciesFragment(p))
    .filter((p) => p.length > 0);
  return [...new Set(parts)];
}

export function aggregateDashboard(
  fc: FeatureCollection,
  categoryField: string,
  fallbackCategory: string,
): {
  byYear: YearCategoryCount;
  byState: StateCategoryCount;
  stateSpecies: StateSpeciesCount;
  years: number[];
  states: string[];
  speciesList: string[];
} {
  const byYear: YearCategoryCount = {};
  const byState: StateCategoryCount = {};
  const stateSpecies: StateSpeciesCount = {};
  const speciesTotals: Record<string, number> = {};

  for (const f of fc.features || []) {
    const props = (f as Feature).properties as Record<string, unknown> | null;
    if (!props) continue;
    const cat = normCategory(props, categoryField, fallbackCategory);
    const y = parseYearFromProps(props);
    if (y !== null) {
      if (!byYear[y]) byYear[y] = {};
      byYear[y][cat] = (byYear[y][cat] || 0) + 1;
    }
    const state = deriveStateFromLocation(props.primary_location);
    if (!byState[state]) byState[state] = {};
    byState[state][cat] = (byState[state][cat] || 0) + 1;

    const tokens = splitSpeciesTokens(props.species);
    if (!stateSpecies[state]) stateSpecies[state] = {};
    for (const sp of tokens) {
      stateSpecies[state][sp] = (stateSpecies[state][sp] || 0) + 1;
      speciesTotals[sp] = (speciesTotals[sp] || 0) + 1;
    }
  }

  const years = Object.keys(byYear)
    .map((x) => parseInt(x, 10))
    .sort((a, b) => a - b);
  const states = Object.keys(byState)
    .filter((s) => s !== "Unknown")
    .sort((a, b) => {
      const ta = Object.values(byState[a] || {}).reduce((x, y) => x + y, 0);
      const tb = Object.values(byState[b] || {}).reduce((x, y) => x + y, 0);
      return tb - ta;
    });
  if (byState["Unknown"]) states.push("Unknown");

  const speciesList = Object.entries(speciesTotals)
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .map(([sp]) => sp);

  return { byYear, byState, stateSpecies, years, states, speciesList };
}

/** Normalize damage_cause_raw for grouping (lowercase snake). */
function normDamageCauseRaw(raw: string): string {
  const t = raw.trim().toLowerCase().replace(/\s+/g, "_");
  if (!t || t === "nan" || t === "none") return "unknown";
  return t;
}

/**
 * Split compound damage strings ("Unseasonal Rain, Pest Other", "hailstorm,unseasonal_rain")
 * into normalized tokens (same rules as {@link normDamageCauseRaw} per fragment).
 */
export function splitDamageCauseTokens(raw: unknown): string[] {
  let s0 = String(raw ?? "").trim();
  if (!s0 || s0.toLowerCase() === "nan" || s0.toLowerCase() === "none") return [];
  s0 = s0.replace(/\s+and\s+/gi, ",").replace(/\s*&\s*/g, ",");
  const parts = s0
    .split(/\s*[,;/]\s*/)
    .map((p) => normDamageCauseRaw(p))
    .filter((p) => p !== "unknown");
  return [...new Set(parts)];
}

/** Merge paddy/rice and mixed/multiple-style crop labels for the crop×damage heatmap. */
export function normalizeCropTokenForHeatmap(token: string): string {
  const t = token.trim().toLowerCase().replace(/\s+/g, "_");
  if (!t || t === "nan" || t === "none") return "unknown";
  if (t === "paddy" || t === "rice") return "paddy_or_rice";
  if (
    t === "mixed" ||
    t === "multiple" ||
    t === "mixed/multiple" ||
    t === "multiple_categories" ||
    (t.includes("multiple") && t.includes("categories"))
  ) {
    return "mixed_or_multiple";
  }
  return t;
}

export function formatCropHeatmapCropLabel(key: string): string {
  if (key === "paddy_or_rice") return "Paddy / rice";
  if (key === "mixed_or_multiple") return "Mixed / multiple";
  return formatSpeciesLabel(key);
}

export function formatDamageCauseLabel(key: string): string {
  if (key === "unknown") return "Unknown";
  if (key === "heavy_rain") return "Heavy rain";
  return key
    .split("_")
    .filter(Boolean)
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase())
    .join(" ");
}

/**
 * Normalized damage_cause tokens allowed on the Weather & drought stacked chart
 * (cropdamage_india_meta map_style: weather_extreme + drought + common free-text weather).
 * Excludes pest/disease/locust/fire so compound ``damage_cause_raw`` rows do not mix series.
 */
const WEATHER_DROUGHT_STACK_TOKENS = new Set<string>([
  "unseasonal_rain",
  "hailstorm",
  "hail",
  "flood",
  "flooding",
  "waterlogging",
  "frost",
  "heat_wave",
  "heat",
  "cyclone",
  "cyclonic",
  "drought",
  "heavy_rain",
  "rain",
  "storm",
  "snow",
  "snowfall",
  "gale",
  "thunderstorm",
  "lightning",
  "inundation",
  "innundation",
  "typhoon",
  "landslide",
  "landslides",
  "torrential",
  "downpour",
  "monsoon",
  "cloudburst",
  "wind",
  "winds",
]);

/** Merge near-duplicate weather tokens into one stacked series (Weather & drought chart). */
const WEATHER_DROUGHT_STACK_CANONICAL: Record<string, string> = {
  snowfall: "snow",
  inundation: "waterlogging",
  innundation: "waterlogging",
  landslides: "landslide",
  rain: "heavy_rain",
  gale: "storm",
};

function canonicalWeatherDroughtStackKeys(keys: string[]): string[] {
  const mapped = keys.map((k) => WEATHER_DROUGHT_STACK_CANONICAL[k] ?? k);
  return [...new Set(mapped)];
}

/** Normalized tokens for Pest / disease chart (merge_groups pest_disease only). */
const PEST_DISEASE_STACK_TOKENS = new Set<string>([
  "armyworm",
  "pest_other",
  "whitefly",
  "disease_other",
  "disease_fungal",
  "disease_bacterial",
  "disease_viral",
]);

export type CropDamageStackKind = "weather_drought" | "pest_disease";

function filterCropDamageStackTokens(
  keys: string[],
  stackKind: CropDamageStackKind | undefined,
): string[] {
  if (!stackKind) return keys;
  const allow =
    stackKind === "weather_drought" ? WEATHER_DROUGHT_STACK_TOKENS : PEST_DISEASE_STACK_TOKENS;
  return keys.filter((k) => allow.has(k));
}

/**
 * Stacked counts by calendar year and damage_cause tokens, only for rows whose
 * map_category is in the filter (e.g. pest_disease, weather_extreme, or both).
 * Compound ``damage_cause_raw`` values (comma/semicolon/& "and") are split so each
 * cause increments its series (same rules as {@link splitDamageCauseTokens}).
 * When ``stackKind`` is set, only tokens that belong on that chart are counted (avoids
 * listing locust/pest causes on weather stacks or waterlogging on pest stacks).
 */
export function aggregateCropDamageCauseByCategory(
  fc: FeatureCollection,
  mapCategoryFilter: string | string[],
  rawField = "damage_cause_raw",
  stackKind?: CropDamageStackKind,
): {
  byYear: Record<number, Record<string, number>>;
  years: number[];
  causes: string[];
} {
  const filters = Array.isArray(mapCategoryFilter)
    ? mapCategoryFilter.map((x) => x.trim().toLowerCase())
    : [mapCategoryFilter.trim().toLowerCase()];
  const filterSet = new Set(filters);

  const byYear: Record<number, Record<string, number>> = {};
  const causeTotals: Record<string, number> = {};

  for (const f of fc.features || []) {
    const props = (f as Feature).properties as Record<string, unknown> | null;
    if (!props) continue;
    const cat = String(props.map_category ?? "").trim().toLowerCase();
    if (!filterSet.has(cat)) continue;
    const y = parseYearFromProps(props);
    if (y === null) continue;
    const rawIn = props[rawField];
    const tokens = splitDamageCauseTokens(rawIn);
    let keys =
      tokens.length > 0
        ? tokens
        : [normDamageCauseRaw(String(rawIn ?? ""))];
    keys = filterCropDamageStackTokens(keys, stackKind);
    if (stackKind === "weather_drought") {
      keys = canonicalWeatherDroughtStackKeys(keys);
    }
    if (keys.length === 0) continue;
    if (!byYear[y]) byYear[y] = {};
    for (const key of keys) {
      byYear[y][key] = (byYear[y][key] || 0) + 1;
      causeTotals[key] = (causeTotals[key] || 0) + 1;
    }
  }

  const years = Object.keys(byYear)
    .map((x) => parseInt(x, 10))
    .sort((a, b) => a - b);
  const causes = Object.entries(causeTotals)
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .map(([k]) => k);

  return { byYear, years, causes };
}

const CAUSE_STACK_PALETTE = [
  "#5470c6",
  "#91cc75",
  "#fac858",
  "#ee6666",
  "#73c0de",
  "#3ba272",
  "#fc8452",
  "#9a60b4",
  "#ea7ccc",
  "#637cea",
  "#d4a373",
];

export function colorForCauseIndex(i: number): string {
  return CAUSE_STACK_PALETTE[i % CAUSE_STACK_PALETTE.length];
}

/**
 * Heatmap counts: x = crop tokens (merged paddy/rice, mixed/multiple), y = split
 * damage_cause tokens (excludes locust on the damage axis). Top crops/damages by marginal frequency.
 */
export function aggregateCropTypeDamageHeatmap(
  fc: FeatureCollection,
  maxCrops = 20,
  maxDamages = 18,
): {
  crops: string[];
  damages: string[];
  data: [number, number, number][];
  maxVal: number;
} {
  const pairCounts = new Map<string, number>();
  const cropTotals: Record<string, number> = {};
  const damageTotals: Record<string, number> = {};

  for (const f of fc.features || []) {
    const props = (f as Feature).properties as Record<string, unknown> | null;
    if (!props) continue;
    const cropRaw = splitSpeciesTokens(props.crop_type);
    if (!cropRaw.length) continue;
    const cropsNorm = [...new Set(cropRaw.map(normalizeCropTokenForHeatmap))].filter(
      (c) => c !== "unknown",
    );
    if (!cropsNorm.length) continue;

    const dmgRaw = splitDamageCauseTokens(props.damage_cause_raw);
    const damagesNorm = [...new Set(dmgRaw)].filter((d) => d !== "locust");
    if (!damagesNorm.length) continue;

    for (const d of damagesNorm) {
      damageTotals[d] = (damageTotals[d] || 0) + 1;
    }
    for (const c of cropsNorm) {
      cropTotals[c] = (cropTotals[c] || 0) + 1;
    }
    for (const c of cropsNorm) {
      for (const d of damagesNorm) {
        const key = `${c}\0${d}`;
        pairCounts.set(key, (pairCounts.get(key) || 0) + 1);
      }
    }
  }

  const crops = Object.entries(cropTotals)
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .slice(0, maxCrops)
    .map(([c]) => c);
  const damages = Object.entries(damageTotals)
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .slice(0, maxDamages)
    .map(([d]) => d);

  const cropIdx = new Map(crops.map((c, i) => [c, i] as const));
  const dmgIdx = new Map(damages.map((d, i) => [d, i] as const));

  const data: [number, number, number][] = [];
  for (const [key, v] of pairCounts) {
    const sep = key.indexOf("\0");
    if (sep < 0) continue;
    const c = key.slice(0, sep);
    const d = key.slice(sep + 1);
    const i = cropIdx.get(c);
    const j = dmgIdx.get(d);
    if (i === undefined || j === undefined || v <= 0) continue;
    data.push([i, j, v]);
  }

  const maxVal = data.length ? Math.max(...data.map((x) => x[2]), 1) : 1;
  return { crops, damages, data, maxVal };
}

export function formatSpeciesLabel(key: string): string {
  if (key === "other") return "Other";
  return key
    .split(/\s+/g)
    .filter(Boolean)
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase())
    .join(" ");
}

export type SpeciesStackOptions = {
  /** Tokens to drop (e.g. mammals for avian charts). */
  excludeSpecies?: Set<string>;
  /** Merge species with total count &lt; this into &quot;other&quot; before the top-N cap. */
  minSpeciesTotal?: number;
};

/**
 * Stacked counts by calendar year; each stack segment is a species token from
 * {@link splitSpeciesTokens} on `props.species` (commas, semicolons, "and", etc.).
 * When there are more than `maxSpecies` distinct species, the remainder merge into "other".
 */
export function aggregateSpeciesStackByYear(
  fc: FeatureCollection,
  maxSpecies = 28,
  opts?: SpeciesStackOptions,
): {
  byYear: Record<number, Record<string, number>>;
  years: number[];
  species: string[];
} {
  const exclude = opts?.excludeSpecies ?? new Set<string>();
  const minTotal = opts?.minSpeciesTotal ?? 1;

  const rawByYear: Record<number, Record<string, number>> = {};
  const speciesTotals: Record<string, number> = {};

  for (const f of fc.features || []) {
    const props = (f as Feature).properties as Record<string, unknown> | null;
    if (!props) continue;
    const y = parseYearFromProps(props);
    if (y === null) continue;
    const tokens = splitSpeciesTokens(props.species).filter((t) => !exclude.has(t));
    if (!tokens.length) continue;
    if (!rawByYear[y]) rawByYear[y] = {};
    for (const sp of tokens) {
      rawByYear[y][sp] = (rawByYear[y][sp] || 0) + 1;
      speciesTotals[sp] = (speciesTotals[sp] || 0) + 1;
    }
  }

  if (minTotal > 1) {
    const low = new Set(
      Object.entries(speciesTotals)
        .filter(([, c]) => c < minTotal)
        .map(([k]) => k),
    );
    for (const yStr of Object.keys(rawByYear)) {
      const y = parseInt(yStr, 10);
      const row = rawByYear[y];
      let o = 0;
      for (const k of low) {
        o += row[k] ?? 0;
        delete row[k];
      }
      if (o > 0) {
        row.other = (row.other || 0) + o;
      }
    }
    const nextTotals: Record<string, number> = {};
    for (const row of Object.values(rawByYear)) {
      for (const [k, v] of Object.entries(row)) {
        nextTotals[k] = (nextTotals[k] || 0) + v;
      }
    }
    Object.keys(speciesTotals).forEach((k) => delete speciesTotals[k]);
    Object.assign(speciesTotals, nextTotals);
  }

  const allSorted = Object.entries(speciesTotals)
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .map(([sp]) => sp);

  let species: string[];
  let byYear: Record<number, Record<string, number>>;

  if (allSorted.length <= maxSpecies) {
    species = allSorted;
    byYear = rawByYear;
  } else {
    const keepList = allSorted.slice(0, maxSpecies - 1);
    const keep = new Set(keepList);
    const rest = new Set(allSorted.slice(maxSpecies - 1));
    species = [...keepList, "other"];
    byYear = {};
    for (const yStr of Object.keys(rawByYear)) {
      const y = parseInt(yStr, 10);
      const row = rawByYear[y];
      const outRow: Record<string, number> = {};
      let other = 0;
      for (const [k, v] of Object.entries(row)) {
        if (keep.has(k)) {
          outRow[k] = v;
        } else if (rest.has(k)) {
          other += v;
        }
      }
      if (other > 0) {
        outRow.other = (outRow.other || 0) + other;
      }
      byYear[y] = outRow;
    }
  }

  const years = Object.keys(byYear)
    .map((x) => parseInt(x, 10))
    .sort((a, b) => a - b);

  return { byYear, years, species };
}
