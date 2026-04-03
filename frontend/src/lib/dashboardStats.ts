import type { Feature, FeatureCollection } from "geojson";
import { deriveStateFromLocation } from "./indiaStates";

export function parseYearFromProps(props: Record<string, unknown>): number | null {
  const ed = props.event_date ?? props.pub_date;
  const s = ed === null || ed === undefined ? "" : String(ed).trim();
  if (!s) return null;
  const iso = s.match(/^(\d{4})-\d{2}-\d{2}/);
  if (iso) return parseInt(iso[1], 10);
  const ymd = s.match(/^(\d{4})\d{4}T/);
  if (ymd) return parseInt(ymd[1], 10);
  const four = s.match(/\b(19|20)\d{2}\b/);
  if (four) return parseInt(four[0], 10);
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
