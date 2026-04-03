import { useEffect, useMemo, useState } from "react";
import type { FeatureCollection } from "geojson";
import ReactECharts from "echarts-for-react";
import { api } from "./api";
import { aggregateDashboard } from "./lib/dashboardStats";
import "./App.css";
import "./dashboard.css";

type StylePayload = {
  colors_hex: Record<string, string>;
  category_field: string;
  merge_groups?: { id: string }[];
  singleton_event_types?: string[];
  fallback_category?: string;
};

const GITHUB_REPO = "https://github.com/aaditeshwar/gdelt-wildlife";
const GITHUB_META_DIR = "https://github.com/aaditeshwar/gdelt-wildlife/tree/main/meta";

type MetaSummary = {
  domain: { id: string; title: string; description: string };
  methodology: {
    gdelt_keywords: string[];
    gkg_primary_themes: string[];
    gkg_secondary_themes: string[];
    species_examples: string[];
    fetch_start_date?: string | null;
    fetch_end_date?: string | null;
  };
};

function yearFromMetaDate(s: string | null | undefined): number | null {
  if (!s || typeof s !== "string") return null;
  const m = s.trim().match(/^(\d{4})/);
  return m ? parseInt(m[1], 10) : null;
}

function yearRangeInclusive(
  start: string | null | undefined,
  end: string | null | undefined,
): { min: number; max: number } | null {
  const y0 = yearFromMetaDate(start ?? undefined);
  const y1 = yearFromMetaDate(end ?? undefined);
  if (y0 === null || y1 === null) return null;
  return { min: Math.min(y0, y1), max: Math.max(y0, y1) };
}

function categoryOrder(style: StylePayload): string[] {
  const colors = style.colors_hex || {};
  return Object.keys(colors);
}

export default function DashboardPage() {
  const params = new URLSearchParams(typeof window !== "undefined" ? window.location.search : "");
  const layer = params.get("layer") || "";

  const [fc, setFc] = useState<FeatureCollection | null>(null);
  const [style, setStyle] = useState<StylePayload | null>(null);
  const [summary, setSummary] = useState<MetaSummary | null>(null);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    if (!layer) {
      setErr("Missing ?layer= meta id (e.g. hwc_india_conflict).");
      return;
    }
    let cancelled = false;
    setErr(null);
    Promise.all([
      api<FeatureCollection>(`/api/layers/${encodeURIComponent(layer)}/geojson`),
      api<StylePayload>(`/api/layers/${encodeURIComponent(layer)}/style`),
      api<MetaSummary>(`/api/layers/${encodeURIComponent(layer)}/meta-summary`),
    ])
      .then(([g, s, m]) => {
        if (!cancelled) {
          setFc(g);
          setStyle(s);
          setSummary(m);
        }
      })
      .catch((e) => {
        if (!cancelled) setErr(String(e));
      });
    return () => {
      cancelled = true;
    };
  }, [layer]);

  const catField = style?.category_field || "map_category";
  const fallback = style?.fallback_category || "other";
  const colors = style?.colors_hex || {};

  const agg = useMemo(() => {
    if (!fc || !style) return null;
    return aggregateDashboard(fc, catField, fallback);
  }, [fc, style, catField, fallback]);

  const cats = useMemo(() => categoryOrder(style || { colors_hex: {} }), [style]);

  const isHwc = summary?.domain?.id === "hwc_india";

  const timelineYearRange = useMemo(() => {
    if (!summary?.methodology) return null;
    return yearRangeInclusive(
      summary.methodology.fetch_start_date,
      summary.methodology.fetch_end_date,
    );
  }, [summary]);

  const timelineYears = useMemo(() => {
    if (!agg) return [];
    const { byYear, years: dataYears } = agg;
    if (timelineYearRange) {
      const out: number[] = [];
      for (let y = timelineYearRange.min; y <= timelineYearRange.max; y++) {
        out.push(y);
      }
      return out;
    }
    return dataYears;
  }, [agg, timelineYearRange]);

  const timelineOpt = useMemo(() => {
    if (!agg || !style) return null;
    const { byYear } = agg;
    const years = timelineYears;
    if (years.length === 0) return null;
    const series = cats.map((c) => ({
      name: c,
      type: "bar" as const,
      stack: "total",
      emphasis: { focus: "series" as const },
      data: years.map((y) => byYear[y]?.[c] ?? 0),
      itemStyle: { color: colors[c] || "#888" },
    }));
    return {
      tooltip: { trigger: "axis" as const, axisPointer: { type: "shadow" as const } },
      legend: { type: "scroll" as const, bottom: 0 },
      grid: { left: 48, right: 16, top: 24, bottom: 72 },
      xAxis: { type: "category" as const, data: years.map(String) },
      yAxis: { type: "value" as const, name: "Events" },
      series,
    };
  }, [agg, cats, colors, style, timelineYears]);

  const stateBarOpt = useMemo(() => {
    if (!agg || !style) return null;
    const { byState, states } = agg;
    const top = states.slice(0, 24);
    if (top.length === 0) return null;
    const series = cats.map((c) => ({
      name: c,
      type: "bar" as const,
      stack: "total",
      emphasis: { focus: "series" as const },
      data: top.map((st) => byState[st]?.[c] ?? 0),
      itemStyle: { color: colors[c] || "#888" },
    }));
    return {
      tooltip: { trigger: "axis" as const, axisPointer: { type: "shadow" as const } },
      legend: { type: "scroll" as const, bottom: 0 },
      grid: { left: 48, right: 16, top: 24, bottom: 72 },
      xAxis: { type: "category" as const, data: top, axisLabel: { rotate: 35 } },
      yAxis: { type: "value" as const, name: "Events" },
      series,
    };
  }, [agg, cats, colors, style]);

  const heatmapOpt = useMemo(() => {
    if (!agg || !isHwc) return null;
    const { stateSpecies, states, speciesList } = agg;
    const topStates = states.filter((s) => s !== "Unknown");
    const topSpec = speciesList.slice(0, 16);
    if (topStates.length === 0 || topSpec.length === 0) return null;
    const data: [number, number, number][] = [];
    topStates.forEach((st, i) => {
      topSpec.forEach((sp, j) => {
        const v = stateSpecies[st]?.[sp] ?? 0;
        if (v > 0) data.push([i, j, v]);
      });
    });
    if (data.length === 0) return null;
    const maxV = Math.max(...data.map((d) => d[2]), 1);
    const nStates = topStates.length;
    const bottomPad = Math.min(220, 96 + Math.min(nStates, 28) * 4);
    return {
      tooltip: { position: "top" as const },
      grid: { left: 120, right: 88, top: 24, bottom: bottomPad },
      xAxis: {
        type: "category" as const,
        data: topStates,
        splitArea: { show: true },
        axisLabel: {
          interval: 0,
          rotate: 90,
          fontSize: 10,
          hideOverlap: false,
          margin: 8,
        },
      },
      yAxis: { type: "category" as const, data: topSpec, splitArea: { show: true } },
      visualMap: {
        min: 0,
        max: maxV,
        calculable: true,
        orient: "vertical" as const,
        right: 16,
        top: "center" as const,
      },
      series: [
        {
          name: "Count",
          type: "heatmap" as const,
          data,
          label: { show: false },
          emphasis: { itemStyle: { shadowBlur: 10, shadowColor: "rgba(0,0,0,0.5)" } },
        },
      ],
    };
  }, [agg, isHwc]);

  const nFeat = fc?.features?.length ?? 0;

  return (
    <div className="dashboard">
      <header className="dashboard-head">
        <h1>{summary?.domain?.title || "Analytics dashboard"}</h1>
        {layer && <p className="dashboard-layer muted">Layer: {layer}</p>}
        <p className="dashboard-back">
          <a href={`${import.meta.env.BASE_URL}`}>← Back to map</a>
        </p>
      </header>

      {err && <p className="banner">{err}</p>}

      {!layer && !err && <p className="muted">Add ?layer= to the URL.</p>}

      {summary && (
        <section className="dashboard-method">
          <h2>Methodology</h2>
          <p>
            GDELT was used to query articles based on the keywords and themes below. Articles
            likely to be specific events rather than policy discussions were identified using LLM
            prompts. Geocoding was done by extracting as precise a location about the event using
            LLMs, and geocoded using Google APIs.
          </p>
          <p>
            <a href={GITHUB_REPO} target="_blank" rel="noopener noreferrer">
              Source code (GitHub)
            </a>
            {" · "}
            <a href={GITHUB_META_DIR} target="_blank" rel="noopener noreferrer">
              Domain meta JSON directory
            </a>
            {layer && (
              <>
                {" · "}
                <a
                  href={`${GITHUB_REPO}/blob/main/meta/${encodeURIComponent(layer)}.json`}
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  This layer&apos;s meta file
                </a>
              </>
            )}
          </p>
          <p>
            <strong>Discovery (examples):</strong> GDELT DOC queries sample keywords include{" "}
            {summary.methodology.gdelt_keywords.slice(0, 8).join("; ")}
            {summary.methodology.gdelt_keywords.length > 8 ? " …" : "."}
          </p>
          <p>
            <strong>GKG theme codes (examples):</strong>{" "}
            {summary.methodology.gkg_primary_themes.slice(0, 6).join(", ")}
            {summary.methodology.gkg_primary_themes.length > 6 ? " …" : ""}
            {summary.methodology.gkg_secondary_themes.length > 0 && (
              <>
                {" "}
                Secondary: {summary.methodology.gkg_secondary_themes.slice(0, 4).join(", ")}
                {summary.methodology.gkg_secondary_themes.length > 4 ? " …" : ""}
              </>
            )}
          </p>
          <p>
            <strong>Species taxonomy (examples):</strong>{" "}
            {summary.methodology.species_examples.slice(0, 12).join(", ")}
            {summary.methodology.species_examples.length > 12 ? " …" : "."}
          </p>
        </section>
      )}

      {layer && !err && fc && (
        <p className="muted dashboard-count">
          {nFeat} point{nFeat === 1 ? "" : "s"} in GeoJSON.
        </p>
      )}

      {timelineOpt && (
        <section className="dashboard-chart">
          <h2>Events by year (stacked by category)</h2>
          {timelineYearRange &&
            summary?.methodology?.fetch_start_date &&
            summary?.methodology?.fetch_end_date && (
              <p className="hint">
                Years shown follow the GDELT fetch window in meta:{" "}
                {summary.methodology.fetch_start_date} … {summary.methodology.fetch_end_date}
              </p>
            )}
          <ReactECharts option={timelineOpt} style={{ height: 420 }} notMerge lazyUpdate />
        </section>
      )}

      {layer && !err && agg && !timelineOpt && (
        <p className="muted">
          No timeline chart: add parsable event dates in the data, or ensure meta includes
          fetch_start_date / fetch_end_date for the year axis.
        </p>
      )}

      {stateBarOpt && (
        <section className="dashboard-chart">
          <h2>Events by state (stacked by category)</h2>
          <p className="hint">
            States are inferred from location text (see map pipeline); some rows may be Unknown.
          </p>
          <ReactECharts option={stateBarOpt} style={{ height: 460 }} notMerge lazyUpdate />
        </section>
      )}

      {isHwc && (
        <section className="dashboard-chart">
          <h2>State × species (heatmap of counts)</h2>
          <p className="hint">
            Each species label is split on commas, semicolons, and &quot;and&quot; so one row is a
            single species (e.g. elephant vs elephant + tiger as two species). The y-axis lists the{" "}
            <strong>top 16 species by total count</strong> across all states (not alphabetical).
          </p>
          {!heatmapOpt && (
            <p className="muted">Not enough state/species pairs for this layer.</p>
          )}
          {heatmapOpt && (
            <ReactECharts option={heatmapOpt} style={{ height: 580 }} notMerge lazyUpdate />
          )}
        </section>
      )}
    </div>
  );
}
