import { useEffect, useMemo, useState } from "react";
import type { FeatureCollection } from "geojson";
import ReactECharts from "echarts-for-react";
import { api, formatFetchWindowLine } from "./api";
import {
  aggregateAvianSpeciesHabitatHeatmap,
  aggregateCategoryHabitatStack,
  aggregateCropDamageCauseByCategory,
  aggregateCropTypeDamageHeatmap,
  aggregateDashboard,
  aggregateSpeciesStackByYear,
  AVIAN_SPECIES_CHART_EXCLUDE,
  colorForCauseIndex,
  formatCropHeatmapCropLabel,
  formatDamageCauseLabel,
  formatHabitatLabel,
  formatSpeciesLabel,
} from "./lib/dashboardStats";
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

function formatMapCategoryLabel(id: string): string {
  return id
    .split("_")
    .filter(Boolean)
    .map((w) => {
      const lower = w.toLowerCase();
      if (lower === "ai") return "AI";
      return w.charAt(0).toUpperCase() + w.slice(1).toLowerCase();
    })
    .join(" ");
}

function categoryOrder(style: StylePayload): string[] {
  const colors = style.colors_hex || {};
  const keys = Object.keys(colors);
  const tail = new Set(["other", "unknown", "unclassified"]);
  const main = keys.filter((k) => !tail.has(k));
  const end = keys.filter((k) => tail.has(k)).sort((a, b) => a.localeCompare(b));
  return [...main, ...end];
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
  const catsNoLocust = useMemo(() => cats.filter((c) => c !== "locust"), [cats]);

  const isHwc = summary?.domain?.id === "hwc_india";
  const isCropDamage = summary?.domain?.id === "cropdamage_india";
  const isAvianMortality = summary?.domain?.id === "avian_influenza_surveillance_india";
  const showLocustLine = isCropDamage && cats.includes("locust");

  const timelineYearRange = useMemo(() => {
    if (!summary?.methodology) return null;
    return yearRangeInclusive(
      summary.methodology.fetch_start_date,
      summary.methodology.fetch_end_date,
    );
  }, [summary]);

  const fetchWindowLine = useMemo(
    () =>
      formatFetchWindowLine(
        summary?.methodology?.fetch_start_date,
        summary?.methodology?.fetch_end_date,
      ),
    [summary?.methodology?.fetch_start_date, summary?.methodology?.fetch_end_date],
  );

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

    if (showLocustLine) {
      const locustData = years.map((y) => byYear[y]?.locust ?? 0);
      const barSeries = catsNoLocust.map((c) => ({
        name: c,
        type: "bar" as const,
        stack: "total",
        yAxisIndex: 0,
        emphasis: { focus: "series" as const },
        data: years.map((y) => byYear[y]?.[c] ?? 0),
        itemStyle: { color: colors[c] || "#888" },
      }));
      return {
        tooltip: { trigger: "axis" as const, axisPointer: { type: "shadow" as const } },
        legend: { type: "scroll" as const, bottom: 0 },
        grid: { left: 48, right: 56, top: 24, bottom: 72 },
        xAxis: { type: "category" as const, data: years.map(String) },
        yAxis: [
          { type: "value" as const, name: "Events (stacked, excl. locust)" },
          { type: "value" as const, name: "Locust", position: "right" as const },
        ],
        series: [
          ...barSeries,
          {
            name: "Locust",
            type: "line" as const,
            yAxisIndex: 1,
            data: locustData,
            itemStyle: { color: colors.locust || "#c99402" },
            symbol: "circle",
            symbolSize: 6,
          },
        ],
      };
    }

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
  }, [agg, cats, catsNoLocust, colors, showLocustLine, style, timelineYears]);

  const stateBarOpt = useMemo(() => {
    if (!agg || !style) return null;
    const { byState, states } = agg;
    const top = states.slice(0, 24);
    if (top.length === 0) return null;

    if (showLocustLine) {
      const locustData = top.map((st) => byState[st]?.locust ?? 0);
      const barSeries = catsNoLocust.map((c) => ({
        name: c,
        type: "bar" as const,
        stack: "total",
        yAxisIndex: 0,
        emphasis: { focus: "series" as const },
        data: top.map((st) => byState[st]?.[c] ?? 0),
        itemStyle: { color: colors[c] || "#888" },
      }));
      return {
        tooltip: { trigger: "axis" as const, axisPointer: { type: "shadow" as const } },
        legend: { type: "scroll" as const, bottom: 0 },
        grid: { left: 48, right: 56, top: 24, bottom: 72 },
        xAxis: { type: "category" as const, data: top, axisLabel: { rotate: 35 } },
        yAxis: [
          { type: "value" as const, name: "Events (stacked, excl. locust)" },
          { type: "value" as const, name: "Locust", position: "right" as const },
        ],
        series: [
          ...barSeries,
          {
            name: "Locust",
            type: "line" as const,
            yAxisIndex: 1,
            data: locustData,
            itemStyle: { color: colors.locust || "#c99402" },
            symbol: "circle",
            symbolSize: 6,
          },
        ],
      };
    }

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
  }, [agg, cats, catsNoLocust, colors, showLocustLine, style]);

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

  const pestCauseAgg = useMemo(() => {
    if (!fc || !isCropDamage) return null;
    return aggregateCropDamageCauseByCategory(fc, "pest_disease", "damage_cause_raw", "pest_disease");
  }, [fc, isCropDamage]);

  const weatherCauseAgg = useMemo(() => {
    if (!fc || !isCropDamage) return null;
    return aggregateCropDamageCauseByCategory(
      fc,
      ["weather_extreme", "drought"],
      "damage_cause_raw",
      "weather_drought",
    );
  }, [fc, isCropDamage]);

  const cropTypeDamageHeatmapAgg = useMemo(() => {
    if (!fc || !isCropDamage) return null;
    return aggregateCropTypeDamageHeatmap(fc);
  }, [fc, isCropDamage]);

  const pestCauseOpt = useMemo(() => {
    if (!pestCauseAgg || pestCauseAgg.causes.length === 0) return null;
    const { byYear, causes } = pestCauseAgg;
    const years =
      timelineYears.length > 0 ? timelineYears : pestCauseAgg.years;
    if (years.length === 0) return null;
    const barSeries = causes.map((c, i) => ({
      name: formatDamageCauseLabel(c),
      type: "bar" as const,
      stack: "pest",
      yAxisIndex: 0,
      emphasis: { focus: "series" as const },
      data: years.map((y) => byYear[y]?.[c] ?? 0),
      itemStyle: { color: colorForCauseIndex(i) },
    }));

    if (showLocustLine && agg) {
      const locustData = years.map((y) => agg.byYear[y]?.locust ?? 0);
      return {
        tooltip: { trigger: "axis" as const, axisPointer: { type: "shadow" as const } },
        legend: { type: "scroll" as const, bottom: 0 },
        grid: { left: 48, right: 56, top: 24, bottom: 72 },
        xAxis: { type: "category" as const, data: years.map(String) },
        yAxis: [
          { type: "value" as const, name: "Pest / disease (by cause)" },
          { type: "value" as const, name: "Locust (map category)", position: "right" as const },
        ],
        series: [
          ...barSeries,
          {
            name: "Locust",
            type: "line" as const,
            yAxisIndex: 1,
            data: locustData,
            itemStyle: { color: colors.locust || "#c99402" },
            symbol: "circle",
            symbolSize: 6,
          },
        ],
      };
    }

    return {
      tooltip: { trigger: "axis" as const, axisPointer: { type: "shadow" as const } },
      legend: { type: "scroll" as const, bottom: 0 },
      grid: { left: 48, right: 16, top: 24, bottom: 72 },
      xAxis: { type: "category" as const, data: years.map(String) },
      yAxis: { type: "value" as const, name: "Events" },
      series: barSeries,
    };
  }, [agg, colors, pestCauseAgg, showLocustLine, timelineYears]);

  const weatherCauseOpt = useMemo(() => {
    if (!weatherCauseAgg || weatherCauseAgg.causes.length === 0) return null;
    const { byYear, causes } = weatherCauseAgg;
    const years =
      timelineYears.length > 0 ? timelineYears : weatherCauseAgg.years;
    if (years.length === 0) return null;
    const series = causes.map((c, i) => ({
      name: formatDamageCauseLabel(c),
      type: "bar" as const,
      stack: "weather",
      emphasis: { focus: "series" as const },
      data: years.map((y) => byYear[y]?.[c] ?? 0),
      itemStyle: { color: colorForCauseIndex(i) },
    }));
    return {
      tooltip: { trigger: "axis" as const, axisPointer: { type: "shadow" as const } },
      legend: { type: "scroll" as const, bottom: 0 },
      grid: { left: 48, right: 16, top: 24, bottom: 72 },
      xAxis: { type: "category" as const, data: years.map(String) },
      yAxis: { type: "value" as const, name: "Events" },
      series,
    };
  }, [weatherCauseAgg, timelineYears]);

  const cropTypeDamageHeatmapOpt = useMemo(() => {
    if (!cropTypeDamageHeatmapAgg) return null;
    const { crops, damages, data, maxVal } = cropTypeDamageHeatmapAgg;
    if (crops.length === 0 || damages.length === 0 || data.length === 0) return null;
    const nCrops = crops.length;
    const bottomPad = Math.min(260, 96 + Math.min(nCrops, 32) * 5);
    const cropLabels = crops.map((c) => formatCropHeatmapCropLabel(c));
    const damageLabels = damages.map((d) => formatDamageCauseLabel(d));
    return {
      tooltip: { position: "top" as const },
      grid: { left: 160, right: 88, top: 24, bottom: bottomPad },
      xAxis: {
        type: "category" as const,
        data: cropLabels,
        splitArea: { show: true },
        axisLabel: {
          interval: 0,
          rotate: 45,
          fontSize: 10,
          hideOverlap: false,
          margin: 8,
        },
      },
      yAxis: {
        type: "category" as const,
        data: damageLabels,
        splitArea: { show: true },
      },
      visualMap: {
        min: 0,
        max: maxVal,
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
  }, [cropTypeDamageHeatmapAgg]);

  const avianCategoryHabitatAgg = useMemo(() => {
    if (!fc || !isAvianMortality || !style) return null;
    return aggregateCategoryHabitatStack(fc, catField, fallback, cats, 14);
  }, [fc, isAvianMortality, style, catField, fallback, cats]);

  const avianCategoryHabitatOpt = useMemo(() => {
    if (!avianCategoryHabitatAgg) return null;
    const { categories, habitats, counts } = avianCategoryHabitatAgg;
    if (categories.length === 0 || habitats.length === 0) return null;
    const series = categories.map((c, i) => ({
      name: formatMapCategoryLabel(c),
      type: "bar" as const,
      stack: "total",
      emphasis: { focus: "series" as const },
      data: habitats.map((h) => counts[c]?.[h] ?? 0),
      itemStyle: { color: colors[c] || colorForCauseIndex(i) },
    }));
    return {
      tooltip: { trigger: "axis" as const, axisPointer: { type: "shadow" as const } },
      legend: { type: "scroll" as const, bottom: 6, left: "center" as const, itemGap: 10 },
      grid: { left: 48, right: 16, top: 24, bottom: 92 },
      xAxis: {
        type: "category" as const,
        data: habitats.map((h) => formatHabitatLabel(h)),
        axisLabel: { rotate: 30 },
      },
      yAxis: { type: "value" as const, name: "Events" },
      series,
    };
  }, [avianCategoryHabitatAgg, colors]);

  const speciesYearAgg = useMemo(() => {
    if (!fc || !isAvianMortality) return null;
    return aggregateSpeciesStackByYear(fc, 12, {
      excludeSpecies: AVIAN_SPECIES_CHART_EXCLUDE,
      minSpeciesTotal: 3,
    });
  }, [fc, isAvianMortality]);

  const avianSpeciesHabitatHeatmapAgg = useMemo(() => {
    if (!fc || !isAvianMortality) return null;
    return aggregateAvianSpeciesHabitatHeatmap(fc, 12, 14, AVIAN_SPECIES_CHART_EXCLUDE);
  }, [fc, isAvianMortality]);

  const avianSpeciesHabitatHeatmapOpt = useMemo(() => {
    if (!avianSpeciesHabitatHeatmapAgg) return null;
    const { species, habitats, data, maxVal } = avianSpeciesHabitatHeatmapAgg;
    if (species.length === 0 || habitats.length === 0 || data.length === 0) return null;
    const nSpec = species.length;
    const bottomPad = Math.min(260, 96 + Math.min(nSpec, 28) * 5);
    const specLabels = species.map((s) => formatSpeciesLabel(s));
    const habLabels = habitats.map((h) => formatHabitatLabel(h));
    return {
      tooltip: { position: "top" as const },
      grid: { left: 140, right: 88, top: 24, bottom: bottomPad },
      xAxis: {
        type: "category" as const,
        data: specLabels,
        splitArea: { show: true },
        axisLabel: {
          interval: 0,
          rotate: 45,
          fontSize: 10,
          hideOverlap: false,
          margin: 8,
        },
      },
      yAxis: {
        type: "category" as const,
        data: habLabels,
        splitArea: { show: true },
      },
      visualMap: {
        min: 0,
        max: maxVal,
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
  }, [avianSpeciesHabitatHeatmapAgg]);

  const speciesYearOpt = useMemo(() => {
    if (!speciesYearAgg || speciesYearAgg.species.length === 0) return null;
    const { byYear, species } = speciesYearAgg;
    const years =
      timelineYears.length > 0 ? timelineYears : speciesYearAgg.years;
    if (years.length === 0) return null;
    const series = species.map((sp, i) => ({
      name: formatSpeciesLabel(sp),
      type: "bar" as const,
      stack: "avian_species",
      emphasis: { focus: "series" as const },
      data: years.map((y) => byYear[y]?.[sp] ?? 0),
      itemStyle: { color: colorForCauseIndex(i) },
    }));
    return {
      tooltip: { trigger: "axis" as const, axisPointer: { type: "shadow" as const } },
      legend: { type: "scroll" as const, bottom: 0 },
      grid: { left: 48, right: 16, top: 24, bottom: 72 },
      xAxis: { type: "category" as const, data: years.map(String) },
      yAxis: { type: "value" as const, name: "Events" },
      series,
    };
  }, [speciesYearAgg, timelineYears]);

  const nFeat = fc?.features?.length ?? 0;

  return (
    <div className="dashboard">
      <header className="dashboard-head">
        <h1>{summary?.domain?.title || "Analytics dashboard"}</h1>
        {layer && <p className="dashboard-layer muted">Layer: {layer}</p>}
        {fetchWindowLine && (
          <p className="dashboard-fetch-range muted">{fetchWindowLine}</p>
        )}
        <p className="dashboard-back">
          <a
            href={
              layer
                ? `${import.meta.env.BASE_URL}?layer=${encodeURIComponent(layer)}`
                : `${import.meta.env.BASE_URL}`
            }
          >
            ← Back to map
          </a>
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
          {isCropDamage && showLocustLine && (
            <p className="hint">
              Stacked bars exclude the <code>locust</code> map category; locust counts appear on the
              right-hand axis as a line.
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
          {isCropDamage && showLocustLine && (
            <p className="hint">
              Bars exclude <code>locust</code>; the line uses the same locust counts by state (right
              axis).
            </p>
          )}
          <ReactECharts option={stateBarOpt} style={{ height: 460 }} notMerge lazyUpdate />
        </section>
      )}

      {isAvianMortality && avianCategoryHabitatOpt && (
        <section className="dashboard-chart">
          <h2>Event category × habitat type (stacked)</h2>
          <p className="hint">
            X-axis: normalized <code>habitat_type</code> (top 14 by frequency). Stacks:{" "}
            <code>map_category</code>. Compound values such as &quot;poultry farm, urban&quot; are
            split on commas/semicolons. All <code>wetland_*</code> and <code>wetland_named</code> are
            merged into Wetland; all <code>sanctuary_*</code> and <code>sanctuary_named</code> into
            Sanctuary. Multi-habitat rows add one count per (category × habitat) pair.
          </p>
          <ReactECharts option={avianCategoryHabitatOpt} style={{ height: 460 }} notMerge lazyUpdate />
        </section>
      )}

      {isCropDamage && pestCauseOpt && (
        <section className="dashboard-chart">
          <h2>Pest / disease events by year (stacked by damage cause)</h2>
          <p className="hint">
            Only points with <code>map_category</code> = <code>pest_disease</code>. Stacks use{" "}
            <code>damage_cause_raw</code> from the GeoJSON, split on commas/semicolons (and similar)
            so compound causes each get their own series. Only pest/disease taxonomy tokens are
            counted (weather-only tokens on the same row are omitted). Yearly stack totals can exceed
            event counts when one row lists multiple causes.
          </p>
          {showLocustLine && (
            <p className="hint">
              The line shows annual counts with <code>map_category</code> = <code>locust</code>{" "}
              (not part of the pest stacks).
            </p>
          )}
          <ReactECharts option={pestCauseOpt} style={{ height: 420 }} notMerge lazyUpdate />
        </section>
      )}

      {isCropDamage && weatherCauseOpt && (
        <section className="dashboard-chart">
          <h2>Weather &amp; drought events by year (stacked by damage cause)</h2>
          <p className="hint">
            Points with <code>map_category</code> = <code>weather_extreme</code> or{" "}
            <code>drought</code>. <code>damage_cause_raw</code> is split on commas/semicolons (and
            similar); only weather/drought taxonomy tokens are counted (pest/locust tokens on the
            same row are omitted). Yearly stack sums can exceed the number of points when causes are
            combined on one row.
          </p>
          <ReactECharts option={weatherCauseOpt} style={{ height: 420 }} notMerge lazyUpdate />
        </section>
      )}

      {isCropDamage && (
        <section className="dashboard-chart">
          <h2>Crop type × damage type (heatmap)</h2>
          <p className="hint">
            X-axis: <code>crop_type</code> tokens (split on comma/semicolon; paddy and rice merged;
            mixed / multiple merged). Y-axis: <code>damage_cause_raw</code> split on commas and
            similar separators so compound causes count separately; <code>locust</code> is omitted
            from the damage axis. Top 20 crops and 18 damage types by frequency.
          </p>
          {!cropTypeDamageHeatmapOpt && (
            <p className="muted">Not enough crop/damage pairs for this layer.</p>
          )}
          {cropTypeDamageHeatmapOpt && (
            <ReactECharts
              option={cropTypeDamageHeatmapOpt}
              style={{ height: 560 }}
              notMerge
              lazyUpdate
            />
          )}
        </section>
      )}

      {isAvianMortality && speciesYearOpt && (
        <section className="dashboard-chart">
          <h2>Events by year (stacked by species)</h2>
          <p className="hint">
            Y-axis: event count per year. Species tokens come from the <code>species</code> field
            (comma/semicolon/&quot;and&quot; split). Human, tiger, leopard, cat, tigress, and lion
            are excluded. Species with fewer than 3 total mentions are merged into{" "}
            <strong>Other</strong>; at most 11 named species plus Other (12 series).
          </p>
          <ReactECharts option={speciesYearOpt} style={{ height: 420 }} notMerge lazyUpdate />
        </section>
      )}

      {isAvianMortality && (
        <section className="dashboard-chart">
          <h2>Species × habitat type (heatmap)</h2>
          <p className="hint">
            X-axis: species tokens (same exclusions and top-12 cap as the year chart). Y-axis:{" "}
            <code>habitat_type</code> with the same wetland/sanctuary merging and comma splitting as
            the category×habitat chart. <code>environmental_stress</code> is omitted from the
            habitat axis. Cells count species–habitat pairs (top 14 habitats).
          </p>
          {!avianSpeciesHabitatHeatmapOpt && (
            <p className="muted">Not enough species/habitat pairs for this layer.</p>
          )}
          {avianSpeciesHabitatHeatmapOpt && (
            <ReactECharts
              option={avianSpeciesHabitatHeatmapOpt}
              style={{ height: 560 }}
              notMerge
              lazyUpdate
            />
          )}
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
