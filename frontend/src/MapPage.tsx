import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { Feature, FeatureCollection } from "geojson";
import maplibregl, { Map, MapMouseEvent, StyleSpecification } from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import { api, apiUrl, formatFetchWindowLine } from "./api";
import { parseYearMonthFromProps } from "./lib/dashboardStats";
import "./App.css";

type LayerInfo = {
  id: string;
  label: string;
  prefix: string;
  has_geojson: boolean;
  geojson_path: string | null;
  has_qml?: boolean;
};

type MergeGroup = { id: string; label?: string; event_types?: string[] };

type StylePayload = {
  colors_hex: Record<string, string>;
  category_field: string;
  merge_groups?: MergeGroup[];
  singleton_event_types?: string[];
  fallback_category?: string;
};

/** Subset of `/api/layers/:id/meta-summary` used on the map page. */
type MetaSummaryBrief = {
  methodology?: {
    fetch_start_date?: string | null;
    fetch_end_date?: string | null;
  };
};

type PendingEdit = {
  edit_id: string;
  point_id: string;
  layer_id: string;
  suggested_properties: Record<string, unknown>;
  note?: string | null;
};

/** Raster satellite basemap (Google tile URL pattern; subject to Google Maps terms of use). */
const GOOGLE_SATELLITE_STYLE: StyleSpecification = {
  version: 8,
  name: "Google Satellite",
  sources: {
    "google-satellite": {
      type: "raster",
      tiles: ["https://mt1.google.com/vt/lyrs=s&x={x}&y={y}&z={z}"],
      tileSize: 256,
      attribution: "© Google",
    },
  },
  layers: [
    {
      id: "google-satellite",
      type: "raster",
      source: "google-satellite",
      minzoom: 0,
      maxzoom: 22,
    },
  ],
};

const SOURCE_ID = "events";
const CIRCLE_LAYER = "events-circle";

function filterFeatures(fc: FeatureCollection, q: string): FeatureCollection {
  const t = q.trim().toLowerCase();
  if (!t) return fc;
  const keys = [
    "title",
    "species",
    "event_type",
    "url",
    "map_category",
    "event_id",
    "primary_location",
  ];
  const feats = (fc.features || []).filter((f) => {
    const p = (f.properties || {}) as Record<string, unknown>;
    const blob = keys.map((k) => String(p[k] ?? "")).join(" ").toLowerCase();
    return blob.includes(t);
  });
  return { type: "FeatureCollection", features: feats };
}

function buildCircleColor(catField: string, colors: Record<string, string>): unknown {
  const flat: unknown[] = ["match", ["get", catField]];
  for (const [k, v] of Object.entries(colors)) {
    flat.push(k, v);
  }
  flat.push("#888888");
  return flat;
}

function escapeHtml(s: string): string {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

/** Safe for double-quoted HTML attribute values (e.g. href). */
function escapeAttr(s: string): string {
  return s.replace(/&/g, "&amp;").replace(/"/g, "&quot;").replace(/</g, "&lt;");
}

function propText(props: Record<string, unknown>, key: string): string {
  const v = props[key];
  if (v === null || v === undefined) return "";
  const s = String(v).trim();
  if (s === "" || s.toLowerCase() === "nan" || s.toLowerCase() === "none") return "";
  return s;
}

function normalizeCategoryForFilter(
  props: Record<string, unknown>,
  catField: string,
  fallback: string,
): string {
  const raw = props[catField];
  const t = String(raw ?? "").trim();
  if (!t || t.toLowerCase() === "nan" || t.toLowerCase() === "none") return fallback;
  return t;
}

function filterByCategories(
  fc: FeatureCollection,
  catField: string,
  fallback: string,
  enabled: Record<string, boolean>,
): FeatureCollection {
  const feats = (fc.features || []).filter((f) => {
    const p = (f.properties || {}) as Record<string, unknown>;
    const cat = normalizeCategoryForFilter(p, catField, fallback);
    if (Object.prototype.hasOwnProperty.call(enabled, cat)) {
      return enabled[cat] === true;
    }
    return true;
  });
  return { type: "FeatureCollection", features: feats };
}

/** Year checkboxes and date filtering only include this calendar year onward. */
const MIN_MAP_YEAR = 2020;

const CALENDAR_MONTHS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12] as const;
const MONTH_SHORT = [
  "Jan",
  "Feb",
  "Mar",
  "Apr",
  "May",
  "Jun",
  "Jul",
  "Aug",
  "Sep",
  "Oct",
  "Nov",
  "Dec",
];

function collectYearsFromGeojson(fc: FeatureCollection): number[] {
  const ys = new Set<number>();
  for (const f of fc.features || []) {
    const p = (f.properties || {}) as Record<string, unknown>;
    const ym = parseYearMonthFromProps(p);
    if (ym && ym.year >= MIN_MAP_YEAR) ys.add(ym.year);
  }
  return [...ys].sort((a, b) => a - b);
}

function filterByYearMonth(
  fc: FeatureCollection,
  enabledYears: Record<number, boolean>,
  enabledMonths: Record<number, boolean>,
  yearsInData: number[],
): FeatureCollection {
  const allMonthsOn = CALENDAR_MONTHS.every((m) => enabledMonths[m] !== false);
  const allYearsOn =
    yearsInData.length === 0 || yearsInData.every((y) => enabledYears[y] !== false);

  const feats = (fc.features || []).filter((f) => {
    const p = (f.properties || {}) as Record<string, unknown>;
    const ym = parseYearMonthFromProps(p);
    if (ym === null) {
      return allYearsOn && allMonthsOn;
    }
    if (ym.year < MIN_MAP_YEAR) return false;
    if (enabledYears[ym.year] === false) return false;
    if (ym.month === null) {
      return allMonthsOn;
    }
    if (enabledMonths[ym.month] === false) return false;
    return true;
  });
  return { type: "FeatureCollection", features: feats };
}

/** First character uppercased for map legend rows (keeps remainder as authored). */
function capitalizeLegendLabel(label: string): string {
  const t = label.trim();
  if (!t) return label;
  return t.charAt(0).toUpperCase() + t.slice(1);
}

/** Avian mortality: put Human bird flu after Other avian disease / non-AI mortality. */
const AVIAN_LEGEND_ID_ORDER = [
  "ai_wild",
  "ai_domestic",
  "ai_mixed",
  "other_disease",
  "human_h5",
];

function avianLegendOrderRank(id: string): number {
  const i = AVIAN_LEGEND_ID_ORDER.indexOf(id);
  return i === -1 ? 999 : i;
}

function buildLegendEntries(
  style: StylePayload,
  layerId: string,
): { id: string; label: string; color: string }[] {
  const colors = style.colors_hex || {};
  const labelById: Record<string, string> = {};
  for (const g of style.merge_groups || []) {
    if (g.id) labelById[g.id] = (g.label || g.id).trim();
  }
  for (const s of style.singleton_event_types || []) {
    if (labelById[s] === undefined) labelById[s] = s.replace(/_/g, " ");
  }
  if (style.fallback_category && labelById[style.fallback_category] === undefined) {
    labelById[style.fallback_category] = style.fallback_category.replace(/_/g, " ");
  }
  const out: { id: string; label: string; color: string }[] = [];
  for (const [id, color] of Object.entries(colors)) {
    out.push({
      id,
      label: labelById[id] || id.replace(/_/g, " "),
      color,
    });
  }
  const tailIds = new Set(["other", "unknown", "unclassified"]);
  let main = out.filter((e) => !tailIds.has(e.id));
  const tail = out.filter((e) => tailIds.has(e.id));
  if (layerId === "cropdamage_india") {
    main = main.filter((e) => e.id !== "unknown");
  }
  main.sort((a, b) => {
    if (layerId === "avianmortality_india") {
      const da = avianLegendOrderRank(a.id);
      const db = avianLegendOrderRank(b.id);
      if (da !== db) return da - db;
    }
    return a.label.localeCompare(b.label, undefined, { sensitivity: "base" });
  });
  tail.sort((a, b) => a.id.localeCompare(b.id));
  return [...main, ...tail].map((e) => ({
    ...e,
    label: capitalizeLegendLabel(e.label),
  }));
}

export default function MapPage() {
  const mapEl = useRef<HTMLDivElement | null>(null);
  const mapRef = useRef<Map | null>(null);
  const popupRef = useRef<maplibregl.Popup | null>(null);
  /** Set when the map fires `load` (style ready). Drives GeoJSON effect so layers re-apply after React Strict Mode remounts the map. */
  const [mapReady, setMapReady] = useState<Map | null>(null);
  const [layers, setLayers] = useState<LayerInfo[]>([]);
  const [layerId, setLayerId] = useState<string>("");
  const [rawGeojson, setRawGeojson] = useState<FeatureCollection | null>(null);
  const [stylePayload, setStylePayload] = useState<StylePayload | null>(null);
  const [metaSummary, setMetaSummary] = useState<MetaSummaryBrief | null>(null);
  const [search, setSearch] = useState("");
  const [selected, setSelected] = useState<Feature | null>(null);
  const [editJson, setEditJson] = useState("{}");
  const [editNote, setEditNote] = useState("");
  const [msg, setMsg] = useState<string | null>(null);
  const [auth, setAuth] = useState<{ user: string } | null>(null);
  const [loginOpen, setLoginOpen] = useState(false);
  const [loginUser, setLoginUser] = useState("");
  const [loginPass, setLoginPass] = useState("");
  const [pendingEdits, setPendingEdits] = useState<PendingEdit[]>([]);
  const [legendOpen, setLegendOpen] = useState(true);
  /** Category id -> visible (all true by default when legend loads). */
  const [enabledCategories, setEnabledCategories] = useState<Record<string, boolean>>({});
  /** Calendar year -> visible (default all years in layer). */
  const [enabledYears, setEnabledYears] = useState<Record<number, boolean>>({});
  /** Month 1–12 -> visible (default all on). */
  const [enabledMonths, setEnabledMonths] = useState<Record<number, boolean>>({});

  const uniqueYears = useMemo(
    () => (rawGeojson ? collectYearsFromGeojson(rawGeojson) : []),
    [rawGeojson],
  );

  useEffect(() => {
    if (!rawGeojson) {
      setEnabledYears({});
      setEnabledMonths({});
      return;
    }
    const ys = collectYearsFromGeojson(rawGeojson);
    setEnabledYears(Object.fromEntries(ys.map((y) => [y, true])));
    setEnabledMonths(Object.fromEntries(CALENDAR_MONTHS.map((m) => [m, true])));
  }, [rawGeojson]);

  const legendEntries = useMemo(
    () => (stylePayload && layerId ? buildLegendEntries(stylePayload, layerId) : []),
    [stylePayload, layerId],
  );

  useEffect(() => {
    const next: Record<string, boolean> = {};
    for (const e of legendEntries) next[e.id] = true;
    setEnabledCategories(next);
  }, [legendEntries]);

  const mapFilteredGeojson = useMemo(() => {
    if (!rawGeojson || !stylePayload) return null;
    let fc = filterFeatures(rawGeojson, search);
    fc = filterByYearMonth(fc, enabledYears, enabledMonths, uniqueYears);
    const cat = stylePayload.category_field || "map_category";
    const fb = stylePayload.fallback_category || "other";
    return filterByCategories(fc, cat, fb, enabledCategories);
  }, [
    rawGeojson,
    search,
    stylePayload,
    enabledCategories,
    enabledYears,
    enabledMonths,
    uniqueYears,
  ]);

  const totalPointCount = rawGeojson?.features?.length ?? 0;
  const displayedPointCount = mapFilteredGeojson?.features?.length ?? 0;

  const layerDownloadNames = useMemo(() => {
    if (!layerId) return null;
    const layer = layers.find((l) => l.id === layerId);
    const prefix = layer?.prefix ?? layerId;
    return {
      geojson: `${prefix}_points.geojson`,
      qml: `${prefix}_india_points.qml`,
      geojsonHref: apiUrl(`/api/layers/${encodeURIComponent(layerId)}/geojson`),
      qmlHref: apiUrl(`/api/layers/${encodeURIComponent(layerId)}/qml`),
      hasQml: layer?.has_qml === true,
    };
  }, [layerId, layers]);

  useEffect(() => {
    const params = new URLSearchParams(
      typeof window !== "undefined" ? window.location.search : "",
    );
    const wanted = params.get("layer")?.trim() || "";
    api<LayerInfo[]>("/api/meta/layers")
      .then((ls) => {
        setLayers(ls);
        const hwc = ls.find((l) => l.has_geojson && l.prefix === "hwc");
        const first = hwc || ls.find((l) => l.has_geojson) || ls[0];
        const fromUrl =
          wanted && ls.some((l) => l.id === wanted && l.has_geojson)
            ? ls.find((l) => l.id === wanted)
            : null;
        const pick = fromUrl || first;
        if (pick) setLayerId(pick.id);
      })
      .catch((e) => setMsg(String(e)));
  }, []);

  useEffect(() => {
    api<{ authenticated: boolean; username?: string }>("/api/auth/me")
      .then((r) => {
        if (r.authenticated && r.username) setAuth({ user: r.username });
        else setAuth(null);
      })
      .catch(() => setAuth(null));
  }, []);

  const refreshPending = useCallback(() => {
    if (!auth) {
      setPendingEdits([]);
      return;
    }
    api<PendingEdit[]>("/api/moderation/edits").then(setPendingEdits)
      .catch(() => setPendingEdits([]));
  }, [auth]);

  useEffect(() => {
    refreshPending();
  }, [refreshPending]);

  useEffect(() => {
    if (!layerId) return;
    let cancelled = false;
    Promise.all([
      api<FeatureCollection>(`/api/layers/${encodeURIComponent(layerId)}/geojson`),
      api<StylePayload>(`/api/layers/${encodeURIComponent(layerId)}/style`),
      api<MetaSummaryBrief>(`/api/layers/${encodeURIComponent(layerId)}/meta-summary`),
    ])
      .then(([gj, st, ms]) => {
        if (cancelled) return;
        setRawGeojson(gj);
        setStylePayload(st);
        setMetaSummary(ms);
        setSelected(null);
        setMsg(null);
      })
      .catch((e) => {
        if (!cancelled) {
          setMsg(String(e));
          setMetaSummary(null);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [layerId]);

  const mapFetchWindowLine = useMemo(
    () =>
      formatFetchWindowLine(
        metaSummary?.methodology?.fetch_start_date,
        metaSummary?.methodology?.fetch_end_date,
      ),
    [metaSummary?.methodology?.fetch_start_date, metaSummary?.methodology?.fetch_end_date],
  );

  useEffect(() => {
    popupRef.current?.remove();
    popupRef.current = null;
  }, [layerId]);

  const setupMapLayers = useCallback(
    (map: Map, gj: FeatureCollection, st: StylePayload | null) => {
      if (map.getLayer(CIRCLE_LAYER)) map.removeLayer(CIRCLE_LAYER);
      if (map.getSource(SOURCE_ID)) map.removeSource(SOURCE_ID);
      map.addSource(SOURCE_ID, { type: "geojson", data: gj });
      const cat = st?.category_field || "map_category";
      const colors = st?.colors_hex || {};
      map.addLayer({
        id: CIRCLE_LAYER,
        type: "circle",
        source: SOURCE_ID,
        paint: {
          "circle-radius": 6,
          "circle-color": buildCircleColor(cat, colors) as unknown as maplibregl.ExpressionSpecification,
          "circle-stroke-width": 1,
          "circle-stroke-color": "#222",
        },
      });
      // Ensure points render above the raster basemap (ordering can hide circles otherwise).
      map.moveLayer(CIRCLE_LAYER);
    },
    [],
  );

  useEffect(() => {
    if (!mapEl.current || mapRef.current) return;
    const map = new maplibregl.Map({
      container: mapEl.current,
      style: GOOGLE_SATELLITE_STYLE,
      center: [78.0, 22.0],
      zoom: 4,
    });
    map.addControl(new maplibregl.NavigationControl(), "top-left");
    mapRef.current = map;
    let cancelled = false;
    const onMapLoad = () => {
      if (!cancelled) setMapReady(map);
    };
    if (map.loaded()) onMapLoad();
    else map.on("load", onMapLoad);
    map.on("click", (e: MapMouseEvent) => {
      if (!map.getLayer(CIRCLE_LAYER)) {
        popupRef.current?.remove();
        popupRef.current = null;
        setSelected(null);
        return;
      }
      const hits = map.queryRenderedFeatures(e.point, { layers: [CIRCLE_LAYER] });
      if (hits.length) {
        const feat = hits[0] as Feature;
        setSelected(feat);
        const props = (feat.properties || {}) as Record<string, unknown>;
        const title = String(props.title ?? "Untitled");
        const url = String(props.url ?? "").trim();
        const eventDate = propText(props, "event_date");
        const primaryLoc = propText(props, "primary_location");
        const g = feat.geometry;
        if (g?.type === "Point" && Array.isArray(g.coordinates)) {
          const [lng, lat] = g.coordinates as [number, number];
          popupRef.current?.remove();
          const safeUrl = /^https?:\/\//i.test(url) ? url : "";
          const titleBlock =
            safeUrl !== ""
              ? `<a class="map-popup-link" href="${escapeAttr(safeUrl)}" target="_blank" rel="noopener noreferrer">${escapeHtml(title)}</a>`
              : escapeHtml(title);
          const metaRows: string[] = [];
          if (eventDate) {
            metaRows.push(
              `<div class="map-popup-row"><span class="map-popup-label">Date</span><span class="map-popup-value">${escapeHtml(eventDate)}</span></div>`,
            );
          }
          if (primaryLoc) {
            metaRows.push(
              `<div class="map-popup-row"><span class="map-popup-label">Location</span><span class="map-popup-value">${escapeHtml(primaryLoc)}</span></div>`,
            );
          }
          const metaBlock =
            metaRows.length > 0
              ? `<div class="map-popup-meta">${metaRows.join("")}</div>`
              : "";
          const body = `<div class="map-popup-inner"><div class="map-popup-title">${titleBlock}</div>${metaBlock}</div>`;
          const popup = new maplibregl.Popup({
            closeButton: true,
            closeOnClick: true,
            maxWidth: "min(280px, 90vw)",
            className: "map-popup-wrap",
          })
            .setLngLat([lng, lat])
            .setHTML(body)
            .addTo(map);
          popup.on("close", () => {
            if (popupRef.current === popup) popupRef.current = null;
          });
          popupRef.current = popup;
        }
      } else {
        popupRef.current?.remove();
        popupRef.current = null;
        setSelected(null);
      }
    });
    return () => {
      cancelled = true;
      popupRef.current?.remove();
      popupRef.current = null;
      setMapReady(null);
      map.remove();
      mapRef.current = null;
    };
  }, []);

  useEffect(() => {
    if (!mapReady || !mapFilteredGeojson || !stylePayload) return;
    setupMapLayers(mapReady, mapFilteredGeojson, stylePayload);
    try {
      const b = new maplibregl.LngLatBounds();
      let n = 0;
      for (const f of mapFilteredGeojson.features || []) {
        const g = f.geometry;
        if (g?.type === "Point" && Array.isArray(g.coordinates)) {
          b.extend(g.coordinates as [number, number]);
          n++;
        }
      }
      if (n) mapReady.fitBounds(b, { padding: 48, maxZoom: 12 });
    } catch {
      /* ignore */
    }
  }, [mapReady, mapFilteredGeojson, stylePayload, setupMapLayers]);

  const pointEdits = useMemo(() => {
    if (!selected || !layerId) return [];
    const pid =
      (selected.properties as Record<string, unknown>)?.event_id ||
      selected.id ||
      "";
    return pendingEdits.filter(
      (e) =>
        e.layer_id === layerId &&
        String(e.point_id) === String(pid),
    );
  }, [pendingEdits, selected, layerId]);

  const submitEdit = async () => {
    setMsg(null);
    try {
      const suggested = JSON.parse(editJson || "{}") as Record<string, unknown>;
      const pid =
        (selected?.properties as Record<string, unknown>)?.event_id ||
        selected?.id ||
        "";
      if (!layerId || !pid) throw new Error("Select a point first");
      await api("/api/edits", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          point_id: String(pid),
          layer_id: layerId,
          suggested_properties: suggested,
          note: editNote || null,
        }),
      });
      setMsg("Suggestion submitted (pending moderation).");
      refreshPending();
    } catch (e) {
      setMsg(String(e));
    }
  };

  const doLogin = async () => {
    setMsg(null);
    try {
      await api("/api/auth/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username: loginUser, password: loginPass }),
      });
      setAuth({ user: loginUser });
      setLoginOpen(false);
      setLoginPass("");
      refreshPending();
    } catch {
      setMsg("Login failed");
    }
  };

  const logout = async () => {
    await api("/api/auth/logout", { method: "POST" });
    setAuth(null);
    setPendingEdits([]);
  };

  const applyEdit = async (editId: string) => {
    setMsg(null);
    try {
      await api(`/api/moderation/edits/${encodeURIComponent(editId)}/apply`, {
        method: "POST",
      });
      setMsg("Edit applied.");
      refreshPending();
      const gj = await api<FeatureCollection>(
        `/api/layers/${encodeURIComponent(layerId)}/geojson`,
      );
      setRawGeojson(gj);
    } catch (e) {
      setMsg(String(e));
    }
  };

  const deleteEdit = async (editId: string) => {
    setMsg(null);
    try {
      await api(`/api/moderation/edits/${encodeURIComponent(editId)}`, {
        method: "DELETE",
      });
      setMsg("Edit deleted.");
      refreshPending();
    } catch (e) {
      setMsg(String(e));
    }
  };

  const props = (selected?.properties || {}) as Record<string, unknown>;

  const toggleCategory = (id: string) => {
    setEnabledCategories((prev) => ({ ...prev, [id]: !prev[id] }));
  };

  const toggleYear = (y: number) => {
    setEnabledYears((prev) => {
      const cur = prev[y] !== false;
      return { ...prev, [y]: !cur };
    });
  };

  const toggleMonth = (m: number) => {
    setEnabledMonths((prev) => {
      const cur = prev[m] !== false;
      return { ...prev, [m]: !cur };
    });
  };

  return (
    <div className="layout">
      <div className="map-wrap">
        <div ref={mapEl} className="map" />
        {stylePayload && legendEntries.length > 0 && (
          <div
            className={`map-legend ${legendOpen ? "map-legend--open" : "map-legend--collapsed"}`}
          >
            <button
              type="button"
              className="map-legend-toggle"
              onClick={() => setLegendOpen((o) => !o)}
              aria-expanded={legendOpen}
              aria-label={legendOpen ? "Minimize legend" : "Maximize legend"}
            >
              <span className="map-legend-toggle-title">Legend</span>
              <span className="map-legend-toggle-hint" aria-hidden>
                {legendOpen ? "Minimize" : "Maximize"}
              </span>
              <span className="map-legend-toggle-icon" aria-hidden>
                {legendOpen ? "▼" : "▶"}
              </span>
            </button>
            {legendOpen && (
              <div className="map-legend-body">
                <div className="map-legend-heading">Event type</div>
                <ul className="map-legend-list">
                  {legendEntries.map((row) => (
                    <li key={row.id} className="map-legend-item">
                      <label className="map-legend-check">
                        <input
                          type="checkbox"
                          checked={enabledCategories[row.id] !== false}
                          onChange={() => toggleCategory(row.id)}
                        />
                        <span
                          className="map-legend-swatch"
                          style={{ backgroundColor: row.color }}
                          aria-hidden
                        />
                        <span className="map-legend-label">{row.label}</span>
                      </label>
                    </li>
                  ))}
                </ul>
              </div>
            )}
          </div>
        )}
      </div>
      <aside className="panel panel--compact">
        <header className="panel-head">
          <h1>GDELT map</h1>
          <div className="panel-actions">
            {auth ? (
              <span className="muted">
                {auth.user}{" "}
                <button type="button" onClick={() => void logout()}>
                  Log out
                </button>
              </span>
            ) : (
              <button type="button" onClick={() => setLoginOpen(true)}>
                Moderator
              </button>
            )}
          </div>
        </header>

        {mapFetchWindowLine && (
          <p className="muted panel-fetch-range">{mapFetchWindowLine}</p>
        )}

        <div className="layer-select-wrap">
          <select
            className="layer-select"
            value={layerId}
            onChange={(e) => setLayerId(e.target.value)}
            aria-label="Layer"
          >
            {layers.map((l) => (
              <option key={l.id} value={l.id} disabled={!l.has_geojson}>
                {l.label}
                {!l.has_geojson ? " (no GeoJSON)" : ""}
              </option>
            ))}
          </select>
        </div>

        {layerId &&
          layers.find((l) => l.id === layerId)?.has_geojson && (
            <div className="layer-dashboard-link muted">
              <a
                href={`${import.meta.env.BASE_URL}dashboard?layer=${encodeURIComponent(layerId)}`}
                target="_blank"
                rel="noopener noreferrer"
              >
                Dashboard
              </a>
            </div>
          )}

        {layerDownloadNames && (
          <div className="layer-downloads muted">
            <a
              href={layerDownloadNames.geojsonHref}
              download={layerDownloadNames.geojson}
            >
              Download GeoJSON
            </a>
            {layerDownloadNames.hasQml && (
              <>
                <span aria-hidden> · </span>
                <a
                  href={layerDownloadNames.qmlHref}
                  download={layerDownloadNames.qml}
                >
                  Download QGIS style (QML)
                </a>
              </>
            )}
          </div>
        )}

        <label className="field">
          <span>Search</span>
          <input
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Filter by title, species, url, …"
          />
        </label>

        {layerId &&
          layers.find((l) => l.id === layerId)?.has_geojson &&
          rawGeojson &&
          rawGeojson.features &&
          rawGeojson.features.length > 0 && (
            <div className="panel-date-filters">
              <div className="panel-date-filters-head">Date filter</div>
              {uniqueYears.length > 0 && (
                <fieldset className="panel-date-fieldset">
                  <legend>Year</legend>
                  <ul className="panel-date-checklist">
                    {uniqueYears.map((y) => (
                      <li key={y}>
                        <label className="panel-date-check">
                          <input
                            type="checkbox"
                            checked={enabledYears[y] !== false}
                            onChange={() => toggleYear(y)}
                          />
                          <span>{y}</span>
                        </label>
                      </li>
                    ))}
                  </ul>
                </fieldset>
              )}
              <fieldset className="panel-date-fieldset">
                <legend>Month</legend>
                <ul className="panel-date-checklist panel-date-checklist--months">
                  {CALENDAR_MONTHS.map((m) => (
                    <li key={m}>
                      <label className="panel-date-check">
                        <input
                          type="checkbox"
                          checked={enabledMonths[m] !== false}
                          onChange={() => toggleMonth(m)}
                        />
                        <span>{MONTH_SHORT[m - 1]}</span>
                      </label>
                    </li>
                  ))}
                </ul>
              </fieldset>
            </div>
          )}

        {layerId && layers.find((l) => l.id === layerId)?.has_geojson && rawGeojson ? (
          <p className="muted map-point-count" aria-live="polite">
            Points: <strong>{displayedPointCount}</strong> / {totalPointCount}
          </p>
        ) : null}

        {msg && <p className="banner">{msg}</p>}

        <section className="section section--compact">
          <h2>Selected</h2>
          {!selected && (
            <p className="muted section-hint-below">Click on a point on the map.</p>
          )}
          {selected ? <pre className="json">{JSON.stringify(props, null, 2)}</pre> : null}
        </section>

        <section className="section">
          <h2>Suggest edit</h2>
          <p className="hint">
            JSON object of properties to merge (e.g.{" "}
            <code>{`{"species": "tiger"}`}</code>).
          </p>
          <textarea
            className="textarea"
            rows={6}
            value={editJson}
            onChange={(e) => setEditJson(e.target.value)}
          />
          <label className="field">
            <span>Note (optional)</span>
            <input
              value={editNote}
              onChange={(e) => setEditNote(e.target.value)}
            />
          </label>
          <button type="button" onClick={() => void submitEdit()}>
            Submit suggestion
          </button>
        </section>

        {auth && (
          <section className="section">
            <h2>Pending edits (this point)</h2>
            {pointEdits.length === 0 ? (
              <p className="muted">None for this point.</p>
            ) : (
              <ul className="edit-list">
                {pointEdits.map((e) => (
                  <li key={e.edit_id}>
                    <code>{e.edit_id.slice(0, 8)}…</code>
                    <pre className="json small">
                      {JSON.stringify(e.suggested_properties, null, 2)}
                    </pre>
                    <div className="row">
                      <button
                        type="button"
                        onClick={() => void applyEdit(e.edit_id)}
                      >
                        Apply
                      </button>
                      <button
                        type="button"
                        className="secondary"
                        onClick={() => void deleteEdit(e.edit_id)}
                      >
                        Delete
                      </button>
                    </div>
                  </li>
                ))}
              </ul>
            )}
          </section>
        )}
      </aside>

      {loginOpen && (
        <div className="modal-backdrop" role="presentation">
          <div className="modal" role="dialog">
            <h2>Moderator login</h2>
            <label className="field">
              <span>Username</span>
              <input
                value={loginUser}
                onChange={(e) => setLoginUser(e.target.value)}
                autoComplete="username"
              />
            </label>
            <label className="field">
              <span>Password</span>
              <input
                type="password"
                value={loginPass}
                onChange={(e) => setLoginPass(e.target.value)}
                autoComplete="current-password"
              />
            </label>
            <div className="row">
              <button type="button" onClick={() => void doLogin()}>
                Log in
              </button>
              <button
                type="button"
                className="secondary"
                onClick={() => setLoginOpen(false)}
              >
                Cancel
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
