/** Indian states and UTs for substring matching in location strings (longest-first). */
const NAMES: string[] = [
  "Andaman and Nicobar Islands",
  "Andhra Pradesh",
  "Arunachal Pradesh",
  "Assam",
  "Bihar",
  "Chandigarh",
  "Chhattisgarh",
  "Dadra and Nagar Haveli and Daman and Diu",
  "Delhi",
  "Goa",
  "Gujarat",
  "Haryana",
  "Himachal Pradesh",
  "Jammu and Kashmir",
  "Jharkhand",
  "Karnataka",
  "Kerala",
  "Ladakh",
  "Lakshadweep",
  "Madhya Pradesh",
  "Maharashtra",
  "Manipur",
  "Meghalaya",
  "Mizoram",
  "Nagaland",
  "Odisha",
  "Orissa",
  "Puducherry",
  "Punjab",
  "Rajasthan",
  "Sikkim",
  "Tamil Nadu",
  "Telangana",
  "Tripura",
  "Uttar Pradesh",
  "Uttarakhand",
  "Uttaranchal",
  "West Bengal",
];

/** Sorted by length descending so "West Bengal" wins over "Bengal" in ambiguous cases. */
const SORTED = [...NAMES].sort((a, b) => b.length - a.length);

const ABBREV: Record<string, string> = {
  up: "Uttar Pradesh",
  mp: "Madhya Pradesh",
  mh: "Maharashtra",
  tn: "Tamil Nadu",
  jk: "Jammu and Kashmir",
};

/**
 * Best-effort state/UT from a free-text location (e.g. primary_location).
 */
export function deriveStateFromLocation(raw: unknown): string {
  if (raw === null || raw === undefined) return "Unknown";
  let s = String(raw).trim();
  if (!s || s.toLowerCase() === "nan" || s.toLowerCase() === "none") return "Unknown";
  const lower = s.toLowerCase();
  for (const name of SORTED) {
    if (lower.includes(name.toLowerCase())) {
      if (name === "Orissa") return "Odisha";
      if (name === "Uttaranchal") return "Uttarakhand";
      return name;
    }
  }
  const tail = s.split(",").map((x) => x.trim());
  const last = tail[tail.length - 1] || "";
  const beforeIndia = tail.filter((p) => !/^india$/i.test(p));
  const candidate = beforeIndia.length >= 2 ? beforeIndia[beforeIndia.length - 2] : last;
  const candLower = candidate.toLowerCase();
  if (ABBREV[candLower]) return ABBREV[candLower];
  for (const name of NAMES) {
    if (name.toLowerCase() === candLower) return name === "Orissa" ? "Odisha" : name;
  }
  return "Unknown";
}
