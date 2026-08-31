# Handoff: BeatsCheck WebUI Redesign

## Overview
A full visual + interaction redesign of **BeatsCheck** — a self-hosted audio-library integrity checker (Docker/Unraid app that decodes every track with ffmpeg and flags corrupt files). This handoff covers the entire web interface: **Dashboard, Corrupt Files, Configuration, Logs, Login, and Setup**. The redesign keeps the project's spectrogram/audio-heat identity but sharpens it into a modern, dark "audio-tool" dashboard with a disciplined signal-green accent, a working light mode, and a signature spectrogram scan-progress visualization.

The original app serves a static `index.html` + `app.js` from a FastAPI backend (`app/main.py`), polling JSON endpoints for status/progress/corrupt-list/logs. This redesign is a drop-in replacement for that front-end layer — same data, new presentation.

## About the Design Files
The files in this bundle are **design references created in HTML** — a prototype showing the intended look and behavior, **not production code to copy directly**. The prototype is authored as a single "Design Component" (`BeatsCheck.dc.html`) that uses a small custom template runtime (`support.js`); **do not ship that runtime**. The `{{ }}` holes, `<sc-if>`, `<sc-for>`, and `<x-dc>`/`<helmet>` tags are authoring conveniences, not part of the target stack.

Your task is to **recreate these designs in BeatsCheck's existing front-end environment** — the app currently uses vanilla JS (`app/static/app.js`) talking to a FastAPI backend, so the most faithful path is plain HTML/CSS/JS (or a light framework like Preact/Alpine if you prefer) wired to the existing JSON endpoints. Reuse the backend's real data shapes; replace only the presentation. If you choose to introduce a framework, match the project's lightweight, dependency-light philosophy.

## Fidelity
**High-fidelity (hifi).** Final colors, typography, spacing, radii, and interactions are all specified below and should be reproduced precisely. All sample data (file names, log lines, stats) is placeholder — wire the real backend data in its place.

---

## Design Tokens

### Color palette — Dark theme (default)
| Token | Hex | Use |
|---|---|---|
| `--bg`   | `#0a0b0e` | App background (main content area) |
| `--bg1`  | `#121319` | Surfaces: sidebar, header, cards, tables |
| `--bg2`  | `#1a1c23` | Inset surfaces: inputs, selects, secondary buttons, album group headers |
| `--bgh`  | `#22242d` | Hover surface |
| `--bd`   | `#23252e` | Default borders / row dividers |
| `--bds`  | `#30333d` | Stronger borders (logo chip, scrollbar) |
| `--tx`   | `#ecedf1` | Primary text |
| `--txm`  | `#9396a3` | Muted text (labels, secondary) |
| `--txd`  | `#5f626d` | Dim text (meta, mono captions) |
| terminal bg | `#08090c` | Logs viewer background |

### Color palette — Light theme
| Token | Hex |
|---|---|
| `--bg`   | `#eef0f4` |
| `--bg1`  | `#ffffff` |
| `--bg2`  | `#f3f4f8` |
| `--bgh`  | `#e7e9f0` |
| `--bd`   | `#e3e5ed` |
| `--bds`  | `#d2d5df` |
| `--tx`   | `#16171c` |
| `--txm`  | `#5b5f6b` |
| `--txd`  | `#9094a0` |

### Accent (default = "signal" green)
| Token | Dark | Light | Notes |
|---|---|---|---|
| `--ac`  (accent) | `#34e0a0` | `#0f9d6e` | Primary buttons, active nav, status-OK, scan % |
| `--acd` (accent dim) | `rgba(52,224,160,.14)` | same | Active nav bg, selected row bg, accent-tinted panels |
| `--acft` (accent foreground) | `#06120d` | `#06120d` | Text on accent buttons (near-black) |

Two alternate accents were built as theme options (swap `--ac`/`--acd`/`--acft` together):
- **amber**: dark `#f5a623` / light `#bf7d0c`, dim `rgba(245,166,35,.15)`, fg `#06120d`
- **indigo**: dark `#6c7bff` / light `#4a59e6`, dim `rgba(108,123,255,.16)`, fg `#ffffff`

### Semantic colors
| Token | Dark | Light | Use |
|---|---|---|---|
| `--dg` (danger) | `#ff4d6d` | `#d6294e` | Corrupt counts, errors, delete buttons, corruption spikes |
| `--dgd` (danger dim) | `rgba(255,77,109,.13)` | `rgba(214,41,78,.1)` | Badge backgrounds |
| `--wn` (warning) | `#f5b94a` | `#b9831a` | Scanning status dot, WARNING log chips |
| `--inf` (info) | `#5a9cf0` | `#2f72d6` | INFO log chips |

### Log-level chip colors (terminal)
| Level | Text | Tint bg |
|---|---|---|
| INFO | `#5a9cf0` | `rgba(90,156,240,.14)` |
| WARNING | `#f5b94a` | `rgba(245,185,74,.14)` |
| ERROR | `#ff5775` | `rgba(255,87,117,.15)` |
| DEBUG | `#6b6f7d` | `rgba(107,111,125,.18)` |
| CRITICAL | `#ff3868` | `rgba(255,56,104,.16)` |

Log message body text: ERROR rows `#ff8095`; all others `#c9ccd6` (DEBUG at 0.65 opacity, others 0.95). Timestamp `#5a5d68`.

### Spectrum (signature gradient)
`linear-gradient(180deg,#6c5cff,#4b86ff,#34e0a0,#9ad94a,#f5c24a,#ff7a4d)` — used for the logo mark and as the per-frequency color ramp in the scan visualization. Spectrogram bar color stops (by height bucket, low→high): `#6c5cff`, `#4b86ff`, `#34e0a0`, `#9ad94a`, `#f5c24a`, `#ff7a4d`. Corrupt bars override to `#ff4d6d` with glow.

### Typography
- **Display / UI**: `'Hanken Grotesk', system-ui, -apple-system, sans-serif` — weights 400/500/600/700/800. `-webkit-font-smoothing: antialiased`.
- **Mono**: `'IBM Plex Mono', monospace` — weights 400/500/600. Used for file paths, log lines, stat captions, config keys, version string, nav numbers, section eyebrows.
- Google Fonts import: `Hanken+Grotesk:wght@400;500;600;700;800` and `IBM+Plex+Mono:wght@400;500;600`.

Key sizes (px): page title 18/700; card stat value 21–23/700 (letter-spacing −.02em); card eyebrow 9.5 mono uppercase, letter-spacing .1em; scan % 30/800 (−.03em); nav item 13.5/600; body/secondary 12.5–13; mono captions 10.5–11; table mono cells 12–12.5; log lines 12/1.75.

### Spacing / radii
- Card radius **14px**; large panels (scan hero, auth cards) **16–18px**; buttons **8–10px**; inputs **8–10px**; chips/badges **999px** (pills) or **5px** (log chips); logo chip **11–13px**.
- Main content padding `26px 30px`; header padding `15px 30px`; card padding `18px` comfortable / `13px` compact (token `--cardpad`).
- Table row padding `13px 16px` comfortable / `9px` compact (token `--rowpad`).
- Stat grid gap `14px` comfortable / `10px` compact (token `--gap`).
- Sidebar width **238px** (fixed, sticky, full height).

### Shadows / glows
- Status dot glow: `0 0 8px <color>`.
- Scan sweep line: `box-shadow: 0 0 14px var(--ac)`, with an 8px dot head glowing `0 0 10px var(--ac)`.
- Corrupt spectrogram bars: `box-shadow: 0 0 8px rgba(255,77,109,.85)`.
- Switch knob: `0 1px 2px rgba(0,0,0,.3)`.

### Animations (keyframes)
- `bc-pulse` (1.4s ease-in-out infinite): `opacity 1 → .28 → 1` — scanning status dot.
- `bc-sweep` (1.6s ease-in-out infinite): `opacity .55 → 1 → .55` — scan playhead line.
- Row/selection/switch transitions: `.15s–.2s`.

---

## Layout (app shell)
Two-column flex. **Left:** fixed 238px sidebar (`--bg1`, right border `--bd`, sticky, 100vh, flex-column). **Right:** flex-1 column = sticky header (`--bg1`, bottom border) + scrolling `<main>` (padding `26px 30px`). Most screens cap content at `max-width: 1080px` (config 840px, auth centered cards).

### Sidebar
- **Logo block** (padding `20px 20px 16px`): 36px rounded chip — `radial-gradient(circle at 50% 38%, #1d1f28, #08090c)`, border `--bds` — containing a 4×15px spectrum-gradient bar + a 4px `#ff4d6d` dot near the bottom. Wordmark "Beats**Check**" (Check in `--ac`), 16/800, −.02em; sub-line mono 9.5 `--txd`: "v1.0.0 · integrity".
- **Sections** with mono eyebrows (9.5, letter-spacing .14em, `--txd`): "MONITOR" (Dashboard, Corrupt files, Configuration, Logs) and "ACCESS PREVIEW" (Login, Setup wizard).
- **Nav item**: flex, gap 11px, padding `9px 14px`, margin `1px 12px`, radius 9px, 13.5/600. Each item has a mono 2-digit index (`01`–`06`, color `--txd`, width 15px). Active item: text `--ac`, background `--acd`. Inactive text `--txm`. Corrupt-files item has a right-aligned pill badge (mono 10/700, color `--dg`, bg `--dgd`, padding `1px 7px`) showing the live corrupt count.
- **Status card** (bottom, margin-top auto): inset card (`--bg2`, border `--bd`, radius 11px) with a status dot + word ("Scanning"/"Idle") and mono meta: "6 workers · nice(10)" / "read-only mount · ro".

### Header
Flex space-between. Left: screen title 18/700. Right: a status pill (`--bg2`, border `--bd`, radius 999px, padding `6px 13px`) with status dot + text ("Scanning · 62%" or "Idle"), and a **Light/Dark toggle** button (transparent, border `--bd`, `--txm`, padding `7px 13px`, radius 8px) labeled with the *other* theme's name.

**Status dot**: 8px circle. Idle → `--ac` + green glow, no animation. Scanning → `--wn` (amber) + amber glow + `bc-pulse` animation.

---

## Screens / Views

### 1. Dashboard
**Purpose:** at-a-glance library health + start/monitor scans.
**Layout (max-width 1080):**
- **Stat grid**: `repeat(auto-fill, minmax(168px, 1fr))`, gap `--gap`. Six cards (`--bg1`, border `--bd`, radius 14, padding `--cardpad`), each = mono uppercase eyebrow + big value:
  - Status (dot + "Idle"/"Scanning"), Mode ("Report"), Library ("98,432" + mono "files · 2.8 TB"), Corrupt (value in `--dg` + "flagged · 142.5 MB"), Workers ("6"), Uptime ("3d 4h").
- **Scan hero** — conditionally shows one of:
  - **Scanning state** (`--bg1`, border `--bd`, radius 16, padding `20px 22px`): top row = "Decoding library" 13/700 + mono current-file path (`--txd`, truncated) on the left; big "62%" in `--ac` 30/800 + mono "61,030 / 98,432" on the right. **Spectrogram** (height 118px, flex row of ~150 bars, `align-items:flex-end`, gap 2px): bars flex `1 1 0`, height 8–98%, radius 2px; color from the spectrum ramp by height; **scanned region** (left 62%) full opacity, unscanned region opacity .15; corrupt bars `#ff4d6d` + glow and taller. An accent gradient wash fills the scanned region behind bars (`linear-gradient(90deg, --acd, transparent)`). A 2px **playhead** at left:62% in `--ac`, glowing, `bc-sweep` animation, with a dot head. Footer stat row: "61,030 / 98,432 checked", "N corrupt found" (`--dg`), "ETA 1h 24m" (`--ac`), "6 workers active".
  - **Idle state**: 52px rounded accent-dim tile with a `✓`, "Scanner idle" 17/700 + meta "Last scan 2h ago · 98,432 files · N flagged", and a right-aligned **Start scan** accent button.
- **Action row**: primary "Scan — Report" (accent), secondary "Scan — Move" (`--bg2`, border), "Cancel scan" (danger, only while scanning), and a "Fresh scan (ignore resume cache)" checkbox (accent-color `--ac`).
- **Recently flagged** list (`--bg1`, border, radius 14): rows with a glowing 6px `--dg` dot, mono filename, `--dg` error line, mono size.

### 2. Corrupt Files
**Purpose:** review and remove flagged files.
**Layout (max-width 1080):**
- **Toolbar**: search input (flex-1, `--bg1`), "Group by album" / "Flat view" toggle (`--bg2`), "Delete selected (N)" (danger; disabled at 40% opacity + `not-allowed` when nothing selected), "Clear" (ghost).
- **Table** (`--bg1`, border, radius 14, overflow hidden). Column grid: `34px 1fr 230px 84px 74px` (checkbox · File · Error · Size · action). Mono uppercase header row (9.5, letter-spacing .08em).
  - **Flat view**: one row per file — checkbox (accent-color), file block (mono name + `--txd` folder path, both truncated), error in `--dg` (truncated), mono right-aligned size, and a per-row outline "Delete" button (`--dg` text, border `--bd`, radius 7). **Selected rows** get `--acd` background.
  - **Album view**: group header rows (`--bg2`, bold album name + mono `--dg` count like "2 files"), each followed by its file rows (same row component).
  - Empty state: centered "No corrupt files — library is clean." (`--txd`).

### 3. Configuration
**Purpose:** edit `beatscheck.conf` (applies next scan).
**Layout (max-width 840):** intro line referencing `beatscheck.conf` in a mono code chip. Four sections, each: section title 14/700 in `--ac`, help line `--txm` 12, then a card (`--bg1`, border, radius 14) of field rows. **Field row** grid `210px 1fr`, padding `14px 18px`, bottom border `--bd`: left = mono key (12.5/600) + `--txd` 10.5 description; right (justify-end) = the control. Controls:
  - **Switch**: 40×22 pill track (`--ac` when on, else `--bg2` + border `--bds`), 16px white knob sliding left 2px→20px, `.2s`.
  - **Select**: `--bg2`, border `--bd`, radius 8, mono 12.5, min-width 210.
  - **Text/number**: same styling as select.
  Sections & fields: **Scanning** (mode select [setup/report/delete/move], workers, run_interval, min_file_age, log_level select), **Deletion & quarantine** (delete_after, max_auto_delete, output_dir), **Lidarr integration** (lidarr_url, lidarr_api_key, lidarr_search switch, lidarr_blocklist switch), **Web interface** (webui switch, webui_port). Footer: "Save configuration" accent button + "Last saved 2h ago".

### 4. Logs
**Purpose:** live tail of scanner output.
**Layout (full width):** toolbar = "Auto-scroll" checkbox, level filter `<select>` (All/Error/Warning/Info/Debug), search input (flex-1), "Copy" + "Download" ghost buttons. **Terminal**: bg `#08090c`, border `--bd`, radius 14, padding `14px 16px`, mono 12 / line-height 1.75, max-height 64vh, scroll. Each line: mono timestamp (`#5a5d68`) + fixed-width level chip (min-width 60, centered, radius 5, colors per table above) + message (color by level).

### 5. Login
Centered 368px card (`--bg1`, border, radius 18, padding `32px 30px`). Logo chip (46px, same construction as sidebar), "Welcome back" 19/800, sub "Sign in to BeatsCheck". Username + password fields (labels 12/600 `--txm`; inputs `--bg2`, border, radius 10, padding `11px 13px`, 14px). Full-width accent "Log in" button. Mono `--txd` footer hint: "Forgot your password? / docker exec beatscheck reset-webui-password".

### 6. Setup wizard
Centered 380px card, same construction. "Create your credentials" 19/800 + first-run sub-copy. Username, password, confirm-password fields. Full-width accent "Create account" button.

---

## Interactions & Behavior
- **Nav**: clicking a sidebar item switches the active screen (client-side state, no reload). Active styling per above. In the real app these map to the existing routes/tabs.
- **Theme toggle** (header): flips dark/light; all colors are CSS-variable driven, so swap the token set on a root element (e.g. `data-theme` attr or inline `--*` vars). Persist the choice (localStorage) in production.
- **Start scan / Cancel scan**: toggles the scanning state, which drives the status dot (idle green ↔ amber pulse), header status text, and the dashboard hero (idle card ↔ live spectrogram). Wire to the backend scan start/stop endpoints.
- **Corrupt search**: filters rows live by filename + folder + error text (case-insensitive substring).
- **Group by album toggle**: switches flat ↔ album-grouped rendering of the same filtered set.
- **Row checkbox / select**: toggles selection; selected rows tint `--acd`; "Delete selected (N)" reflects count and enables/disables. "Clear" empties selection.
- **Delete (row or bulk)**: removes file(s) from the list and drops them from selection; corrupt count + sidebar badge update. Wire to the real delete/quarantine endpoint with a confirm step.
- **Logs**: level `<select>` filters by exact level; search filters message text; both compose.
- **Transitions**: status dot `bc-pulse` 1.4s; scan playhead `bc-sweep` 1.6s; row/selection/switch `.15–.2s`. (An optional page-enter fade `bc-page` exists in the prototype but was disabled — reintroduce only if desired.)
- **Responsive**: stat grid auto-fills; toolbars wrap (`flex-wrap`). The 238px sidebar is fixed — for narrow/mobile, collapse it to a drawer (not built in the prototype).

## State Management
- `screen`: which view is active (`dashboard|corrupt|config|logs|login|setup`).
- `theme`: `dark|light`.
- `scanning`: boolean → drives status dot/text + dashboard hero. In production, derive from backend status/progress polling.
- `selected`: map of file-path → boolean (corrupt-files multi-select).
- `removed`: map of file-path → boolean (locally removed; replace with real backend mutations).
- `corruptSearch`, `logSearch`, `logLevel`: filter inputs.
- Tweakable design props in the prototype (map to build-time/user settings, not runtime UI): `accent` (signal/amber/indigo), `density` (comfortable/compact), `defaultTheme`, `scanState`.

### Data the backend already provides (wire these in)
- **Status/progress**: mode, scanning flag, % complete, current file path, checked/total counts, corrupt count, ETA, worker count, uptime, library size.
- **Corrupt list**: per file → path, folder, filename, album (derive from path), human size, error/reason string.
- **Config**: the `beatscheck.conf` keys shown in §3.
- **Logs**: timestamp, level, message lines.

## Assets
- **Logo**: built in pure CSS/markup (gradient chip + spectrum bar + red dot) — no image needed. A raster `icon.png` exists in the repo if a favicon/app icon is wanted.
- **Fonts**: Hanken Grotesk + IBM Plex Mono via Google Fonts (self-host for an offline/air-gapped Docker deploy).
- **Icons**: the design uses almost none — a single `✓` glyph on the idle hero. Status/indicator dots are CSS. If you add icons, keep them minimal and monochrome to match.
- No SVG illustrations; the spectrogram is generated from data (array of bars), not an asset.

## Files
- `BeatsCheck.dc.html` — the full hi-fi prototype (all six screens, theme system, interactions). Authored in a custom HTML template runtime; **read it for exact values, don't ship it**. The template markup is the visual source of truth; the `class Component` logic block holds the sample data, the spectrogram bar generator (`buildBars`), config schema (`configData`), and log fixtures (`logData`).
- `support.js` — prototype runtime only. **Do not port.**
- `screenshots/` — reference renders (if included).
- `icon.png` — original app icon from the repo.
