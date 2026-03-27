from __future__ import annotations

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse

from .config import load_settings
from .db import (
    bootstrap_schema,
    cleanup_old_logs,
    get_dashboard_metrics,
    get_data_quality_summary,
    get_logs,
    get_run_history,
    get_run_trends,
    get_service_health,
    get_top_errors,
)

app = FastAPI(title="Simple ETL Dashboard", version="1.0.0")


@app.on_event("startup")
def on_startup() -> None:
    settings = load_settings()
    bootstrap_schema(settings.dsn)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/metrics")
def api_metrics(hours: int = Query(default=24, ge=1, le=168)) -> dict[str, object]:
    settings = load_settings()
    return get_dashboard_metrics(settings.dsn, hours=hours)


@app.get("/api/run-history")
def api_run_history(limit: int = Query(default=50, ge=1, le=200)) -> list[dict[str, object]]:
    settings = load_settings()
    return get_run_history(settings.dsn, limit=limit)


@app.get("/api/data-quality")
def api_data_quality(hours: int = Query(default=24, ge=1, le=168)) -> dict[str, object]:
    settings = load_settings()
    return get_data_quality_summary(settings.dsn, hours=hours)


@app.get("/api/trends")
def api_trends(hours: int = Query(default=24, ge=1, le=168)) -> list[dict[str, object]]:
    settings = load_settings()
    return get_run_trends(settings.dsn, hours=hours)


@app.get("/api/top-errors")
def api_top_errors(limit: int = Query(default=10, ge=1, le=50)) -> list[dict[str, object]]:
    settings = load_settings()
    return get_top_errors(settings.dsn, limit=limit)


@app.get("/api/health")
def api_health() -> dict[str, object]:
    settings = load_settings()
    return get_service_health(settings.dsn)


@app.get("/api/logs")
def api_logs(
    level: str | None = Query(default=None),
    search: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
) -> list[dict[str, object]]:
    settings = load_settings()
    return get_logs(settings.dsn, level=level, search=search, limit=limit)


@app.get("/", response_class=HTMLResponse)
def dashboard_home() -> str:
    settings = load_settings()
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Blue Tide ETL Console</title>
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
  <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=Sora:wght@600;700&display=swap" rel="stylesheet" />
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet" />
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.css" />
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
  <style>
    :root {{
      --sea-050: #f2fbff;
      --sea-100: #e5f7ff;
      --sea-200: #cdeefe;
      --sea-300: #a8ddf7;
      --sea-400: #79c6ef;
      --sea-500: #5bc0eb;
      --sea-600: #3fa9db;
      --sea-700: #2e88bf;
      --sea-800: #1f6e8c;
      --ink-900: #173042;
      --ink-700: #36566b;
      --ink-500: #5f7d8a;
      --line: #d8ecf6;
      --surface: #f4fbff;
      --card: #ffffff;
      --ok: #1fa971;
      --warn: #f2b544;
      --danger: #e05757;
      --radius-lg: 18px;
      --radius-md: 14px;
      --shadow-soft: 0 12px 32px rgba(31, 110, 140, 0.10);
      --shadow-card: 0 8px 20px rgba(23, 48, 66, 0.08);
    }}

    * {{ box-sizing: border-box; }}

    body {{
      margin: 0;
      color: var(--ink-900);
      background:
        radial-gradient(circle at 8% 0%, #dff5ff 0%, transparent 40%),
        radial-gradient(circle at 96% 10%, #d9f0fb 0%, transparent 32%),
        linear-gradient(180deg, #f9feff 0%, var(--surface) 100%);
      font-family: "Plus Jakarta Sans", "Segoe UI", sans-serif;
      min-height: 100vh;
    }}

    .app-shell {{
      display: grid;
      grid-template-columns: 260px 1fr;
      gap: 20px;
      min-height: 100vh;
      padding: 20px;
    }}

    .sidebar {{
      background: linear-gradient(165deg, #ffffff 0%, #f1fbff 100%);
      border: 1px solid var(--line);
      border-radius: var(--radius-lg);
      box-shadow: var(--shadow-soft);
      padding: 22px 16px;
      position: sticky;
      top: 20px;
      height: calc(100vh - 40px);
      overflow: auto;
    }}

    .brand-title {{
      font-family: "Sora", sans-serif;
      font-size: 1.05rem;
      font-weight: 700;
      color: var(--sea-800);
      letter-spacing: 0.02em;
    }}

    .brand-sub {{
      font-size: 0.78rem;
      color: var(--ink-500);
      margin-top: 2px;
      margin-bottom: 18px;
    }}

    .nav-link.cool-tab {{
      border: 1px solid transparent;
      border-radius: 12px;
      color: var(--ink-700);
      font-weight: 600;
      margin-bottom: 8px;
      padding: 10px 12px;
      transition: all 0.2s ease;
      display: flex;
      align-items: center;
      gap: 10px;
    }}

    .nav-link.cool-tab:hover {{
      background: var(--sea-100);
      border-color: var(--sea-200);
      color: var(--sea-800);
    }}

    .nav-link.cool-tab.active {{
      background: linear-gradient(130deg, var(--sea-500), var(--sea-700));
      color: #fff;
      box-shadow: 0 10px 18px rgba(63, 169, 219, 0.28);
    }}

    .sidebar-hint {{
      margin-top: 18px;
      padding: 12px;
      border-radius: 12px;
      background: var(--sea-050);
      border: 1px dashed var(--sea-300);
      color: var(--ink-500);
      font-size: 0.78rem;
      line-height: 1.45;
    }}

    .main-panel {{
      display: flex;
      flex-direction: column;
      gap: 18px;
      min-width: 0;
    }}

    .topbar {{
      background: rgba(255, 255, 255, 0.92);
      border: 1px solid var(--line);
      border-radius: var(--radius-lg);
      box-shadow: var(--shadow-card);
      padding: 14px 18px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 14px;
      backdrop-filter: blur(8px);
    }}

    .topbar-title {{
      font-family: "Sora", sans-serif;
      font-size: 1.28rem;
      margin: 0;
    }}

    .topbar-sub {{
      color: var(--ink-500);
      margin: 0;
      font-size: 0.86rem;
    }}

    .status-chip {{
      border-radius: 999px;
      padding: 6px 12px;
      font-size: 0.76rem;
      font-weight: 700;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      border: 1px solid transparent;
      display: inline-flex;
      align-items: center;
      gap: 6px;
    }}

    .chip-sea {{
      background: var(--sea-100);
      border-color: var(--sea-200);
      color: var(--sea-800);
    }}

    .chip-soft {{
      background: #f6fbff;
      border-color: var(--line);
      color: var(--ink-700);
    }}

    .refresh-btn {{
      border-radius: 999px;
      border: 1px solid var(--sea-300);
      background: #fff;
      color: var(--sea-800);
      font-weight: 700;
      font-size: 0.82rem;
      padding: 8px 14px;
    }}

    .refresh-btn:hover {{
      background: var(--sea-100);
    }}

    .surface {{
      background: #fff;
      border: 1px solid var(--line);
      border-radius: var(--radius-lg);
      box-shadow: var(--shadow-card);
      padding: 16px;
    }}

    .surface-title {{
      font-size: 1rem;
      font-weight: 700;
      margin-bottom: 12px;
      color: var(--ink-900);
      display: flex;
      align-items: center;
      gap: 8px;
    }}

    .kpi-grid {{
      display: grid;
      grid-template-columns: repeat(12, 1fr);
      gap: 12px;
      margin-bottom: 14px;
    }}

    .kpi-card {{
      background: linear-gradient(160deg, #ffffff 0%, #f2fbff 100%);
      border: 1px solid var(--line);
      border-radius: var(--radius-md);
      padding: 14px;
      min-height: 116px;
      position: relative;
      overflow: hidden;
    }}

    .kpi-card.hero {{
      background: linear-gradient(145deg, var(--sea-700), var(--sea-500));
      border: none;
      color: #fff;
      box-shadow: 0 14px 24px rgba(63, 169, 219, 0.32);
    }}

    .kpi-2x {{ grid-column: span 3; }}
    .kpi-1x {{ grid-column: span 2; }}

    .kpi-label {{
      font-size: 0.78rem;
      font-weight: 700;
      letter-spacing: 0.05em;
      text-transform: uppercase;
      opacity: 0.86;
      margin-bottom: 4px;
    }}

    .kpi-value {{
      font-size: 1.95rem;
      font-weight: 800;
      line-height: 1;
      margin-bottom: 6px;
    }}

    .kpi-note {{
      font-size: 0.78rem;
      color: var(--ink-500);
    }}

    .kpi-card.hero .kpi-note {{ color: rgba(255, 255, 255, 0.84); }}

    .spark {{
      position: absolute;
      right: 6px;
      bottom: 6px;
      width: 66px;
      height: 28px;
      opacity: 0.22;
      border-radius: 999px;
      background: linear-gradient(90deg, transparent, rgba(255, 255, 255, 0.9));
    }}

    .matrix {{
      display: grid;
      grid-template-columns: 1.15fr 0.85fr;
      gap: 14px;
    }}

    .table-sleek th {{
      font-size: 0.75rem;
      letter-spacing: 0.04em;
      color: var(--ink-500);
      text-transform: uppercase;
      border-bottom: 1px solid var(--line);
      background: #f8fdff;
      position: sticky;
      top: 0;
      z-index: 1;
    }}

    .table-sleek td {{
      font-size: 0.85rem;
      color: var(--ink-700);
      border-bottom: 1px solid #edf6fb;
      vertical-align: middle;
    }}

    .table-sleek tbody tr:hover {{ background: #f5fbff; }}

    .badge-soft {{
      border-radius: 999px;
      padding: 4px 8px;
      font-size: 0.72rem;
      font-weight: 700;
    }}

    .b-ok {{ background: rgba(31, 169, 113, 0.14); color: #128a5a; }}
    .b-warn {{ background: rgba(242, 181, 68, 0.20); color: #aa740d; }}
    .b-danger {{ background: rgba(224, 87, 87, 0.16); color: #b73d3d; }}
    .b-info {{ background: rgba(63, 169, 219, 0.14); color: #1f6e8c; }}

    .chart-wrap {{
      height: 320px;
      border-radius: 14px;
      border: 1px solid var(--line);
      padding: 10px;
      background: linear-gradient(180deg, #ffffff 0%, #f6fcff 100%);
    }}

    .log-console {{
      background: #092235;
      color: #bfe9ff;
      border-radius: 12px;
      border: 1px solid #114160;
      min-height: 420px;
      max-height: 520px;
      overflow: auto;
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      font-size: 0.79rem;
    }}

    .log-row {{
      display: grid;
      grid-template-columns: 150px 82px 170px 1fr;
      gap: 10px;
      padding: 8px 10px;
      border-bottom: 1px solid rgba(191, 233, 255, 0.09);
    }}

    .log-row:hover {{ background: rgba(123, 205, 239, 0.08); }}
    .log-level {{ font-weight: 700; }}
    .log-level.INFO {{ color: #79c6ef; }}
    .log-level.WARNING {{ color: #ffd175; }}
    .log-level.ERROR, .log-level.EXCEPTION {{ color: #ff9f9f; }}

    .stage {{ animation: rise 0.42s ease both; }}

    @keyframes rise {{
      from {{ opacity: 0; transform: translateY(6px); }}
      to {{ opacity: 1; transform: translateY(0); }}
    }}

    .mobile-tabs {{
      display: none;
      position: fixed;
      bottom: 10px;
      left: 12px;
      right: 12px;
      background: rgba(255, 255, 255, 0.96);
      border: 1px solid var(--line);
      border-radius: 999px;
      box-shadow: var(--shadow-card);
      padding: 6px;
      z-index: 20;
    }}

    .mobile-tabs .nav-link {{
      border-radius: 999px;
      color: var(--ink-500);
      font-size: 0.8rem;
      padding: 8px 10px;
    }}

    .mobile-tabs .nav-link.active {{
      background: var(--sea-600);
      color: #fff;
    }}

    .empty {{
      padding: 18px;
      border-radius: 12px;
      border: 1px dashed var(--sea-300);
      background: var(--sea-050);
      text-align: center;
      color: var(--ink-500);
      font-size: 0.85rem;
    }}

    @media (max-width: 1280px) {{
      .kpi-2x {{ grid-column: span 4; }}
      .kpi-1x {{ grid-column: span 4; }}
    }}

    @media (max-width: 992px) {{
      .app-shell {{ grid-template-columns: 1fr; padding-bottom: 88px; }}
      .sidebar {{ display: none; }}
      .mobile-tabs {{ display: block; }}
      .matrix {{ grid-template-columns: 1fr; }}
      .kpi-grid {{ grid-template-columns: repeat(6, 1fr); }}
      .kpi-2x, .kpi-1x {{ grid-column: span 3; }}
      .topbar {{ flex-direction: column; align-items: flex-start; }}
    }}

    @media (max-width: 650px) {{
      .kpi-grid {{ grid-template-columns: repeat(2, 1fr); }}
      .kpi-2x, .kpi-1x {{ grid-column: span 2; }}
      .log-row {{ grid-template-columns: 1fr; gap: 2px; }}
    }}
  </style>
</head>
<body>
  <div class="app-shell">
    <aside class="sidebar stage">
      <div class="brand-title"><i class="bi bi-water"></i> Monitoring</div>
      <div class="brand-sub">Dashboard for ETL pipeline</div>
      <div class="nav flex-column nav-pills" role="tablist">
        <button class="nav-link cool-tab active" data-bs-toggle="pill" data-bs-target="#overview" type="button"><i class="bi bi-grid-1x2-fill"></i> Overview</button>
        <button class="nav-link cool-tab" data-bs-toggle="pill" data-bs-target="#trends" type="button"><i class="bi bi-graph-up-arrow"></i> Trends</button>
        <button class="nav-link cool-tab" data-bs-toggle="pill" data-bs-target="#health" type="button"><i class="bi bi-heart-pulse-fill"></i> Health</button>
        <button class="nav-link cool-tab" data-bs-toggle="pill" data-bs-target="#logs" type="button"><i class="bi bi-terminal-fill"></i> Logs</button>
      </div>
      <div class="sidebar-hint">
        <strong>Hint</strong><br />
        Logs disimpan maksimal 2 hari dan otomatis dibersihkan periodik agar storage tetap efisien.
      </div>
    </aside>

    <main class="main-panel">
      <section class="topbar stage">
        <div>
          <h1 class="topbar-title">Simple ETL Dashboard for monitoring systems</h1>
          <p class="topbar-sub">Pipeline observability, data quality, and realtime operations in one place.</p>
        </div>
        <div class="d-flex align-items-center gap-2 flex-wrap">
          <span class="status-chip chip-sea" id="chipEnvironment"><i class="bi bi-server"></i> Internal</span>
          <span class="status-chip chip-soft" id="lastUpdate"><i class="bi bi-arrow-repeat"></i> Updated: -</span>
          <button class="refresh-btn" id="refreshNow"><i class="bi bi-arrow-clockwise"></i> Refresh</button>
        </div>
      </section>

      <section class="surface stage">
        <div class="tab-content">
          <div class="tab-pane fade show active" id="overview" role="tabpanel">
            <div class="kpi-grid" id="kpiGrid">
              <article class="kpi-card hero kpi-2x"><div class="kpi-label">Succeeded Runs</div><div class="kpi-value" id="kpi-success">-</div><div class="kpi-note">last 24h</div><div class="spark"></div></article>
              <article class="kpi-card hero kpi-2x"><div class="kpi-label">Failed Runs</div><div class="kpi-value" id="kpi-failed">-</div><div class="kpi-note">last 24h</div><div class="spark"></div></article>
              <article class="kpi-card kpi-1x"><div class="kpi-label">Avg Duration</div><div class="kpi-value" id="kpi-duration">-</div><div class="kpi-note">milliseconds</div></article>
              <article class="kpi-card kpi-1x"><div class="kpi-label">Records Loaded</div><div class="kpi-value" id="kpi-loaded">-</div><div class="kpi-note">last 24h</div></article>
              <article class="kpi-card kpi-1x"><div class="kpi-label">Latest Data</div><div class="kpi-value" id="kpi-latest" style="font-size:1.15rem">-</div><div class="kpi-note">observed_at</div></article>
              <article class="kpi-card kpi-1x"><div class="kpi-label">Freshness</div><div class="kpi-value" id="kpi-freshness">-</div><div class="kpi-note">hours behind</div></article>
            </div>

            <div class="matrix">
              <div class="surface" style="padding:0;">
                <div class="surface-title px-3 pt-3"><i class="bi bi-clock-history"></i> Recent Run History</div>
                <div class="table-responsive" style="max-height:420px;">
                  <table class="table table-sleek mb-0">
                    <thead><tr><th>Started</th><th>Status</th><th>Transformed</th><th>Loaded</th><th>Duration</th><th>Error</th></tr></thead>
                    <tbody id="runsBody"><tr><td colspan="6"><div class="empty">Loading run history...</div></td></tr></tbody>
                  </table>
                </div>
              </div>

              <div class="d-flex flex-column gap-3">
                <div class="surface" style="padding:0;">
                  <div class="surface-title px-3 pt-3"><i class="bi bi-shield-check"></i> Data Quality Summary</div>
                  <div class="table-responsive" style="max-height:200px;">
                    <table class="table table-sleek mb-0">
                      <thead><tr><th>Check</th><th>Pass</th><th>Warn</th><th>Fail</th></tr></thead>
                      <tbody id="dqBody"><tr><td colspan="4"><div class="empty">Loading quality checks...</div></td></tr></tbody>
                    </table>
                  </div>
                </div>

                <div class="surface" style="padding:0;">
                  <div class="surface-title px-3 pt-3"><i class="bi bi-exclamation-diamond"></i> Latest Issues</div>
                  <div class="table-responsive" style="max-height:200px;">
                    <table class="table table-sleek mb-0">
                      <thead><tr><th>Time</th><th>Check</th><th>Severity</th><th>Details</th></tr></thead>
                      <tbody id="issuesBody"><tr><td colspan="4"><div class="empty">Loading issues...</div></td></tr></tbody>
                    </table>
                  </div>
                </div>
              </div>
            </div>
          </div>

          <div class="tab-pane fade" id="trends" role="tabpanel">
            <div class="row g-3">
              <div class="col-12 col-xl-4">
                <div class="surface">
                  <div class="surface-title"><i class="bi bi-stopwatch"></i> Run Duration Trend</div>
                  <div class="chart-wrap"><canvas id="durationChart"></canvas></div>
                </div>
              </div>
              <div class="col-12 col-xl-4">
                <div class="surface">
                  <div class="surface-title"><i class="bi bi-box-seam"></i> Data Volume Trend</div>
                  <div class="chart-wrap"><canvas id="volumeChart"></canvas></div>
                </div>
              </div>
              <div class="col-12 col-xl-4">
                <div class="surface">
                  <div class="surface-title"><i class="bi bi-percent"></i> Success Rate Timeline</div>
                  <div class="chart-wrap"><canvas id="successChart"></canvas></div>
                </div>
              </div>
            </div>
          </div>

          <div class="tab-pane fade" id="health" role="tabpanel">
            <div class="row g-3">
              <div class="col-12 col-lg-5">
                <div class="surface">
                  <div class="surface-title"><i class="bi bi-heart-pulse"></i> Service Health</div>
                  <div id="healthStatus"><div class="empty">Loading health data...</div></div>
                </div>
              </div>
              <div class="col-12 col-lg-7">
                <div class="surface" style="padding:0;">
                  <div class="surface-title px-3 pt-3"><i class="bi bi-exclamation-triangle"></i> Top Errors</div>
                  <div class="table-responsive" style="max-height:420px;">
                    <table class="table table-sleek mb-0">
                      <thead><tr><th>Error Message</th><th>Count</th><th>Last Occurred</th></tr></thead>
                      <tbody id="topErrorsBody"><tr><td colspan="3"><div class="empty">Loading top errors...</div></td></tr></tbody>
                    </table>
                  </div>
                </div>
              </div>
            </div>
          </div>

          <div class="tab-pane fade" id="logs" role="tabpanel">
            <div class="surface">
              <div class="d-flex flex-wrap align-items-end gap-2 mb-3">
                <div>
                  <label class="form-label small mb-1">Level</label>
                  <select class="form-select form-select-sm" id="logLevelFilter" style="min-width: 140px;">
                    <option value="">All</option><option value="INFO">INFO</option><option value="WARNING">WARNING</option><option value="ERROR">ERROR</option><option value="EXCEPTION">EXCEPTION</option>
                  </select>
                </div>
                <div class="flex-grow-1">
                  <label class="form-label small mb-1">Search</label>
                  <input type="text" class="form-control form-control-sm" id="logSearchBox" placeholder="Search by message, context, run id..." />
                </div>
                <button class="refresh-btn" id="searchLogs"><i class="bi bi-search"></i> Apply</button>
              </div>
              <div class="log-console" id="logsBody">
                <div class="empty" style="margin:10px;">Loading logs...</div>
              </div>
            </div>
          </div>
        </div>
      </section>
    </main>
  </div>

  <div class="mobile-tabs nav nav-pills" role="tablist">
    <button class="nav-link active" data-bs-toggle="pill" data-bs-target="#overview" type="button">Overview</button>
    <button class="nav-link" data-bs-toggle="pill" data-bs-target="#trends" type="button">Trends</button>
    <button class="nav-link" data-bs-toggle="pill" data-bs-target="#health" type="button">Health</button>
    <button class="nav-link" data-bs-toggle="pill" data-bs-target="#logs" type="button">Logs</button>
  </div>

  <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>
  <script>
    const chartRegistry = {{}};
    const scheduleIntervalHours = {max(settings.schedule_interval_hours, 1)};
    let refreshCountdown = 30;

    function fmt(value) {{
      if (value === null || value === undefined || value === "") return "-";
      if (typeof value === "string" && value.includes("T")) {{
        return new Date(value).toLocaleString("en-US", {{
          month: "short", day: "2-digit", hour: "2-digit", minute: "2-digit"
        }});
      }}
      return String(value);
    }}

    function levelBadge(level) {{
      if (level === "ERROR" || level === "EXCEPTION") return "b-danger";
      if (level === "WARNING") return "b-warn";
      if (level === "INFO") return "b-info";
      return "b-info";
    }}

    function severityBadge(level) {{
      if (level === "critical") return "b-danger";
      if (level === "warning") return "b-warn";
      return "b-info";
    }}

    function setLastUpdate() {{
      document.getElementById("lastUpdate").innerHTML = `<i class=\"bi bi-arrow-repeat\"></i> Updated: ${{new Date().toLocaleTimeString()}}`;
    }}

    function buildRunRows(runs) {{
      if (!runs || runs.length === 0) {{
        return `<tr><td colspan=\"6\"><div class=\"empty\">No runs found.</div></td></tr>`;
      }}
      return runs.map((run) => `
        <tr>
          <td>${{fmt(run.started_at)}}</td>
          <td><span class="badge-soft ${{run.status === "success" ? "b-ok" : "b-danger"}}">${{run.status}}</span></td>
          <td>${{run.records_transformed ?? "-"}}</td>
          <td>${{run.records_loaded ?? "-"}}</td>
          <td>${{run.duration_ms ?? "-"}} ms</td>
          <td title="${{run.error_message || ""}}">${{(run.error_message || "-").slice(0, 46)}}</td>
        </tr>
      `).join("");
    }}

    function buildQualityRows(checks) {{
      const entries = Object.entries(checks || {{}});
      if (entries.length === 0) {{
        return `<tr><td colspan=\"4\"><div class=\"empty\">No quality checks yet.</div></td></tr>`;
      }}
      return entries.map(([name, stat]) => `
        <tr>
          <td>${{name}}</td>
          <td><span class="badge-soft b-ok">${{stat.pass || 0}}</span></td>
          <td><span class="badge-soft b-warn">${{stat.warn || 0}}</span></td>
          <td><span class="badge-soft b-danger">${{stat.fail || 0}}</span></td>
        </tr>
      `).join("");
    }}

    function buildIssueRows(issues) {{
      if (!issues || issues.length === 0) {{
        return `<tr><td colspan=\"4\"><div class=\"empty\">No active issues.</div></td></tr>`;
      }}
      return issues.map((it) => `
        <tr>
          <td>${{fmt(it.started_at)}}</td>
          <td>${{it.check_name}}</td>
          <td><span class="badge-soft ${{severityBadge(it.severity)}}">${{it.severity}}</span></td>
          <td title='${{JSON.stringify(it.details || {{}})}}'>${{JSON.stringify(it.details || {{}}).slice(0, 44)}}</td>
        </tr>
      `).join("");
    }}

    function initChart(elemId, type, labels, datasets, yMax = null) {{
      const ctx = document.getElementById(elemId);
      if (!ctx) return;
      if (chartRegistry[elemId]) chartRegistry[elemId].destroy();
      chartRegistry[elemId] = new Chart(ctx, {{
        type,
        data: {{ labels, datasets }},
        options: {{
          maintainAspectRatio: false,
          responsive: true,
          interaction: {{ intersect: false, mode: "index" }},
          plugins: {{ legend: {{ position: "bottom", labels: {{ boxWidth: 10 }} }} }},
          scales: {{
            x: {{ grid: {{ color: "rgba(31, 110, 140, 0.08)" }} }},
            y: {{
              beginAtZero: true,
              suggestedMax: yMax,
              grid: {{ color: "rgba(31, 110, 140, 0.08)" }}
            }}
          }}
        }}
      }});
    }}

    function renderHealthCard(health) {{
      const nextRunLabel = health.last_run_at
        ? fmt(new Date(new Date(health.last_run_at).getTime() + scheduleIntervalHours * 3600 * 1000).toISOString())
        : "-";
      const chip = health.db_healthy
        ? `<span class=\"status-chip chip-sea\"><i class=\"bi bi-check-circle\"></i> DB Healthy</span>`
        : `<span class=\"status-chip\" style=\"background:#ffe9e9;color:#8f2f2f;border-color:#f6c8c8;\"><i class=\"bi bi-x-circle\"></i> DB Down</span>`;

      return `
        <div class="d-grid gap-2">
          <div class="d-flex align-items-center justify-content-between">${{chip}}<span class="status-chip chip-soft">Next Run: ${{nextRunLabel}}</span></div>
          <div class="row g-2">
            <div class="col-sm-6"><div class="p-2 rounded border" style="border-color:var(--line)!important;"><small class="text-secondary">Last Run</small><div class="fw-semibold">${{fmt(health.last_run_at)}}</div></div></div>
            <div class="col-sm-6"><div class="p-2 rounded border" style="border-color:var(--line)!important;"><small class="text-secondary">Last Success</small><div class="fw-semibold">${{fmt(health.last_success_at)}}</div></div></div>
            <div class="col-sm-6"><div class="p-2 rounded border" style="border-color:var(--line)!important;"><small class="text-secondary">Freshness</small><div class="fw-semibold">${{health.data_freshness_hours ?? "-"}} hours</div></div></div>
            <div class="col-sm-6"><div class="p-2 rounded border" style="border-color:var(--line)!important;"><small class="text-secondary">Loaded (24h)</small><div class="fw-semibold">${{health.records_loaded_24h}}</div></div></div>
          </div>
        </div>
      `;
    }}

    function renderTopErrors(errors) {{
      if (!errors || errors.length === 0) {{
        return `<tr><td colspan=\"3\"><div class=\"empty\">No recurring errors.</div></td></tr>`;
      }}
      return errors.map((er) => `
        <tr>
          <td title="${{er.error_message || ""}}">${{(er.error_message || "N/A").slice(0, 88)}}</td>
          <td><span class="badge-soft b-danger">${{er.count}}</span></td>
          <td>${{fmt(er.last_occurred)}}</td>
        </tr>
      `).join("");
    }}

    function renderLogs(logs) {{
      if (!logs || logs.length === 0) {{
        return `<div class=\"empty\" style=\"margin:10px;background:#0f2b41;border-color:#2a5d7d;color:#8dc7e1;\">No logs for selected filter.</div>`;
      }}
      return logs.map((log) => `
        <div class="log-row">
          <div>${{fmt(log.created_at)}}</div>
          <div class="log-level ${{log.level}}">${{log.level}}</div>
          <div>${{log.logger || "-"}}</div>
          <div title="${{(log.message || "").replace(/\"/g, '&quot;')}}">${{(log.message || "-").slice(0, 280)}}</div>
        </div>
      `).join("");
    }}

    async function loadOverview() {{
      const [metrics, runs, dq, health] = await Promise.all([
        fetch("/api/metrics?hours=24").then((r) => r.json()),
        fetch("/api/run-history?limit=18").then((r) => r.json()),
        fetch("/api/data-quality?hours=24").then((r) => r.json()),
        fetch("/api/health").then((r) => r.json()),
      ]);

      document.getElementById("kpi-success").textContent = metrics.succeeded_runs ?? "-";
      document.getElementById("kpi-failed").textContent = metrics.failed_runs ?? "-";
      document.getElementById("kpi-duration").textContent = Math.round(metrics.avg_duration_ms || 0);
      document.getElementById("kpi-loaded").textContent = metrics.records_loaded ?? "-";
      document.getElementById("kpi-latest").textContent = fmt(metrics.latest_observed_at);
      document.getElementById("kpi-freshness").textContent = health.data_freshness_hours ?? "-";

      document.getElementById("runsBody").innerHTML = buildRunRows(runs);
      document.getElementById("dqBody").innerHTML = buildQualityRows(dq.checks || {{}});
      document.getElementById("issuesBody").innerHTML = buildIssueRows(dq.latest_issues || []);
    }}

    async function loadTrends() {{
      const trends = await fetch("/api/trends?hours=24").then((r) => r.json());
      if (!trends || trends.length === 0) {{
        ["durationChart", "volumeChart", "successChart"].forEach((id) => {{
          const parent = document.getElementById(id)?.parentElement;
          if (parent) parent.innerHTML = "<div class='empty'>No trend data yet.</div>";
        }});
        return;
      }}
      const labels = trends.map((t) => new Date(t.started_at).toLocaleTimeString("en-US", {{ hour: "2-digit", minute: "2-digit" }}));
      const durations = trends.map((t) => t.duration_ms || 0);
      const volumes = trends.map((t) => t.records_loaded || 0);
      const success = trends.map((t) => (t.success || 0) * 100);

      initChart("durationChart", "line", labels, [{{
        label: "Duration (ms)",
        data: durations,
        borderColor: "#3fa9db",
        backgroundColor: "rgba(63, 169, 219, 0.18)",
        fill: true,
        tension: 0.35,
      }}]);

      initChart("volumeChart", "line", labels, [{{
        label: "Records Loaded",
        data: volumes,
        borderColor: "#1f6e8c",
        backgroundColor: "rgba(31, 110, 140, 0.16)",
        fill: true,
        tension: 0.35,
      }}]);

      initChart("successChart", "line", labels, [{{
        label: "Success Rate (%)",
        data: success,
        borderColor: "#1fa971",
        backgroundColor: "rgba(31, 169, 113, 0.14)",
        fill: true,
        tension: 0.35,
      }}], 100);
    }}

    async function loadHealth() {{
      const [health, errors] = await Promise.all([
        fetch("/api/health").then((r) => r.json()),
        fetch("/api/top-errors?limit=10").then((r) => r.json()),
      ]);
      document.getElementById("healthStatus").innerHTML = renderHealthCard(health);
      document.getElementById("topErrorsBody").innerHTML = renderTopErrors(errors);
    }}

    async function loadLogs() {{
      const level = document.getElementById("logLevelFilter").value;
      const search = document.getElementById("logSearchBox").value;
      const params = new URLSearchParams();
      if (level) params.set("level", level);
      if (search) params.set("search", search);
      params.set("limit", "50");
      const logs = await fetch(`/api/logs?${{params.toString()}}`).then((r) => r.json());
      document.getElementById("logsBody").innerHTML = renderLogs(logs);
    }}

    async function loadAllData() {{
      try {{
        await Promise.all([loadOverview(), loadTrends(), loadHealth(), loadLogs()]);
      }} catch (error) {{
        console.error("dashboard refresh failed", error);
      }} finally {{
        setLastUpdate();
      }}
    }}

    function tickRefresh() {{
      refreshCountdown -= 1;
      if (refreshCountdown <= 0) {{
        refreshCountdown = 30;
        loadAllData();
      }}
    }}

    document.getElementById("refreshNow").addEventListener("click", () => {{
      refreshCountdown = 30;
      loadAllData();
    }});
    document.getElementById("searchLogs").addEventListener("click", () => loadLogs());
    document.getElementById("logLevelFilter").addEventListener("change", () => loadLogs());
    document.getElementById("logSearchBox").addEventListener("keydown", (e) => {{
      if (e.key === "Enter") loadLogs();
    }});

    loadAllData();
    setInterval(tickRefresh, 1000);
  </script>
</body>
</html>
"""