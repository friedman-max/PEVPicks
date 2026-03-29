/**
 * PrizePicks +EV Finder — Frontend
 *
 * - Polls /api/status every 10s for live countdown & scraping state
 * - Fetches /api/bets after each refresh completes
 * - Renders sortable, filterable bets table
 * - Slip builder: select 2-6 bets → POST /api/slip → show Power/Flex EV%
 */

// ── State ──────────────────────────────────────────────────────────────────
const state = {
  allBets:       [],          // raw bet objects from API
  filteredBets:  [],          // after filters applied
  selected:      new Set(),   // set of bet_ids
  sortCol:       "individual_ev_pct",
  sortDir:       "desc",
  lastBetCount:  -1,          // detect when new bets arrive
  isScrapingPrev: false,
};

// ── DOM refs ───────────────────────────────────────────────────────────────
const $ = id => document.getElementById(id);
const tbody           = $("bets-tbody");
const totalBadge      = $("total-badge");
const statusDot       = $("status-dot");
const statusLabel     = $("status-label");
const lastRefreshEl   = $("last-refresh");
const nextRefreshEl   = $("next-refresh");
const selectedCountEl = $("selected-count");
const btnRefresh      = $("btn-refresh");
const btnBuildSlip    = $("btn-build-slip");
const btnCalculate    = $("btn-calculate");
const btnClearSel     = $("btn-clear-selection");
const slipLegsEl      = $("slip-legs");
const slipResultsEl   = $("slip-results");
const bankrollInput   = $("bankroll-input");

// ── Helpers ────────────────────────────────────────────────────────────────
const fmt = {
  pct:   v => v == null ? "—" : (v >= 0 ? "+" : "") + (v * 100).toFixed(2) + "%",
  prob:  v => v == null ? "—" : (v * 100).toFixed(1) + "%",
  odds:  v => v == null ? "—" : (v > 0 ? "+" : "") + v,
  dollar:v => v == null ? "—" : (v >= 0 ? "+$" : "-$") + Math.abs(v).toFixed(2),
  time:  iso => {
    if (!iso) return "—";
    const d = new Date(iso);
    return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  },
};

function evClass(ev_pct) {
  if (ev_pct >= 0.03) return "ev-high";
  if (ev_pct >= 0.01) return "ev-medium";
  return "ev-low";
}

// ── Filters ────────────────────────────────────────────────────────────────
function applyFilters() {
  const league  = $("filter-league").value.toUpperCase();
  const prop    = $("filter-prop").value.toLowerCase().trim();
  const minEv   = parseFloat($("filter-min-ev").value) / 100 || 0;
  const side    = $("filter-side").value.toLowerCase();

  state.filteredBets = state.allBets.filter(b => {
    if (league && b.league !== league)                           return false;
    if (prop   && !b.prop_type.toLowerCase().includes(prop))    return false;
    if (b.individual_ev_pct < minEv)                            return false;
    if (side   && b.side !== side)                              return false;
    return true;
  });

  renderTable();
}

["filter-league", "filter-prop", "filter-min-ev", "filter-side"].forEach(id => {
  $(id).addEventListener("input", applyFilters);
  $(id).addEventListener("change", applyFilters);
});

$("btn-clear-filters").addEventListener("click", () => {
  $("filter-league").value = "";
  $("filter-prop").value   = "";
  $("filter-min-ev").value = "1";
  $("filter-side").value   = "";
  applyFilters();
});

// ── Sorting ────────────────────────────────────────────────────────────────
document.querySelectorAll("th.sortable").forEach(th => {
  th.addEventListener("click", () => {
    const col = th.dataset.col;
    if (state.sortCol === col) {
      state.sortDir = state.sortDir === "desc" ? "asc" : "desc";
    } else {
      state.sortCol = col;
      state.sortDir = "desc";
    }
    document.querySelectorAll("th.sortable").forEach(t => {
      t.classList.remove("active", "asc", "desc");
    });
    th.classList.add("active", state.sortDir);
    renderTable();
  });
});

function sortBets(bets) {
  return [...bets].sort((a, b) => {
    let va = a[state.sortCol] ?? "";
    let vb = b[state.sortCol] ?? "";
    if (typeof va === "string") va = va.toLowerCase();
    if (typeof vb === "string") vb = vb.toLowerCase();
    if (va < vb) return state.sortDir === "asc" ? -1 : 1;
    if (va > vb) return state.sortDir === "asc" ? 1 : -1;
    return 0;
  });
}

// ── Table rendering ────────────────────────────────────────────────────────
function renderTable() {
  const sorted = sortBets(state.filteredBets);

  if (sorted.length === 0) {
    tbody.innerHTML = `<tr id="empty-row"><td colspan="11" class="empty-msg">
      ${state.allBets.length === 0 ? 'Click "Refresh Now" to load bets.' : "No bets match current filters."}
    </td></tr>`;
    totalBadge.textContent = "0 bets";
    return;
  }

  totalBadge.textContent = `${sorted.length} bet${sorted.length !== 1 ? "s" : ""}`;

  tbody.innerHTML = sorted.map(b => {
    const checked   = state.selected.has(b.bet_id) ? "checked" : "";
    const rowClass  = state.selected.has(b.bet_id) ? "selected" : "";
    const lineDiff  = b.pp_line !== b.fd_line
      ? `<span class="line-diff"> ≠${b.fd_line}</span>` : "";

    const fdOdds = b.side === "over"
      ? fmt.odds(b.over_odds)
      : b.side === "under"
        ? fmt.odds(b.under_odds)
        : `${fmt.odds(b.over_odds)} / ${fmt.odds(b.under_odds)}`;

    return `<tr class="${rowClass}" data-id="${b.bet_id}">
      <td><input type="checkbox" class="row-chk" data-id="${b.bet_id}" ${checked} /></td>
      <td>${b.player_name}</td>
      <td>${b.league}</td>
      <td>${b.prop_type}</td>
      <td>${b.pp_line}</td>
      <td>${b.fd_line}${lineDiff}</td>
      <td class="side-${b.side}">${b.side.toUpperCase()}</td>
      <td>${fmt.prob(b.true_prob)}</td>
      <td class="${evClass(b.edge)}">${fmt.pct(b.edge)}</td>
      <td class="${evClass(b.individual_ev_pct)}">${fmt.pct(b.individual_ev_pct)}</td>
      <td style="color:var(--text-muted)">${fdOdds}</td>
    </tr>`;
  }).join("");

  // Row checkbox listeners
  tbody.querySelectorAll(".row-chk").forEach(chk => {
    chk.addEventListener("change", e => toggleBet(e.target.dataset.id, e.target.checked));
  });

  // Row click (not on checkbox)
  tbody.querySelectorAll("tr[data-id]").forEach(row => {
    row.addEventListener("click", e => {
      if (e.target.type === "checkbox") return;
      const id  = row.dataset.id;
      const chk = row.querySelector(".row-chk");
      const newVal = !state.selected.has(id);
      chk.checked = newVal;
      toggleBet(id, newVal);
    });
  });

  updateSelectionUI();
}

// ── Selection ──────────────────────────────────────────────────────────────
function toggleBet(id, selected) {
  if (selected) {
    state.selected.add(id);
  } else {
    state.selected.delete(id);
  }
  updateSelectionUI();
  renderTable(); // refresh row highlight
}

function updateSelectionUI() {
  const n = state.selected.size;
  selectedCountEl.textContent = `${n} selected`;
  const valid = n >= 2 && n <= 6;
  btnBuildSlip.disabled  = !valid;
  btnCalculate.disabled  = !valid;
}

$("chk-all").addEventListener("change", e => {
  const checked = e.target.checked;
  const visible = sortBets(state.filteredBets);
  const toToggle = checked ? visible.slice(0, 6) : visible;
  toToggle.forEach(b => {
    if (checked) state.selected.add(b.bet_id);
    else         state.selected.delete(b.bet_id);
  });
  renderTable();
});

btnClearSel.addEventListener("click", () => {
  state.selected.clear();
  renderTable();
  resetSlipPanel();
});

// ── Slip panel ─────────────────────────────────────────────────────────────
function renderSlipLegs() {
  const betMap = Object.fromEntries(state.allBets.map(b => [b.bet_id, b]));
  const selected = [...state.selected].map(id => betMap[id]).filter(Boolean);

  if (selected.length === 0) {
    slipLegsEl.innerHTML = `<p class="empty-msg">Select 2–6 bets from the table.</p>`;
    return;
  }

  slipLegsEl.innerHTML = selected.map(b => `
    <div class="slip-leg">
      <div class="slip-leg-name">${b.player_name}</div>
      <div class="slip-leg-detail">${b.prop_type} ${b.side.toUpperCase()} ${b.pp_line} · ${b.league}</div>
      <div class="slip-leg-prob">True Prob: ${fmt.prob(b.true_prob)} · EV: ${fmt.pct(b.individual_ev_pct)}</div>
    </div>
  `).join("");
}

function resetSlipPanel() {
  slipLegsEl.innerHTML = `<p class="empty-msg">Select 2–6 bets from the table.</p>`;
  slipResultsEl.classList.add("hidden");
  btnCalculate.disabled = true;
}

btnBuildSlip.addEventListener("click", () => {
  renderSlipLegs();
  slipResultsEl.classList.add("hidden");
});

btnCalculate.addEventListener("click", async () => {
  const betIds   = [...state.selected];
  const bankroll = parseFloat(bankrollInput.value) || 100;

  btnCalculate.disabled = true;
  btnCalculate.textContent = "Calculating…";

  try {
    const resp = await fetch("/api/slip", {
      method:  "POST",
      headers: { "Content-Type": "application/json" },
      body:    JSON.stringify({ bet_ids: betIds, bankroll }),
    });
    if (!resp.ok) {
      const err = await resp.json();
      alert("Slip error: " + (err.detail || resp.statusText));
      return;
    }
    const data = await resp.json();
    renderSlipResults(data);
  } catch (e) {
    alert("Network error: " + e.message);
  } finally {
    btnCalculate.disabled = false;
    btnCalculate.textContent = "Calculate EV";
  }
});

function renderSlipResults(data) {
  const powerEl  = $("power-ev");
  const flexEl   = $("flex-ev");
  const bestLabel = $("best-play-label");
  const bestEl   = $("best-play-ev");
  const profitEl = $("expected-profit");

  powerEl.textContent  = data.power_ev_pct != null ? fmt.pct(data.power_ev_pct) : "N/A";
  powerEl.className    = "ev-value " + (data.power_ev_pct >= 0 ? "ev-high" : "ev-low");

  flexEl.textContent   = data.flex_ev_pct != null ? fmt.pct(data.flex_ev_pct) : "N/A";
  flexEl.className     = "ev-value " + (data.flex_ev_pct >= 0 ? "ev-high" : "ev-low");

  bestLabel.textContent = `Best: ${data.n_picks}-Pick ${data.best_play_type || "—"}`;
  bestEl.textContent   = data.best_ev_pct != null ? fmt.pct(data.best_ev_pct) : "—";

  profitEl.textContent = data.expected_profit != null ? fmt.dollar(data.expected_profit) : "—";

  slipResultsEl.classList.remove("hidden");
}

// ── API calls ──────────────────────────────────────────────────────────────
async function fetchBets() {
  try {
    const resp = await fetch("/api/bets");
    const data = await resp.json();
    state.allBets = data.bets || [];
    applyFilters();
  } catch (e) {
    console.error("Failed to fetch bets:", e);
  }
}

async function fetchStatus() {
  try {
    const resp = await fetch("/api/status");
    const data = await resp.json();

    // Update status dot + label
    if (data.is_scraping) {
      statusDot.className   = "status-dot scraping";
      statusLabel.textContent = "Scraping…";
      btnRefresh.disabled   = true;
    } else {
      statusDot.className   = "status-dot idle";
      statusLabel.textContent = "Idle";
      btnRefresh.disabled   = false;
    }

    // Detect scraping just finished → fetch fresh bets
    if (state.isScrapingPrev && !data.is_scraping) {
      await fetchBets();
    }
    state.isScrapingPrev = data.is_scraping;

    lastRefreshEl.textContent = data.last_refresh
      ? "Updated " + fmt.time(data.last_refresh)
      : "Never refreshed";

    if (data.next_refresh && !data.is_scraping) {
      const secs = Math.max(0, Math.round((new Date(data.next_refresh) - Date.now()) / 1000));
      const mm   = String(Math.floor(secs / 60)).padStart(2, "0");
      const ss   = String(secs % 60).padStart(2, "0");
      nextRefreshEl.textContent = `Next: ${mm}:${ss}`;
    } else {
      nextRefreshEl.textContent = "";
    }

  } catch (e) {
    statusDot.className = "status-dot error";
    statusLabel.textContent = "Server error";
  }
}

btnRefresh.addEventListener("click", async () => {
  btnRefresh.disabled = true;
  try {
    await fetch("/api/refresh", { method: "POST" });
    statusDot.className     = "status-dot scraping";
    statusLabel.textContent = "Scraping…";
  } catch (e) {
    alert("Failed to start refresh: " + e.message);
    btnRefresh.disabled = false;
  }
});

// ── Settings modal ─────────────────────────────────────────────────────────
$("btn-settings").addEventListener("click", async () => {
  const resp = await fetch("/api/config");
  const cfg  = await resp.json();
  $("setting-interval").value = cfg.interval_min;
  $("setting-min-ev").value   = (cfg.min_ev_pct * 100).toFixed(1);
  document.querySelectorAll(".league-toggle").forEach(chk => {
    chk.checked = cfg.active_leagues[chk.dataset.league] !== false;
  });
  $("settings-modal").classList.remove("hidden");
});

["btn-close-settings", "btn-cancel-settings"].forEach(id => {
  $(id).addEventListener("click", () => $("settings-modal").classList.add("hidden"));
});

$("btn-save-settings").addEventListener("click", async () => {
  const leagues = {};
  document.querySelectorAll(".league-toggle").forEach(chk => {
    leagues[chk.dataset.league] = chk.checked;
  });

  await fetch("/api/config", {
    method:  "POST",
    headers: { "Content-Type": "application/json" },
    body:    JSON.stringify({
      interval_min:   parseInt($("setting-interval").value),
      min_ev_pct:     parseFloat($("setting-min-ev").value) / 100,
      active_leagues: leagues,
    }),
  });
  $("settings-modal").classList.add("hidden");
});

// ── Tab switching ─────────────────────────────────────────────────────────
document.querySelectorAll(".tab").forEach(tab => {
  tab.addEventListener("click", () => {
    document.querySelectorAll(".tab").forEach(t => t.classList.remove("active"));
    tab.classList.add("active");

    const target = tab.dataset.tab;
    if (target === "ev") {
      $("ev-view").classList.remove("hidden");
      $("ev-filters").classList.remove("hidden");
      $("pp-view").classList.add("hidden");
      $("pp-filters").classList.add("hidden");
    } else {
      $("ev-view").classList.add("hidden");
      $("ev-filters").classList.add("hidden");
      $("pp-view").classList.remove("hidden");
      $("pp-filters").classList.remove("hidden");
    }
  });
});

// ── PrizePicks Lines ──────────────────────────────────────────────────────
const ppState = {
  allLines:      [],
  filteredLines: [],
  sortCol:       "player_name",
  sortDir:       "asc",
};

const ppTbody       = $("pp-tbody");
const ppTotalBadge  = $("pp-total-badge");
const ppStatusLabel = $("pp-status-label");

function applyPPFilters() {
  const league = $("pp-filter-league").value.toUpperCase();
  const stat   = $("pp-filter-stat").value.toLowerCase().trim();
  const player = $("pp-filter-player").value.toLowerCase().trim();

  ppState.filteredLines = ppState.allLines.filter(l => {
    if (league && l.league !== league) return false;
    if (stat && !l.stat_type.toLowerCase().includes(stat)) return false;
    if (player && !l.player_name.toLowerCase().includes(player)) return false;
    return true;
  });

  renderPPTable();
}

["pp-filter-league", "pp-filter-stat", "pp-filter-player"].forEach(id => {
  $(id).addEventListener("input", applyPPFilters);
  $(id).addEventListener("change", applyPPFilters);
});

$("btn-clear-pp-filters").addEventListener("click", () => {
  $("pp-filter-league").value = "";
  $("pp-filter-stat").value   = "";
  $("pp-filter-player").value = "";
  applyPPFilters();
});

// Sorting
document.querySelectorAll("th.sortable-pp").forEach(th => {
  th.addEventListener("click", () => {
    const col = th.dataset.col;
    if (ppState.sortCol === col) {
      ppState.sortDir = ppState.sortDir === "desc" ? "asc" : "desc";
    } else {
      ppState.sortCol = col;
      ppState.sortDir = col === "line_score" ? "desc" : "asc";
    }
    document.querySelectorAll("th.sortable-pp").forEach(t => {
      t.classList.remove("active", "asc", "desc");
    });
    th.classList.add("active", ppState.sortDir);
    renderPPTable();
  });
});

function sortPPLines(lines) {
  return [...lines].sort((a, b) => {
    let va = a[ppState.sortCol] ?? "";
    let vb = b[ppState.sortCol] ?? "";
    if (typeof va === "string") va = va.toLowerCase();
    if (typeof vb === "string") vb = vb.toLowerCase();
    if (va < vb) return ppState.sortDir === "asc" ? -1 : 1;
    if (va > vb) return ppState.sortDir === "asc" ? 1 : -1;
    return 0;
  });
}

function renderPPTable() {
  const sorted = sortPPLines(ppState.filteredLines);
  ppTotalBadge.textContent = `${sorted.length} lines`;

  if (sorted.length === 0) {
    ppTbody.innerHTML = `<tr><td colspan="5" class="empty-msg">
      ${ppState.allLines.length === 0 ? 'Click "Load PrizePicks Lines" to fetch data.' : "No lines match current filters."}
    </td></tr>`;
    return;
  }

  ppTbody.innerHTML = sorted.map(l => {
    let gameTime = "—";
    if (l.start_time) {
      const d = new Date(l.start_time);
      gameTime = d.toLocaleDateString([], { month: "numeric", day: "numeric" }) +
        " " + d.toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });
    }
    return `<tr>
      <td>${l.player_name}</td>
      <td><span class="league-tag league-${l.league}">${l.league}</span></td>
      <td>${l.stat_type}</td>
      <td class="line-value">${l.line_score}</td>
      <td class="game-time">${gameTime}</td>
    </tr>`;
  }).join("");
}

// Load button
$("btn-load-pp").addEventListener("click", async () => {
  const btn = $("btn-load-pp");
  btn.disabled = true;
  btn.textContent = "Loading...";
  ppStatusLabel.textContent = "Fetching from PrizePicks...";

  try {
    // Trigger scrape
    await fetch("/api/prizepicks/refresh", { method: "POST" });

    // Poll until done
    let done = false;
    while (!done) {
      await new Promise(r => setTimeout(r, 2000));
      const resp = await fetch("/api/prizepicks");
      const data = await resp.json();
      if (!data.is_scraping) {
        ppState.allLines = data.lines || [];
        done = true;
      }
    }

    ppStatusLabel.textContent = `Loaded at ${new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}`;
    applyPPFilters();
  } catch (e) {
    ppStatusLabel.textContent = "Error: " + e.message;
  } finally {
    btn.disabled = false;
    btn.textContent = "Load PrizePicks Lines";
  }
});

// ── Init ───────────────────────────────────────────────────────────────────
fetchStatus();
setInterval(fetchStatus, 10_000);
