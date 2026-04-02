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
const btnAutoBuild    = $("btn-auto-build");
const btnClearSel     = $("btn-clear-selection");
const slipLegsEl      = $("slip-legs");
const slipResultsEl   = $("slip-results");
const bankrollInput   = $("bankroll-input");

// ── Helpers ────────────────────────────────────────────────────────────────
const fmt = {
  pct:   v => v == null ? "—" : (v >= 0 ? "+" : "") + (v * 100).toFixed(2) + "%",
  prob:  v => v == null ? "—" : (v * 100).toFixed(1) + "%",
  odds:  v => v == null ? "—" : (v > 0 ? "+" : "") + v,
  trueOdds: v => v == null ? "—" : (v > 0 ? "+" : "") + Number(v).toFixed(2),
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
  const side    = $("filter-side").value.toLowerCase();
  const maxOddsStr = $("filter-max-odds").value.trim();
  const maxOdds = maxOddsStr ? parseInt(maxOddsStr, 10) : null;
  const minEvStr = $("filter-min-ev").value.trim();
  const minEv = minEvStr ? parseFloat(minEvStr) / 100 : 0;

  state.filteredBets = state.allBets.filter(b => {
    if (league && b.league !== league)                           return false;
    if (prop   && !b.prop_type.toLowerCase().includes(prop))    return false;
    if (side   && b.side !== side)                              return false;
    if (maxOdds !== null && b.true_odds > maxOdds)              return false;
    if (minEv !== null && b.individual_ev_pct < minEv)          return false;
    return true;
  });

  renderTable();
}

["filter-league", "filter-prop", "filter-max-odds", "filter-min-ev", "filter-side"].forEach(id => {
  $(id).addEventListener("input", applyFilters);
  $(id).addEventListener("change", applyFilters);
});

$("btn-clear-filters").addEventListener("click", () => {
  $("filter-league").value = "";
  $("filter-prop").value   = "";
  $("filter-max-odds").value = "";
  $("filter-min-ev").value   = "0";
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
    tbody.innerHTML = `<tr id="empty-row"><td colspan="10" class="empty-msg">
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
      ? `<span class="line-diff"> (FD: ${b.fd_line})</span>` : "";

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
      <td>${b.pp_line}${lineDiff}</td>
      <td class="side-${b.side}">${b.side.toUpperCase()}</td>
      <td>${fmt.trueOdds(b.true_odds)}</td>
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
      <div class="slip-leg-prob">True Odds: ${fmt.trueOdds(b.true_odds)} · EV: ${fmt.pct(b.individual_ev_pct)}</div>
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

btnAutoBuild.addEventListener("click", async () => {
  const sorted = [...state.filteredBets].sort((a, b) => (b.individual_ev_pct || 0) - (a.individual_ev_pct || 0));
  
  const pickedPlayers = new Set();
  const betIds = [];
  
  for (const b of sorted) {
    if (!pickedPlayers.has(b.player_name)) {
      pickedPlayers.add(b.player_name);
      betIds.push(b.bet_id);
      if (betIds.length === 6) break;
    }
  }

  if (betIds.length < 2) {
    alert("Not enough valid unique players matching the current filters to build a slip.");
    return;
  }

  const bankroll = parseFloat(bankrollInput.value) || 100;
  
  btnAutoBuild.disabled = true;
  btnAutoBuild.textContent = "Auto-Building...";
  
  try {
    const resp = await fetch("/api/slip/auto", {
      method:  "POST",
      headers: { "Content-Type": "application/json" },
      body:    JSON.stringify({ bet_ids: betIds, bankroll }),
    });
    if (!resp.ok) {
      const err = await resp.json();
      alert("Auto-Slip error: " + (err.detail || resp.statusText));
      return;
    }
    const data = await resp.json();
    
    state.selected.clear();
    for (const bid of data.optimal_bet_ids) {
      state.selected.add(bid);
    }
    
    updateSelectionUI();
    renderTable(); 
    renderSlipLegs();
    renderSlipResults(data);
    
  } catch (e) {
    alert("Network error: " + e.message);
  } finally {
    btnAutoBuild.disabled = false;
    btnAutoBuild.textContent = "Auto-Build Best Slip";
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
      await fetchMatched();
      await fetchPP();
      await fetchFD();
      await fetchDK();
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
  $("settings-modal").classList.remove("hidden");
});

["btn-close-settings", "btn-cancel-settings"].forEach(id => {
  $(id).addEventListener("click", () => $("settings-modal").classList.add("hidden"));
});

$("btn-save-settings").addEventListener("click", async () => {
  try {
    const btn = $("btn-save-settings");
    btn.disabled = true;
    btn.textContent = "Saving...";
    
    const resp = await fetch("/api/config", {
      method:  "POST",
      headers: { "Content-Type": "application/json" },
      body:    JSON.stringify({
        interval_min:   parseInt($("setting-interval").value),
        min_ev_pct:     -10.0
      }),
    });
    
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    $("settings-modal").classList.add("hidden");
    
  } catch (e) {
    alert("Error saving settings: " + e.message);
  } finally {
    const btn = $("btn-save-settings");
    btn.disabled = false;
    btn.textContent = "Save";
  }
});

// ── Tab switching ─────────────────────────────────────────────────────────
document.querySelectorAll(".tab").forEach(tab => {
  tab.addEventListener("click", () => {
    document.querySelectorAll(".tab").forEach(t => t.classList.remove("active"));
    tab.classList.add("active");

    const target = tab.dataset.tab;
    
    // Hide all
    $("ev-view").classList.add("hidden");
    $("ev-filters").classList.add("hidden");
    $("matched-view").classList.add("hidden");
    $("matched-filters").classList.add("hidden");
    $("pp-view").classList.add("hidden");
    $("pp-filters").classList.add("hidden");
    $("fd-view").classList.add("hidden");
    $("fd-filters").classList.add("hidden");
    $("dk-view").classList.add("hidden");
    $("dk-filters").classList.add("hidden");

    if (target === "ev") {
      $("ev-view").classList.remove("hidden");
      $("ev-filters").classList.remove("hidden");
    } else if (target === "matched") {
      $("matched-view").classList.remove("hidden");
      $("matched-filters").classList.remove("hidden");
    } else if (target === "pp") {
      $("pp-view").classList.remove("hidden");
      $("pp-filters").classList.remove("hidden");
    } else if (target === "fd") {
      $("fd-view").classList.remove("hidden");
      $("fd-filters").classList.remove("hidden");
    } else if (target === "dk") {
      $("dk-view").classList.remove("hidden");
      $("dk-filters").classList.remove("hidden");
    }
  });
});

// ── Combined Lines ────────────────────────────────────────────────────────
const matchedState = {
  allLines:      [],
  filteredLines: [],
  sortCol:       "player_name",
  sortDir:       "asc",
};

const matchedTbody      = $("matched-tbody");
const matchedTotalBadge = $("matched-total-badge");

function applyMatchedFilters() {
  const league = $("matched-filter-league").value.toUpperCase();
  const prop   = $("matched-filter-prop").value.toLowerCase().trim();
  const player = $("matched-filter-player").value.toLowerCase().trim();

  matchedState.filteredLines = matchedState.allLines.filter(l => {
    if (league && l.league !== league) return false;
    if (prop && !l.stat_type.toLowerCase().includes(prop)) return false;
    if (player && !l.player_name.toLowerCase().includes(player)) return false;
    return true;
  });

  renderMatchedTable();
}

["matched-filter-league", "matched-filter-prop", "matched-filter-player"].forEach(id => {
  $(id).addEventListener("input", applyMatchedFilters);
  $(id).addEventListener("change", applyMatchedFilters);
});

$("btn-clear-matched-filters").addEventListener("click", () => {
  $("matched-filter-league").value = "";
  $("matched-filter-prop").value   = "";
  $("matched-filter-player").value = "";
  applyMatchedFilters();
});

// Sorting
document.querySelectorAll("th.sortable-matched").forEach(th => {
  th.addEventListener("click", () => {
    const col = th.dataset.col;
    if (matchedState.sortCol === col) {
      matchedState.sortDir = matchedState.sortDir === "desc" ? "asc" : "desc";
    } else {
      matchedState.sortCol = col;
      matchedState.sortDir = col === "pp_line" || col === "fd_line" ? "desc" : "asc";
    }
    document.querySelectorAll("th.sortable-matched").forEach(t => {
      t.classList.remove("active", "asc", "desc");
    });
    th.classList.add("active", matchedState.sortDir);
    renderMatchedTable();
  });
});

function sortMatchedLines(lines) {
  return [...lines].sort((a, b) => {
    let va = a[matchedState.sortCol] ?? "";
    let vb = b[matchedState.sortCol] ?? "";
    if (typeof va === "string") va = va.toLowerCase();
    if (typeof vb === "string") vb = vb.toLowerCase();
    if (va < vb) return matchedState.sortDir === "asc" ? -1 : 1;
    if (va > vb) return matchedState.sortDir === "asc" ? 1 : -1;
    return 0;
  });
}

function renderMatchedTable() {
  const sorted = sortMatchedLines(matchedState.filteredLines);
  matchedTotalBadge.textContent = `${sorted.length} lines`;

  if (sorted.length === 0) {
    matchedTbody.innerHTML = `<tr><td colspan="10" class="empty-msg">
      ${matchedState.allLines.length === 0 ? 'Click "Refresh Now" to load bets.' : "No lines match current filters."}
    </td></tr>`;
    return;
  }

  matchedTbody.innerHTML = sorted.map(l => {
    let gameTime = "—";
    if (l.start_time) {
      const d = new Date(l.start_time);
      gameTime = d.toLocaleDateString([], { month: "numeric", day: "numeric" }) +
        " " + d.toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });
    }
    const lineDiff = l.pp_line !== l.fd_line ? `<span class="line-diff"> (FD: ${l.fd_line})</span>` : "";
    const sideClass = l.side === "over" ? "side-over" : "side-under";
    
    return `<tr>
      <td>${l.player_name}</td>
      <td><span class="league-tag league-${l.league}">${l.league}</span></td>
      <td>${l.stat_type}</td>
      <td class="line-value">${l.pp_line}${lineDiff}</td>
      <td class="${sideClass}">${l.side.toUpperCase()}</td>
      <td class="line-value">${fmt.trueOdds(l.true_odds)}</td>
      <td class="line-value">${fmt.odds(l.best_odds)}</td>
      <td class="line-value">${fmt.odds(l.fd_odds)}</td>
      <td class="line-value">${fmt.odds(l.dk_odds)}</td>
      <td class="game-time">${gameTime}</td>
    </tr>`;
  }).join("");
}

async function fetchMatched() {
  try {
    const resp = await fetch("/api/matched");
    const data = await resp.json();
    matchedState.allLines = data.matches || [];
    applyMatchedFilters();
  } catch (e) {
    console.error("Failed to fetch matched lines:", e);
  }
}

async function fetchPP() {
  try {
    const resp = await fetch("/api/prizepicks");
    const data = await resp.json();
    ppState.allLines = data.lines || [];
    applyPPFilters();
  } catch (e) {
    console.error("Failed to fetch PrizePicks lines:", e);
  }
}

async function fetchFD() {
  try {
    const resp = await fetch("/api/fanduel");
    const data = await resp.json();
    fdState.allLines = data.lines || [];
    applyFDFilters();
  } catch (e) {
    console.error("Failed to fetch FanDuel lines:", e);
  }
}

async function fetchDK() {
  try {
    const resp = await fetch("/api/draftkings");
    const data = await resp.json();
    dkState.allLines = data.lines || [];
    applyDKFilters();
  } catch (e) {
    console.error("Failed to fetch DraftKings lines:", e);
  }
}

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

// ── FanDuel Lines ──────────────────────────────────────────────────────
const fdState = {
  allLines:      [],
  filteredLines: [],
  sortCol:       "player_name",
  sortDir:       "asc",
};

const fdTbody       = $("fd-tbody");
const fdTotalBadge  = $("fd-total-badge");
const fdStatusLabel = $("fd-status-label");

function applyFDFilters() {
  const league = $("fd-filter-league").value.toUpperCase();
  const stat   = $("fd-filter-stat").value.toLowerCase().trim();
  const player = $("fd-filter-player").value.toLowerCase().trim();

  fdState.filteredLines = fdState.allLines.filter(l => {
    if (league && l.league !== league) return false;
    if (stat && !l.stat_type.toLowerCase().includes(stat)) return false;
    if (player && !l.player_name.toLowerCase().includes(player)) return false;
    return true;
  });

  renderFDTable();
}

["fd-filter-league", "fd-filter-stat", "fd-filter-player"].forEach(id => {
  $(id).addEventListener("input", applyFDFilters);
  $(id).addEventListener("change", applyFDFilters);
});

$("btn-clear-fd-filters").addEventListener("click", () => {
  $("fd-filter-league").value = "";
  $("fd-filter-stat").value   = "";
  $("fd-filter-player").value = "";
  applyFDFilters();
});

// Sorting
document.querySelectorAll("th.sortable-fd").forEach(th => {
  th.addEventListener("click", () => {
    const col = th.dataset.col;
    if (fdState.sortCol === col) {
      fdState.sortDir = fdState.sortDir === "desc" ? "asc" : "desc";
    } else {
      fdState.sortCol = col;
      fdState.sortDir = col === "line_score" ? "desc" : "asc";
    }
    document.querySelectorAll("th.sortable-fd").forEach(t => {
      t.classList.remove("active", "asc", "desc");
    });
    th.classList.add("active", fdState.sortDir);
    renderFDTable();
  });
});

function sortFDLines(lines) {
  return [...lines].sort((a, b) => {
    let va = a[fdState.sortCol] ?? "";
    let vb = b[fdState.sortCol] ?? "";
    if (typeof va === "string") va = va.toLowerCase();
    if (typeof vb === "string") vb = vb.toLowerCase();
    if (va < vb) return fdState.sortDir === "asc" ? -1 : 1;
    if (va > vb) return fdState.sortDir === "asc" ? 1 : -1;
    return 0;
  });
}

function renderFDTable() {
  const sorted = sortFDLines(fdState.filteredLines);
  fdTotalBadge.textContent = `${sorted.length} lines`;

  if (sorted.length === 0) {
    fdTbody.innerHTML = `<tr><td colspan="6" class="empty-msg">
      ${fdState.allLines.length === 0 ? 'Click "Load FanDuel Lines" to fetch data.' : "No lines match current filters."}
    </td></tr>`;
    return;
  }

  fdTbody.innerHTML = sorted.map(l => {
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
      <td class="line-value">${fmt.trueOdds(l.true_odds)}</td>
      <td class="line-value">${fmt.odds(l.line_odds)}</td>
      <td class="game-time">${gameTime}</td>
    </tr>`;
  }).join("");
}

// Load button
$("btn-load-fd").addEventListener("click", async () => {
  const btn = $("btn-load-fd");
  btn.disabled = true;
  btn.textContent = "Loading...";
  fdStatusLabel.textContent = "Fetching FanDuel...";

  try {
    // Trigger scrape
    await fetch("/api/fanduel/refresh", { method: "POST" });

    // Poll until done
    let done = false;
    while (!done) {
      await new Promise(r => setTimeout(r, 2000));
      const resp = await fetch("/api/fanduel");
      const data = await resp.json();
      if (!data.is_scraping) {
        fdState.allLines = data.lines || [];
        done = true;
      }
    }

    fdStatusLabel.textContent = `Loaded at ${new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}`;
    applyFDFilters();
  } catch (e) {
    fdStatusLabel.textContent = "Error: " + e.message;
  } finally {
    btn.disabled = false;
    btn.textContent = "Load FanDuel Lines";
  }
});

// ── DraftKings Lines ──────────────────────────────────────────────────────
const dkState = {
  allLines:      [],
  filteredLines: [],
  sortCol:       "player_name",
  sortDir:       "asc",
};

const dkTbody       = $("dk-tbody");
const dkTotalBadge  = $("dk-total-badge");
const dkStatusLabel = $("dk-status-label");

function applyDKFilters() {
  const league = $("dk-filter-league").value.toUpperCase();
  const stat   = $("dk-filter-stat").value.toLowerCase().trim();
  const player = $("dk-filter-player").value.toLowerCase().trim();

  dkState.filteredLines = dkState.allLines.filter(l => {
    if (league && l.league !== league) return false;
    if (stat && !l.stat_type.toLowerCase().includes(stat)) return false;
    if (player && !l.player_name.toLowerCase().includes(player)) return false;
    return true;
  });

  renderDKTable();
}

["dk-filter-league", "dk-filter-stat", "dk-filter-player"].forEach(id => {
  $(id).addEventListener("input", applyDKFilters);
  $(id).addEventListener("change", applyDKFilters);
});

$("btn-clear-dk-filters").addEventListener("click", () => {
  $("dk-filter-league").value = "";
  $("dk-filter-stat").value   = "";
  $("dk-filter-player").value = "";
  applyDKFilters();
});

// Sorting
document.querySelectorAll("th.sortable-dk").forEach(th => {
  th.addEventListener("click", () => {
    const col = th.dataset.col;
    if (dkState.sortCol === col) {
      dkState.sortDir = dkState.sortDir === "desc" ? "asc" : "desc";
    } else {
      dkState.sortCol = col;
      dkState.sortDir = col === "line_score" ? "desc" : "asc";
    }
    document.querySelectorAll("th.sortable-dk").forEach(t => {
      t.classList.remove("active", "asc", "desc");
    });
    th.classList.add("active", dkState.sortDir);
    renderDKTable();
  });
});

function sortDKLines(lines) {
  return [...lines].sort((a, b) => {
    let va = a[dkState.sortCol] ?? "";
    let vb = b[dkState.sortCol] ?? "";
    if (typeof va === "string") va = va.toLowerCase();
    if (typeof vb === "string") vb = vb.toLowerCase();
    if (va < vb) return dkState.sortDir === "asc" ? -1 : 1;
    if (va > vb) return dkState.sortDir === "asc" ? 1 : -1;
    return 0;
  });
}

function renderDKTable() {
  const sorted = sortDKLines(dkState.filteredLines);
  dkTotalBadge.textContent = `${sorted.length} lines`;

  if (sorted.length === 0) {
    dkTbody.innerHTML = `<tr><td colspan="6" class="empty-msg">
      ${dkState.allLines.length === 0 ? 'Click "Load DraftKings Lines" to fetch data.' : "No lines match current filters."}
    </td></tr>`;
    return;
  }

  dkTbody.innerHTML = sorted.map(l => {
    return `<tr>
      <td>${l.player_name}</td>
      <td><span class="league-tag league-${l.league}">${l.league}</span></td>
      <td>${l.stat_type}</td>
      <td class="line-value">${l.line_score}</td>
      <td class="line-value">${fmt.trueOdds(l.true_odds)}</td>
      <td class="line-value">${fmt.odds(l.line_odds)}</td>
    </tr>`;
  }).join("");
}

// Load button
$("btn-load-dk").addEventListener("click", async () => {
  const btn = $("btn-load-dk");
  btn.disabled = true;
  btn.textContent = "Loading...";
  dkStatusLabel.textContent = "Fetching DraftKings...";

  try {
    // Trigger scrape
    await fetch("/api/draftkings/refresh", { method: "POST" });

    // Poll until done
    let done = false;
    while (!done) {
      await new Promise(r => setTimeout(r, 2000));
      const resp = await fetch("/api/draftkings");
      const data = await resp.json();
      if (!data.is_scraping) {
        dkState.allLines = data.lines || [];
        done = true;
      }
    }

    dkStatusLabel.textContent = `Loaded at ${new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}`;
    applyDKFilters();
  } catch (e) {
    dkStatusLabel.textContent = "Error: " + e.message;
  } finally {
    btn.disabled = false;
    btn.textContent = "Load DraftKings Lines";
  }
});

// ── Init ───────────────────────────────────────────────────────────────────
fetchStatus();
fetchBets();
fetchMatched();
fetchPP();
fetchFD();
fetchDK();
setInterval(fetchStatus, 10_000);
