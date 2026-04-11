/**
 * PrizePicks +EV Finder — Frontend
 *
 * - Polls /api/status every 10s for live countdown & scraping state
 * - Fetches /api/bets after each refresh completes
 * - Renders sortable, filterable bets table
 * - Slip builder: select 2-6 bets → POST /api/slip → show Power/Flex EV%
 */

const POWER_PAYOUTS = { 2: 3.0, 3: 6.0, 4: 10.0, 5: 20.0, 6: 40.0 };
const FLEX_PAYOUTS = {
  3: { 2: 1.0, 3: 3.0 },
  4: { 3: 1.5, 4: 6.0 },
  5: { 3: 0.4, 4: 2.0, 5: 10.0 },
  6: { 4: 0.4, 5: 2.0, 6: 25.0 }
};

// ── State ──────────────────────────────────────────────────────────────────
const state = {
  allBets:       [],          // raw bet objects from API
  filteredBets:  [],          // after filters applied
  selected:      new Set(),   // set of bet_ids
  sortCol:       "individual_ev_pct",
  sortDir:       "desc",
  lastBetCount:  -1,          // detect when new bets arrive
  isScrapingPrev: false,
  page:          1,
  pageSize:      100,
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
const btnBuildSlip       = $("btn-build-slip");
const btnAddToBacktest   = $("btn-add-to-backtest");
const btnCalculate       = $("btn-calculate");
const btnAutoBuild       = $("btn-auto-build");
const btnClearSel        = $("btn-clear-selection");
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

// ── Pagination Helper ──────────────────────────────────────────────────────
function renderPagination(containerId, stateObj, totalItems, renderCallback) {
  const container = $(containerId);
  if (!container) return;
  const totalPages = Math.ceil(totalItems / stateObj.pageSize) || 1;
  if (stateObj.page > totalPages) stateObj.page = totalPages;
  if (stateObj.page < 1) stateObj.page = 1;

  if (totalItems === 0) {
    container.innerHTML = "";
    return;
  }

  container.innerHTML = `
    <div class="pagination">
      <button class="btn btn-secondary btn-sm" ${stateObj.page === 1 ? "disabled" : ""} id="${containerId}-prev">&lt;</button>
      <span>P. ${stateObj.page} / ${totalPages}</span>
      <button class="btn btn-secondary btn-sm" ${stateObj.page === totalPages ? "disabled" : ""} id="${containerId}-next">&gt;</button>
    </div>
  `;

  const btnPrev = document.getElementById(`${containerId}-prev`);
  const btnNext = document.getElementById(`${containerId}-next`);
  if (btnPrev) {
    btnPrev.addEventListener("click", () => {
      if (stateObj.page > 1) {
        stateObj.page--;
        renderCallback();
      }
    });
  }
  if (btnNext) {
    btnNext.addEventListener("click", () => {
      if (stateObj.page < totalPages) {
        stateObj.page++;
        renderCallback();
      }
    });
  }
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

  state.page = 1;
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
    state.page = 1;
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

  const totalItems = sorted.length;
  const totalPages = Math.ceil(totalItems / state.pageSize) || 1;
  if (state.page > totalPages) state.page = totalPages;
  if (state.page < 1) state.page = 1;
  const startIdx = (state.page - 1) * state.pageSize;
  const paginated = sorted.slice(startIdx, startIdx + state.pageSize);

  renderPagination("ev-pagination", state, totalItems, renderTable);

  if (totalItems === 0) {
    tbody.innerHTML = `<tr id="empty-row"><td colspan="10" class="empty-msg">
      ${state.allBets.length === 0 ? 'Click "Refresh Now" to load bets.' : "No bets match current filters."}
    </td></tr>`;
    totalBadge.textContent = "0 bets";
    return;
  }

  totalBadge.textContent = `${totalItems} bet${totalItems !== 1 ? "s" : ""}`;

  tbody.innerHTML = paginated.map(b => {
    const checked   = state.selected.has(b.bet_id) ? "checked" : "";
    const isLive    = b.in_backtest === true;
    const rowClass  = [
      state.selected.has(b.bet_id) ? "selected" : "",
      isLive ? "in-backtest" : "",
    ].filter(Boolean).join(" ");

    const lineDiff = (b.fd_line != null && b.pp_line !== b.fd_line)
      ? `<span class="line-diff"> (FD: ${b.fd_line})</span>` : "";

    const loggedBadge = isLive
      ? ` <span class="logged-badge">LOGGED</span>`
      : "";

    // Build book odds display with source tags
    const bookOddsEntries = [
      { label: "FD", odds: b.fd_odds_book },
      { label: "DK", odds: b.dk_odds_book },
      { label: "PIN", odds: b.pin_odds_book },
    ].filter(e => e.odds != null);
    const bookOddsHtml = bookOddsEntries.length > 0
      ? bookOddsEntries.map(e => `${fmt.odds(e.odds)} <span class="book-tag book-${e.label.toLowerCase()}">${e.label}</span>`).join(" ")
      : "—";

    return `<tr class="${rowClass}" data-id="${b.bet_id}">
      <td><input type="checkbox" class="row-chk" data-id="${b.bet_id}" ${checked} /></td>
      <td>${b.player_name}${loggedBadge}</td>
      <td>${b.league}</td>
      <td>${b.prop_type}</td>
      <td>${b.pp_line}${lineDiff}</td>
      <td class="side-${b.side}">${b.side.toUpperCase()}</td>
      <td>${fmt.trueOdds(b.true_odds)}</td>
      <td class="${evClass(b.edge)}">${fmt.pct(b.edge)}</td>
      <td class="${evClass(b.individual_ev_pct)}">${fmt.pct(b.individual_ev_pct)}</td>
      <td style="color:var(--text-muted)">${bookOddsHtml}</td>
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
  btnBuildSlip.disabled      = !valid;
  btnCalculate.disabled      = !valid;
  btnAddToBacktest.disabled  = !valid;
}

$("chk-all").addEventListener("change", e => {
  const checked = e.target.checked;
  const totalItems = state.filteredBets.length;
  const startIdx = (state.page - 1) * state.pageSize;
  const paginated = sortBets(state.filteredBets).slice(startIdx, startIdx + state.pageSize);
  
  const toToggle = checked ? paginated.slice(0, 6) : paginated;
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

// ── Add to Backtest ────────────────────────────────────────────────────────
btnAddToBacktest.addEventListener("click", async () => {
  const betMap = Object.fromEntries(state.allBets.map(b => [b.bet_id, b]));
  const selected = [...state.selected].map(id => betMap[id]).filter(Boolean);
  if (selected.length < 2 || selected.length > 6) return;

  btnAddToBacktest.disabled = true;
  btnAddToBacktest.textContent = "Logging...";

  try {
    const resp = await fetch("/api/backtest/add-slip", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ bet_ids: selected.map(b => b.bet_id) }),
    });
    const data = await resp.json();

    if (!resp.ok) {
      alert("Failed to log slip: " + (data.detail || "Unknown error"));
    } else {
      // Flash the button green briefly
      btnAddToBacktest.textContent = "✓ Logged!";
      btnAddToBacktest.style.background = "var(--green)";
      btnAddToBacktest.style.color = "#000";

      // Show notification banner with the new slip
      if (data.slip) {
        lastSeenSlipId = data.slip.slip_id;
        playBeep();
        showSlipNotification(data.slip);
      }

      setTimeout(() => {
        btnAddToBacktest.textContent = "+ Add to Backtest";
        btnAddToBacktest.style.background = "";
        btnAddToBacktest.style.color = "";
        btnAddToBacktest.disabled = (state.selected.size < 2 || state.selected.size > 6);
      }, 2500);
    }
  } catch (e) {
    alert("Error logging slip: " + e.message);
    btnAddToBacktest.textContent = "+ Add to Backtest";
    btnAddToBacktest.disabled = (state.selected.size < 2 || state.selected.size > 6);
  }
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
      await fetchPin();
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
    $("pin-view").classList.add("hidden");
    $("pin-filters").classList.add("hidden");
    $("backtest-view").classList.add("hidden");
    $("backtest-filters").classList.add("hidden");
    $("analytics-view").classList.add("hidden");

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
    } else if (target === "pin") {
      $("pin-view").classList.remove("hidden");
      $("pin-filters").classList.remove("hidden");
      // Always re-fetch/render when switching to pin tab
      if (pinState.allLines.length > 0) {
        renderPinTable();
      } else {
        fetchPin();
      }
    } else if (target === "backtest") {
      $("backtest-view").classList.remove("hidden");
      $("backtest-filters").classList.remove("hidden");
      fetchBacktest();
    } else if (target === "analytics") {
      $("analytics-view").classList.remove("hidden");
      fetchCalibration();
    }
  });
});

// ── Combined Lines ────────────────────────────────────────────────────────
const matchedState = {
  allLines:      [],
  filteredLines: [],
  sortCol:       "player_name",
  sortDir:       "asc",
  page:          1,
  pageSize:      100,
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

  matchedState.page = 1;
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
    matchedState.page = 1;
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
  
  const totalItems = sorted.length;
  const totalPages = Math.ceil(totalItems / matchedState.pageSize) || 1;
  if (matchedState.page > totalPages) matchedState.page = totalPages;
  if (matchedState.page < 1) matchedState.page = 1;
  const startIdx = (matchedState.page - 1) * matchedState.pageSize;
  const paginated = sorted.slice(startIdx, startIdx + matchedState.pageSize);

  renderPagination("matched-pagination", matchedState, totalItems, renderMatchedTable);

  matchedTotalBadge.textContent = `${totalItems} lines`;

  if (totalItems === 0) {
    matchedTbody.innerHTML = `<tr><td colspan="11" class="empty-msg">
      ${matchedState.allLines.length === 0 ? 'Click "Refresh Now" to load bets.' : "No lines match current filters."}
    </td></tr>`;
    return;
  }

  matchedTbody.innerHTML = paginated.map(l => {
    let gameTime = "—";
    if (l.start_time) {
      const d = new Date(l.start_time);
      gameTime = d.toLocaleDateString([], { month: "numeric", day: "numeric" }) +
        " " + d.toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });
    }
    const lineDiff = (l.fd_line != null && l.pp_line !== l.fd_line) ? `<span class="line-diff"> (FD: ${l.fd_line})</span>` : "";
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
      <td class="line-value">${fmt.odds(l.pin_odds)}</td>
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

async function fetchPin() {
  try {
    const resp = await fetch("/api/pinnacle");
    const data = await resp.json();
    pinState.allLines = data.lines || [];
    applyPinFilters();
  } catch (e) {
    console.error("Failed to fetch Pinnacle lines:", e);
  }
}

// ── PrizePicks Lines ──────────────────────────────────────────────────────
const ppState = {
  allLines:      [],
  filteredLines: [],
  sortCol:       "player_name",
  sortDir:       "asc",
  page:          1,
  pageSize:      100,
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

  ppState.page = 1;
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
    ppState.page = 1;
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
  
  const totalItems = sorted.length;
  const totalPages = Math.ceil(totalItems / ppState.pageSize) || 1;
  if (ppState.page > totalPages) ppState.page = totalPages;
  if (ppState.page < 1) ppState.page = 1;
  const startIdx = (ppState.page - 1) * ppState.pageSize;
  const paginated = sorted.slice(startIdx, startIdx + ppState.pageSize);

  renderPagination("pp-pagination", ppState, totalItems, renderPPTable);

  ppTotalBadge.textContent = `${totalItems} lines`;

  if (totalItems === 0) {
    ppTbody.innerHTML = `<tr><td colspan="6" class="empty-msg">
      ${ppState.allLines.length === 0 ? 'Click "Load PrizePicks Lines" to fetch data.' : "No lines match current filters."}
    </td></tr>`;
    return;
  }

  ppTbody.innerHTML = paginated.map(l => {
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
      <td class="side-${l.side}">${l.side.toUpperCase()}</td>
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
  page:          1,
  pageSize:      100,
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

  fdState.page = 1;
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
    fdState.page = 1;
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
  
  const totalItems = sorted.length;
  const totalPages = Math.ceil(totalItems / fdState.pageSize) || 1;
  if (fdState.page > totalPages) fdState.page = totalPages;
  if (fdState.page < 1) fdState.page = 1;
  const startIdx = (fdState.page - 1) * fdState.pageSize;
  const paginated = sorted.slice(startIdx, startIdx + fdState.pageSize);

  renderPagination("fd-pagination", fdState, totalItems, renderFDTable);

  fdTotalBadge.textContent = `${totalItems} lines`;

  if (totalItems === 0) {
    fdTbody.innerHTML = `<tr><td colspan="8" class="empty-msg">
      ${fdState.allLines.length === 0 ? 'Click "Load FanDuel Lines" to fetch data.' : "No lines match current filters."}
    </td></tr>`;
    return;
  }

  fdTbody.innerHTML = paginated.map(l => {
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
      <td class="side-${l.side}">${l.side.toUpperCase()}</td>
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
  page:          1,
  pageSize:      100,
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

  dkState.page = 1;
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
    dkState.page = 1;
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
  
  const totalItems = sorted.length;
  const totalPages = Math.ceil(totalItems / dkState.pageSize) || 1;
  if (dkState.page > totalPages) dkState.page = totalPages;
  if (dkState.page < 1) dkState.page = 1;
  const startIdx = (dkState.page - 1) * dkState.pageSize;
  const paginated = sorted.slice(startIdx, startIdx + dkState.pageSize);

  renderPagination("dk-pagination", dkState, totalItems, renderDKTable);

  dkTotalBadge.textContent = `${totalItems} lines`;

  if (totalItems === 0) {
    dkTbody.innerHTML = `<tr><td colspan="8" class="empty-msg">
      ${dkState.allLines.length === 0 ? 'Click "Load DraftKings Lines" to fetch data.' : "No lines match current filters."}
    </td></tr>`;
    return;
  }

  dkTbody.innerHTML = paginated.map(l => {
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
      <td class="side-${l.side}">${l.side.toUpperCase()}</td>
      <td class="line-value">${fmt.trueOdds(l.true_odds)}</td>
      <td class="line-value">${fmt.odds(l.line_odds)}</td>
      <td class="game-time">${gameTime}</td>
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

// ── Pinnacle Lines ──────────────────────────────────────────────────────
const pinState = {
  allLines:      [],
  filteredLines: [],
  sortCol:       "player_name",
  sortDir:       "asc",
  page:          1,
  pageSize:      100,
};

const pinTbody       = $("pin-tbody");
const pinTotalBadge  = $("pin-total-badge");
const pinStatusLabel = $("pin-status-label");

function applyPinFilters() {
  const league = $("pin-filter-league").value.toUpperCase();
  const stat   = $("pin-filter-stat").value.toLowerCase().trim();
  const player = $("pin-filter-player").value.toLowerCase().trim();

  pinState.filteredLines = pinState.allLines.filter(l => {
    if (league && l.league !== league) return false;
    if (stat && !l.stat_type.toLowerCase().includes(stat)) return false;
    if (player && !l.player_name.toLowerCase().includes(player)) return false;
    return true;
  });

  pinState.page = 1;
  renderPinTable();
}

["pin-filter-league", "pin-filter-stat", "pin-filter-player"].forEach(id => {
  $(id).addEventListener("input", applyPinFilters);
  $(id).addEventListener("change", applyPinFilters);
});

$("btn-clear-pin-filters").addEventListener("click", () => {
  $("pin-filter-league").value = "";
  $("pin-filter-stat").value   = "";
  $("pin-filter-player").value = "";
  applyPinFilters();
});

// Sorting
document.querySelectorAll("th.sortable-pin").forEach(th => {
  th.addEventListener("click", () => {
    const col = th.dataset.col;
    if (pinState.sortCol === col) {
      pinState.sortDir = pinState.sortDir === "desc" ? "asc" : "desc";
    } else {
      pinState.sortCol = col;
      pinState.sortDir = col === "line_score" ? "desc" : "asc";
    }
    document.querySelectorAll("th.sortable-pin").forEach(t => {
      t.classList.remove("active", "asc", "desc");
    });
    th.classList.add("active", pinState.sortDir);
    pinState.page = 1;
    renderPinTable();
  });
});

function sortPinLines(lines) {
  return [...lines].sort((a, b) => {
    let va = a[pinState.sortCol] ?? "";
    let vb = b[pinState.sortCol] ?? "";
    if (typeof va === "string") va = va.toLowerCase();
    if (typeof vb === "string") vb = vb.toLowerCase();
    if (va < vb) return pinState.sortDir === "asc" ? -1 : 1;
    if (va > vb) return pinState.sortDir === "asc" ? 1 : -1;
    return 0;
  });
}

function renderPinTable() {
  const sorted = sortPinLines(pinState.filteredLines);
  
  const totalItems = sorted.length;
  const totalPages = Math.ceil(totalItems / pinState.pageSize) || 1;
  if (pinState.page > totalPages) pinState.page = totalPages;
  if (pinState.page < 1) pinState.page = 1;
  const startIdx = (pinState.page - 1) * pinState.pageSize;
  const paginated = sorted.slice(startIdx, startIdx + pinState.pageSize);

  renderPagination("pin-pagination", pinState, totalItems, renderPinTable);

  pinTotalBadge.textContent = `${totalItems} lines`;

  if (totalItems === 0) {
    pinTbody.innerHTML = `<tr><td colspan="8" class="empty-msg">
      ${pinState.allLines.length === 0 ? 'Click "Load Pinnacle Lines" to fetch data.' : "No lines match current filters."}
    </td></tr>`;
    return;
  }

  pinTbody.innerHTML = paginated.map(l => {
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
      <td class="side-${l.side}">${l.side.toUpperCase()}</td>
      <td class="line-value">${fmt.trueOdds(l.true_odds)}</td>
      <td class="line-value">${fmt.odds(l.line_odds)}</td>
      <td class="game-time">${gameTime}</td>
    </tr>`;
  }).join("");
}

// Load button
$("btn-load-pin").addEventListener("click", async () => {
  const btn = $("btn-load-pin");
  btn.disabled = true;
  btn.textContent = "Loading...";
  pinStatusLabel.textContent = "Fetching Pinnacle...";

  try {
    // Trigger scrape
    await fetch("/api/pinnacle/refresh", { method: "POST" });

    // Poll until done
    let done = false;
    while (!done) {
      await new Promise(r => setTimeout(r, 2000));
      const resp = await fetch("/api/pinnacle");
      const data = await resp.json();
      if (!data.is_scraping) {
        pinState.allLines = data.lines || [];
        done = true;
      }
    }

    pinStatusLabel.textContent = `Loaded at ${new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}`;
    applyPinFilters();
  } catch (e) {
    pinStatusLabel.textContent = "Error: " + e.message;
  } finally {
    btn.disabled = false;
    btn.textContent = "Load Pinnacle Lines";
  }
});

// ── Backtest Dashboard ────────────────────────────────────────────────────

let btSlips = [];   // raw slip objects from API
const btState = {
  page: 1,
  pageSize: 100
};

async function fetchBacktest() {
  try {
    const resp = await fetch("/api/backtest/slips");
    if (!resp.ok) return;
    const data = await resp.json();
    btSlips = data.slips || [];
    renderBacktest();
  } catch (e) { console.error("Backtest fetch error:", e); }
}

function renderBacktest() {
  const filterResult = $("bt-filter-result").value;
  const filterLeague = $("bt-filter-league").value.toUpperCase();

  // Build a payout lookup by slip_id from the API data
  const payoutBySlip = {};
  for (const slip of btSlips) {
    payoutBySlip[slip.slip_id] = {
      payout: slip.payout,       // null if incomplete, number if complete
      hits: slip.hits,
      completed: slip.completed,
    };
  }

  // Flatten all legs for the table, applying filters
  let allLegs = [];
  for (const slip of btSlips) {
    for (const leg of (slip.legs || [])) {
      const row = { ...leg, slip_id: slip.slip_id, timestamp: slip.timestamp,
                     slip_type: slip.slip_type, n_legs: slip.n_legs,
                     proj_slip_ev_pct: slip.proj_slip_ev_pct,
                     slip_payout: slip.payout, slip_hits: slip.hits,
                     slip_completed: slip.completed };
      if (filterResult && (leg.result || "pending") !== filterResult) continue;
      if (filterLeague && (leg.league || "").toUpperCase() !== filterLeague) continue;
      allLegs.push(row);
    }
  }

  // ── Summary stats (slip-level) ─────────────────────────────────────────
  const totalSlips = btSlips.length;
  const completedSlips = btSlips.filter(s => s.completed);
  const pendingSlips = totalSlips - completedSlips.length;

  // Slip hit rate: payout > 1x counts as a "hit"
  const slipHits = completedSlips.filter(s => s.payout > 1.0).length;
  const slipHitRate = completedSlips.length > 0
    ? ((slipHits / completedSlips.length) * 100).toFixed(1) + "%"
    : "—";

  // Actual ROI: (total payouts - total wagered) / total wagered × 100
  // Each slip is 1 unit wagered
  const totalPayouts = completedSlips.reduce((sum, s) => sum + (s.payout || 0), 0);
  const totalWagered = completedSlips.length;  // 1 unit per slip
  const roi = totalWagered > 0
    ? (((totalPayouts - totalWagered) / totalWagered) * 100).toFixed(1) + "%"
    : "—";
  const roiPositive = totalWagered > 0 && totalPayouts > totalWagered;

  // Avg projected EV
  const evVals = btSlips.map(s => parseFloat(s.proj_slip_ev_pct) || 0);
  const avgEv = evVals.length > 0
    ? ((evVals.reduce((a, b) => a + b, 0) / evVals.length) * 100).toFixed(1) + "%"
    : "—";



  const totalLegsCount = allLegs.length;
  const checkedLegs = allLegs.filter(l => l.result === "hit" || l.result === "miss");
  const hitLegs = checkedLegs.filter(l => l.result === "hit").length;
  const completedLegsCount = checkedLegs.length;

  let legHitRateText = "—";
  let legHitRateClass = "bt-card-value";
  if (completedLegsCount > 0) {
    const pHat = hitLegs / completedLegsCount;
    const margin = 1.96 * Math.sqrt((pHat * (1 - pHat)) / completedLegsCount);
    const lower = Math.max(0, pHat - margin);
    const upper = Math.min(1, pHat + margin);
    const ciText = `[${(lower * 100).toFixed(1)}%, ${(upper * 100).toFixed(1)}%]`;
    const rateText = (pHat * 100).toFixed(1) + "%";
    
    legHitRateText = `${rateText} <span style="font-size:0.5em; opacity:0.8; vertical-align:middle; margin-left:4px;">${ciText}</span>`;
    
    const target = 0.540833;
    if (lower > target) {
      legHitRateClass += " positive";
    } else if (upper < target) {
      legHitRateClass += " negative";
    }
  }

  $("bt-total-slips").textContent = totalSlips;
  $("bt-completed").textContent = completedSlips.length;
  $("bt-hit-rate").textContent = slipHitRate;
  $("bt-hit-rate").className = "bt-card-value" + (completedSlips.length > 0 && slipHits / completedSlips.length >= 0.3 ? " positive" : completedSlips.length > 0 ? " negative" : "");
  
  $("bt-total-legs").textContent = totalLegsCount;
  $("bt-completed-legs").textContent = completedLegsCount;
  if ($("bt-leg-hit-rate")) {
    $("bt-leg-hit-rate").innerHTML = legHitRateText;
    $("bt-leg-hit-rate").className = legHitRateClass;
  }

  $("bt-roi").textContent = roi;
  $("bt-roi").className = "bt-card-value" + (roiPositive ? " positive" : totalWagered > 0 ? " negative" : "");
  $("bt-avg-ev").textContent = avgEv;
  $("bt-avg-ev").className = "bt-card-value" + (evVals.length > 0 && evVals.reduce((a, b) => a + b, 0) / evVals.length > 0 ? " positive" : "");


  // ── Table ──────────────────────────────────────────────────────────────
  const tbody = $("bt-tbody");

  const totalItems = allLegs.length;
  const totalPages = Math.ceil(totalItems / btState.pageSize) || 1;
  if (btState.page > totalPages) btState.page = totalPages;
  if (btState.page < 1) btState.page = 1;
  const startIdx = (btState.page - 1) * btState.pageSize;
  const paginated = allLegs.slice(startIdx, startIdx + btState.pageSize);

  renderPagination("bt-pagination", btState, totalItems, renderBacktest);

  if (totalItems === 0) {
    tbody.innerHTML = `<tr><td colspan="20" class="empty-msg">No backtest data yet. Slips will appear here as they are logged.</td></tr>`;
    return;
  }

  let prevSlipId = null;
  tbody.innerHTML = paginated.map(l => {
    const isFirst = l.slip_id !== prevSlipId;
    prevSlipId = l.slip_id;
    const evPct = l.proj_slip_ev_pct != null ? (parseFloat(l.proj_slip_ev_pct) * 100).toFixed(1) + "%" : "";
    const indEv = l.ind_ev_pct != null ? (parseFloat(l.ind_ev_pct) * 100).toFixed(1) + "%" : "";
    const trueP = l.true_prob != null ? (parseFloat(l.true_prob) * 100).toFixed(1) + "%" : "";
    const closeP = l.closing_prob ? (parseFloat(l.closing_prob) * 100).toFixed(1) + "%" : "—";
    const clvPctVal = l.clv_pct ? parseFloat(l.clv_pct) : null;
    const clvPctText = clvPctVal != null ? (clvPctVal > 0 ? "+" : "") + (clvPctVal * 100).toFixed(1) + "%" : "—";
    const clvCls = clvPctVal != null ? (clvPctVal > 0 ? "ev-high" : clvPctVal < 0 ? "ev-low" : "") : "";

    const resultCls = l.result === "hit" ? "result-hit" : l.result === "miss" ? "result-miss" : "result-pending";
    const resultText = l.result || "pending";
    const gameTime = l.game_start ? new Date(l.game_start).toLocaleString([], { month:"short", day:"numeric", hour:"2-digit", minute:"2-digit" }) : "";
    const ts = l.timestamp ? new Date(l.timestamp).toLocaleString([], { month:"short", day:"numeric", hour:"2-digit", minute:"2-digit" }) : "";

    // Payout cell: only show on the first row of a slip
    let payoutHtml = "";
    if (isFirst) {
      if (l.slip_completed) {
        const p = l.slip_payout || 0;
        const cls = p > 1 ? "ev-high" : p > 0 ? "ev-medium" : "ev-low";
        const hitsLabel = l.slip_hits + "/" + l.n_legs;
        payoutHtml = `<span class="${cls}" style="font-weight:700;">${p}x</span> <span style="color:var(--text-muted);font-size:11px;">(${hitsLabel})</span>`;
      } else {
        payoutHtml = `<span class="result-pending">—</span>`;
      }
    }

    const deleteBtn = isFirst
      ? `<button class="btn-delete-slip" data-slip-id="${l.slip_id}" title="Remove this slip">🗑</button>`
      : "";

    return `<tr class="${isFirst ? "slip-first" : ""}">
      <td>${deleteBtn}</td>
      <td><code>${l.slip_id || ""}</code></td>
      <td>${ts}</td>
      <td>${l.slip_type || ""}</td>
      <td>${l.n_legs || ""}</td>
      <td class="ev-high">${isFirst ? evPct : ""}</td>
      <td>${payoutHtml}</td>
      <td><strong>${l.player || ""}</strong></td>
      <td><span class="league-tag league-${(l.league || "").toUpperCase()}">${l.league || ""}</span></td>
      <td>${l.prop || ""}</td>
      <td class="line-value">${l.line || ""}</td>
      <td class="${l.side === "over" ? "side-over" : "side-under"}">${(l.side || "").toUpperCase()}</td>
      <td>${trueP}</td>
      <td>${closeP}</td>
      <td class="${clvCls}" style="font-weight:600;">${clvPctText}</td>
      <td class="ev-medium">${indEv}</td>
      <td>${l.urgency === "HIGH" ? '<span style="color:var(--yellow);font-weight:700;">HIGH</span>' : "NORMAL"}</td>
      <td>${gameTime}</td>
      <td><span class="${resultCls}">${resultText.toUpperCase()}</span></td>
      <td>${l.stat_actual || "—"}</td>
    </tr>`;
  }).join("");

  // Wire up delete buttons
  tbody.querySelectorAll(".btn-delete-slip").forEach(btn => {
    btn.addEventListener("click", (e) => {
      e.stopPropagation();
      const slipId = btn.dataset.slipId;
      deleteBacktestSlip(slipId);
    });
  });
}

// Filter events
$("bt-filter-result").addEventListener("change", () => {
  btState.page = 1;
  renderBacktest();
});
$("bt-filter-league").addEventListener("change", () => {
  btState.page = 1;
  renderBacktest();
});

async function deleteBacktestSlip(slipId) {
  if (!confirm(`Remove slip ${slipId}? This will delete all legs from the backtest CSV and free those players for future slips.`)) {
    return;
  }
  try {
    const resp = await fetch(`/api/backtest/slip/${slipId}`, { method: "DELETE" });
    if (!resp.ok) {
      const data = await resp.json();
      alert("Failed to remove slip: " + (data.detail || "Unknown error"));
      return;
    }
    await fetchBacktest();
  } catch (e) {
    alert("Error removing slip: " + e.message);
  }
}

// Download CSV
$("btn-bt-download").addEventListener("click", () => {
  window.open("/api/backtest/download-csv", "_blank");
});

// Check results manually
$("btn-bt-check-results").addEventListener("click", async () => {
  const btn = $("btn-bt-check-results");
  btn.disabled = true;
  btn.textContent = "Checking...";
  try {
    await fetch("/api/backtest/check-results", { method: "POST" });
    // Wait a few seconds then refresh
    setTimeout(async () => {
      await fetchBacktest();
      btn.disabled = false;
      btn.textContent = "Check Results (ESPN)";
    }, 5000);
  } catch (e) {
    btn.disabled = false;
    btn.textContent = "Check Results (ESPN)";
  }
});

// Refresh button
$("btn-bt-refresh").addEventListener("click", fetchBacktest);

// ── Slip Notification ──────────────────────────────────────────────────────

let lastSeenSlipId = null;
let isInitializingLatestSlip = true;

function playBeep() {
  try {
    const ctx = new (window.AudioContext || window.webkitAudioContext)();
    const freqs = [880, 1100, 1320];
    freqs.forEach((freq, i) => {
      const osc = ctx.createOscillator();
      const gain = ctx.createGain();
      osc.connect(gain);
      gain.connect(ctx.destination);
      osc.type = "sine";
      osc.frequency.value = freq;
      gain.gain.setValueAtTime(0.15, ctx.currentTime + i * 0.12);
      gain.gain.exponentialRampToValueAtTime(0.001, ctx.currentTime + i * 0.12 + 0.18);
      osc.start(ctx.currentTime + i * 0.12);
      osc.stop(ctx.currentTime + i * 0.12 + 0.2);
    });
  } catch (e) { /* audio not available */ }
}

function showSlipNotification(slip) {
  const banner = document.getElementById("slip-banner");
  if (!banner) return;

  const evPct = slip.proj_slip_ev_pct != null
    ? (slip.proj_slip_ev_pct * 100).toFixed(1)
    : "?";

  // Find earliest game start for countdown
  const starts = (slip.legs || [])
    .map(l => l.game_start ? new Date(l.game_start) : null)
    .filter(Boolean);
  const earliest = starts.length ? new Date(Math.min(...starts)) : null;
  let countdownStr = "";
  if (earliest) {
    const minsLeft = Math.round((earliest - Date.now()) / 60000);
    if (minsLeft > 0) countdownStr = ` · First game in ${minsLeft}m`;
  }

  const legsHtml = (slip.legs || []).map(l => {
    const urgTag = l.urgency === "HIGH"
      ? ' <span class="urgency-high">⚡HIGH</span>' : "";
    return `<span class="slip-banner-leg">${l.player} ${l.prop} ${l.side.toUpperCase()} ${l.line}${urgTag}</span>`;
  }).join("");

  banner.innerHTML = `
    <div class="slip-banner-icon">🎯</div>
    <div class="slip-banner-body">
      <div class="slip-banner-title">
        New Slip: ${slip.n_legs}-Leg ${slip.slip_type} &nbsp;|&nbsp; EV: +${evPct}%${countdownStr}
      </div>
      <div class="slip-banner-legs">${legsHtml}</div>
      <div class="slip-banner-meta">Slip ID: ${slip.slip_id} &nbsp;·&nbsp; ${slip.timestamp || ""}</div>
    </div>
    <button class="slip-banner-close" onclick="document.getElementById('slip-banner').classList.add('hidden')">✕</button>
  `;
  banner.classList.remove("hidden");

  // Auto-dismiss after 5 seconds
  setTimeout(() => banner.classList.add("hidden"), 5000);
}

async function pollLatestSlip() {
  try {
    const resp = await fetch("/api/backtest/latest-slip");
    if (!resp.ok) return;
    const data = await resp.json();
    const slip = data.slip;
    if (slip && slip.slip_id && slip.slip_id !== lastSeenSlipId) {
      const prevId = lastSeenSlipId;
      lastSeenSlipId = slip.slip_id;

      // Don't show notification on the very first poll (page refresh)
      if (isInitializingLatestSlip) {
        isInitializingLatestSlip = false;
        return;
      }
      // Check if the slip is fresh (less than 15 seconds old)
      // This prevents stale banners when a backgrounded tab wakes up
      let isFresh = true;
      if (slip.timestamp) {
        const slipDate = new Date(slip.timestamp);
        // Add "Z" if the timestamp doesn't have timezone, to treat it as local or UTC appropriately.
        // Actually, Python isoformat without tz is local time, so Date parsing might be slightly off
        // depending on browser timezone handling without a 'Z'. 
        // A safer way is checking difference:
        const diffMs = Math.abs(Date.now() - slipDate.getTime());
        if (diffMs > 15000) {
          isFresh = false;
        }
      }

      if (isFresh) {
        playBeep();
        showSlipNotification(slip);
      }
    }
    isInitializingLatestSlip = false; // ensure we clear this even if no slip found
  } catch (e) { /* silent */ }
}

// ── Analytics / Calibration ────────────────────────────────────────────────

async function fetchCalibration() {
  try {
    const resp = await fetch("/api/calibration");
    if (!resp.ok) return;
    const data = await resp.json();
    renderCalibration(data);
  } catch (e) {
    console.error("Calibration fetch error:", e);
  }
}

function renderCalibration(data) {
  // Summary cards
  const brierEl = $("cal-brier");
  if (data.brier_score != null) {
    const bs = data.brier_score;
    brierEl.textContent = bs.toFixed(4);
    // Below 0.25 = good (beating coin flip)
    brierEl.className = "bt-card-value" + (bs < 0.25 ? " positive" : bs < 0.30 ? "" : " negative");
  } else {
    brierEl.textContent = "\u2014";
    brierEl.className = "bt-card-value";
  }

  const llEl = $("cal-logloss");
  if (data.log_loss != null) {
    const ll = data.log_loss;
    llEl.textContent = ll.toFixed(4);
    llEl.className = "bt-card-value" + (ll < 0.693 ? " positive" : ll < 0.80 ? "" : " negative");
  } else {
    llEl.textContent = "\u2014";
    llEl.className = "bt-card-value";
  }

  $("cal-resolved").textContent = data.n_resolved || 0;
  $("cal-won").textContent = data.n_won || 0;
  $("cal-lost").textContent = data.n_lost || 0;

  if (data.hit_rate != null) {
    $("cal-hitrate").textContent = (data.hit_rate * 100).toFixed(1) + "%";
    $("cal-hitrate").className = "bt-card-value" + (data.hit_rate >= 0.54 ? " positive" : data.hit_rate >= 0.48 ? "" : " negative");
  } else {
    $("cal-hitrate").textContent = "\u2014";
  }

  if (data.avg_predicted_prob != null) {
    $("cal-avgpred").textContent = (data.avg_predicted_prob * 100).toFixed(1) + "%";
  } else {
    $("cal-avgpred").textContent = "\u2014";
  }

  // Calibration buckets table
  const bucketsTbody = $("cal-buckets-tbody");
  const buckets = data.calibration_buckets || [];

  if (buckets.length === 0) {
    bucketsTbody.innerHTML = '<tr><td colspan="5" class="empty-msg">No resolved data available yet.</td></tr>';
    return;
  }

  bucketsTbody.innerHTML = buckets.map(b => {
    const predicted = (b.predicted_avg * 100).toFixed(1) + "%";
    const actual = (b.actual_avg * 100).toFixed(1) + "%";
    const diff = b.actual_avg - b.predicted_avg;
    const diffPct = (diff * 100).toFixed(1);
    const diffSign = diff >= 0 ? "+" : "";
    const diffClass = Math.abs(diff) < 0.05 ? "ev-medium" : (diff >= 0 ? "ev-high" : "ev-low");
    const calibLabel = Math.abs(diff) < 0.03 ? "\u2705 Well calibrated"
                     : diff > 0 ? "\u2B06\uFE0F Underconfident" : "\u2B07\uFE0F Overconfident";

    return `<tr>
      <td><strong>${b.bucket}</strong></td>
      <td>${predicted}</td>
      <td class="${diffClass}">${actual}</td>
      <td>${b.count}</td>
      <td><span class="${diffClass}">${diffSign}${diffPct}pp</span> ${calibLabel}</td>
    </tr>`;
  }).join("");
  // CLV Tracking section
  $("clv-count").textContent = data.n_clv_tracked || 0;

  const clvPlusEl = $("clv-positive-rate");
  if (data.clv_plus_rate != null) {
    clvPlusEl.textContent = (data.clv_plus_rate * 100).toFixed(1) + "%";
    clvPlusEl.className = "bt-card-value" + (data.clv_plus_rate >= 0.50 ? " positive" : " negative");
  } else {
    clvPlusEl.textContent = "\u2014";
    clvPlusEl.className = "bt-card-value";
  }

  const avgClvEl = $("clv-avg-pct");
  if (data.avg_clv_pct != null) {
    const r = data.avg_clv_pct;
    avgClvEl.textContent = (r > 0 ? "+" : "") + (r * 100).toFixed(2) + "%";
    avgClvEl.className = "bt-card-value" + (r > 0 ? " positive" : r < 0 ? " negative" : "");
  } else {
    avgClvEl.textContent = "\u2014";
    avgClvEl.className = "bt-card-value";
  }
}

$("btn-cal-refresh").addEventListener("click", fetchCalibration);

// ── Init ───────────────────────────────────────────────────────────────────
fetchStatus();
fetchBets();
fetchMatched();
fetchPP();
fetchFD();
fetchDK();
fetchPin();
setInterval(fetchStatus, 10_000);
setInterval(pollLatestSlip, 10_000);
pollLatestSlip();  // check immediately on load
