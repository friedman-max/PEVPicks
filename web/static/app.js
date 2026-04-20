
// The Supabase CDN exposes a global `supabase` (the module namespace) on
// `window`. Using the same name as a `let` would throw SyntaxError and abort
// the script entirely — so our authenticated client is named `sbClient`.
let sbClient = null;
let currentSession = null;

async function initAuth() {
    try {
        // Use server-injected config (no network round-trip)
        const config = window.__COREPROP_CONFIG;
        if (!config) {
            // Fallback for direct file access / dev
            const res = await fetch('/api/ui-config');
            const fallback = await res.json();
            sbClient = window.supabase.createClient(fallback.supabase_url, fallback.supabase_anon_key);
        } else {
            sbClient = window.supabase.createClient(config.supabase_url, config.supabase_anon_key);
        }

        const { data: { session } } = await sbClient.auth.getSession();
        currentSession = session;
        handleSessionUpdate(session);

        sbClient.auth.onAuthStateChange((_event, session) => {
            currentSession = session;
            handleSessionUpdate(session);
        });

        // ── Auth tab switching ──
        document.getElementById('auth-tab-login').addEventListener('click', () => {
            document.getElementById('auth-tab-login').classList.add('active');
            document.getElementById('auth-tab-signup').classList.remove('active');
            document.getElementById('auth-form-login').style.display = 'flex';
            document.getElementById('auth-form-signup').style.display = 'none';
            document.getElementById('auth-error').textContent = '';
        });
        document.getElementById('auth-tab-signup').addEventListener('click', () => {
            document.getElementById('auth-tab-signup').classList.add('active');
            document.getElementById('auth-tab-login').classList.remove('active');
            document.getElementById('auth-form-signup').style.display = 'flex';
            document.getElementById('auth-form-login').style.display = 'none';
            document.getElementById('auth-error').textContent = '';
        });

        // ── Login handler ──
        document.getElementById('btn-login').addEventListener('click', async () => {
            const email = document.getElementById('login-email').value.trim();
            const password = document.getElementById('login-password').value;
            const errorEl = document.getElementById('auth-error');
            if (!email || !password) { errorEl.textContent = 'Please fill in all fields.'; return; }
            errorEl.textContent = 'Logging in…';

            const { error } = await sbClient.auth.signInWithPassword({ email, password });
            if (error) {
                errorEl.textContent = error.message;
            } else {
                errorEl.textContent = '';
            }
        });

        // ── Signup username check (debounced) ──
        let usernameTimer = null;
        let usernameAvailable = false;
        const usernameInput = document.getElementById('signup-username');
        const usernameStatus = document.getElementById('username-status');

        usernameInput.addEventListener('input', () => {
            clearTimeout(usernameTimer);
            usernameAvailable = false;
            const val = usernameInput.value.trim();
            if (val.length < 2) {
                usernameStatus.textContent = val.length > 0 ? 'Min 2 characters' : '';
                usernameStatus.className = 'username-status taken';
                return;
            }
            usernameStatus.textContent = 'Checking…';
            usernameStatus.className = 'username-status checking';
            usernameTimer = setTimeout(async () => {
                try {
                    const r = await fetch(`/api/auth/check-username?username=${encodeURIComponent(val)}`);
                    const d = await r.json();
                    if (!r.ok) {
                        usernameStatus.textContent = d.detail || 'Invalid username';
                        usernameStatus.className = 'username-status taken';
                    } else if (d.available) {
                        usernameAvailable = true;
                        usernameStatus.textContent = '✓ Available';
                        usernameStatus.className = 'username-status available';
                    } else {
                        usernameStatus.textContent = '✗ Already taken';
                        usernameStatus.className = 'username-status taken';
                    }
                } catch {
                    usernameStatus.textContent = 'Could not check';
                    usernameStatus.className = 'username-status taken';
                }
            }, 400);
        });

        // ── Signup handler ──
        document.getElementById('btn-signup').addEventListener('click', async () => {
            const username = usernameInput.value.trim();
            const email = document.getElementById('signup-email').value.trim();
            const password = document.getElementById('signup-password').value;
            const errorEl = document.getElementById('auth-error');

            if (!username || !email || !password) { errorEl.textContent = 'Please fill in all fields.'; return; }
            if (username.length < 2) { errorEl.textContent = 'Username must be at least 2 characters.'; return; }
            if (!usernameAvailable) { errorEl.textContent = 'Please choose an available username.'; return; }
            if (password.length < 6) { errorEl.textContent = 'Password must be at least 6 characters.'; return; }

            errorEl.textContent = 'Creating account…';
            const { error } = await sbClient.auth.signUp({
                email,
                password,
                options: { data: { username } }
            });
            if (error) {
                errorEl.textContent = error.message;
            } else {
                errorEl.textContent = '';
            }
        });

        // ── Close modal ──
        document.getElementById('btn-close-auth').addEventListener('click', () => {
            document.getElementById('auth-overlay').style.display = 'none';
        });

        // ── Nav login button ──
        document.getElementById('btn-nav-login').addEventListener('click', () => {
            document.getElementById('auth-overlay').style.display = 'flex';
        });

        // ── Logout ──
        document.getElementById('btn-logout').addEventListener('click', async () => {
            await sbClient.auth.signOut();
            document.getElementById('user-dropdown').classList.remove('open');
        });

        // ── Avatar dropdown toggle ──
        document.getElementById('user-avatar').addEventListener('click', (e) => {
            e.stopPropagation();
            const dropdown = document.getElementById('user-dropdown');
            const rect = e.currentTarget.getBoundingClientRect();
            
            dropdown.style.position = 'fixed';
            dropdown.style.top = (rect.bottom + 8) + 'px';
            dropdown.style.right = (window.innerWidth - rect.right) + 'px';
            
            dropdown.classList.toggle('open');
        });
        document.addEventListener('click', () => {
            const dropdown = document.getElementById('user-dropdown');
            if (dropdown) dropdown.classList.remove('open');
        });

    } catch (e) {
        console.error('Auth init failed', e);
        hideLoadingOverlay();
    }
}

let isDataLoaded = false;

/**
 * handleSessionUpdate — UI-ONLY. Updates avatar, skeleton, overlay.
 * All data loading is handled by the DOMContentLoaded orchestrator.
 */
function handleSessionUpdate(session) {
    const overlay = document.getElementById('auth-overlay');
    const btnLogin = document.getElementById('btn-nav-login');
    const avatarWrap = document.getElementById('user-avatar-wrap');
    const avatar = document.getElementById('user-avatar');
    const displayName = document.getElementById('user-display-name');
    const emailEl = document.getElementById('user-email');
    
    // Hide the skeleton placeholder now that auth has resolved
    const skeleton = document.getElementById('user-skeleton');
    if (skeleton) skeleton.style.display = 'none';

    if (!session) {
        overlay.style.display = 'none';
        btnLogin.style.display = 'inline-block';
        avatarWrap.style.display = 'none';
    } else {
        overlay.style.display = 'none';
        btnLogin.style.display = 'none';
        avatarWrap.style.display = 'flex';

        const meta = session.user.user_metadata || {};
        const username = meta.username || session.user.email.split('@')[0];
        const initial = username.charAt(0).toUpperCase();

        avatar.textContent = initial;
        displayName.textContent = username;
        emailEl.textContent = session.user.email;
    }

    document.querySelectorAll('.app-content').forEach(e => e.style.display = 'flex');
}

async function apiFetch(url, options = {}) {
    if (!options.headers) options.headers = {};
    if (currentSession) {
        options.headers['Authorization'] = `Bearer ${currentSession.access_token}`;
    }
    const res = await fetch(url, options);
    return res;
}

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
  lastRefresh:   null,        // ISO string from server (for staleness display)
};

// ── localStorage Cache ─────────────────────────────────────────────────────
const CACHE_KEY = "coreprop_bootstrap_cache";
const CACHE_FRESH_SECONDS = 60; // Skip background fetch if cache is younger than this

/**
 * Save the entire bootstrap payload to localStorage for instant-load on refresh.
 */
function saveToCache(data) {
  try {
    const payload = {
      timestamp: Date.now(),
      data: data
    };
    localStorage.setItem(CACHE_KEY, JSON.stringify(payload));
  } catch (e) {
    console.warn("localStorage save failed:", e);
  }
}

/**
 * Hydrate the application state from localStorage if available.
 * Returns { success: boolean, ageSeconds: number | null }
 */
function hydrateFromCache() {
  try {
    const raw = localStorage.getItem(CACHE_KEY);
    if (!raw) return { success: false, ageSeconds: null };
    const { timestamp, data } = JSON.parse(raw);
    if (!data) return { success: false, ageSeconds: null };

    const ageSeconds = timestamp ? (Date.now() - timestamp) / 1000 : null;

    // Populate all sub-states
    state.allBets          = data.bets      || [];
    matchedState.allLines  = data.matches   || [];
    ppState.allLines       = data.pp_lines  || [];
    fdState.allLines       = data.fd_lines  || [];
    dkState.allLines       = data.dk_lines  || [];
    pinState.allLines      = data.pin_lines || [];
    if (data.last_refresh) state.lastRefresh = data.last_refresh;

    // Trigger initial renders
    applyFilters();
    applyMatchedFilters();
    applyPPFilters();
    applyFDFilters();
    applyDKFilters();
    applyPinFilters();
    
    return { success: true, ageSeconds };
  } catch (e) {
    console.warn("localStorage hydration failed:", e);
    return { success: false, ageSeconds: null };
  }
}

// ── DOM refs ───────────────────────────────────────────────────────────────
const $ = id => document.getElementById(id);
const tbody           = $("bets-tbody");
const totalBadge      = $("total-badge");
const selectedCountEl = $("selected-count");
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
      <td data-label="Player">${b.player_name}${loggedBadge}</td>
      <td data-label="League">${b.league}</td>
      <td data-label="Prop">${b.prop_type}</td>
      <td data-label="Line">${b.pp_line}${lineDiff}</td>
      <td data-label="Side" class="side-${b.side}">${b.side.toUpperCase()}</td>
      <td data-label="True Odds">${fmt.trueOdds(b.true_odds)}</td>
      <td data-label="Edge" class="${evClass(b.edge)}">${fmt.pct(b.edge)}</td>
      <td data-label="Ind. EV%" class="${evClass(b.individual_ev_pct)}">${fmt.pct(b.individual_ev_pct)}</td>
      <td data-label="Book Odds" style="color:var(--text-muted)">${bookOddsHtml}</td>
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
  
  if (!currentSession) {
    document.getElementById('auth-overlay').style.display = 'flex';
    return;
  }

  btnAddToBacktest.disabled = true;
  btnAddToBacktest.textContent = "Logging...";

  try {
    const resp = await apiFetch("/api/backtest/add-slip", {
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
    const resp = await apiFetch("/api/slip", {
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
    const resp = await apiFetch("/api/slip/auto", {
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
    const resp = await apiFetch("/api/bets");
    const data = await resp.json();
    state.allBets = data.bets || [];
    if (data.last_refresh) state.lastRefresh = data.last_refresh;
    applyFilters();
  } catch (e) {
    console.error("Failed to fetch bets:", e);
  }
}

async function fetchStatus() {
  try {
    const resp = await apiFetch("/api/status");
    const data = await resp.json();
    // UI updates removed

    // Detect scraping just finished → refresh every dataset in one shot.
    if (state.isScrapingPrev && !data.is_scraping) {
      const ok = await fetchBootstrap();
      if (!ok) {
        await Promise.all([fetchBets(), fetchMatched(), fetchPP(), fetchFD(), fetchDK(), fetchPin()]);
      }
      await fetchBacktest();
      await fetchCalibration();
    }
    state.isScrapingPrev = data.is_scraping;

    // UI updates removed
  } catch (e) {
    // Silent fail
  }
}



// ── Tab switching ─────────────────────────────────────────────────────────
document.querySelectorAll(".tab").forEach(tab => {
  tab.addEventListener("click", () => {
    document.querySelectorAll(".tab").forEach(t => t.classList.remove("active"));
    tab.classList.add("active");

    const target = tab.dataset.tab;
    
    // Auth guard for Backtest and Analytics tabs
    if ((target === "backtest" || target === "analytics") && !currentSession) {
      document.getElementById('auth-overlay').style.display = 'flex';
      return; 
    }

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
      <td data-label="Player">${l.player_name}</td>
      <td data-label="League"><span class="league-tag league-${l.league}">${l.league}</span></td>
      <td data-label="Prop">${l.stat_type}</td>
      <td data-label="Line" class="line-value">${l.pp_line}${lineDiff}</td>
      <td data-label="Side" class="${sideClass}">${l.side.toUpperCase()}</td>
      <td data-label="True Odds" class="line-value">${fmt.trueOdds(l.true_odds)}</td>
      <td data-label="Best Odds" class="line-value">${fmt.odds(l.best_odds)}</td>
      <td data-label="FD Odds" class="line-value">${fmt.odds(l.fd_odds)}</td>
      <td data-label="DK Odds" class="line-value">${fmt.odds(l.dk_odds)}</td>
      <td data-label="PIN Odds" class="line-value">${fmt.odds(l.pin_odds)}</td>
      <td data-label="Game Time" class="game-time">${gameTime}</td>
    </tr>`;
  }).join("");
}

async function fetchMatched() {
  try {
    const resp = await apiFetch("/api/matched");
    const data = await resp.json();
    matchedState.allLines = data.matches || [];
    if (data.last_refresh) state.lastRefresh = data.last_refresh;
    applyMatchedFilters();
  } catch (e) {
    console.error("Failed to fetch matched lines:", e);
  }
}

async function fetchPP() {
  try {
    const resp = await apiFetch("/api/prizepicks");
    const data = await resp.json();
    ppState.allLines = data.lines || [];
    if (data.last_refresh) state.lastRefresh = data.last_refresh;
    applyPPFilters();
  } catch (e) {
    console.error("Failed to fetch PrizePicks lines:", e);
  }
}

async function fetchFD() {
  try {
    const resp = await apiFetch("/api/fanduel");
    const data = await resp.json();
    fdState.allLines = data.lines || [];
    if (data.last_refresh) state.lastRefresh = data.last_refresh;
    applyFDFilters();
  } catch (e) {
    console.error("Failed to fetch FanDuel lines:", e);
  }
}

async function fetchDK() {
  try {
    const resp = await apiFetch("/api/draftkings");
    const data = await resp.json();
    dkState.allLines = data.lines || [];
    if (data.last_refresh) state.lastRefresh = data.last_refresh;
    applyDKFilters();
  } catch (e) {
    console.error("Failed to fetch DraftKings lines:", e);
  }
}

async function fetchPin() {
  try {
    const resp = await apiFetch("/api/pinnacle");
    const data = await resp.json();
    pinState.allLines = data.lines || [];
    if (data.last_refresh) state.lastRefresh = data.last_refresh;
    applyPinFilters();
  } catch (e) {
    console.error("Failed to fetch Pinnacle lines:", e);
  }
}

// ── Bootstrap: single-request path for initial load / refresh ─────────────
// Always fetches fresh — cache: 'no-store' + cache-buster query param so no
// intermediate HTTP cache (service worker, CDN, browser) can serve stale data.
async function fetchBootstrap() {
  try {
    const resp = await apiFetch("/api/bootstrap?t=" + Date.now(), { cache: "no-store" });
    if (!resp.ok) throw new Error("bootstrap HTTP " + resp.status);
    const data = await resp.json();

    // Cache for next time
    saveToCache(data);

    state.allBets          = data.bets      || [];
    matchedState.allLines  = data.matches   || [];
    ppState.allLines       = data.pp_lines  || [];
    fdState.allLines       = data.fd_lines  || [];
    dkState.allLines       = data.dk_lines  || [];
    pinState.allLines      = data.pin_lines || [];
    if (data.last_refresh) state.lastRefresh = data.last_refresh;

    applyFilters();
    applyMatchedFilters();
    applyPPFilters();
    applyFDFilters();
    applyDKFilters();
    applyPinFilters();
    return true;
  } catch (e) {
    console.error("Bootstrap fetch failed, falling back to per-endpoint:", e);
    return false;
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
    ppTbody.innerHTML = `<tr><td colspan="6" class="empty-msg">No lines match current filters.</td></tr>`;
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
      <td data-label="Player">${l.player_name}</td>
      <td data-label="League"><span class="league-tag league-${l.league}">${l.league}</span></td>
      <td data-label="Prop">${l.stat_type}</td>
      <td data-label="Line" class="line-value">${l.line_score}</td>
      <td data-label="Side" class="side-${l.side}">${l.side.toUpperCase()}</td>
      <td data-label="Game Time" class="game-time">${gameTime}</td>
    </tr>`;
  }).join("");
}


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
    fdTbody.innerHTML = `<tr><td colspan="8" class="empty-msg">No lines match current filters.</td></tr>`;
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
      <td data-label="Player">${l.player_name}</td>
      <td data-label="League"><span class="league-tag league-${l.league}">${l.league}</span></td>
      <td data-label="Prop">${l.stat_type}</td>
      <td data-label="Line" class="line-value">${l.line_score}</td>
      <td data-label="Side" class="side-${l.side}">${l.side.toUpperCase()}</td>
      <td data-label="True Odds" class="line-value">${fmt.trueOdds(l.true_odds)}</td>
      <td data-label="Book Odds" class="line-value">${fmt.odds(l.line_odds)}</td>
      <td data-label="Game Time" class="game-time">${gameTime}</td>
    </tr>`;
  }).join("");
}


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
    dkTbody.innerHTML = `<tr><td colspan="8" class="empty-msg">No lines match current filters.</td></tr>`;
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
      <td data-label="Player">${l.player_name}</td>
      <td data-label="League"><span class="league-tag league-${l.league}">${l.league}</span></td>
      <td data-label="Prop">${l.stat_type}</td>
      <td data-label="Line" class="line-value">${l.line_score}</td>
      <td data-label="Side" class="side-${l.side}">${l.side.toUpperCase()}</td>
      <td data-label="True Odds" class="line-value">${fmt.trueOdds(l.true_odds)}</td>
      <td data-label="Book Odds" class="line-value">${fmt.odds(l.line_odds)}</td>
      <td data-label="Game Time" class="game-time">${gameTime}</td>
    </tr>`;
  }).join("");
}


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
    pinTbody.innerHTML = `<tr><td colspan="8" class="empty-msg">No lines match current filters.</td></tr>`;
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
      <td data-label="Player">${l.player_name}</td>
      <td data-label="League"><span class="league-tag league-${l.league}">${l.league}</span></td>
      <td data-label="Prop">${l.stat_type}</td>
      <td data-label="Line" class="line-value">${l.line_score}</td>
      <td data-label="Side" class="side-${l.side}">${l.side.toUpperCase()}</td>
      <td data-label="True Odds" class="line-value">${fmt.trueOdds(l.true_odds)}</td>
      <td data-label="Book Odds" class="line-value">${fmt.odds(l.line_odds)}</td>
      <td data-label="Game Time" class="game-time">${gameTime}</td>
    </tr>`;
  }).join("");
}


// ── Backtest Dashboard ────────────────────────────────────────────────────

let btSlips = [];   // raw slip objects from API
const btState = {
  page: 1,
  pageSize: 100
};

async function fetchBacktest() {
  try {
    const resp = await apiFetch("/api/backtest/slips");
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




  const totalLegsCount = allLegs.length;
  const checkedLegs = allLegs.filter(l => l.result === "hit" || l.result === "miss");
  const hitLegs = checkedLegs.filter(l => l.result === "hit").length;
  const completedLegsCount = checkedLegs.length;

  let legHitRateText = "—";
  let legHitRateClass = "bt-card-value";
  let expectedHitRateText = "—";

  if (completedLegsCount > 0) {
    const avgExp = checkedLegs.reduce((sum, l) => sum + (parseFloat(l.true_prob) || 0), 0) / completedLegsCount;
    expectedHitRateText = (avgExp * 100).toFixed(1) + "%";
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

  if ($("bt-slips-stat")) {
    $("bt-slips-stat").textContent = `${completedSlips.length} / ${totalSlips}`;
  }
  $("bt-hit-rate").textContent = slipHitRate;
  $("bt-hit-rate").className = "bt-card-value" + (completedSlips.length > 0 && slipHits / completedSlips.length >= 0.3 ? " positive" : completedSlips.length > 0 ? " negative" : "");
  
  if ($("bt-legs-stat")) {
    $("bt-legs-stat").textContent = `${completedLegsCount} / ${totalLegsCount}`;
  }
  if ($("bt-leg-hit-rate")) {
    $("bt-leg-hit-rate").innerHTML = legHitRateText;
    $("bt-leg-hit-rate").className = legHitRateClass;
  }
  if ($("bt-expected-hit-rate")) {
    $("bt-expected-hit-rate").textContent = expectedHitRateText;
  }

  $("bt-roi").textContent = roi;
  $("bt-roi").className = "bt-card-value" + (roiPositive ? " positive" : totalWagered > 0 ? " negative" : "");


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
    const closeP = (l.closing_prob !== undefined && l.closing_prob !== null && l.closing_prob !== "") ? (parseFloat(l.closing_prob) * 100).toFixed(1) + "%" : "—";
    const clvPctVal = (l.clv_pct !== undefined && l.clv_pct !== null && l.clv_pct !== "") ? parseFloat(l.clv_pct) : null;
    const clvPctText = clvPctVal !== null ? (clvPctVal > 0 ? "+" : "") + (clvPctVal * 100).toFixed(1) + "%" : "—";
    const clvCls = clvPctVal !== null ? (clvPctVal > 0 ? "ev-high" : clvPctVal < 0 ? "ev-low" : "") : "";

    const resultCls = l.result === "hit" ? "result-hit" : l.result === "miss" ? "result-miss" : l.result === "dnp" ? "result-dnp" : "result-pending";
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

    let headerHtml = "";
    if (isFirst) {
      headerHtml = `<tr class="slip-header-row">
        <td colspan="12">
          <div class="slip-header-content">
            <span class="slip-header-stat">
              <span class="slip-header-label">Slip</span>
              <span class="slip-header-id">${l.slip_id}</span>
            </span>
            <span class="slip-header-stat">
              <span class="slip-header-value">${ts}</span>
            </span>
            <span class="slip-header-stat">
              <span class="slip-header-value">${l.slip_type}</span>
              <span class="slip-header-label">(${l.n_legs} Legs)</span>
            </span>
            <span class="slip-header-stat">
              <span class="slip-header-label">Proj EV</span>
              <span class="slip-header-value ev-high">${evPct}</span>
            </span>
            <span class="slip-header-stat">
              <span class="slip-header-label">Payout</span>
              <span class="slip-header-value" style="color:var(--yellow);">${payoutHtml}</span>
            </span>
          </div>
        </td>
      </tr>`;
    }

    return headerHtml + `<tr>
      <td data-label="Player"><strong>${l.player || ""}</strong></td>
      <td data-label="League"><span class="league-tag league-${(l.league || "").toUpperCase()}">${l.league || ""}</span></td>
      <td data-label="Prop">${l.prop || ""}</td>
      <td data-label="Line" class="line-value">${l.line || ""}</td>
      <td data-label="Side" class="${l.side === "over" ? "side-over" : "side-under"}">${(l.side || "").toUpperCase()}</td>
      <td data-label="True Prob">${trueP}</td>
      <td data-label="Close Prob">${closeP}</td>
      <td data-label="CLV%" class="${clvCls}" style="font-weight:600;">${clvPctText}</td>
      <td data-label="Ind. EV%" class="ev-medium">${indEv}</td>
      <td data-label="Game Time">${gameTime}</td>
      <td data-label="Result"><span class="${resultCls}">${resultText.toUpperCase()}</span></td>
      <td data-label="Actual">${(l.stat_actual !== null && l.stat_actual !== undefined && l.stat_actual !== "") ? l.stat_actual : "—"}</td>
    </tr>`;
  }).join("");
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

// Backtest action buttons (Refresh / Download CSV / Check Results) removed:
// Supabase is authoritative and everything refreshes automatically now.

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
    const resp = await apiFetch("/api/backtest/latest-slip");
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

// Chart.js instances — kept so we can destroy before redraw.
const _charts = { pnl: null, cal: null, slipMix: null };

async function fetchCalibration() {
  // Retained name for backwards-compat with existing callers, but hits the
  // richer /api/analytics endpoint now.
  try {
    const resp = await apiFetch("/api/analytics");
    if (!resp.ok) return;
    const data = await resp.json();
    renderCalibration(data);
    renderAnalyticsExtras(data);
  } catch (e) {
    console.error("Analytics fetch error:", e);
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
  if (llEl) {
    if (data.log_loss != null) {
      llEl.textContent = data.log_loss.toFixed(4);
      llEl.className = "bt-card-value" + (data.log_loss < 0.65 ? " positive" : data.log_loss < 0.70 ? "" : " negative");
    } else {
      llEl.textContent = "\u2014";
      llEl.className = "bt-card-value";
    }
  }

  $("cal-resolved").textContent = data.n_resolved || 0;

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

  // Hit Rate Delta: actual - expected, in percentage points
  const deltaEl = $("cal-delta");
  if (deltaEl) {
    if (data.hit_rate != null && data.avg_predicted_prob != null) {
      const d = (data.hit_rate - data.avg_predicted_prob) * 100;
      deltaEl.textContent = (d >= 0 ? "+" : "") + d.toFixed(1) + "pp";
      deltaEl.className = "bt-card-value" + (d > 0.5 ? " positive" : d < -0.5 ? " negative" : "");
    } else {
      deltaEl.textContent = "\u2014";
      deltaEl.className = "bt-card-value";
    }
  }

  // Calibration buckets table (50-80%)
  const tbody = $("cal-tbody");
  const buckets = data.calibration_buckets || [];

  if (!tbody) return;
  if (buckets.length === 0) {
    tbody.innerHTML = '<tr><td colspan="5" class="empty-msg">No high-prop data available yet.</td></tr>';
    return;
  }

  tbody.innerHTML = buckets.map(b => {
    if (b.count === 0) {
      return `<tr>
        <td><div style="font-weight:700;">${b.bucket}</div></td>
        <td style="font-family:var(--font-mono); opacity:0.3;">\u2014</td>
        <td style="font-family:var(--font-mono); opacity:0.3;">\u2014</td>
        <td style="opacity:0.3;">0</td>
        <td><span class="cal-tag" style="opacity:0.2;">No Data</span></td>
      </tr>`;
    }

    const predicted = (b.predicted_avg * 100).toFixed(1) + "%";
    const actual = (b.actual_avg * 100).toFixed(1) + "%";
    const diff = b.actual_avg - b.predicted_avg;
    const diffPct = (diff * 100).toFixed(1);
    const diffSign = diff >= 0 ? "+" : "";
    
    // Status Logic
    const absDiff = Math.abs(diff);
    let statusClass = "off";
    if (absDiff < 0.02) statusClass = "perfect";
    else if (absDiff < 0.05) statusClass = "good";

    const alignLabel = diff > 0.02 ? "Under" : diff < -0.02 ? "Over" : "OK";

    return `<tr>
      <td data-label="Bucket"><div style="font-weight:700;">${b.bucket}</div></td>
      <td data-label="Predicted" style="font-family:var(--font-mono); opacity:0.8;">${predicted}</td>
      <td data-label="Actual" style="font-family:var(--font-mono); font-weight:700;">${actual}</td>
      <td data-label="Count" style="opacity:0.7;">${b.count}</td>
      <td data-label="Edge">
        <div style="display:flex; align-items:center; gap:6px;">
          <span class="cal-delta ${statusClass}">${diffSign}${diffPct}pp</span>
          <span class="cal-tag">${alignLabel}</span>
        </div>
      </td>
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


// ── Analytics extras: charts + per-league/prop tables ─────────────────────
function renderAnalyticsExtras(data) {
  if (typeof Chart === "undefined") {
    console.warn("Chart.js not loaded — skipping analytics charts.");
    return;
  }
  _renderPnlChart(data);
  _renderCalibrationPlot(data);
  _renderSlipMixChart(data);
  _renderPerfTable("league-perf-tbody", data.by_league || []);
  _renderPerfTable("prop-perf-tbody",   data.by_prop   || []);

  // PnL summary line
  const subtitle = $("pnl-summary");
  if (subtitle) {
    const n = data.resolved_slips || 0;
    const roi = data.roi_per_slip;
    const total = (data.pnl_timeline && data.pnl_timeline.length)
      ? data.pnl_timeline[data.pnl_timeline.length - 1].cum_pnl : 0;
    subtitle.textContent = n > 0
      ? `${n} resolved slips · net ${total >= 0 ? "+" : ""}${total.toFixed(2)}u · ROI/slip ${roi != null ? (roi * 100).toFixed(1) + "%" : "—"}`
      : "No resolved slips yet.";
  }

  const mixSub = $("slip-mix-subtitle");
  if (mixSub) {
    const m = data.slip_mix || {};
    mixSub.textContent = `Won ${m.won || 0} · Partial ${m.partial || 0} · Lost ${m.lost || 0} · Pending ${m.pending || 0}`;
  }
}

function _renderPerfTable(tbodyId, rows) {
  const tbody = $(tbodyId);
  if (!tbody) return;
  if (!rows.length) {
    tbody.innerHTML = '<tr><td colspan="6" class="empty-msg">No resolved legs yet.</td></tr>';
    return;
  }
  tbody.innerHTML = rows.map(r => {
    const actual = r.actual != null ? (r.actual * 100).toFixed(1) + "%" : "—";
    const expect = r.expected != null ? (r.expected * 100).toFixed(1) + "%" : "—";
    const d = r.delta;
    const dTxt = d != null ? ((d >= 0 ? "+" : "") + (d * 100).toFixed(1) + "pp") : "—";
    const dCls = d == null ? "" : d > 0.005 ? "positive" : d < -0.005 ? "negative" : "";
    return `<tr>
      <td>${r.key}</td>
      <td>${r.legs}</td>
      <td>${r.hits}</td>
      <td>${actual}</td>
      <td style="opacity:0.75">${expect}</td>
      <td class="${dCls}">${dTxt}</td>
    </tr>`;
  }).join("");
}

function _chartTextColor() {
  return getComputedStyle(document.body).getPropertyValue("--text") || "#ddd";
}

function _renderPnlChart(data) {
  const ctx = document.getElementById("chart-pnl");
  if (!ctx) return;
  const points = data.pnl_timeline || [];
  const labels = points.map((_, i) => i + 1);
  const cum    = points.map(p => p.cum_pnl);

  if (_charts.pnl) _charts.pnl.destroy();
  _charts.pnl = new Chart(ctx, {
    type: "line",
    data: {
      labels,
      datasets: [{
        label: "Cumulative P&L (units)",
        data: cum,
        borderColor: "#4ade80",
        backgroundColor: "rgba(74, 222, 128, 0.15)",
        fill: true,
        tension: 0.2,
        pointRadius: 0,
        borderWidth: 2,
      }],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { title: { display: true, text: "Resolved slip #" }, ticks: { color: _chartTextColor() } },
        y: { title: { display: true, text: "Units"   }, ticks: { color: _chartTextColor() } },
      },
    },
  });
}

function _renderCalibrationPlot(data) {
  const ctx = document.getElementById("chart-cal");
  if (!ctx) return;
  const buckets = (data.calibration_buckets || []).filter(b => b.count > 0);
  const pts = buckets.map(b => ({ x: b.predicted_avg, y: b.actual_avg, r: Math.max(4, Math.sqrt(b.count) * 2) }));

  if (_charts.cal) _charts.cal.destroy();
  _charts.cal = new Chart(ctx, {
    type: "bubble",
    data: {
      datasets: [
        {
          label: "Buckets",
          data: pts,
          backgroundColor: "rgba(96, 165, 250, 0.6)",
          borderColor: "#60a5fa",
        },
        {
          type: "line",
          label: "Perfect",
          data: [{ x: 0.5, y: 0.5 }, { x: 0.8, y: 0.8 }],
          borderColor: "rgba(255,255,255,0.4)",
          borderDash: [5, 5],
          pointRadius: 0,
          fill: false,
          borderWidth: 1,
        },
      ],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { min: 0.45, max: 0.85, title: { display: true, text: "Predicted" }, ticks: { color: _chartTextColor() } },
        y: { min: 0.30, max: 1.00, title: { display: true, text: "Actual"   }, ticks: { color: _chartTextColor() } },
      },
    },
  });
}

function _renderSlipMixChart(data) {
  const ctx = document.getElementById("chart-slip-mix");
  if (!ctx) return;
  const m = data.slip_mix || {};
  const values = [m.won || 0, m.partial || 0, m.lost || 0, m.pending || 0];

  if (_charts.slipMix) _charts.slipMix.destroy();
  _charts.slipMix = new Chart(ctx, {
    type: "doughnut",
    data: {
      labels: ["Won", "Partial", "Lost", "Pending"],
      datasets: [{
        data: values,
        backgroundColor: ["#4ade80", "#fbbf24", "#f87171", "#6b7280"],
        borderWidth: 0,
      }],
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { position: "bottom", labels: { color: _chartTextColor() } } },
    },
  });
}


// ── Loading overlay ────────────────────────────────────────────────────────
// Shown on every open until the first bootstrap response arrives, so users
// know data is coming instead of seeing stale-looking empty tables.
function showLoadingOverlay(msg) {
  const el = document.getElementById("loading-overlay");
  if (!el) return;
  if (msg) {
    const t = el.querySelector(".loading-text") || document.getElementById("loading-overlay-text");
    if (t) t.textContent = msg;
  }
  el.classList.remove("hidden");
}
function hideLoadingOverlay() {
  const el = document.getElementById("loading-overlay");
  if (el) el.classList.add("hidden");
}

// ── Init ───────────────────────────────────────────────────────────────────
// Single orchestrator: hydrate cache → parallel auth + data → auth-gated data.
window.addEventListener('DOMContentLoaded', async () => {
    // Step 1: Instant render from localStorage (< 10ms)
    const cache = hydrateFromCache();
    if (cache.success) {
        isDataLoaded = true;
        hideLoadingOverlay();
        document.querySelectorAll('.app-content').forEach(e => e.style.display = 'flex');
    }

    // Step 2: Determine if we need a background data refresh
    const cacheIsFresh = cache.success && cache.ageSeconds !== null && cache.ageSeconds < CACHE_FRESH_SECONDS;
    const bootstrapPromise = cacheIsFresh
        ? Promise.resolve(true)  // Cache is fresh enough, skip network
        : fetchBootstrap().catch(err => { console.error("Bootstrap failed:", err); return false; });

    // Step 3: Auth + data fetch run IN PARALLEL
    const authPromise = initAuth().catch(err => console.error("Auth init failed:", err));
    
    // Wait for both to settle
    const [bootstrapOk] = await Promise.all([bootstrapPromise, authPromise]);

    // Step 4: If we had no cache AND bootstrap just completed, mark loaded
    if (!isDataLoaded) {
        isDataLoaded = true;
    }
    hideLoadingOverlay();
    document.querySelectorAll('.app-content').forEach(e => e.style.display = 'flex');

    // Step 5: Auth-gated data (only if user is logged in)
    if (currentSession) {
        Promise.all([fetchBacktest(), fetchCalibration()]).catch(() => {});
    }
});

// Slip panel drawer toggle for mobile (one-time binding)
window.addEventListener('DOMContentLoaded', () => {
    const sp = document.getElementById("slip-panel");
    const h2 = sp && sp.querySelector("h2");
    if (h2) {
        h2.addEventListener("click", () => {
            if (window.innerWidth <= 900) sp.classList.toggle("open");
        });
    }
});

fetchStatus();
setInterval(fetchStatus, 10_000);
// Auto-logging was removed, so polling latest slip is no longer needed
// setInterval(pollLatestSlip, 10_000);
// pollLatestSlip();
