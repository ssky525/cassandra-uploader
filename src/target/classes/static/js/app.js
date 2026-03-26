const $ = (id) => document.getElementById(id);

function showError(msg) {
  const b = $("errorBanner");
  b.textContent = msg || "";
  b.classList.toggle("visible", !!msg);
}

async function api(path, opts = {}) {
  const r = await fetch(path, {
    credentials: "include",
    headers: {
      "Content-Type": "application/json",
      ...(opts.headers || {}),
    },
    ...opts,
  });
  if (r.status === 401) {
    setConnectedUi(false);
    throw new Error("Сессия не подключена к Cassandra. Подключитесь снова.");
  }
  return r;
}

function profileFromForm() {
  const sel = $("profileSelect");
  const existingId = sel.value && sel.selectedOptions[0]?.dataset?.id;
  return {
    id: existingId || undefined,
    displayName: $("profileName").value.trim(),
    contactPoints: $("contactPoints").value.trim(),
    port: parseInt($("port").value.trim(), 10) || 9042,
    localDatacenter: $("localDc").value.trim(),
    username: $("username").value.trim(),
    password: $("password").value,
  };
}

function fillForm(p) {
  $("profileName").value = p.displayName || "";
  $("contactPoints").value = p.contactPoints || "";
  $("port").value = String(p.port || 9042);
  $("localDc").value = p.localDatacenter || "";
  $("username").value = p.username || "";
  $("password").value = p.password || "";
}

async function loadProfiles() {
  const r = await api("/api/connections");
  const list = await r.json();
  const sel = $("profileSelect");
  sel.innerHTML = '<option value="">— новый профиль —</option>';
  for (const p of list) {
    const o = document.createElement("option");
    o.value = p.displayName;
    o.dataset.id = p.id;
    o.textContent = p.displayName || p.contactPoints;
    sel.appendChild(o);
  }
}

$("profileSelect").addEventListener("change", async () => {
  const sel = $("profileSelect");
  if (!sel.value) return;
  const r = await api("/api/connections");
  const list = await r.json();
  const id = sel.selectedOptions[0]?.dataset?.id;
  const p = list.find((x) => x.id === id);
  if (p) fillForm(p);
});

$("btnSaveProfile").addEventListener("click", async () => {
  showError("");
  const p = profileFromForm();
  if (!p.displayName) {
    showError("Укажите имя профиля");
    return;
  }
  const r = await api("/api/connections", { method: "POST", body: JSON.stringify(p) });
  if (!r.ok) {
    showError("Не удалось сохранить профиль");
    return;
  }
  await loadProfiles();
});

function setConnectedUi(on) {
  $("connStatus").textContent = on ? "Подключено" : "Не подключено";
  $("connStatus").className = "status " + (on ? "on" : "off");
  ["btnTopology", "btnRefreshSchema", "btnDbSample", "btnPreview", "btnLoad"].forEach((id) => {
    $(id).disabled = !on;
  });
}

$("btnConnect").addEventListener("click", async () => {
  showError("");
  const p = profileFromForm();
  const r = await api("/api/session/connect", { method: "POST", body: JSON.stringify(p) });
  const data = await r.json();
  if (!r.ok || !data.ok) {
    showError(data.error || "Ошибка подключения");
    setConnectedUi(false);
    return;
  }
  setConnectedUi(true);
  const hintEl = $("hints");
  if (data.hints && data.hints.length) {
    hintEl.textContent = data.hints.join(" ");
  } else {
    hintEl.textContent = "";
  }
  await refreshKeyspaces();
});

$("btnDisconnect").addEventListener("click", async () => {
  showError("");
  $("hints").textContent = "";
  await api("/api/session/disconnect", { method: "POST" });
  setConnectedUi(false);
  $("topologyBox").style.display = "none";
});

async function checkStatus() {
  try {
    const r = await api("/api/session/status");
    const j = await r.json();
    setConnectedUi(!!j.connected);
    if (j.connected) await refreshKeyspaces();
  } catch {
    setConnectedUi(false);
  }
}

$("btnTopology").addEventListener("click", async () => {
  const r = await api("/api/session/topology");
  const t = await r.json();
  const lines = [t.clusterName + ":", ...t.nodes.map((n) =>
    `  ${n.endpoint}  DC=${n.datacenter}  rack=${n.rack}  ${n.state}`)];
  $("topologyBox").textContent = lines.join("\n");
  $("topologyBox").style.display = "block";
});

async function refreshKeyspaces() {
  const r = await api("/api/schema/keyspaces");
  const ks = await r.json();
  const dl = $("keyspace-list");
  dl.innerHTML = "";
  for (const k of ks) {
    const o = document.createElement("option");
    o.value = k;
    dl.appendChild(o);
  }
}

async function refreshTables() {
  const ks = $("keyspace").value.trim();
  if (!ks) return;
  const r = await api("/api/schema/tables?keyspace=" + encodeURIComponent(ks));
  const tables = await r.json();
  const dl = $("table-list");
  dl.innerHTML = "";
  for (const t of tables) {
    const o = document.createElement("option");
    o.value = t;
    dl.appendChild(o);
  }
}

$("btnRefreshSchema").addEventListener("click", async () => {
  showError("");
  await refreshKeyspaces();
  await refreshTables();
});

$("keyspace").addEventListener("change", refreshTables);
$("keyspace").addEventListener("blur", refreshTables);

function renderTable(headId, bodyId, columns, rows) {
  const head = $(headId);
  const body = $(bodyId);
  head.innerHTML = "";
  body.innerHTML = "";
  for (const c of columns) {
    const th = document.createElement("th");
    th.textContent = c;
    head.appendChild(th);
  }
  for (const row of rows) {
    const tr = document.createElement("tr");
    for (const cell of row) {
      const td = document.createElement("td");
      td.textContent = cell == null ? "" : String(cell);
      tr.appendChild(td);
    }
    body.appendChild(tr);
  }
}

$("btnPreview").addEventListener("click", async () => {
  showError("");
  const f = $("csvFile").files[0];
  if (!f) {
    showError("Выберите CSV файл");
    return;
  }
  const fd = new FormData();
  fd.append("file", f);
  fd.append("firstRowHeader", $("firstRowHeader").checked);
  fd.append("keyspace", $("keyspace").value.trim());
  fd.append("table", $("table").value.trim());
  const r = await fetch("/api/csv/preview", { method: "POST", body: fd, credentials: "include" });
  if (!r.ok) {
    const t = await r.text();
    showError(t || "Ошибка предпросмотра");
    return;
  }
  const data = await r.json();
  renderTable("previewHead", "previewBody", data.columns, data.rows);
});

$("btnDbSample").addEventListener("click", async () => {
  showError("");
  const ks = $("keyspace").value.trim();
  const tb = $("table").value.trim();
  const r = await api(
    "/api/query/sample?keyspace=" + encodeURIComponent(ks) + "&table=" + encodeURIComponent(tb)
  );
  if (!r.ok) {
    showError(await r.text());
    return;
  }
  const data = await r.json();
  renderTable("dbHead", "dbBody", data.columns, data.rows);
});

let pollTimer = null;

$("btnLoad").addEventListener("click", async () => {
  showError("");
  const f = $("csvFile").files[0];
  if (!f) {
    showError("Выберите CSV файл");
    return;
  }
  $("log").value = "";
  const fd = new FormData();
  fd.append("file", f);
  fd.append("firstRowHeader", $("firstRowHeader").checked);
  fd.append("keyspace", $("keyspace").value.trim());
  fd.append("table", $("table").value.trim());
  fd.append("maxRows", String(parseInt($("maxRows").value, 10) || 1));
  const r = await fetch("/api/csv/load", { method: "POST", body: fd, credentials: "include" });
  if (!r.ok) {
    showError(await r.text());
    return;
  }
  const { jobId } = await r.json();
  if (pollTimer) clearInterval(pollTimer);
  pollTimer = setInterval(async () => {
    const pr = await fetch("/api/csv/load/" + encodeURIComponent(jobId), { credentials: "include" });
    if (!pr.ok) return;
    const st = await pr.json();
    $("log").value = (st.log || []).join("\n");
    $("log").scrollTop = $("log").scrollHeight;
    if (st.done) {
      clearInterval(pollTimer);
      pollTimer = null;
      if (st.error) showError(st.error);
    }
  }, 400);
});

loadProfiles();
checkStatus();
