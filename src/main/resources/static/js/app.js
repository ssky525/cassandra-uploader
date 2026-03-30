const $ = (id) => document.getElementById(id);

let profiles = [];
let selectedId = null;
let modalEditingId = null;

/** Последняя таблица для кнопки «Показать» и смены LIMIT */
let lastBrowse = { ks: null, tb: null };

function clearSchemaTreeSelection() {
  $("schemaTree").querySelectorAll(".tree-label.tree-selected").forEach((el) => {
    el.classList.remove("tree-selected");
  });
}

function clearSchemaBrowser() {
  $("schemaTree").innerHTML = "";
  $("schemaMeta").textContent = "";
  $("browseHead").innerHTML = "";
  $("browseBody").innerHTML = "";
  $("browseTableLabel").textContent = "";
  lastBrowse = { ks: null, tb: null };
  $("btnBrowseApply").disabled = true;
}

function showError(msg) {
  const b = $("errorBanner");
  b.textContent = msg || "";
  b.classList.toggle("visible", !!msg);
}

function escapeHtml(s) {
  if (s == null) return "";
  const d = document.createElement("div");
  d.textContent = s;
  return d.innerHTML;
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

function fillModalForm(p) {
  $("m_profileName").value = p.displayName || "";
  $("m_contactPoints").value = p.contactPoints || "";
  $("m_port").value = String(p.port || 9042);
  $("m_localDc").value = p.localDatacenter || "";
  $("m_username").value = p.username || "";
  $("m_password").value = p.password || "";
}

function readModalForm() {
  return {
    id: modalEditingId || undefined,
    displayName: $("m_profileName").value.trim(),
    contactPoints: $("m_contactPoints").value.trim(),
    port: parseInt($("m_port").value.trim(), 10) || 9042,
    localDatacenter: $("m_localDc").value.trim(),
    username: $("m_username").value.trim(),
    password: $("m_password").value,
  };
}

function openModalAdd() {
  modalEditingId = null;
  $("connModalTitle").textContent = "Новое подключение";
  fillModalForm({});
  $("connModal").classList.add("open");
  $("m_profileName").focus();
}

function openModalEdit() {
  if (!selectedId) return;
  const p = profiles.find((x) => x.id === selectedId);
  if (!p) return;
  modalEditingId = p.id;
  $("connModalTitle").textContent = "Редактировать подключение";
  fillModalForm(p);
  $("connModal").classList.add("open");
  $("m_profileName").focus();
}

function closeModal() {
  $("connModal").classList.remove("open");
  modalEditingId = null;
}

function renderConnectionList() {
  const ul = $("connList");
  ul.innerHTML = "";
  for (const p of profiles) {
    const li = document.createElement("li");
    li.className = "conn-item" + (p.id === selectedId ? " selected" : "");
    li.setAttribute("role", "option");
    li.setAttribute("aria-selected", p.id === selectedId ? "true" : "false");
    li.dataset.id = p.id;
    li.innerHTML =
      '<div class="conn-item-main"><strong>' +
      escapeHtml(p.displayName || p.contactPoints) +
      "</strong>" +
      '<span class="conn-item-sub">' +
      escapeHtml(p.contactPoints || "") +
      " · порт " +
      escapeHtml(String(p.port || 9042)) +
      " · DC: " +
      escapeHtml(p.localDatacenter || "—") +
      "</span></div>";
    li.addEventListener("click", () => {
      selectedId = p.id;
      renderConnectionList();
    });
    li.addEventListener("dblclick", () => {
      selectedId = p.id;
      renderConnectionList();
      $("btnConnect").click();
    });
    ul.appendChild(li);
  }
  $("btnConnEdit").disabled = !selectedId;
  $("btnConnDelete").disabled = !selectedId;
}

async function loadProfiles() {
  const r = await api("/api/connections");
  profiles = await r.json();
  if (selectedId && !profiles.some((p) => p.id === selectedId)) {
    selectedId = null;
  }
  renderConnectionList();
}

$("btnConnAdd").addEventListener("click", () => {
  showError("");
  openModalAdd();
});

$("btnConnEdit").addEventListener("click", () => {
  showError("");
  openModalEdit();
});

$("btnConnDelete").addEventListener("click", async () => {
  if (!selectedId) return;
  if (!confirm("Удалить выбранное подключение из списка?")) return;
  showError("");
  const r = await fetch("/api/connections/" + encodeURIComponent(selectedId), {
    method: "DELETE",
    credentials: "include",
  });
  if (!r.ok) {
    showError("Не удалось удалить");
    return;
  }
  selectedId = null;
  await loadProfiles();
});

$("btnConnModalSave").addEventListener("click", async () => {
  showError("");
  const p = readModalForm();
  if (!p.displayName) {
    alert("Укажите имя профиля");
    return;
  }
  const r = await api("/api/connections", { method: "POST", body: JSON.stringify(p) });
  if (!r.ok) {
    showError("Проверьте поля подключения (contact points, Local DC и т.д.)");
    return;
  }
  const saved = await r.json();
  selectedId = saved.id;
  closeModal();
  await loadProfiles();
});

$("btnConnModalCancel").addEventListener("click", closeModal);
$("connModalBackdrop").addEventListener("click", closeModal);

document.addEventListener("keydown", (e) => {
  if (e.key === "Escape" && $("connModal").classList.contains("open")) {
    closeModal();
  }
});

function setConnectedUi(on) {
  $("connStatus").textContent = on ? "Подключено" : "Не подключено";
  $("connStatus").className = "status " + (on ? "on" : "off");
  ["btnTopology", "btnSchemaTreeRefresh", "btnRefreshSchema", "btnPreview", "btnLoad"].forEach((id) => {
    $(id).disabled = !on;
  });
  const sb = $("schemaBrowserSection");
  if (sb) {
    sb.style.opacity = on ? "1" : "0.45";
    sb.style.pointerEvents = on ? "auto" : "none";
  }
  if (!on) {
    clearSchemaBrowser();
  }
}

$("btnConnect").addEventListener("click", async () => {
  showError("");
  const p = profiles.find((x) => x.id === selectedId);
  if (!p) {
    showError("Выберите подключение в списке (клик по строке)");
    return;
  }
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
  const lines = [
    t.clusterName + ":",
    ...t.nodes.map((n) => `  ${n.endpoint}  DC=${n.datacenter}  rack=${n.rack}  ${n.state}`),
  ];
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
  await loadSchemaTree();
}

async function loadSchemaTree() {
  try {
    const r = await api("/api/schema/tree");
    if (!r.ok) return;
    const tree = await r.json();
    renderSchemaTree(tree);
  } catch (_) {
    /* сессия не подключена */
  }
}

async function loadKeyspaceMeta(name) {
  try {
    const r = await api("/api/schema/keyspace/metadata?name=" + encodeURIComponent(name));
    if (!r.ok) return;
    const j = await r.json();
    $("schemaMeta").textContent = j.description || "";
  } catch (_) {
    /* игнор */
  }
}

async function loadTableMeta(ks, tb) {
  try {
    const r = await api(
      "/api/schema/table/metadata?keyspace=" + encodeURIComponent(ks) + "&table=" + encodeURIComponent(tb)
    );
    if (!r.ok) return;
    const j = await r.json();
    $("schemaMeta").textContent = j.description || "";
  } catch (_) {
    /* игнор */
  }
}

async function loadBrowseTableData(ks, tb) {
  const raw = parseInt($("browseLimit").value, 10);
  const lim = Math.min(1000, Math.max(1, Number.isFinite(raw) ? raw : 10));
  $("browseLimit").value = String(lim);
  showError("");
  const r = await api(
    `/api/query/table-data?keyspace=${encodeURIComponent(ks)}&table=${encodeURIComponent(tb)}&limit=${lim}`
  );
  if (!r.ok) {
    showError(await r.text());
    return;
  }
  const data = await r.json();
  renderTable("browseHead", "browseBody", data.columns, data.rows);
}

function renderSchemaTree(nodes) {
  const root = $("schemaTree");
  root.innerHTML = "";
  if (!nodes || !nodes.length) {
    const p = document.createElement("p");
    p.className = "hint";
    p.textContent = "Нет keyspace (или пустой кластер).";
    root.appendChild(p);
    return;
  }
  for (const n of nodes) {
    const wrap = document.createElement("div");
    wrap.className = "tree-keyspace";
    const head = document.createElement("div");
    head.className = "tree-keyspace-head";
    const caret = document.createElement("button");
    caret.type = "button";
    caret.className = "tree-caret";
    caret.setAttribute("aria-expanded", "true");
    caret.textContent = "▼";
    const ksLabel = document.createElement("span");
    ksLabel.className = "tree-label tree-ks";
    ksLabel.textContent = n.name;
    ksLabel.title = n.name;
    ksLabel.addEventListener("click", (e) => {
      e.stopPropagation();
      clearSchemaTreeSelection();
      ksLabel.classList.add("tree-selected");
      lastBrowse = { ks: null, tb: null };
      $("browseTableLabel").textContent = "";
      $("btnBrowseApply").disabled = true;
      loadKeyspaceMeta(n.name);
    });
    head.appendChild(caret);
    head.appendChild(ksLabel);
    const body = document.createElement("div");
    body.className = "tree-tables";
    for (const t of n.tables || []) {
      const tr = document.createElement("div");
      tr.className = "tree-table-row";
      const tbLabel = document.createElement("span");
      tbLabel.className = "tree-label tree-tb";
      tbLabel.textContent = t;
      tbLabel.title = n.name + "." + t;
      tbLabel.addEventListener("click", (e) => {
        e.stopPropagation();
        clearSchemaTreeSelection();
        tbLabel.classList.add("tree-selected");
        $("keyspace").value = n.name;
        $("table").value = t;
        refreshTables();
        lastBrowse = { ks: n.name, tb: t };
        $("browseTableLabel").textContent = n.name + "." + t;
        $("btnBrowseApply").disabled = false;
        loadTableMeta(n.name, t);
      });
      tbLabel.addEventListener("dblclick", (e) => {
        e.stopPropagation();
        $("keyspace").value = n.name;
        $("table").value = t;
        refreshTables();
        lastBrowse = { ks: n.name, tb: t };
        $("browseTableLabel").textContent = n.name + "." + t;
        $("btnBrowseApply").disabled = false;
        loadBrowseTableData(n.name, t);
      });
      tr.appendChild(tbLabel);
      body.appendChild(tr);
    }
    caret.addEventListener("click", () => {
      const collapsed = body.style.display === "none";
      body.style.display = collapsed ? "block" : "none";
      caret.textContent = collapsed ? "▼" : "▶";
      caret.setAttribute("aria-expanded", collapsed ? "true" : "false");
    });
    wrap.appendChild(head);
    wrap.appendChild(body);
    root.appendChild(wrap);
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

$("btnSchemaTreeRefresh").addEventListener("click", async () => {
  showError("");
  await refreshKeyspaces();
});

$("btnBrowseApply").addEventListener("click", async () => {
  if (!lastBrowse.ks || !lastBrowse.tb) return;
  await loadBrowseTableData(lastBrowse.ks, lastBrowse.tb);
});

$("browseLimit").addEventListener("change", () => {
  if (lastBrowse.ks && lastBrowse.tb) {
    loadBrowseTableData(lastBrowse.ks, lastBrowse.tb);
  }
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
