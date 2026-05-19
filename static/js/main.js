function showOutput(el, data, isError = false) {
  el.textContent = JSON.stringify(data, null, 2);
  el.classList.add("visible");
  el.classList.toggle("error", isError);
}

async function fetchJSON(url, options = {}) {
  const res = await fetch(url, options);
  return res.json();
}

document.getElementById("btn-health").addEventListener("click", async () => {
  const el = document.getElementById("output-health");
  try {
    const data = await fetchJSON("/health");
    showOutput(el, data);
  } catch (e) {
    showOutput(el, { error: e.message }, true);
  }
});

document.getElementById("btn-hello").addEventListener("click", async () => {
  const el = document.getElementById("output-hello");
  const name = document.getElementById("name-input").value || "World";
  try {
    const data = await fetchJSON(`/api/hello?name=${encodeURIComponent(name)}`);
    showOutput(el, data);
  } catch (e) {
    showOutput(el, { error: e.message }, true);
  }
});

document.getElementById("btn-echo").addEventListener("click", async () => {
  const el = document.getElementById("output-echo");
  let body;
  try {
    body = JSON.parse(document.getElementById("echo-input").value);
  } catch {
    showOutput(el, { error: "Invalid JSON in body" }, true);
    return;
  }
  try {
    const data = await fetchJSON("/api/echo", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body)
    });
    showOutput(el, data);
  } catch (e) {
    showOutput(el, { error: e.message }, true);
  }
});
