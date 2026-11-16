// Vercel serverless function: Google Gemini (free tier) ko call karta hai aur jawab stream karta hai.
// API key sirf yahan (Vercel env GEMINI_API_KEY) rehti hai, kabhi frontend ya repo mein nahi.
//
// Frontend (www/index.html) Anthropic-style SSE events padhta hai (content_block_delta / text_delta).
// Isliye Gemini ka stream niche usi format mein badal kar bheja jaata hai, taaki frontend ko
// kuch badalna na pade.

const GEMINI_BASE = process.env.GEMINI_BASE || "https://generativelanguage.googleapis.com/v1beta";

// Free tier ke model badalte rehte hain. Pehla model na chale (404 / quota / busy) to agla try hota hai.
// Apna model dena ho to Vercel env GEMINI_MODEL mein comma se alag karke likho, jaise: "gemini-3.6-flash,gemini-3.5-flash-lite"
const MODELS = (process.env.GEMINI_MODEL ||
  "gemini-3.6-flash,gemini-3.8-flash,gemini-3.5-flash-lite,gemini-3.1-flash-lite,gemini-2.5-flash")
  .split(",").map(function (s) { return s.trim(); }).filter(Boolean);

// In status par agla model try karte hain. 400/401/403 (galat key waghera) par ruk jaate hain.
const RETRY_STATUS = { 404: 1, 429: 1, 500: 1, 502: 1, 503: 1, 504: 1 };

const SYSTEM_RULE =
  "Reply with JSON Lines only: exactly one complete JSON object per line, no code fences, no markdown, " +
  "no commentary before or after. Never split one JSON object across several lines.";

function cors(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "content-type, x-app-code");
}

async function callGemini(model, prompt, key) {
  return fetch(GEMINI_BASE + "/models/" + encodeURIComponent(model) + ":streamGenerateContent?alt=sse", {
    method: "POST",
    headers: { "content-type": "application/json", "x-goog-api-key": key },
    body: JSON.stringify({
      systemInstruction: { parts: [{ text: SYSTEM_RULE }] },
      contents: [{ role: "user", parts: [{ text: prompt }] }],
      generationConfig: { maxOutputTokens: 12000 }
    })
  });
}

// Gemini ke SSE ko frontend wale format mein badalta hai.
async function relay(upstream, res) {
  const dec = new TextDecoder();
  let buf = "";

  function emit(text) {
    res.write(
      "event: content_block_delta\ndata: " +
        JSON.stringify({ type: "content_block_delta", index: 0, delta: { type: "text_delta", text: text } }) +
        "\n\n"
    );
  }
  function handleLine(line) {
    line = line.trim();
    if (line.indexOf("data:") !== 0) return;
    let ev;
    try { ev = JSON.parse(line.slice(5)); } catch (e) { return; }
    const cand = ev && ev.candidates && ev.candidates[0];
    const parts = (cand && cand.content && cand.content.parts) || [];
    for (let i = 0; i < parts.length; i++) {
      if (parts[i].text && !parts[i].thought) emit(parts[i].text);
    }
  }

  for await (const chunk of upstream.body) {
    buf += dec.decode(chunk, { stream: true });
    const lines = buf.split("\n");
    buf = lines.pop();
    lines.forEach(handleLine);
  }
  handleLine(buf);
  res.write('event: message_stop\ndata: {"type":"message_stop"}\n\n');
}

module.exports = async function handler(req, res) {
  cors(res);
  if (req.method === "OPTIONS") return res.status(204).end();
  if (req.method !== "POST") return res.status(405).json({ error: "POST only" });

  const need = process.env.APP_CODE;
  if (need && req.headers["x-app-code"] !== need) {
    return res.status(401).json({ error: "bad app code" });
  }
  const key = process.env.GEMINI_API_KEY || process.env.GOOGLE_API_KEY;
  if (!key) {
    return res.status(500).json({ error: "GEMINI_API_KEY not set" });
  }

  let body;
  try {
    body = typeof req.body === "string" ? JSON.parse(req.body || "{}") : req.body || {};
  } catch (e) {
    return res.status(400).json({ error: "bad json" });
  }
  const prompt = String(body.prompt || "").slice(0, 60000);
  if (!prompt) return res.status(400).json({ error: "prompt missing" });

  let upstream = null;
  let lastStatus = 502;
  for (let i = 0; i < MODELS.length; i++) {
    try {
      upstream = await callGemini(MODELS[i], prompt, key);
    } catch (e) {
      console.error("gemini fetch failed (" + MODELS[i] + "):", e && e.message);
      lastStatus = 502;
      upstream = null;
      continue;
    }
    if (upstream.ok && upstream.body) break;

    lastStatus = upstream.status;
    let detail = "";
    try { detail = (await upstream.text()).slice(0, 300); } catch (e) {}
    console.error("gemini " + upstream.status + " (" + MODELS[i] + "): " + detail);
    upstream = null;
    if (!RETRY_STATUS[lastStatus]) break;
  }

  if (!upstream) {
    return res.status(lastStatus === 429 ? 429 : 502).json({ error: "upstream " + lastStatus });
  }

  res.setHeader("Content-Type", "text/event-stream; charset=utf-8");
  res.setHeader("Cache-Control", "no-cache, no-transform");
  res.setHeader("X-Accel-Buffering", "no");

  try {
    await relay(upstream, res);
  } catch (e) {
    // client ne band kar diya ya connection toot gaya
  }
  res.end();
};
