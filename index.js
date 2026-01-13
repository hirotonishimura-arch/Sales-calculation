// index.js (Node.js 20推奨 / CommonJS)
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const puppeteer = require("puppeteer");

const ROOT = process.cwd();
const STATE_FILE = path.join(ROOT, "cv_data.json");
const PRICE_FILE = path.join(ROOT, "prices.json");
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function mustEnv(name, v) {
  if (!v) throw new Error(`Missing env: ${name}`);
  return v;
}

function readJson(p, fallback) {
  try {
    if (!fs.existsSync(p)) return fallback;
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return fallback;
  }
}

function writeJson(p, obj) {
  obj.updatedAt = new Date().toISOString();
  fs.writeFileSync(p, JSON.stringify(obj, null, 2) + "\n", "utf8");
}

function norm(s) {
  return String(s ?? "").replace(/\s+/g, " ").trim();
}

function sha1(s) {
  return crypto.createHash("sha1").update(s).digest("hex");
}

function fmtYen(n) {
  const v = Math.round(Number(n) || 0);
  return `${new Intl.NumberFormat("ja-JP").format(v)}円`;
}

function monthKeyFrom(orderAtStr) {
  const s = norm(orderAtStr);
  return s.length >= 7 ? s.slice(0, 7) : "unknown";
}

function getNowMonthKeyJst() {
  const d = new Date();
  const y = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Tokyo",
    year: "numeric",
  }).format(d);
  const m = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Tokyo",
    month: "2-digit",
  }).format(d);
  return `${y}-${m}`;
}

async function postSlack(webhookUrl, text) {
  const payload = { text };
  // Incoming Webhookは通常チャンネル固定（必要なら後でSlack App側で作り直し）
  if (process.env.SLACK_CHANNEL) payload.channel = process.env.SLACK_CHANNEL;

  const res = await fetch(webhookUrl, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`Slack webhook failed: ${res.status} ${res.statusText} ${body}`);
  }
}

function getUnitPrice(prices, adId) {
  const id = String(adId || "").trim();
  if (id && prices.byAdId && prices.byAdId[id] != null) return Number(prices.byAdId[id]) || 0;
  return Number(prices.defaultUnitPrice) || 0;
}

function pruneSeen(seenKeys, maxItems = 3000) {
  if (!Array.isArray(seenKeys)) return [];
  return seenKeys.slice(-maxItems);
}

/**
 * 次ページへ進めるなら進む（遷移/非遷移どちらでも耐える）
 */
async function clickNextPage(page) {
  const selectors = [
    'a.paginate_button.next:not(.disabled)',
    'a.next:not(.disabled)',
    'li.next:not(.disabled) a',
    'a[rel="next"]',
    'button[aria-label="Next"]:not([disabled])',
    'a[aria-label="Next"]:not(.disabled)',
  ];

  const beforeFirstRow = await page
    .evaluate(() => {
      const tr = document.querySelector("tbody tr");
      return tr ? (tr.innerText || "") : "";
    })
    .catch(() => "");

  async function waitForChangeOrNav() {
    await Promise.race([
      page.waitForNavigation({ waitUntil: "networkidle2", timeout: 5000 }).catch(() => null),
      page
        .waitForFunction(
          (prev) => {
            const tr = document.querySelector("tbody tr");
            if (!tr) return false;
            const now = tr.innerText || "";
            return now && now !== prev;
          },
          { timeout: 5000 },
          beforeFirstRow
        )
        .catch(() => null),
    ]);
    await sleep(500);
  }

  // セレクタ優先
  for (const sel of selectors) {
    const el = await page.$(sel);
    if (!el) continue;

    await el.click().catch(() => null);
    await waitForChangeOrNav();
    return true;
  }

  // 文字で探すフォールバック（次へ/Next）
  const clicked = await page
    .evaluate(() => {
      const isDisabled = (el) => {
        const cls = (el.getAttribute("class") || "").toLowerCase();
        if (cls.includes("disabled")) return true;
        if (el.getAttribute("aria-disabled") === "true") return true;
        if (el.disabled) return true;
        return false;
      };

      const candidates = Array.from(document.querySelectorAll("a,button"));
      const next = candidates.find((el) => {
        const t = (el.textContent || "").trim();
        if (!(t === "次へ" || t === "Next" || t === "›" || t === ">")) return false;
        if (isDisabled(el)) return false;
        const r = el.getBoundingClientRect();
        return r.width > 0 && r.height > 0;
      });

      if (!next) return false;
      next.click();
      return true;
    })
    .catch(() => false);

  if (clicked) {
    await waitForChangeOrNav();
    return true;
  }
  return false;
}

/**
 * CVテーブルが描画されるのを待つ（ヘッダ名で判定）
 */
async function waitForCvTable(page, headerOrderAt, headerAdId, headerAdName) {
  await page.waitForFunction(
    (h1, h2, h3) => {
      const norm = (s) => String(s ?? "").replace(/\s+/g, " ").trim();
      const tables = Array.from(document.querySelectorAll("table"));
      for (const t of tables) {
        const ths = Array.from(t.querySelectorAll("thead th")).map((x) => norm(x.textContent));
        const has =
          ths.some((x) => x === h1 || x.includes(h1)) &&
          ths.some((x) => x === h2 || x.includes(h2)) &&
          ths.some((x) => x === h3 || x.includes(h3));
        if (!has) continue;
        const rows = t.querySelectorAll("tbody tr");
        if (rows && rows.length > 0) return true;
      }
      return false;
    },
    { timeout: 60000 },
    headerOrderAt,
    headerAdId,
    headerAdName
  );
}

/**
 * 画面上の「最もそれっぽいテーブル」から行を抜く
 */
async function extractRowsFromBestTable(page, headerMap) {
  await waitForCvTable(page, headerMap.orderAt, headerMap.adId, headerMap.adName);

  return await page.evaluate((hm) => {
    const norm = (s) => String(s ?? "").replace(/\s+/g, " ").trim();
    const tables = Array.from(document.querySelectorAll("table"));

    function headerIndex(headers, target) {
      let i = headers.findIndex((h) => h === target);
      if (i >= 0) return i;
      i = headers.findIndex((h) => h.includes(target));
      return i;
    }

    function scoreTable(t) {
      const headers = Array.from(t.querySelectorAll("thead th")).map((x) => norm(x.textContent));
      const need = [hm.orderAt, hm.adId, hm.adName, hm.siteName, hm.clickAt];
      let score = 0;
      for (const n of need) {
        if (!n) continue;
        if (headers.some((h) => h === n || h.includes(n))) score += 1;
      }
      const rows = t.querySelectorAll("tbody tr").length;
      return score * 1000 + rows;
    }

    const best = tables
      .map((t) => ({ t, s: scoreTable(t) }))
      .sort((a, b) => b.s - a.s)[0]?.t;

    if (!best) return [];

    const headers = Array.from(best.querySelectorAll("thead th")).map((x) => norm(x.textContent));

    const idx = {
      orderAt: headerIndex(headers, hm.orderAt),
      clickAt: hm.clickAt ? headerIndex(headers, hm.clickAt) : -1,
      adId: headerIndex(headers, hm.adId),
      adName: headerIndex(headers, hm.adName),
      siteName: hm.siteName ? headerIndex(headers, hm.siteName) : -1,
    };

    const rows = Array.from(best.querySelectorAll("tbody tr"));
    const data = [];

    for (const tr of rows) {
      const tds = Array.from(tr.querySelectorAll("td")).map((td) => norm(td.textContent));
      if (!tds.length) continue;

      const get = (i) => (i >= 0 ? (tds[i] ?? "") : "");

      data.push({
        orderAt: get(idx.orderAt),
        clickAt: get(idx.clickAt),
        adId: get(idx.adId),
        adName: get(idx.adName),
        siteName: get(idx.siteName),
      });
    }

    return data;
  }, headerMap);
}

/**
 * rows -> 正規化（key/unit/monthKey 付与）
 */
function normalizeRows(rows, prices) {
  return rows
    .map((r) => {
      const orderAt = norm(r.orderAt);
      const clickAt = norm(r.clickAt);
      const adId = norm(r.adId);
      const adName = norm(r.adName);
      const siteName = norm(r.siteName);

      if (!orderAt || !adId) return null;

      const key = sha1(`${orderAt}|${clickAt}|${adId}|${siteName}`);
      const unit = getUnitPrice(prices, adId);
      const monthKey = monthKeyFrom(orderAt);

      return { key, orderAt, clickAt, adId, adName, siteName, unit, monthKey };
    })
    .filter(Boolean);
}

/**
 * 初回：今月分が尽きる（=前月が出る）までページングして集める
 * ※ ページに「次へ」が無い場合は 1ページ（最大20件）で終了します。
 */
async function collectThisMonthRows(page, headerMap, prices, maxPages = 50) {
  const targetMonth = getNowMonthKeyJst();
  const collected = [];

  for (let p = 0; p < maxPages; p++) {
    const rows = await extractRowsFromBestTable(page, headerMap);
    const normalized = normalizeRows(rows, prices);

    for (const x of normalized) {
      if (x.monthKey < targetMonth) return collected; // 前月が出たら終了
      if (x.monthKey === targetMonth) collected.push(x);
    }

    const moved = await clickNextPage(page);
    if (!moved) return collected;
  }
  return collected;
}

/**
 * 通常運用：新規CVが20件を超える可能性があるので、
 * 「新規がなくなるまで」複数ページを辿って拾う（安全策）
 */
async function collectNewRowsUntilSeen(page, headerMap, prices, seenSet, maxPages = 10) {
  const collected = [];

  for (let p = 0; p < maxPages; p++) {
    const rows = await extractRowsFromBestTable(page, headerMap);
    const normalized = normalizeRows(rows, prices);

    let newInPage = 0;
    for (const x of normalized) {
      if (!seenSet.has(x.key)) {
        collected.push(x);
        newInPage += 1;
      }
    }

    // このページに新規が1件も無い = もう過去領域なので終了
    if (newInPage === 0) break;

    const moved = await clickNextPage(page);
    if (!moved) break;
  }

  // 念のため重複除去
  const uniq = [];
  const ks = new Set();
  for (const x of collected) {
    if (!ks.has(x.key)) {
      ks.add(x.key);
      uniq.push(x);
    }
  }
  return uniq;
}

async function main() {
  // 必須
  const ADSERVICE_ID = mustEnv("ADSERVICE_ID", process.env.ADSERVICE_ID);
  const ADSERVICE_PASS = mustEnv("ADSERVICE_PASS", process.env.ADSERVICE_PASS);
  const SLACK_WEBHOOK_URL = mustEnv("SLACK_WEBHOOK_URL", process.env.SLACK_WEBHOOK_URL);
  const CV_LOG_URL = mustEnv("CV_LOG_URL", process.env.CV_LOG_URL);

  // ログイン情報
  const LOGIN_URL = process.env.LOGIN_URL || "https://admin.adservice.jp/";
  const AFTER_LOGIN_URL_PREFIX =
    process.env.AFTER_LOGIN_URL_PREFIX || "https://admin.adservice.jp/partneradmin/";

  const USERNAME_SELECTOR = process.env.USERNAME_SELECTOR || 'input[name="loginId"]';
  const PASSWORD_SELECTOR = process.env.PASSWORD_SELECTOR || 'input[name="password"]';
  const SUBMIT_SELECTOR =
    process.env.SUBMIT_SELECTOR || 'button[type="submit"], input[type="submit"]';

  // テーブルヘッダー名（必要なら env で上書き）
  const headerMap = {
    orderAt: process.env.HEADER_ORDER_AT || "注文日時",
    clickAt: process.env.HEADER_CLICK_AT || "クリック日時",
    adId: process.env.HEADER_AD_ID || "広告ID",
    adName: process.env.HEADER_AD_NAME || "広告名",
    siteName: process.env.HEADER_SITE_NAME || "サイト名",
  };

  // state / prices
  const state = readJson(STATE_FILE, {
    version: 1,
    initialized: false,
    seenKeys: [],
    monthly: {},
    updatedAt: null,
  });
  const prices = readJson(PRICE_FILE, null);
  if (!prices) throw new Error("prices.json not found or invalid");

  const seenSet = new Set(state.seenKeys || []);

  const browser = await puppeteer.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage", "--no-zygote"],
  });

  try {
    const page = await browser.newPage();
    page.setDefaultTimeout(60000);
    page.setDefaultNavigationTimeout(60000);

    // login
    await page.goto(LOGIN_URL, { waitUntil: "domcontentloaded" });
    await page.waitForSelector(USERNAME_SELECTOR);
    await page.type(USERNAME_SELECTOR, ADSERVICE_ID, { delay: 10 });
    await page.type(PASSWORD_SELECTOR, ADSERVICE_PASS, { delay: 10 });

    await Promise.all([
      page.waitForNavigation({ waitUntil: "networkidle2" }).catch(() => null),
      page.click(SUBMIT_SELECTOR),
    ]);

    // login success check
    await sleep(800);
    if (!page.url().startsWith(AFTER_LOGIN_URL_PREFIX)) {
      throw new Error(`Login seems failed. current url=${page.url()}`);
    }

    // go cv log page
    await page.goto(CV_LOG_URL, { waitUntil: "networkidle2" });

    // 初回：今月分をページングして月次合計を作る（通知しない）
    if (!state.initialized) {
      const maxPages = Number(process.env.MAX_PAGES || 50);
      const monthRows = await collectThisMonthRows(page, headerMap, prices, maxPages);

      const nowMonth = getNowMonthKeyJst();
      state.monthly = state.monthly || {};
      state.monthly[nowMonth] = { revenue: 0, count: 0 };

      for (const x of monthRows) {
        state.monthly[nowMonth].count += 1;
        state.monthly[nowMonth].revenue += x.unit;
      }

      state.seenKeys = pruneSeen((state.seenKeys || []).concat(monthRows.map((x) => x.key)));
      state.initialized = true;

      writeJson(STATE_FILE, state);
      console.log(`[INFO] Bootstrapped month total from ${monthRows.length} rows (no notify).`);
      return;
    }

    // 通常：新規CVを複数ページから拾う（>20件対策）
    const maxPagesNormal = Number(process.env.MAX_PAGES_NORMAL || 10);
    const newOnes = await collectNewRowsUntilSeen(page, headerMap, prices, seenSet, maxPagesNormal);

    if (newOnes.length === 0) {
      console.log("[INFO] No new CV. No notify.");
      return;
    }

    // 月次合計更新（単価で加算）
    state.monthly = state.monthly || {};
    const unknown = [];

    for (const x of newOnes) {
      if (x.unit === 0 && !(prices.byAdId && prices.byAdId[String(x.adId)] != null)) {
        unknown.push(`${x.adId} ${x.adName}`);
      }
      const cur = state.monthly[x.monthKey] || { revenue: 0, count: 0 };
      cur.count += 1;
      cur.revenue += x.unit;
      state.monthly[x.monthKey] = cur;
    }

    // 通知（1CV=1通）
    for (const x of newOnes) {
      const monthTotal = state.monthly[x.monthKey] || { revenue: 0, count: 0 };
      const unitStr = x.unit > 0 ? fmtYen(x.unit) : "未設定（prices.jsonに追加してください）";

      const msg =
        `🎉 新しい成果が発生しました！\n\n` +
        `日時: ${x.orderAt}\n` +
        `案件: ${x.adName || "(不明)"}\n` +
        `サイト: ${x.siteName || "(不明)"}\n` +
        `報酬単価: ${unitStr}\n` +
        `今月の売上合計（現在）: ${fmtYen(monthTotal.revenue)}（${x.monthKey}）\n` +
        `管理画面を確認する: <${CV_LOG_URL}|管理画面を確認する>`;

      await postSlack(SLACK_WEBHOOK_URL, msg);
    }

    // 単価未設定の警告（任意）
    if (unknown.length > 0) {
      const warn =
        `⚠️ 単価が未設定の広告IDがあります（prices.jsonに追加してください）\n` +
        unknown.slice(0, 20).map((s) => `- ${s}`).join("\n");
      await postSlack(SLACK_WEBHOOK_URL, warn);
    }

    // state保存
    state.seenKeys = pruneSeen((state.seenKeys || []).concat(newOnes.map((x) => x.key)));
    writeJson(STATE_FILE, state);

    console.log(`[INFO] Notified ${newOnes.length} CV(s) and updated state.`);
  } finally {
    await browser.close().catch(() => {});
  }
}

main().catch((err) => {
  console.error("[ERROR]", err);
  process.exitCode = 1;
});
