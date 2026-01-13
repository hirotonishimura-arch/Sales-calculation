// index.js (Node.js 20 / CommonJS)
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

function monthKeyFrom(dateTimeStr) {
  const s = norm(dateTimeStr);
  return s.length >= 7 ? s.slice(0, 7) : "unknown";
}

function getNowMonthKeyJst() {
  const d = new Date();
  const y = new Intl.DateTimeFormat("en-CA", { timeZone: "Asia/Tokyo", year: "numeric" }).format(d);
  const m = new Intl.DateTimeFormat("en-CA", { timeZone: "Asia/Tokyo", month: "2-digit" }).format(d);
  return `${y}-${m}`;
}

async function postSlack(webhookUrl, text) {
  const payload = { text };
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

// prices.json は byAdId が基本。byAdName も任意で対応。
function getUnitPrice(prices, adId, adName) {
  const id = String(adId || "").trim();
  if (id && prices.byAdId && prices.byAdId[id] != null) return Number(prices.byAdId[id]) || 0;

  const name = String(adName || "").trim();
  if (name && prices.byAdName && prices.byAdName[name] != null) return Number(prices.byAdName[name]) || 0;

  return Number(prices.defaultUnitPrice) || 0;
}

// seenKeys を「重複なし」「新しいもの優先」で保持
function mergeSeenKeys(prev, add, maxItems = 3000) {
  const all = (prev || []).concat(add || []);
  const seen = new Set();
  const outRev = [];
  for (let i = all.length - 1; i >= 0; i--) {
    const k = all[i];
    if (!k) continue;
    if (seen.has(k)) continue;
    seen.add(k);
    outRev.push(k);
    if (outRev.length >= maxItems) break;
  }
  return outRev.reverse();
}

function uniqByKey(items) {
  const s = new Set();
  const out = [];
  for (const x of items || []) {
    if (!x || !x.key) continue;
    if (s.has(x.key)) continue;
    s.add(x.key);
    out.push(x);
  }
  return out;
}

/**
 * 画面上のCVテーブルを特定し、行を抜く（必要列は headerMap で指定）
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

async function extractRowsFromBestTable(page, headerMap) {
  await waitForCvTable(page, headerMap.orderAt, headerMap.adId, headerMap.adName);

  return await page.evaluate((hm) => {
    const norm = (s) => String(s ?? "").replace(/\s+/g, " ").trim();
    const tables = Array.from(document.querySelectorAll("table"));

    function headerIndex(headers, target) {
      if (!target) return -1;
      let i = headers.findIndex((h) => h === target);
      if (i >= 0) return i;
      i = headers.findIndex((h) => h.includes(target));
      return i;
    }

    function scoreTable(t) {
      const headers = Array.from(t.querySelectorAll("thead th")).map((x) => norm(x.textContent));
      const need = [hm.orderAt, hm.adId, hm.adName, hm.siteName, hm.clickAt, hm.os, hm.referrer];
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
      clickAt: headerIndex(headers, hm.clickAt),
      adId: headerIndex(headers, hm.adId),
      adName: headerIndex(headers, hm.adName),
      siteName: headerIndex(headers, hm.siteName),
      os: headerIndex(headers, hm.os),
      referrer: headerIndex(headers, hm.referrer),
      status: headerIndex(headers, hm.status), // キーには使わない（変わる可能性がある）
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
        os: get(idx.os),
        referrer: get(idx.referrer),
        status: get(idx.status),
        // rowText: tds.join(" | "), // デバッグに使いたければ
      });
    }
    return data;
  }, headerMap);
}

/**
 * キー生成を強化：同秒・同案件が続いても潰れにくいよう
 * eventAt(注文日時優先/なければクリック日時) + adId/adName + siteName + os + referrer を使う
 * ※ status は変わり得るので key には入れない
 */
function normalizeRows(rows, prices) {
  return (rows || [])
    .map((r) => {
      const orderAt = norm(r.orderAt);
      const clickAt = norm(r.clickAt);
      const eventAt = orderAt || clickAt;

      const adId = norm(r.adId);
      const adName = norm(r.adName);
      const siteName = norm(r.siteName);
      const os = norm(r.os);
      const referrer = norm(r.referrer);

      // adId が空でも adName があれば拾う（単価は byAdName でも対応可）
      const adKey = adId || adName;
      if (!eventAt || !adKey) return null;

      const unit = getUnitPrice(prices, adId, adName);
      const monthKey = monthKeyFrom(eventAt);

      const keySource = `${eventAt}|${adKey}|${siteName}|${os}|${referrer}|${adName}`;
      const key = sha1(keySource);

      return { key, eventAt, orderAt, clickAt, adId, adName, siteName, os, referrer, unit, monthKey };
    })
    .filter(Boolean);
}

/**
 * ページングが本当に進んだか判定するための「テーブル署名」
 */
async function getTableSignature(page, headerMap) {
  return await page
    .evaluate((hm) => {
      const norm = (s) => String(s ?? "").replace(/\s+/g, " ").trim();
      const tables = Array.from(document.querySelectorAll("table"));

      function scoreTable(t) {
        const headers = Array.from(t.querySelectorAll("thead th")).map((x) => norm(x.textContent));
        const need = [hm.orderAt, hm.adId, hm.adName];
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

      if (!best) return "";

      const trs = Array.from(best.querySelectorAll("tbody tr"));
      const first = trs[0]?.innerText || "";
      const last = trs[trs.length - 1]?.innerText || "";
      return `${first}||${last}`.slice(0, 2000);
    }, headerMap)
    .catch(() => "");
}

/**
 * 次ページへ進めるなら進む。進めなかったら false。
 */
async function clickNextPage(page, headerMap) {
  const before = await getTableSignature(page, headerMap);

  const selectors = [
    'a.paginate_button.next:not(.disabled)',
    'a.next:not(.disabled)',
    'li.next:not(.disabled) a',
    'a[rel="next"]',
    'button[aria-label="Next"]:not([disabled])',
    'a[aria-label="Next"]:not(.disabled)',
  ];

  const tryClick = async (fnClick) => {
    await fnClick();
    await Promise.race([
      page.waitForNavigation({ waitUntil: "networkidle2", timeout: 5000 }).catch(() => null),
      page
        .waitForFunction(
          (hm, prev) => {
            const norm = (s) => String(s ?? "").replace(/\s+/g, " ").trim();
            const tables = Array.from(document.querySelectorAll("table"));

            function scoreTable(t) {
              const headers = Array.from(t.querySelectorAll("thead th")).map((x) => norm(x.textContent));
              const need = [hm.orderAt, hm.adId, hm.adName];
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

            if (!best) return false;
            const trs = Array.from(best.querySelectorAll("tbody tr"));
            const first = trs[0]?.innerText || "";
            const last = trs[trs.length - 1]?.innerText || "";
            const sig = `${first}||${last}`.slice(0, 2000);
            return sig && sig !== prev;
          },
          { timeout: 5000 },
          headerMap,
          before
        )
        .catch(() => null),
    ]);

    await sleep(300);
    const after = await getTableSignature(page, headerMap);
    return after && after !== before;
  };

  for (const sel of selectors) {
    const el = await page.$(sel);
    if (!el) continue;
    const moved = await tryClick(() => el.click().catch(() => null));
    if (moved) return true;
  }

  const movedByText = await tryClick(() =>
    page.evaluate(() => {
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
  );

  return movedByText;
}

/**
 * 初回：今月分を全部拾う（ページの終端まで辿る / “今月が出なくなったら終了”）
 */
async function collectThisMonthRows(page, headerMap, prices, maxPages = 50) {
  const targetMonth = getNowMonthKeyJst();
  const out = [];

  for (let p = 0; p < maxPages; p++) {
    const rows = await extractRowsFromBestTable(page, headerMap);
    const normalized = normalizeRows(rows, prices);

    const inMonth = normalized.filter((x) => x.monthKey === targetMonth);
    out.push(...inMonth);

    // このページが全部 “前月以前” なら、ここで終わり（並び順が多少怪しくても安全寄り）
    const hasAny = normalized.length > 0;
    const allOlder = hasAny && normalized.every((x) => x.monthKey < targetMonth);
    if (allOlder) break;

    const moved = await clickNextPage(page, headerMap);
    if (!moved) break;
  }

  return uniqByKey(out);
}

/**
 * 通常：新規が無くなるまでページを辿る（>20件バースト対策）
 */
async function collectNewRowsUntilSeen(page, headerMap, prices, seenSet, maxPages = 10) {
  const out = [];
  for (let p = 0; p < maxPages; p++) {
    const rows = await extractRowsFromBestTable(page, headerMap);
    const normalized = normalizeRows(rows, prices);

    let newCount = 0;
    for (const x of normalized) {
      if (!seenSet.has(x.key)) {
        out.push(x);
        newCount++;
      }
    }

    if (newCount === 0) break;

    const moved = await clickNextPage(page, headerMap);
    if (!moved) break;
  }
  return uniqByKey(out);
}

async function detectTotalCountIfPossible(page) {
  // DataTables系の「xx件中」表示があれば拾ってログに出す（無ければ無視）
  const txt = await page.$eval(".dataTables_info", (el) => el.textContent || "").catch(() => "");
  const t = norm(txt);
  if (!t) return null;

  // 英語: "Showing 1 to 20 of 79 entries"
  let m = t.match(/of\s+([\d,]+)\s+entries/i);
  if (m) return parseInt(m[1].replace(/,/g, ""), 10);

  // 日本語: "全79件" や "79件中"
  m = t.match(/全\s*([\d,]+)\s*件/);
  if (m) return parseInt(m[1].replace(/,/g, ""), 10);

  m = t.match(/([\d,]+)\s*件中/);
  if (m) return parseInt(m[1].replace(/,/g, ""), 10);

  return null;
}

async function main() {
  const ADSERVICE_ID = mustEnv("ADSERVICE_ID", process.env.ADSERVICE_ID);
  const ADSERVICE_PASS = mustEnv("ADSERVICE_PASS", process.env.ADSERVICE_PASS);
  const SLACK_WEBHOOK_URL = mustEnv("SLACK_WEBHOOK_URL", process.env.SLACK_WEBHOOK_URL);
  const CV_LOG_URL = mustEnv("CV_LOG_URL", process.env.CV_LOG_URL);

  const LOGIN_URL = process.env.LOGIN_URL || "https://admin.adservice.jp/";
  const AFTER_LOGIN_URL_PREFIX = process.env.AFTER_LOGIN_URL_PREFIX || "https://admin.adservice.jp/partneradmin/";

  const USERNAME_SELECTOR = process.env.USERNAME_SELECTOR || 'input[name="loginId"]';
  const PASSWORD_SELECTOR = process.env.PASSWORD_SELECTOR || 'input[name="password"]';
  const SUBMIT_SELECTOR = process.env.SUBMIT_SELECTOR || 'button[type="submit"], input[type="submit"]';

  // CV明細テーブルのヘッダ名（必要なら env で上書き）
  const headerMap = {
    orderAt: process.env.HEADER_ORDER_AT || "注文日時",
    clickAt: process.env.HEADER_CLICK_AT || "クリック日時",
    adId: process.env.HEADER_AD_ID || "広告ID",
    adName: process.env.HEADER_AD_NAME || "広告名",
    siteName: process.env.HEADER_SITE_NAME || "サイト名",
    os: process.env.HEADER_OS || "OS",
    referrer: process.env.HEADER_REFERRER || "リファラ",
    status: process.env.HEADER_STATUS || "ステータス",
  };

  const state = readJson(STATE_FILE, { version: 1, initialized: false, seenKeys: [], monthly: {}, updatedAt: null });
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

    await sleep(800);
    if (!page.url().startsWith(AFTER_LOGIN_URL_PREFIX)) {
      throw new Error(`Login seems failed. current url=${page.url()}`);
    }

    await page.goto(CV_LOG_URL, { waitUntil: "networkidle2" });

    const total = await detectTotalCountIfPossible(page);
    if (total != null) console.log(`[INFO] Detected total entries (from UI): ${total}`);

    // 初回：今月分をページング収集して月次合計を作る（通知しない）
    if (!state.initialized) {
      const maxPages = Number(process.env.MAX_PAGES || 50);
      const monthRows = await collectThisMonthRows(page, headerMap, prices, maxPages);

      // もし key 衝突がまだ起きてたらログで気づけるように
      const keyCount = new Map();
      for (const x of monthRows) keyCount.set(x.key, (keyCount.get(x.key) || 0) + 1);
      const dup = [...keyCount.entries()].filter(([, c]) => c > 1);
      if (dup.length > 0) console.warn(`[WARN] Duplicate keys still exist in monthRows: ${dup.length} keys`);

      const nowMonth = getNowMonthKeyJst();
      state.monthly ||= {};
      state.monthly[nowMonth] = { revenue: 0, count: 0 };

      for (const x of monthRows) {
        state.monthly[nowMonth].count += 1;
        state.monthly[nowMonth].revenue += x.unit;
      }

      state.seenKeys = mergeSeenKeys(state.seenKeys, monthRows.map((x) => x.key));
      state.initialized = true;

      writeJson(STATE_FILE, state);
      console.log(`[INFO] Bootstrapped month total from ${monthRows.length} rows (no notify).`);
      return;
    }

    // 通常：新規を複数ページから拾う（バースト対策）
    const maxPagesNormal = Number(process.env.MAX_PAGES_NORMAL || 10);
    const newOnes = await collectNewRowsUntilSeen(page, headerMap, prices, seenSet, maxPagesNormal);

    if (newOnes.length === 0) {
      console.log("[INFO] No new CV. No notify.");
      return;
    }

    state.monthly ||= {};
    const unknown = [];

    for (const x of newOnes) {
      if (x.unit === 0) {
        const id = String(x.adId || "").trim();
        const name = String(x.adName || "").trim();
        const hasId = id && prices.byAdId && prices.byAdId[id] != null;
        const hasName = name && prices.byAdName && prices.byAdName[name] != null;
        if (!hasId && !hasName) unknown.push(`${x.adId || "(no id)"} ${x.adName || "(no name)"}`);
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
        `日時: ${x.eventAt}\n` +
        `案件: ${x.adName || "(不明)"}\n` +
        `サイト: ${x.siteName || "(不明)"}\n` +
        `報酬単価: ${unitStr}\n` +
        `今月の売上合計（現在）: ${fmtYen(monthTotal.revenue)}（${x.monthKey}）\n` +
        `管理画面を確認する: <${CV_LOG_URL}|管理画面を確認する>`;

      await postSlack(SLACK_WEBHOOK_URL, msg);
    }

    if (unknown.length > 0) {
      const warn =
        `⚠️ 単価が未設定の広告ID/広告名があります（prices.jsonに追加してください）\n` +
        unknown.slice(0, 20).map((s) => `- ${s}`).join("\n");
      await postSlack(SLACK_WEBHOOK_URL, warn);
    }

    state.seenKeys = mergeSeenKeys(state.seenKeys, newOnes.map((x) => x.key));
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
