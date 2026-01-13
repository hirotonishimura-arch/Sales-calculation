// index.js (Node.js 20推奨)
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

async function postSlack(webhookUrl, text) {
  const payload = { text };
  // channelを後で差し込みたい場合：
  // Incoming Webhookは通常チャンネル固定で、この指定は無視されることが多いです。
  // ただ、許可されているWebhookなら効くので「入れておいて害は少ない」ため任意対応にしています。
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
  // シンプルに最大数で切る（5分おき監視ならこれで十分）
  if (!Array.isArray(seenKeys)) return [];
  return seenKeys.slice(-maxItems);
}

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
      return score * 1000 + rows; // スコア優先、同点なら行数多い方
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
      siteName: hm.siteName ? headerIndex(headers, hm.siteName) : -1
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
        siteName: get(idx.siteName)
      });
    }

    return data;
  }, headerMap);
}

async function main() {
  // 必須
  const ADSERVICE_ID = mustEnv("ADSERVICE_ID", process.env.ADSERVICE_ID);
  const ADSERVICE_PASS = mustEnv("ADSERVICE_PASS", process.env.ADSERVICE_PASS);
  const SLACK_WEBHOOK_URL = mustEnv("SLACK_WEBHOOK_URL", process.env.SLACK_WEBHOOK_URL);
  const CV_LOG_URL = mustEnv("CV_LOG_URL", process.env.CV_LOG_URL);

  // ログイン情報（あなたが提示したnameに合わせる）
  const LOGIN_URL = process.env.LOGIN_URL || "https://admin.adservice.jp/";
  const AFTER_LOGIN_URL_PREFIX = process.env.AFTER_LOGIN_URL_PREFIX || "https://admin.adservice.jp/partneradmin/";

  const USERNAME_SELECTOR = process.env.USERNAME_SELECTOR || 'input[name="loginId"]';
  const PASSWORD_SELECTOR = process.env.PASSWORD_SELECTOR || 'input[name="password"]';
  const SUBMIT_SELECTOR = process.env.SUBMIT_SELECTOR || 'button[type="submit"], input[type="submit"]';

  // テーブルヘッダー名（スクショ基準のデフォルト）
  const headerMap = {
    orderAt: process.env.HEADER_ORDER_AT || "注文日時",
    clickAt: process.env.HEADER_CLICK_AT || "クリック日時",
    adId: process.env.HEADER_AD_ID || "広告ID",
    adName: process.env.HEADER_AD_NAME || "広告名",
    siteName: process.env.HEADER_SITE_NAME || "サイト名"
  };

  // state / prices
  const state = readJson(STATE_FILE, { version: 1, initialized: false, seenKeys: [], monthly: {}, updatedAt: null });
  const prices = readJson(PRICE_FILE, null);
  if (!prices) throw new Error("prices.json not found or invalid");

  const seenSet = new Set(state.seenKeys || []);

  const browser = await puppeteer.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage", "--no-zygote"],
  });

  let rows = [];
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

    // login success check (URL)
    await sleep(800);
    if (!page.url().startsWith(AFTER_LOGIN_URL_PREFIX)) {
      throw new Error(`Login seems failed. current url=${page.url()}`);
    }

    // go cv log page
    await page.goto(CV_LOG_URL, { waitUntil: "networkidle2" });

    // extract rows
    rows = await extractRowsFromBestTable(page, headerMap);
  } finally {
    await browser.close().catch(() => {});
  }

  // 正規化 & キー作成（ステータス等の変動要素は含めない）
  const normalized = rows
    .map((r) => {
      const orderAt = norm(r.orderAt);
      const clickAt = norm(r.clickAt);
      const adId = norm(r.adId);
      const adName = norm(r.adName);
      const siteName = norm(r.siteName);

      if (!orderAt || !adId) return null;

      // ここが「同一CV判定」の肝（statusなどは入れない）
      const keySource = `${orderAt}|${clickAt}|${adId}|${siteName}`;
      const key = sha1(keySource);

      const unit = getUnitPrice(prices, adId);
      const monthKey = monthKeyFrom(orderAt);

      return { key, orderAt, adId, adName, siteName, unit, monthKey };
    })
    .filter(Boolean);

  const newOnes = normalized.filter((x) => !seenSet.has(x.key));

  // 初回は通知せず“既存分を既知として登録”して事故を防ぐ
if (!state.initialized) {
  state.initialized = true;

  // 既存行を全部「既知」として登録
  state.seenKeys = pruneSeen(
    (state.seenKeys || []).concat(normalized.map((x) => x.key))
  );

  // ★ここが追加：月次集計を初期化（既存CVも含める）
  state.monthly = state.monthly || {};
  for (const x of normalized) {
    const cur = state.monthly[x.monthKey] || { revenue: 0, count: 0 };
    cur.count += 1;
    cur.revenue += x.unit;
    state.monthly[x.monthKey] = cur;
  }

  writeJson(STATE_FILE, state);
  console.log(`[INFO] Bootstrapped state (no notify). rows=${normalized.length}`);
  return;
}


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

  // ここでseen更新（通知前後どっちでもOKだが、通知失敗時の二重通知を避けたいなら「通知成功後」にする）
  // → 今回は「Slack送信成功後に保存」に寄せるので、seen更新は後で

  // 通知（あなたの指定フォーマット：1CV=1通）
  // ※ まとめ通知にしたくなったらここを変更
  for (const x of newOnes) {
    const monthTotal = state.monthly[x.monthKey] || { revenue: 0, count: 0 };
    const unitStr = (x.unit && x.unit > 0) ? fmtYen(x.unit) : "未設定（prices.jsonに追加してください）";

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

  // 単価未設定があれば追加の警告（任意）
  if (unknown.length > 0) {
    const warn =
      `⚠️ 単価が未設定の広告IDがあります（prices.jsonに追加してください）\n` +
      unknown.slice(0, 20).map((s) => `- ${s}`).join("\n");
    await postSlack(SLACK_WEBHOOK_URL, warn);
  }

  // state保存（seenKeys / monthly）
  state.seenKeys = pruneSeen((state.seenKeys || []).concat(newOnes.map((x) => x.key)));
  writeJson(STATE_FILE, state);

  console.log(`[INFO] Notified ${newOnes.length} CV(s) and updated state.`);
}

main().catch((err) => {
  console.error("[ERROR]", err);
  process.exitCode = 1;
});
