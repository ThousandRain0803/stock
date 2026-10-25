"use strict";
/* 闻势 MarketPulse 服务端：真实数据抓取 → 研判引擎 → HTTP API + 静态前端
   单端口、无外部数据库（JSON 落盘），可长期运行并按交易日自动更新。 */
const http = require("http");
const fs = require("fs");
const path = require("path");
const cfg = require("./src/config");
const { fetchAll, PLAN } = require("./src/pipeline");
const { analyze } = require("./src/engine");
const store = require("./src/store");
const scheduler = require("./src/scheduler");
const newsMod = require("./src/news");

const PUBLIC = path.join(__dirname, "public");
const MIME = { ".html": "text/html; charset=utf-8", ".js": "application/javascript; charset=utf-8", ".css": "text/css; charset=utf-8", ".json": "application/json; charset=utf-8", ".svg": "image/svg+xml", ".md": "text/markdown; charset=utf-8", ".png": "image/png" };

let CURRENT = store.loadSnapshot();   // 启动时先用落盘快照秒开
let LAST_ERR = null;
let SIM_FAIL = new Set();

/* ---------- 刷新主流程 ---------- */
async function refresh(trigger = "auto", simulateFail = []) {
  const raw = await fetchAll({ simulateFail: [...SIM_FAIL, ...simulateFail] });
  const history = store.historySince(120);
  const out = analyze(Object.assign({}, raw, { history }));

  // 落盘历史：真实抓取值（宽度/两融/估值当日值），并用日K回填历史收盘与成交额
  store.backfill(raw.klines);
  const d = out.derived || {};
  const entry = {
    date: out.asOf,
    close: raw.klines && raw.klines.sh ? raw.klines.sh[raw.klines.sh.length - 1].close : null,
    amount: d.amtNow == null ? null : +d.amtNow.toFixed(0),
    upRatio: d.upRatio == null ? null : +d.upRatio.toFixed(1),
    netRatio: d.netRatio == null ? null : +d.netRatio.toFixed(1),
    margin: raw.margin && raw.margin.length ? +raw.margin[raw.margin.length - 1].margin.toFixed(0) : null,
    pe300: raw.valuation && raw.valuation.hs300 ? +raw.valuation.hs300.pe.toFixed(2) : null,
    ztCount: d.ztCount == null ? null : d.ztCount,
    amtRatio: d.amtRatio == null ? null : +d.amtRatio.toFixed(2)
  };
  store.mergeToday(entry);

  // 图表与钻取所需的原始数据（精简后随快照下发）
  out.series = {};
  ["sh", "hs300", "zz500", "cyb", "kc50"].forEach(k => {
    const rows = raw.klines && raw.klines[k] ? raw.klines[k].slice(-120) : null;
    if (rows) out.series[k] = rows.map(r => ({ d: r.date, c: r.close }));
  });
  out.quotes = raw.quotes || {};
  out.sectors = raw.sectors || [];

  /* 新闻快报：清洗分类 → 按新闻日期归档（累积去重）→ 当日列表随快照下发 */
  const newsItems = newsMod.normalize(raw.news || []);
  const byDate = {};
  newsItems.forEach(n => { (byDate[n.date] = byDate[n.date] || []).push(n); });
  Object.keys(byDate).forEach(d => store.saveNews(d, byDate[d]));
  out.news = newsItems;
  out.newsStats = newsMod.stats(newsItems);
  out.newsDates = store.listNewsDates();

  /* 复盘：按交易日保存快照；状态变化写入演变日志 */
  store.saveReplay(out.asOf, out);
  const prev = (store.loadStateLog() || []).slice(-1)[0];
  if (!prev || prev.to !== out.state.name) {
    store.appendStateLog({ ts: new Date().toISOString(), asOf: out.asOf, from: prev ? prev.to : "—", to: out.state.name, conf: out.state.conf, pos: out.state.pos, neg: out.state.neg });
  }
  out.stateLog = store.loadStateLog().slice(-20);
  // 官方交易日历（扶摇）可用时用于修正交易日判断；不可用时回退内置节假日表
  if (out.calendar && out.calendar.length) scheduler.setTradeDays(out.calendar);
  out.plan = PLAN;
  out.trigger = trigger;
  out.fetchedAt = raw.fetchedAt;
  out.scheduler = { tradingDay: scheduler.isTradingDay(), phase: scheduler.phase() };
  CURRENT = out;
  store.saveSnapshot(out);
  LAST_ERR = null;
  return out;
}

async function safeRefresh(trigger, simulateFail) {
  try { return await refresh(trigger, simulateFail); }
  catch (e) {
    LAST_ERR = String(e.message || e);
    console.error("[MarketPulse] 取数失败:", LAST_ERR, e && e.stack ? e.stack.split("\n")[1] : "");
    if (CURRENT) { CURRENT.staleError = LAST_ERR; CURRENT.scheduler = { tradingDay: scheduler.isTradingDay(), phase: scheduler.phase() }; }
    return CURRENT;
  }
}

/* ---------- 报告导出 ---------- */
function toMarkdown(a) {
  const L = [];
  L.push(`# 闻势 MarketPulse 研判报告`);
  L.push(``);
  L.push(`> 数据时点：**${a.asOfLabel}**｜抓取时间：${new Date(a.fetchedAt).toLocaleString("zh-CN")}｜触发方式：${a.trigger}`);
  L.push(`> 数据源：东方财富公开行情接口（服务端抓取）；本产品所有输出为研究性信息，不构成投资建议，不含涨跌预测与仓位建议。`);
  L.push(``);
  L.push(`## 一、状态研判`);
  L.push(`- 状态：**${a.state.name}**（${a.state.desc}）`);
  L.push(`- 置信度（证据一致度）：${(a.state.conf * 100).toFixed(0)}%｜支持 ${a.state.pos}／制约 ${a.state.neg}／数据不可用 ${a.state.unknown}`);
  L.push(``);
  L.push(`## 二、核心结论`);
  a.conclusions.forEach(c => { L.push(`- 【${c.type}｜置信度 ${(c.conf * 100).toFixed(0)}%】${c.t}（证据 ${c.evs.join(", ")}）${c.note ? "  \n  _" + c.note + "_" : ""}`); });
  L.push(``);
  L.push(`## 三、状态切换条件（判断被推翻/更新的情形）`);
  a.triggers.forEach(t => L.push(`- ${t.cond}｜当前值 ${t.cur}｜切换至：${t.to}`));
  L.push(``);
  L.push(`## 四、证据清单`);
  a.evidence.forEach(e => {
    L.push(`### ${e.id} ${e.name}（${e.type}${e.disabled ? " · 不可用" : ""}）`);
    L.push(`- 内容：${e.disabled ? "数据源不可用，已挂起" : e.text}`);
    L.push(`- 来源：${e.source}`);
    L.push(`- 时点：${e.asOf}｜单位：${e.unit}`);
    L.push(`- 口径：${e.caliber}`);
  });
  L.push(``);
  L.push(`## 五、取数计划`);
  (a.plan || []).forEach(p => L.push(`${p.step}. ${p.desc} —— ${p.detail}`));
  L.push(``);
  L.push(`## 六、数据健康`);
  Object.keys(a.health || {}).forEach(k => { const h = a.health[k]; L.push(`- ${h.label}：${h.status}${h.message ? "（" + h.message + "）" : ""}`); });
  return L.join("\n");
}

/* ---------- HTTP ---------- */
function send(res, code, body, headers = {}) {
  res.writeHead(code, Object.assign({ "Access-Control-Allow-Origin": "*", "Cache-Control": "no-store" }, headers));
  res.end(body);
}
function sendJSON(res, obj) { send(res, 200, JSON.stringify(obj), { "Content-Type": "application/json; charset=utf-8" }); }
// 读取请求体（POST /api/refresh、/api/simulate 支持 JSON body 传参，兼容命令行 curl -d）
function readBody(req) {
  return new Promise(resolve => {
    if (req.method !== "POST" && req.method !== "PUT") return resolve({});
    let s = "";
    req.on("data", c => { s += c; if (s.length > 1e6) req.destroy(); });
    req.on("end", () => { try { resolve(s ? JSON.parse(s) : {}); } catch (e) { resolve({}); } });
    req.on("error", () => resolve({}));
  });
}
function simList(v) { return Array.isArray(v) ? v : String(v || "").split(",").filter(Boolean); }

const server = http.createServer(async (req, res) => {
  const u = new URL(req.url, "http://localhost");
  const p = u.pathname;

  try {
    if (p === "/api/snapshot") {
      if (!CURRENT) { await safeRefresh("boot"); }
      return sendJSON(res, Object.assign({}, CURRENT, { simulateFail: [...SIM_FAIL] }));
    }
    if (p === "/api/health") {
      return sendJSON(res, {
        ok: !LAST_ERR, lastError: LAST_ERR, datasets: (CURRENT && CURRENT.health) || {},
        asOf: CURRENT && CURRENT.asOf, fetchedAt: CURRENT && CURRENT.fetchedAt,
        scheduler: scheduler.isTradingDay() ? { tradingDay: true, phase: scheduler.phase(), intervalMin: scheduler.intervalMin() } : { tradingDay: false, phase: "holiday", intervalMin: cfg.refresh.idleIntervalMin },
        providers: { fuyao: require("./src/providers/fuyao").enabled(), ifind: require("./src/providers/ifind").enabled(), eastmoney: cfg.eastmoney.enabled },
        providersUsed: (CURRENT && CURRENT.providerUsed) || {},
        calendarSource: (CURRENT && CURRENT.calendar && CURRENT.calendar.length) ? { source: "同花顺金融数据API（扶摇）·A股交易日历", days: CURRENT.calendar.length } : { source: "内置节假日表（未配置官方日历）", days: null }
      });
    }
    if (p === "/api/history") {
      const days = Number(u.searchParams.get("days") || 120);
      return sendJSON(res, { items: store.historySince(days) });
    }
    if (p === "/api/refresh") {
      const body = await readBody(req);
      const tok = u.searchParams.get("token") || body.token || "";
      if (cfg.refreshToken && tok !== cfg.refreshToken) return sendJSON(res, { ok: false, error: "token 无效" });
      const sim = simList(u.searchParams.get("simulateFail") || body.simulateFail);
      const out = await safeRefresh("manual", sim);
      return sendJSON(res, { ok: true, asOf: out && out.asOf, health: out && out.health, error: LAST_ERR });
    }
    if (p === "/api/simulate") {
      // 故障演练：设置/清除注入失败的数据集（仅用于演示降级行为）
      const body = await readBody(req);
      const set = u.searchParams.get("set") || body.set || "";
      SIM_FAIL = new Set(simList(set));
      const out = await safeRefresh("simulate");
      return sendJSON(res, { ok: true, failing: [...SIM_FAIL], health: out && out.health });
    }
    if (p === "/api/report") {
      if (!CURRENT) await safeRefresh("boot");
      const md = toMarkdown(CURRENT);
      res.writeHead(200, { "Content-Type": "text/markdown; charset=utf-8", "Content-Disposition": `attachment; filename="MarketPulse_${CURRENT.asOf}.md"` });
      return res.end(md);
    }
    if (p === "/api/plan") return sendJSON(res, { items: PLAN });
    /* 交易日历：官方源可用时返回官方交易日序列，否则回退内置节假日表（并如实标注） */
    if (p === "/api/calendar") {
      const official = (CURRENT && CURRENT.calendar) || [];
      return sendJSON(res, {
        source: official.length ? "同花顺金融数据API（扶摇）·A股交易日历" : "内置节假日表推断（未配置官方日历）",
        official: !!official.length,
        days: official.length ? official : null,
        count: official.length || null
      });
    }
    /* 扶摇连通性自检：仅当配置了 API Key 时才真正发起一次请求（不泄露 Key） */
    if (p === "/api/fuyao/diag") {
      const fy = require("./src/providers/fuyao");
      if (!fy.enabled()) return sendJSON(res, { enabled: false, note: "未配置 FUYAO_API_KEY，当前使用公开源" });
      const probes = [["交易日历", () => fy.calendar()], ["行情快照", () => fy.quotes(["sh"])], ["涨停统计", () => fy.pools(CURRENT ? CURRENT.asOf : new Date().toISOString().slice(0, 10))]];
      const items = [];
      for (const [name, fn] of probes) {
        const t0 = Date.now();
        try { const v = await fn(); items.push({ name, ok: true, ms: Date.now() - t0, sample: JSON.stringify(v).slice(0, 120) }); }
        catch (e) { items.push({ name, ok: false, ms: Date.now() - t0, error: String(e.message || e).slice(0, 120) }); }
      }
      return sendJSON(res, { enabled: true, baseUrl: cfg.fuyao.baseUrl, items });
    }

    /* ---------- 新闻快报 ---------- */
    if (p === "/api/news") {
      const dates = store.listNewsDates();
      let date = u.searchParams.get("date") || "";
      // date=latest / 空：取最近一个已归档的新闻日期，避免周末或休市时前端拿到空列表
      if (!date || date === "latest" || date === "today") date = dates.length ? dates[dates.length - 1] : ((CURRENT && CURRENT.asOf) || "");
      const q = (u.searchParams.get("q") || "").trim();
      const cat = u.searchParams.get("cat") || "";
      const level = u.searchParams.get("level") || "";
      const limit = Number(u.searchParams.get("limit") || 200);
      const all = store.loadNews(date);
      let items = all;
      if (q) items = items.filter(n => (n.title + n.summary).includes(q));
      if (cat) items = items.filter(n => n.category === cat);
      if (level) items = items.filter(n => n.level === level);
      return sendJSON(res, { date, availableDates: dates, total: items.length, stats: newsMod.stats(all), items: items.slice(0, limit) });
    }
    if (p === "/api/news/dates") return sendJSON(res, { dates: store.listNewsDates() });
    if (p === "/api/news/stats") {
      const ds = store.listNewsDates();
      let date = u.searchParams.get("date") || "";
      if (!date || date === "latest") date = ds.length ? ds[ds.length - 1] : ((CURRENT && CURRENT.asOf) || "");
      return sendJSON(res, Object.assign({ date, availableDates: ds }, newsMod.stats(store.loadNews(date))));
    }
    /* ---------- 复盘日历 ---------- */
    if (p === "/api/replay") {
      const date = u.searchParams.get("date");
      const one = store.loadReplay(date);
      return sendJSON(res, one || { error: "该日期无复盘快照：" + date });
    }
    if (p === "/api/replay/dates") return sendJSON(res, { dates: store.listReplayDates() });
    if (p === "/api/state-log") return sendJSON(res, { items: store.loadStateLog().slice(-50) });
    if (p === "/api/diag") {
      // 数据源连通性诊断：用于在新部署环境下快速确认可访问的数据源
      const CAND = [
        ["东财K线 push2his", "https://push2his.eastmoney.com/api/qt/stock/kline/get?secid=1.000001&fields1=f1&fields2=f51,f53&klt=101&fqt=1&lmt=3&end=20500101"],
        ["东财K线 11.push2his", "https://11.push2his.eastmoney.com/api/qt/stock/kline/get?secid=1.000001&fields1=f1&fields2=f51,f53&klt=101&fqt=1&lmt=3&end=20500101"],
        ["腾讯K线 web.ifzq", "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param=sh000001,day,,,5,qfq"],
        ["腾讯K线 ifzq", "https://ifzq.gtimg.cn/appstock/app/fqkline/get?param=sh000001,day,,,5,qfq"],
        ["新浪K线", "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData?symbol=sh000001&scale=240&ma=no&datalen=5"],
        ["东财快照 push2", "https://push2.eastmoney.com/api/qt/ulist.np/get?fltt=2&secids=1.000001&fields=f2,f3,f12"],
        ["东财快照 1.push2", "https://1.push2.eastmoney.com/api/qt/ulist.np/get?fltt=2&secids=1.000001&fields=f2,f3,f12"],
        ["腾讯快照 qt.gtimg", "https://qt.gtimg.cn/q=sh000001"],
        ["东财列表 clist", "https://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=5&po=1&np=1&fltt=2&invt=2&fid=f3&fs=m:90+t:2&fields=f3,f12,f14"],
        ["东财列表 1.push2", "https://1.push2.eastmoney.com/api/qt/clist/get?pn=1&pz=5&po=1&np=1&fltt=2&invt=2&fid=f3&fs=m:90+t:2&fields=f3,f12,f14"],
        ["新浪列表", "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData?page=1&num=5&sort=changepercent&asc=0&node=hs_a"],
        ["新浪实时 hq", "https://hq.sinajs.cn/list=sh000001"],
        ["东财涨停池 push2ex", "https://push2ex.eastmoney.com/getTopicZTPool?ut=7eea3edcaed734bea9cbfc24409ed989&dpt=wz.ztzt&Pageindex=0&pagesize=3&sort=fbt:asc&date=20261023"],
        ["东财数据中心", "https://datacenter-web.eastmoney.com/api/data/v1/get?reportName=RPTA_RZRQ_LSHJ&columns=ALL&pageSize=2&pageNumber=1"],
        ["蛋卷估值", "https://danjuanfunds.com/djapi/index_eva/detail/SH000300"],
        ["东财快讯", "https://np-listapi.eastmoney.com/comm/web/getFastNewsList?client=web&biz=web_news&fastColumn=102&pageSize=3&req_trace=1"]
      ];
      const out = [];
      await Promise.all(CAND.map(async ([name, url]) => {
        const t0 = Date.now();
        try {
          const ctl = new AbortController(); const tm = setTimeout(() => ctl.abort(), 12000);
          const r = await fetch(url, { headers: { "User-Agent": "Mozilla/5.0", "Referer": "https://quote.eastmoney.com/" }, signal: ctl.signal });
          const t = await r.text(); clearTimeout(tm);
          out.push({ name, ok: r.ok, status: r.status, len: t.length, ms: Date.now() - t0 });
        } catch (e) {
          out.push({ name, ok: false, status: String(e.message || e).slice(0, 60), len: 0, ms: Date.now() - t0 });
        }
      }));
      return sendJSON(res, { items: out });
    }

    // 静态资源
    let file = p === "/" ? "/index.html" : p;
    const fp = path.join(PUBLIC, path.normalize(file).replace(/^([/\\])+/, ""));
    if (!fp.startsWith(PUBLIC)) return send(res, 403, "Forbidden");
    fs.stat(fp, (err, st) => {
      if (err || !st.isFile()) return send(res, 404, "Not Found", { "Content-Type": "text/plain; charset=utf-8" });
      res.writeHead(200, { "Content-Type": MIME[path.extname(fp).toLowerCase()] || "application/octet-stream", "Cache-Control": "no-cache" });
      fs.createReadStream(fp).pipe(res);
    });
  } catch (e) {
    send(res, 500, JSON.stringify({ error: String(e.message || e) }), { "Content-Type": "application/json; charset=utf-8" });
  }
});

store.ensure();
server.listen(cfg.PORT, "0.0.0.0", () => {
  console.log(`[MarketPulse] 服务已启动 http://0.0.0.0:${cfg.PORT}`);
  console.log(`[MarketPulse] 数据源：扶摇=${require("./src/providers/fuyao").enabled()} iFinD=${require("./src/providers/ifind").enabled()} 东财=${cfg.eastmoney.enabled}`);
  safeRefresh("boot").then(o => console.log(`[MarketPulse] 首次取数完成：${o && o.asOf}｜状态 ${o && o.state && o.state.name}`))
    .catch(e => console.error("首次取数失败", e));
  scheduler.start(() => safeRefresh("auto"), s => { SCHED = s; });
});
let SCHED = null;

module.exports = server;
