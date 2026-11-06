use std::collections::VecDeque;
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[path = "../crypto_tx.rs"]
mod crypto_tx;

const PUSD_TOKEN: &str = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB";
const USDCE_TOKEN: &str = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174";
const CTF_CONTRACT: &str = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045";
const POLYGON_RPC: &str = "https://polygon-bor-rpc.publicnode.com";

#[derive(Clone, Debug, Default)]
struct OpenPositionView {
    token_id: String,
    slug: String,
    side: String,           // "UP" or "DOWN"
    shares: f64,            // ex: 25.00
    raw_shares: u64,        // ex: 25_000_001 (6 casas decimais on-chain)
    avg_entry: f64,         // ex: 0.42
    cost_usd: f64,          // ex: 10.50
    live_bid: f64,          // Best Bid em tempo real (350ms)
    vwap_bid: f64,          // VWAP Bid real nos top-4 níveis do livro
    cashout_usd: f64,       // Valor real das shares agora ($) = shares * vwap_bid
    profit_usd: f64,        // Lucro líquido agora ($) = cashout_usd - cost_usd
    profit_pct: f64,        // Lucro líquido agora (%)
    expiry_win_usd: f64,    // Valor na expiração ($1.00/share)
    is_current_candle: bool,
}

#[derive(Clone, Debug)]
struct NewsItem {
    title: String,
    source: String,
    pub_time: String,
    sentiment: &'static str, // "BULLISH", "BEARISH", "NEUTRAL"
    impact_score: f64,
}

#[derive(Clone, Debug, Default)]
struct SensorCell {
    id: &'static str,
    name: &'static str,
    vote: i8,       // +1 UP, -1 DOWN, 0 NEUTRAL
    strength: f64,  // 0.0 .. 1.0
    metric: String, // Short human-readable telemetry (e.g., "Gap +11c", "OBI +0.62")
    pnl_usd: f64,   // Lucro acumulado do bot ($)
    wins: u32,      // Operações lucrativas do bot
    losses: u32,    // Operações negativas do bot
    weight: f64,    // Peso de voto Darwiniano baseado no lucro (0.20x .. 2.50x)
}

#[derive(Clone, Debug, Default)]
struct FixedSignalView {
    locked: bool,
    dir: &'static str, // "UP", "DOWN", "CALCULANDO"
    entry_price: f64,
    tp_price: f64,
    stop_price: f64,
    stake_usd: f64,
    shares: f64,
    scalp_profit_usd: f64,
    scalp_roi_pct: f64,
    expiry_profit_usd: f64,
    expiry_roi_pct: f64,
    locked_age_sec: u64,
    locked_weighted_score: f64,
    locked_aligned_bots: u8,
    current_mark: f64,
    live_pnl_usd: f64,
    live_pnl_pct: f64,
    phase_code: &'static str,  // "CALC", "ENTER_NOW", "HOLD", "TAKE_PROFIT", "STOP_WARN"
    phase_title: &'static str,
    phase_reason: String,
    calc_progress_pct: f64,
    session_wins: u32,
    session_losses: u32,
    session_pnl_usd: f64,
    swarm_total_bot_pnl: f64,
    profitable_bots_count: u8,
}

#[derive(Clone, Copy, Debug, Default)]
struct StakeRow {
    stake_usd: f64,
    shares: f64,
    scalp_profit_usd: f64,
    scalp_roi_pct: f64,
    expiry_profit_usd: f64,
    expiry_roi_pct: f64,
}

#[derive(Clone, Debug, Default)]
struct MarketTelemetry {
    key: String,
    label: String,
    slug: String,
    window_ts: u64,
    rem_sec: i64,
    price_to_beat: f64,
    current_price: f64,
    diff_usd: f64,
    diff_pct: f64,
    sigma_1s: f64,
    vel_5s: f64,
    vel_15s: f64,
    vel_45s: f64,
    taker_buy_ratio: f64,
    binance_obi_20: f64,
    binance_vai: f64,
    up_token: String,
    down_token: String,
    up_bid: f64,
    up_ask: f64,
    down_bid: f64,
    down_ask: f64,
    obi: f64,
    whale_up_usd: f64,
    whale_down_usd: f64,
    delta_whale_usd: f64,
    top_wall_up_usd: f64,
    top_wall_down_usd: f64,
    up_bids_top4: [(f64, f64); 4],
    up_asks_top4: [(f64, f64); 4],
    down_bids_top4: [(f64, f64); 4],
    down_asks_top4: [(f64, f64); 4],
    spot_history_60: Vec<f64>,
    // 25-Agent Sovereign Swarm Consensus Outputs (Percentage kept internal!)
    sensors: [SensorCell; 25],
    up_votes: u8,
    down_votes: u8,
    neutral_votes: u8,
    dominant_dir: &'static str, // "UP", "DOWN", "NEUTRO"
    aligned_count: u8,          // 0..=25
    weighted_score: f64,        // 0.0..=25.0 ponderado pelo lucro & log-odds Bayesiano dos 25 bots
    fixed: FixedSignalView,
    // Previsão +15s/+30s (RLS online) e validação ao vivo
    fc_ready: bool,
    calib_n: u32,
    fc15_pred: f64,
    fc30_pred: f64,
    fc15_dspot: f64,
    fc30_dspot: f64,
    fc15_band: f64,
    fc30_band: f64,
    fc15_up: f64,
    fc30_up: f64,
    fc15_gain_c: f64,
    fc30_gain_c: f64,
    validity_label: &'static str,
    validity_code: &'static str,
    streak: u32,
    sigma_regime: f64,
    acc: [AccStats; 6],
    intuition_index: f64,       // 0.0..100.0 (Swarm Conviction + Lead-Time Asymmetry)
    anticipation_status: &'static str, // "JANELA DE OURO ANTECIPADA", "ENTRADA TARDIA", "BLOQUEADO: LIVRO CARO", "AGUARDANDO GATILHO"
    anticipation_code: &'static str,   // "GOLD", "LATE", "BLOCKED", "WAIT"
    oracle_command: &'static str,
    recommended_stake_usd: f64,
    confidence_tier_label: &'static str,
    target_ask: f64,
    entry_lim: f64,
    scalp_tp: f64,
    stop_lvl: f64,
    latency_gap_cents: f64,
    stake_rows: [StakeRow; 3], // $1.00, $2.00, $3.00
}

#[derive(Clone, Debug)]
struct CockpitState {
    updated_ms: u64,
    proxy_wallet: String,
    pusd_balance: f64,
    usdce_balance: f64,
    active_proxy_tag: String,
    proxy_latency_ms: u64,
    markets: Vec<MarketTelemetry>, // Strictly [0] = BTC 5M
    open_positions: Vec<OpenPositionView>,
    news: Vec<NewsItem>,
    news_macro_bias: f64,
    last_action_log: String,
    recent_journal_rows: Vec<String>,
    force_recalc: bool,
    force_pos_refresh: bool,
}

impl Default for CockpitState {
    fn default() -> Self {
        Self {
            updated_ms: 0,
            proxy_wallet: String::new(),
            pusd_balance: 0.0,
            usdce_balance: 0.0,
            active_proxy_tag: "DIRECT+PROXY_READY".to_string(),
            proxy_latency_ms: 0,
            markets: Vec::new(),
            open_positions: Vec::new(),
            news: Vec::new(),
            news_macro_bias: 0.0,
            last_action_log: "Enxame 25 Bots Superinteligência Inicializado — Foco Exclusivo BITCOIN 5M (Predictor GOD Enter $+)".to_string(),
            recent_journal_rows: Vec::new(),
            force_recalc: false,
            force_pos_refresh: true,
        }
    }
}

fn now_sec() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

fn curl_get(url: &str, timeout_sec: f64) -> Option<String> {
    let t_str = format!("{:.1}", timeout_sec);
    let out = Command::new("curl")
        .args([
            "-4",
            "--resolve",
            "gamma-api.polymarket.com:443:3.170.170.24",
            "--resolve",
            "clob.polymarket.com:443:104.18.35.205",
            "-s",
            "-m",
            &t_str,
            "-H",
            "Accept: application/json",
            "-H",
            "User-Agent: Mozilla/5.0",
            url,
        ])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    String::from_utf8(out.stdout).ok()
}

fn extract_json_str(json: &str, key: &str) -> Option<String> {
    let pat = format!("\"{}\":\"", key);
    let idx = json.find(&pat)? + pat.len();
    let rem = &json[idx..];
    let end = rem.find('"')?;
    Some(rem[..end].to_string())
}

fn extract_json_f64(json: &str, key: &str) -> Option<f64> {
    let pat = format!("\"{}\":", key);
    let idx = json.find(&pat)? + pat.len();
    let rem = json[idx..].trim_start();
    if rem.starts_with('"') {
        let s = &rem[1..];
        let end = s.find('"')?;
        s[..end].parse::<f64>().ok()
    } else {
        let end = rem.find(|c: char| c == ',' || c == '}' || c == ']' || c.is_whitespace()).unwrap_or(rem.len());
        rem[..end].parse::<f64>().ok()
    }
}

fn query_erc20_balance(token: &str, wallet: &str) -> Option<f64> {
    let clean = wallet.trim_start_matches("0x").to_lowercase();
    if clean.len() != 40 || clean == "0000000000000000000000000000000000000000" {
        return None;
    }
    let data = format!("0x70a08231000000000000000000000000{}", clean);
    let payload = format!(
        "{{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_call\",\"params\":[{{\"to\":\"{}\",\"data\":\"{}\"}},\"latest\"]}}",
        token, data
    );
    let out = Command::new("curl")
        .args(["-s", "-m", "2.5", "-X", "POST", "-H", "Content-Type: application/json", "--data", &payload, POLYGON_RPC])
        .output()
        .ok()?;
    let body = String::from_utf8(out.stdout).ok()?;
    let res_hex = extract_json_str(&body, "result")?;
    let clean_hex = res_hex.trim_start_matches("0x");
    if clean_hex.is_empty() {
        return Some(0.0);
    }
    let raw = u128::from_str_radix(clean_hex, 16).ok()?;
    Some((raw as f64) / 1_000_000.0)
}

fn dec_str_to_hex32(s: &str) -> String {
    let mut bytes = [0u8; 32];
    for c in s.bytes() {
        if c.is_ascii_digit() {
            let mut carry = (c - b'0') as u32;
            for i in (0..32).rev() {
                let v = (bytes[i] as u32) * 10 + carry;
                bytes[i] = (v & 0xff) as u8;
                carry = v >> 8;
            }
        }
    }
    let mut out = String::with_capacity(64);
    for b in &bytes {
        out.push_str(&format!("{:02x}", b));
    }
    out
}

/// Consulta direta on-chain ERC-1155 `balanceOf(address,uint256)` no contrato CTF da Polygon (0ms de atraso de indexador).
fn query_erc1155_balance_single(wallet: &str, token_id: &str) -> Option<u64> {
    let clean = wallet.trim_start_matches("0x").to_lowercase();
    if clean.len() != 40 || token_id.is_empty() {
        return None;
    }
    let tok_hex = dec_str_to_hex32(token_id);
    let data = format!("0x00fdd58e000000000000000000000000{}{}", clean, tok_hex);
    let payload = format!(
        "{{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_call\",\"params\":[{{\"to\":\"{}\",\"data\":\"{}\"}},\"latest\"]}}",
        CTF_CONTRACT, data
    );
    let out = Command::new("curl")
        .args(["-s", "-m", "1.8", "-X", "POST", "-H", "Content-Type: application/json", "--data", &payload, POLYGON_RPC])
        .output()
        .ok()?;
    let body = String::from_utf8(out.stdout).ok()?;
    let res_hex = extract_json_str(&body, "result")?;
    let clean_hex = res_hex.trim_start_matches("0x").trim_start_matches('0');
    if clean_hex.is_empty() {
        return Some(0);
    }
    u64::from_str_radix(clean_hex, 16).ok()
}

/// Consulta simultânea em lote (JSON-RPC Batch) dos saldos ERC-1155 de UP e DOWN da vela atual (~110ms).
fn query_erc1155_balances_pair(wallet: &str, up_token: &str, down_token: &str) -> (u64, u64) {
    let clean = wallet.trim_start_matches("0x").to_lowercase();
    if clean.len() != 40 || up_token.is_empty() || down_token.is_empty() {
        return (0, 0);
    }
    let up_hex = dec_str_to_hex32(up_token);
    let dn_hex = dec_str_to_hex32(down_token);
    let d1 = format!("0x00fdd58e000000000000000000000000{}{}", clean, up_hex);
    let d2 = format!("0x00fdd58e000000000000000000000000{}{}", clean, dn_hex);
    let payload = format!(
        "[{{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_call\",\"params\":[{{\"to\":\"{}\",\"data\":\"{}\"}},\"latest\"]}},{{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"eth_call\",\"params\":[{{\"to\":\"{}\",\"data\":\"{}\"}},\"latest\"]}}]",
        CTF_CONTRACT, d1, CTF_CONTRACT, d2
    );
    let out = match Command::new("curl")
        .args(["-s", "-m", "1.8", "-X", "POST", "-H", "Content-Type: application/json", "--data", &payload, POLYGON_RPC])
        .output()
    {
        Ok(o) => o,
        Err(_) => return (0, 0),
    };
    let body = String::from_utf8_lossy(&out.stdout);
    let mut up_raw = 0u64;
    let mut dn_raw = 0u64;
    for part in body.split('{').skip(1) {
        let id = extract_json_f64(part, "id").unwrap_or(0.0) as i32;
        if let Some(r_hex) = extract_json_str(part, "result") {
            let ch = r_hex.trim_start_matches("0x").trim_start_matches('0');
            let val = if ch.is_empty() { 0 } else { u64::from_str_radix(ch, 16).unwrap_or(0) };
            if id == 1 {
                up_raw = val;
            } else if id == 2 {
                dn_raw = val;
            }
        }
    }
    (up_raw, dn_raw)
}

#[derive(Clone, Debug)]
struct RawApiPos {
    token_id: String,
    slug: String,
    side: String,
    size: f64,
    avg_price: f64,
    cost_usd: f64,
    cur_price: f64,
}

fn fetch_data_api_open_positions(wallet: &str) -> Vec<RawApiPos> {
    let mut out = Vec::new();
    if wallet.is_empty() {
        return out;
    }
    let url = format!("https://data-api.polymarket.com/v2/positions?user={}", wallet);
    let body = match Command::new("curl")
        .args(["-4", "-s", "-m", "2.0", "-H", "Accept: application/json", &url])
        .output()
    {
        Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout).to_string(),
        _ => return out,
    };
    for block in body.split("{\"proxy_wallet\":").skip(1) {
        let status = extract_json_str(block, "status").unwrap_or_default();
        if status != "OPEN" {
            continue;
        }
        let size = extract_json_f64(block, "current_size").unwrap_or(0.0);
        if size < 0.05 {
            continue;
        }
        let token_id = extract_json_str(block, "token_id").unwrap_or_default();
        let slug = extract_json_str(block, "slug").unwrap_or_default();
        let outcome = extract_json_str(block, "outcome").unwrap_or_default();
        let side = if outcome.eq_ignore_ascii_case("Up") || outcome.eq_ignore_ascii_case("Yes") {
            "UP".to_string()
        } else {
            "DOWN".to_string()
        };
        let avg_price = extract_json_f64(block, "avg_price").unwrap_or(0.50);
        let cost_usd = extract_json_f64(block, "entry_cost_usdc").unwrap_or(size * avg_price);
        let cur_price = extract_json_f64(block, "current_price").unwrap_or(0.0);
        out.push(RawApiPos {
            token_id,
            slug,
            side,
            size,
            avg_price,
            cost_usd,
            cur_price,
        });
    }
    out
}

/// Calcula o preço médio de venda real (VWAP Bid) varrendo os 4 níveis do livro para `shares` cotas.
fn compute_vwap_bid(best_bid: f64, bids_top4: &[(f64, f64); 4], shares: f64) -> f64 {
    if shares <= 0.001 || best_bid <= 0.001 {
        return best_bid.max(0.0);
    }
    let mut rem = shares;
    let mut proceeds = 0.0;
    let mut filled = 0.0;
    for &(p, sz) in bids_top4.iter() {
        if p > 0.001 && sz > 0.01 && rem > 0.0 {
            let take = rem.min(sz);
            proceeds += take * p;
            filled += take;
            rem -= take;
        }
    }
    if filled <= 0.01 {
        return best_bid;
    }
    if rem > 0.0 {
        // Se o lote for maior que os top-4 níveis visíveis, assume execução do restante a (último nível - 1¢)
        let tail_p = bids_top4
            .iter()
            .rev()
            .find(|&&(p, s)| p > 0.001 && s > 0.0)
            .map(|&(p, _)| (p - 0.01).max(0.01))
            .unwrap_or((best_bid - 0.01).max(0.01));
        proceeds += rem * tail_p;
        filled += rem;
    }
    (proceeds / filled).max(0.001)
}

fn fetch_market_tokens_by_slug(slug: &str) -> Option<(String, String)> {
    let url = format!("https://gamma-api.polymarket.com/events?slug={}", slug);
    let body = curl_get(&url, 2.2)?;
    let pat = "\"clobTokenIds\":\"[\\\"";
    let idx = body.find(pat)? + pat.len();
    let rem = &body[idx..];
    let end1 = rem.find("\\\"")?;
    let tok_a = rem[..end1].to_string();
    let rem2 = &rem[end1 + 2..];
    let start2 = rem2.find("\\\"")? + 2;
    let rem3 = &rem2[start2..];
    let end2 = rem3.find("\\\"")?;
    let tok_b = rem3[..end2].to_string();
    Some((tok_a, tok_b))
}

#[derive(Clone, Copy, Debug)]
struct DetailedBook {
    best_bid: f64,
    best_ask: f64,
    bid_depth_top5: f64,
    whale_usd_35to70: f64,
    top_wall_usd: f64,
    bids_top4: [(f64, f64); 4],
    asks_top4: [(f64, f64); 4],
}

fn parse_book_detailed(token_id: &str) -> Option<DetailedBook> {
    if token_id.is_empty() {
        return None;
    }
    let url = format!("https://clob.polymarket.com/book?token_id={}", token_id);
    let body = curl_get(&url, 1.5)?;
    let mut best_bid = 0.0_f64;
    let mut best_ask = 0.99_f64;
    let mut bid_depth_top5 = 0.0_f64;
    let mut whale_usd_35to70 = 0.0_f64;
    let mut top_wall_usd = 0.0_f64;
    let mut all_bids: Vec<(f64, f64)> = Vec::with_capacity(32);
    let mut all_asks: Vec<(f64, f64)> = Vec::with_capacity(32);

    if let Some(b_idx) = body.find("\"bids\":[") {
        let b_rem = &body[b_idx + 8..];
        if let Some(b_end) = b_rem.find(']') {
            let b_slice = &b_rem[..b_end];
            for item in b_slice.split('{').skip(1) {
                let p = extract_json_f64(item, "price").unwrap_or(0.0);
                let s = extract_json_f64(item, "size").unwrap_or(0.0);
                if p > 0.0 && s > 0.0 {
                    let usd = p * s;
                    if p > best_bid {
                        best_bid = p;
                    }
                    if usd > top_wall_usd {
                        top_wall_usd = usd;
                    }
                    if (0.18..=0.72).contains(&p) && s >= 12.0 {
                        whale_usd_35to70 += usd;
                    }
                    all_bids.push((p, s));
                }
            }
        }
    }

    if let Some(a_idx) = body.find("\"asks\":[") {
        let a_rem = &body[a_idx + 8..];
        if let Some(a_end) = a_rem.find(']') {
            let a_slice = &a_rem[..a_end];
            for item in a_slice.split('{').skip(1) {
                let p = extract_json_f64(item, "price").unwrap_or(0.99);
                let s = extract_json_f64(item, "size").unwrap_or(0.0);
                if p > 0.0 && s > 0.0 {
                    if p < best_ask {
                        best_ask = p;
                    }
                    all_asks.push((p, s));
                }
            }
        }
    }

    all_bids.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    all_asks.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

    for (i, &(p, s)) in all_bids.iter().enumerate() {
        if i < 5 {
            bid_depth_top5 += p * s;
        }
    }

    let mut bids_top4 = [(0.0, 0.0); 4];
    let mut asks_top4 = [(0.0, 0.0); 4];
    for i in 0..4 {
        if i < all_bids.len() {
            bids_top4[i] = all_bids[i];
        }
        if i < all_asks.len() {
            asks_top4[i] = all_asks[i];
        }
    }

    Some(DetailedBook {
        best_bid,
        best_ask,
        bid_depth_top5,
        whale_usd_35to70,
        top_wall_usd,
        bids_top4,
        asks_top4,
    })
}

/// Fetches Binance 1s klines (last 65 seconds) -> returns (current, strike_open_est, sigma_1s, v5, v15, v45, taker_buy_ratio, vpin_10s, absorption, history_60)
fn fetch_binance_microstructure(symbol: &str, window_start_ms: u64) -> Option<(f64, f64, f64, f64, f64, f64, f64, f64, f64, Vec<f64>)> {
    let url = format!("https://api.binance.com/api/v3/klines?symbol={}&interval=1s&limit=65", symbol);
    let body = curl_get(&url, 1.5)?;
    let mut closes = Vec::with_capacity(65);
    let mut total_vol = 0.0_f64;
    let mut taker_buy_vol = 0.0_f64;
    let mut vol_10s = 0.0_f64;
    let mut abs_imb_10s = 0.0_f64;

    let rows: Vec<&str> = body.split('[').skip(2).collect();
    let total_rows = rows.len();
    for (idx, row) in rows.iter().enumerate() {
        let cols: Vec<&str> = row.split(',').collect();
        if cols.len() >= 10 {
            let c = cols[4].trim_matches('"').parse::<f64>().unwrap_or(0.0);
            let v = cols[5].trim_matches('"').parse::<f64>().unwrap_or(0.0);
            let tb = cols[9].trim_matches('"').parse::<f64>().unwrap_or(0.0);
            if c > 0.0 {
                closes.push(c);
                total_vol += v;
                taker_buy_vol += tb;
                if idx + 10 >= total_rows {
                    let ts = (v - tb).max(0.0);
                    vol_10s += v;
                    abs_imb_10s += (tb - ts).abs();
                }
            }
        }
    }
    if closes.len() < 15 {
        return None;
    }
    let n = closes.len();
    let current = closes[n - 1];
    let p_5 = closes[n.saturating_sub(6)];
    let p_15 = closes[n.saturating_sub(16)];
    let p_45 = closes[n.saturating_sub(46)];
    let v5 = current - p_5;
    let v15 = current - p_15;
    let v45 = current - p_45;

    let mut sum_sq = 0.0_f64;
    for i in 1..n {
        let d = closes[i] - closes[i - 1];
        sum_sq += d * d;
    }
    let sigma_1s = (sum_sq / ((n - 1) as f64)).sqrt().max(current * 0.00004);
    let taker_ratio = if total_vol > 0.0 { (taker_buy_vol / total_vol).clamp(0.0, 1.0) } else { 0.5 };
    let vpin_10s = if vol_10s > 0.0 { (abs_imb_10s / vol_10s).clamp(0.0, 1.0) } else { 0.0 };

    // Effort vs Result Absorption (Agent-02)
    let dp_10s = (current - closes[n.saturating_sub(11)]).abs();
    let expected_dp = sigma_1s * 10.0_f64.sqrt();
    let absorption = (1.0 + 0.5 * (vpin_10s - (dp_10s / (expected_dp + 1e-6)) * 0.35).clamp(0.0, 1.0)).clamp(1.0, 1.5);

    let strike_url = format!(
        "https://api.binance.com/api/v3/klines?symbol={}&interval=1s&startTime={}&limit=1",
        symbol, window_start_ms
    );
    let mut strike = closes[0];
    if let Some(s_body) = curl_get(&strike_url, 1.2) {
        let s_cols: Vec<&str> = s_body.split(',').collect();
        if s_cols.len() >= 5 {
            if let Ok(open_p) = s_cols[1].trim_matches(|c| c == '"' || c == '[').parse::<f64>() {
                if open_p > 0.0 {
                    strike = open_p;
                }
            }
        }
    }

    let hist_start = n.saturating_sub(60);
    let history_60 = closes[hist_start..].to_vec();

    Some((current, strike, sigma_1s, v5, v15, v45, taker_ratio, vpin_10s, absorption, history_60))
}

/// Agent-07: Fetches Binance 20-level Orderbook Depth (`api/v3/depth?symbol=...&limit=20`)
/// Returns `(obi_20, vai, composite_vote)`
fn fetch_binance_depth_20(symbol: &str) -> (f64, f64, i8) {
    let url = format!("https://api.binance.com/api/v3/depth?symbol={}&limit=20", symbol);
    let body = match curl_get(&url, 1.2) {
        Some(b) => b,
        None => return (0.0, 0.0, 0),
    };

    let parse_side = |section: &str| -> Vec<[f64; 2]> {
        let mut out = Vec::with_capacity(20);
        for item in section.split('[').skip(2) {
            if let Some(end) = item.find(']') {
                let inner = &item[..end];
                let mut parts = inner.split(',');
                if let (Some(ps), Some(qs)) = (parts.next(), parts.next()) {
                    let p = ps.trim_matches('"').parse::<f64>().unwrap_or(0.0);
                    let q = qs.trim_matches('"').parse::<f64>().unwrap_or(0.0);
                    if p > 0.0 && q > 0.0 {
                        out.push([p, q]);
                    }
                }
            }
            if out.len() >= 20 {
                break;
            }
        }
        out
    };

    let bids_sec = body.find("\"bids\":").map(|i| &body[i..]).unwrap_or("");
    let asks_sec = body.find("\"asks\":").map(|i| &body[i..]).unwrap_or("");
    let bids = parse_side(bids_sec);
    let asks = parse_side(asks_sec);

    let n = bids.len().min(asks.len()).min(20);
    if n < 5 {
        return (0.0, 0.0, 0);
    }

    let best_bid = bids[0][0];
    let best_ask = asks[0][0];
    let mid = 0.5 * (best_bid + best_ask);
    if mid <= 0.0 || best_ask <= best_bid {
        return (0.0, 0.0, 0);
    }

    let inv_mid_bps = 1e4 / mid;
    let lambda = 0.35_f64;
    let mut w_bid = 0.0_f64;
    let mut w_ask = 0.0_f64;
    let mut raw_bid = 0.0_f64;
    let mut raw_ask = 0.0_f64;
    let mut max_bid_gap = 0.0_f64;
    let mut max_ask_gap = 0.0_f64;

    for i in 0..n {
        let (bp, bq) = (bids[i][0], bids[i][1]);
        let (ap, aq) = (asks[i][0], asks[i][1]);
        let b_bps = ((mid - bp) * inv_mid_bps).max(0.0);
        let a_bps = ((ap - mid) * inv_mid_bps).max(0.0);
        w_bid += bq * (-lambda * b_bps).exp();
        w_ask += aq * (-lambda * a_bps).exp();
        raw_bid += bq;
        raw_ask += aq;
        if i > 0 {
            let bg = (bids[i - 1][0] - bp).max(0.0);
            let ag = (ap - asks[i - 1][0]).max(0.0);
            if bg > max_bid_gap {
                max_bid_gap = bg;
            }
            if ag > max_ask_gap {
                max_ask_gap = ag;
            }
        }
    }

    let w_sum = w_bid + w_ask;
    if w_sum <= 1e-12 || raw_bid <= 1e-12 || raw_ask <= 1e-12 {
        return (0.0, 0.0, 0);
    }

    let obi_20 = (w_bid - w_ask) / w_sum;
    let bid_span_bps = ((best_bid - bids[n - 1][0]) * inv_mid_bps).max(0.05);
    let ask_span_bps = ((asks[n - 1][0] - best_ask) * inv_mid_bps).max(0.05);
    let impact_ask = (ask_span_bps + 1.5 * max_ask_gap * inv_mid_bps) / raw_ask;
    let impact_bid = (bid_span_bps + 1.5 * max_bid_gap * inv_mid_bps) / raw_bid;
    let vai = (impact_ask - impact_bid) / (impact_ask + impact_bid + 1e-12);
    let comp = 0.55 * obi_20 + 0.45 * vai;

    let vote = if (comp >= 0.24 && obi_20 > 0.08) || (obi_20 >= 0.36 && vai >= 0.15) {
        1
    } else if (comp <= -0.24 && obi_20 < -0.08) || (obi_20 <= -0.36 && vai <= -0.15) {
        -1
    } else {
        0
    };

    (obi_20, vai, vote)
}

fn norm_cdf(x: f64) -> f64 {
    let a1 = 0.254829592;
    let a2 = -0.284496736;
    let a3 = 1.421413741;
    let a4 = -1.453152027;
    let a5 = 1.061405429;
    let p = 0.3275911;
    let sign = if x < 0.0 { -1.0 } else { 1.0 };
    let z = x.abs() / std::f64::consts::SQRT_2;
    let t = 1.0 / (1.0 + p * z);
    let y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * (-z * z).exp();
    0.5 * (1.0 + sign * y)
}


// ====================== MEMÓRIA, APRENDIZADO ONLINE (RLS) E AUDITORIA +15s/+30s ======================
const N_FEAT: usize = 10;
const MIN_CALIB: u32 = 30;
const AUDIT_CSV: &str = "/home/god/.gemini/antigravity/scratch/polymarket_agent_bundle/forecast_audit.csv";

/// Recursive Least Squares com fator de esquecimento: aprende a cada tick, sem lote, O(N²).
#[derive(Clone, Debug)]
struct Rls {
    w: [f64; N_FEAT],
    p: [[f64; N_FEAT]; N_FEAT],
    lambda: f64,
    updates: u32,
}

impl Rls {
    fn new() -> Self {
        let mut p = [[0.0; N_FEAT]; N_FEAT];
        for (i, row) in p.iter_mut().enumerate() {
            row[i] = 10.0;
        }
        Self { w: [0.0; N_FEAT], p, lambda: 0.997, updates: 0 }
    }

    fn predict(&self, x: &[f64; N_FEAT]) -> f64 {
        let mut s = 0.0;
        for i in 0..N_FEAT {
            s += self.w[i] * x[i];
        }
        if s.is_finite() { s.clamp(-3.0, 3.0) } else { 0.0 }
    }

    fn update(&mut self, x: &[f64; N_FEAT], y: f64) {
        if !y.is_finite() || x.iter().any(|v| !v.is_finite()) {
            return;
        }
        let y = y.clamp(-4.0, 4.0);
        let mut px = [0.0; N_FEAT];
        for i in 0..N_FEAT {
            for j in 0..N_FEAT {
                px[i] += self.p[i][j] * x[j];
            }
        }
        let mut denom = self.lambda;
        for i in 0..N_FEAT {
            denom += x[i] * px[i];
        }
        if denom <= 1e-9 {
            return;
        }
        let err = y - self.predict(x);
        for i in 0..N_FEAT {
            self.w[i] += (px[i] / denom) * err;
        }
        for i in 0..N_FEAT {
            for j in 0..N_FEAT {
                self.p[i][j] = (self.p[i][j] - px[i] * px[j] / denom) / self.lambda;
            }
        }
        // Estabilidade numérica: simetriza e limita a covariância
        for i in 0..N_FEAT {
            for j in (i + 1)..N_FEAT {
                let a = 0.5 * (self.p[i][j] + self.p[j][i]);
                self.p[i][j] = a;
                self.p[j][i] = a;
            }
            self.p[i][i] = self.p[i][i].clamp(1e-6, 100.0);
        }
        self.updates = self.updates.saturating_add(1);
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct AccStats {
    hits: u32,
    n: u32,
}

impl AccStats {
    fn rate(&self) -> f64 {
        if self.n == 0 { 0.0 } else { self.hits as f64 / self.n as f64 }
    }
    fn record(&mut self, hit: bool) {
        self.n += 1;
        if hit {
            self.hits += 1;
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct BotLedger {
    pnl_usd: f64,
    wins: u32,
    losses: u32,
    ema_wr: f64,
    weight: f64,
}

impl Default for BotLedger {
    fn default() -> Self {
        Self { pnl_usd: 0.0, wins: 0, losses: 0, ema_wr: 0.52, weight: 1.0 }
    }
}

impl BotLedger {
    fn record_outcome(&mut self, profit_usd: f64) {
        self.pnl_usd += profit_usd;
        let won = if profit_usd > 0.002 {
            self.wins += 1;
            true
        } else if profit_usd < -0.002 {
            self.losses += 1;
            false
        } else {
            return;
        };
        // S25 Bayesian Log-Odds + EWMA Win-Rate + Per-Trade Edge (corrige saturação de tanh!)
        self.ema_wr = 0.94 * self.ema_wr + 0.06 * if won { 1.0 } else { 0.0 };
        let n = (self.wins + self.losses).max(1) as f64;
        let bayes_wr = 0.55 * ((self.wins as f64 + 2.0) / (n + 4.0)) + 0.45 * self.ema_wr;
        let p = bayes_wr.clamp(0.05, 0.95);
        let log_odds = (p / (1.0 - p)).ln(); // 0.0 em 50%, +0.33 em 58.2%, -0.06 em 48.4%
        let conf = (n / (n + 18.0)).sqrt();
        let hedge_gate = 1.0 / (1.0 + (-(log_odds - 0.08) * 24.0).exp());
        let avg_edge = (self.pnl_usd / (n * 0.05)).tanh().clamp(-0.5, 1.0);
        self.weight = if p < 0.495 && n >= 12.0 {
            0.15
        } else {
            (0.20 + (2.25 + 0.55 * avg_edge.max(0.0)) * hedge_gate * conf).clamp(0.15, 3.00)
        };
    }
}

#[derive(Clone, Debug, Default)]
struct LockedSignal {
    active: bool,
    window_ts: u64,
    locked_at_ms: u64,
    dir: &'static str, // "UP" or "DOWN"
    dir_sign: i8,      // +1 or -1
    entry_price: f64,
    tp_price: f64,
    stop_price: f64,
    stake_usd: f64,
    shares: f64,
    scalp_profit_usd: f64,
    scalp_roi_pct: f64,
    expiry_profit_usd: f64,
    expiry_roi_pct: f64,
    locked_weighted_score: f64,
    locked_aligned_bots: u8,
    votes_at_lock: [i8; 25],
    tp_hit_at_ms: u64,
    peak_mark: f64,
    contrary_ticks: u32,
    stop_ticks: u32,
}

#[derive(Clone, Debug)]
struct PendingPred {
    ts_ms: u64,
    window_ts: u64,
    spot: f64,
    sigma: f64,
    up_mid: f64,
    feats: [f64; N_FEAT],
    votes: [i8; 25],
    eval_bots: bool,
    cons_dir: i8,
    aligned: u8,
    rls15_dir: i8,
    rls30_dir: i8,
    done15: bool,
}

/// acc: [0]=Enxame +15s, [1]=Alta confiança +15s, [2]=Enxame +30s, [3]=Alta confiança +30s, [4]=RLS +15s, [5]=RLS +30s
struct MarketMemory {
    poly_hist: VecDeque<(u64, f64, f64, f64, f64)>, // (ts_ms, up_mid, spot, up_ask_depth, down_ask_depth)
    spot_samples: VecDeque<(u64, f64)>,
    pending: VecDeque<PendingPred>,
    rls15: Rls,
    rls30: Rls,
    acc: [AccStats; 6],
    bots: [BotLedger; 25],
    last_bot_eval_ms: u64,
    locked: LockedSignal,
    calc_candidate_dir: i8,
    calc_confirm_ticks: u32,
    cooldown_until_ms: u64,
    session_wins: u32,
    session_losses: u32,
    session_pnl_usd: f64,
    prev_dir: i8,
    streak: u32,
    ema_fast: f64,
    ema_slow: f64,
    window_ts: u64,
}

impl MarketMemory {
    fn new() -> Self {
        Self {
            poly_hist: VecDeque::with_capacity(128),
            spot_samples: VecDeque::with_capacity(256),
            pending: VecDeque::with_capacity(160),
            rls15: Rls::new(),
            rls30: Rls::new(),
            acc: [AccStats::default(); 6],
            bots: [BotLedger::default(); 25],
            last_bot_eval_ms: 0,
            locked: LockedSignal::default(),
            calc_candidate_dir: 0,
            calc_confirm_ticks: 0,
            cooldown_until_ms: 0,
            session_wins: 0,
            session_losses: 0,
            session_pnl_usd: 0.0,
            prev_dir: 0,
            streak: 0,
            ema_fast: 0.0,
            ema_slow: 0.0,
            window_ts: 0,
        }
    }
}

fn hist_at(hist: &VecDeque<(u64, f64, f64, f64, f64)>, target_ms: u64) -> Option<(u64, f64, f64, f64, f64)> {
    hist.iter().rev().find(|h| h.0 <= target_ms).copied()
}

/// Razão σ(últimos 15s) / σ(últimos 120s), com retornos normalizados por √Δt.
fn vol_regime(samples: &VecDeque<(u64, f64)>, now_ms: u64) -> Option<f64> {
    let stdev = |win_ms: u64| -> Option<f64> {
        let pts: Vec<&(u64, f64)> = samples.iter().filter(|s| now_ms.saturating_sub(s.0) <= win_ms).collect();
        if pts.len() < 7 {
            return None;
        }
        let mut acc = 0.0;
        let mut n = 0.0;
        for w in pts.windows(2) {
            let dt = (w[1].0.saturating_sub(w[0].0)) as f64 / 1000.0;
            if dt > 0.05 {
                let r = (w[1].1 - w[0].1) / dt.sqrt();
                acc += r * r;
                n += 1.0;
            }
        }
        if n < 5.0 { None } else { Some((acc / n).sqrt()) }
    };
    let s_short = stdev(15_000)?;
    let s_long = stdev(120_000)?;
    if s_long <= 1e-9 { None } else { Some(s_short / s_long) }
}

/// Confere previsões antigas contra o preço real atual, treina o RLS, atualiza o LUCRO REAL ($) dos 20 bots e a acurácia ao vivo.
fn mature_predictions(mem: &mut MarketMemory, now_ms: u64, spot_now: f64, up_mid_now: f64, market_key: &str) {
    if spot_now <= 0.0 {
        return;
    }
    let mut audit_lines: Vec<String> = Vec::new();
    let eval = |p: &PendingPred, age_s: f64| -> f64 { (spot_now - p.spot) / (p.sigma.max(1e-6) * age_s.sqrt()) };

    // +30s: processa e remove as mais antigas
    while let Some(front) = mem.pending.front() {
        let age_ms = now_ms.saturating_sub(front.ts_ms);
        if age_ms < 30_000 {
            break;
        }
        let p = mem.pending.pop_front().unwrap();
        if age_ms > 45_000 {
            continue; // loop travou (rede); amostra velha demais para ser honesta
        }
        let age_s = age_ms as f64 / 1000.0;
        let y = eval(&p, age_s);
        mem.rls30.update(&p.feats, y);
        if y.abs() >= 0.05 {
            let real: i8 = if y > 0.0 { 1 } else { -1 };
            if p.cons_dir != 0 {
                mem.acc[2].record(p.cons_dir == real);
                if p.aligned >= 13 {
                    mem.acc[3].record(p.cons_dir == real);
                }
            }
            if p.rls30_dir != 0 {
                mem.acc[5].record(p.rls30_dir == real);
            }
            audit_lines.push(format!(
                "{},{},30,{},{},{},{},{:.2},{:.3}",
                now_ms / 1000, market_key, p.cons_dir, p.aligned, p.rls30_dir, real, spot_now - p.spot, y
            ));
        }
    }

    // +15s: marca as que atingiram 15s e audita o LUCRO REAL ($) de cada um dos 20 bots
    for i in 0..mem.pending.len() {
        let (age_ms, done) = {
            let p = &mem.pending[i];
            (now_ms.saturating_sub(p.ts_ms), p.done15)
        };
        if done || age_ms < 15_000 {
            continue;
        }
        mem.pending[i].done15 = true;
        if age_ms > 25_000 {
            continue;
        }
        let p = mem.pending[i].clone();
        let age_s = age_ms as f64 / 1000.0;
        let y = eval(&p, age_s);
        mem.rls15.update(&p.feats, y);

        // Avalia o Lucro Real ($ e %) de cada um dos 25 bots agênticos na janela de delay (15s)
        // Nunca mistura mid-price de duas velas 5m diferentes (evita falso salto 0.95 -> 0.50 no rollover)!
        if p.eval_bots {
            let same_candle = p.window_ts == mem.window_ts && (0.04..=0.96).contains(&p.up_mid) && (0.04..=0.96).contains(&up_mid_now);
            let d_mid = if same_candle { up_mid_now - p.up_mid } else { 0.0 };
            let blended_move = if same_candle && d_mid.abs() >= 0.008 && d_mid.abs() <= 0.32 {
                0.55 * d_mid + 0.45 * (y * 0.06)
            } else {
                y * 0.06
            };
            if blended_move.abs() >= 0.003 || y.abs() >= 0.05 {
                for b_idx in 0..25 {
                    let v = p.votes[b_idx];
                    if v != 0 {
                        let dir_edge = (v as f64) * blended_move;
                        let prof_usd = if dir_edge >= 0.0 {
                            (0.08 + (dir_edge * 2.2).clamp(0.02, 0.20)).clamp(0.08, 0.28)
                        } else {
                            -(0.07 + (dir_edge.abs() * 1.8).clamp(0.02, 0.17)).clamp(0.07, 0.24)
                        };
                        mem.bots[b_idx].record_outcome(prof_usd);
                    }
                }
            }
        }

        if y.abs() >= 0.05 {
            let real: i8 = if y > 0.0 { 1 } else { -1 };
            if p.cons_dir != 0 {
                mem.acc[0].record(p.cons_dir == real);
                if p.aligned >= 13 {
                    mem.acc[1].record(p.cons_dir == real);
                }
            }
            if p.rls15_dir != 0 {
                mem.acc[4].record(p.rls15_dir == real);
            }
            audit_lines.push(format!(
                "{},{},15,{},{},{},{},{:.2},{:.3}",
                now_ms / 1000, market_key, p.cons_dir, p.aligned, p.rls15_dir, real, spot_now - p.spot, y
            ));
        }
    }

    if !audit_lines.is_empty() {
        if let Ok(mut f) = OpenOptions::new().create(true).append(true).open(AUDIT_CSV) {
            for l in &audit_lines {
                let _ = writeln!(f, "{}", l);
            }
        }
    }
}
/// Evaluates the 25 Specialized Sovereign Swarm Agents (`S01`..`S25`) on `BTC 5M`
#[allow(clippy::too_many_arguments)]
fn evaluate_20_agent_swarm(
    m: &mut MarketTelemetry,
    prev_whale_up: f64,
    prev_whale_down: f64,
    vpin_10s: f64,
    absorption: f64,
    cross_z_div: f64,
    _is_btc: bool,
    news_macro_bias: f64,
    mem: &mut MarketMemory,
    now_m: u64,
) {
    let tau = (m.rem_sec.max(3)) as f64;
    let sig = m.sigma_1s.max(1e-6);
    let sigma_rem = (sig * tau.sqrt()).max(1e-6);
    let strike = m.price_to_beat.max(1.0);
    let m_diff = m.diff_usd;

    // Multi-Scale Standardized Velocities
    let z5 = m.vel_5s / (sig * 2.2360679).max(1e-6);
    let z15 = m.vel_15s / (sig * 3.8729833).max(1e-6);
    let z45 = m.vel_45s / (sig * 6.7082039).max(1e-6);

    // Fokker-Planck First-Passage Reflection & Gamma-Pinned Fair Probability
    let z_dist = m_diff / sigma_rem;
    let mu_1s = (0.55 * (m.vel_5s / 5.0) + 0.30 * (m.vel_15s / 15.0) + 0.15 * (m.vel_45s / 45.0)) / sig;
    let kappa_pin = if z_dist.abs() < 0.85 { 0.08 * (-0.5 * z_dist * z_dist).exp() } else { 0.0 };
    let mu_eff = mu_1s - kappa_pin * z_dist;
    let z_plus = z_dist + mu_eff * (tau.min(35.0)).sqrt() * 0.45;
    let z_minus = -z_dist.abs() + mu_eff * m_diff.signum() * (tau.min(35.0)).sqrt() * 0.45;
    let lambda_fp = (-2.0 * mu_eff * z_dist).clamp(-4.0, 4.0).exp().clamp(0.0, 2.5);
    let z_obi = m.obi * 0.48 + m.binance_obi_20 * 0.42;
    let p_fp_raw = norm_cdf(0.64 * z_plus + 0.20 * z15 + 0.16 * z_obi)
        - 0.25 * m_diff.signum() * lambda_fp * norm_cdf(z_minus) * (if mu_eff * m_diff > 0.0 { 1.0 } else { 0.2 });
    let p_fair_up = p_fp_raw.clamp(0.02, 0.98);
    let p_fair_down = 1.0 - p_fair_up;

    // --- AGENT 01 (S01): Hawkes Self-Exciting Impulse & Multi-Scale Kalman Velocity ---
    let v_base_01 = 0.25 * z5 + 0.45 * z15 + 0.30 * z45;
    let accel_01 = 0.55 * (z5 - z15) + 0.45 * (z15 - z45);
    let noise_ratio_01 = (z5 - z15).powi(2) / (1.0 + (z15 * z45).max(0.0));
    let rev_damper_01 = (-0.35 * noise_ratio_01).exp();
    let phase_exc_01 = if z5.signum() == z15.signum() && z15.signum() == z45.signum() { 1.25 } else { 1.0 };
    let hawkes_01 = (v_base_01 + 0.30 * accel_01 * rev_damper_01) * phase_exc_01;
    let gap_up = p_fair_up - m.up_ask;
    let gap_down = p_fair_down - m.down_ask;
    let (v1, s1, m1) = if hawkes_01 >= 0.06 || gap_up >= 0.02 {
        (1i8, (hawkes_01.abs() / 1.5).max(gap_up * 3.5).clamp(0.28, 1.0), format!("H{:+.2} Gap{:+.0}c", hawkes_01, gap_up * 100.0))
    } else if hawkes_01 <= -0.06 || gap_down >= 0.02 {
        (-1i8, (hawkes_01.abs() / 1.5).max(gap_down * 3.5).clamp(0.28, 1.0), format!("H{:+.2} Gap{:+.0}c", hawkes_01, gap_down * 100.0))
    } else if v_base_01 >= 0.0 {
        (1i8, 0.28, format!("H{:+.2} Kal▲", hawkes_01))
    } else {
        (-1i8, 0.28, format!("H{:+.2} Kal▼", hawkes_01))
    };

    // --- AGENT 02 (S02): VPIN Toxicity & Aggressive Taker Flow vs Passive Wall Trap ---
    let taker_imb = (m.taker_buy_ratio - 0.50) * 2.0; // [-1, +1]
    let vpin_gate = ((vpin_10s - 0.35) / 0.40).clamp(0.25, 1.45);
    let vel_norm_02 = (z5 / 1.8).clamp(-1.0, 1.0);
    let align_02 = taker_imb * vel_norm_02;
    let regime_sign_02 = if absorption > 0.62 && align_02 <= 0.01 {
        -0.80 * absorption // Parede passiva absorveu takers sem deslocar preço -> reversão!
    } else {
        (1.0 - 0.40 * absorption) + 0.85 * align_02.max(0.0).sqrt()
    };
    let s02_val = (taker_imb * vpin_gate * regime_sign_02).clamp(-1.0, 1.0);
    let v2: i8 = if s02_val >= 0.0 { 1 } else { -1 };
    let s2 = s02_val.abs().clamp(0.26, 1.0);
    let m2 = format!("Tk {:.0}% VP:{:.2}", m.taker_buy_ratio * 100.0, vpin_10s);

    // --- AGENT 03 (S03): Polymarket 4-Level CLOB + Whale Wall Asymmetry ---
    let up_bid_sum: f64 = m.up_bids_top4.iter().map(|l| l.1).sum();
    let up_ask_depth: f64 = m.up_asks_top4.iter().map(|l| l.1).sum();
    let dn_bid_sum: f64 = m.down_bids_top4.iter().map(|l| l.1).sum();
    let down_ask_depth: f64 = m.down_asks_top4.iter().map(|l| l.1).sum();
    let w_obi = (m.whale_up_usd - m.whale_down_usd) / (m.whale_up_usd + m.whale_down_usd + 1.0);
    let wall_imb = (m.top_wall_up_usd - m.top_wall_down_usd) / (m.top_wall_up_usd + m.top_wall_down_usd + 1.0);
    let up_net = (up_bid_sum - up_ask_depth) / (up_bid_sum + up_ask_depth + 1.0);
    let dn_net = (dn_bid_sum - down_ask_depth) / (dn_bid_sum + down_ask_depth + 1.0);
    let l4_asym = 0.5 * (up_net - dn_net);
    let s03_val = (0.48 * w_obi + 0.32 * l4_asym + 0.20 * wall_imb).clamp(-1.0, 1.0);
    let v3: i8 = if s03_val >= 0.0 { 1 } else { -1 };
    let s3 = s03_val.abs().clamp(0.28, 1.0);
    let m3 = format!("CLOB {:+.2} W:${:.0}", s03_val, m.whale_up_usd.max(m.whale_down_usd));

    // --- AGENT 04 (S04): Cross-Asset ETH->BTC Lead-Lag Cointegration & Catch-Up ---
    let s04_val = (cross_z_div / 1.65).tanh();
    let v4: i8 = if s04_val.abs() >= 0.05 {
        if s04_val >= 0.0 { 1 } else { -1 }
    } else if v_base_01 >= 0.0 {
        1
    } else {
        -1
    };
    let s4 = s04_val.abs().clamp(0.28, 1.0);
    let m4 = format!("Coint Z{:+.2}", cross_z_div);

    // --- AGENT 05 (S05): Fokker-Planck First-Passage Reflection & Barrier Pinning ---
    let v_toward = -m_diff.signum() * mu_eff;
    let abs_z_dist = z_dist.abs();
    let fp_score = z_plus - lambda_fp * m_diff.signum() * norm_cdf(-z_minus.abs());
    let (v5_vote, s5, m5) = if v_toward > 0.24 && abs_z_dist < 1.15 {
        let break_dir = if m_diff < 0.0 { 1i8 } else { -1i8 };
        (break_dir, 0.80, format!("Rompendo K ({:.1}σ)", abs_z_dist))
    } else if fp_score >= 0.0 {
        (1i8, p_fair_up.clamp(0.32, 0.98), format!("FP {:+.2} (+${:.1})", fp_score, m_diff))
    } else {
        (-1i8, p_fair_down.clamp(0.32, 0.98), format!("FP {:+.2} (${:.1})", fp_score, m_diff))
    };

    // --- AGENT 06 (S06): Viscous Taylor 2nd/3rd Order Convexity & Jerk ---
    let rate_5 = m.vel_5s / 5.0;
    let rate_15 = m.vel_15s / 15.0;
    let rate_45 = m.vel_45s / 45.0;
    let v_eff_06 = 0.50 * rate_5 + 0.35 * rate_15 + 0.15 * rate_45;
    let a_fast = (rate_5 - rate_15) / 10.0;
    let a_slow = (rate_15 - rate_45) / 30.0;
    let nu = (0.18 * sig).max(1e-6);
    let j_smooth = ((a_fast - a_slow) / 20.0) * (-0.65 * (a_fast - a_slow).abs() / (a_slow.abs() + 0.05 * nu)).exp();
    let phase_gate_06 = ((v_eff_06 * a_slow) / (nu * nu)).tanh();
    let a_eff_06 = (0.60 * a_fast + 0.40 * a_slow) + 3.0 * j_smooth * phase_gate_06;
    let taylor_ds15 = v_eff_06 * 12.0 + 0.5 * a_eff_06 * 144.0;
    let v6: i8 = if taylor_ds15 >= 0.0 { 1 } else { -1 };
    let s6 = (taylor_ds15.abs() / (sig * 3.5)).clamp(0.28, 1.0);
    let m6 = format!("ΔT15 {:+.1}$ a{:+.2}", taylor_ds15, a_eff_06 * 10.0);

    // --- AGENT 07 (S07): Non-Linear Binance L2 20-Level Vacuum & VAI ---
    let comp_b = 0.55 * m.binance_obi_20 + 0.45 * m.binance_vai;
    let abs_damp_07 = (-1.2 * (-m.binance_obi_20 * taker_imb).max(0.0)).exp();
    let l2_vac_nl = (1.4 * comp_b * abs_damp_07
        + 0.65 * taker_imb.signum() * (m.binance_obi_20 * m.binance_vai).abs().sqrt() * taker_imb.abs())
        .tanh();
    let v7: i8 = if l2_vac_nl >= 0.0 { 1 } else { -1 };
    let s7 = l2_vac_nl.abs().clamp(0.28, 1.0);
    let m7 = format!("Vac:{:+.2} VAI:{:+.2}", l2_vac_nl, m.binance_vai);

    // --- AGENT 09 (S09): Anti-Spoofing Institutional Whale Accumulation ---
    let d_whale = (m.whale_up_usd - prev_whale_up) - (m.whale_down_usd - prev_whale_down);
    m.delta_whale_usd = d_whale;
    let r_wall_u = m.top_wall_up_usd / (m.whale_up_usd + 1.0);
    let r_wall_d = m.top_wall_down_usd / (m.whale_down_usd + 1.0);
    let k_wall = (1.0 - r_wall_u.max(r_wall_d).min(0.95).powi(2)).clamp(0.15, 1.0);
    let g_tick = (1.0 / (1.0 + (-0.05 * w_obi * d_whale).exp())) * k_wall;
    let s09_val = (0.55 * w_obi + 0.20 * wall_imb * k_wall + 0.25 * (d_whale / 150.0).tanh() * g_tick).tanh();
    let v9: i8 = if s09_val >= 0.0 { 1 } else { -1 };
    let s9 = s09_val.abs().clamp(0.28, 1.0);
    let m9 = format!("W:{:+.2} Δ${:+.0}", s09_val, d_whale);

    // --- AGENT 10 (S10): Asymmetric Brownian Bridge Candle Anchor & Macro Drift ---
    let tau_norm = (tau / 300.0).clamp(0.02, 1.0);
    let z_anchor = z_dist;
    let d_proj = m.diff_pct + (m.vel_15s / strike * 100.0) * (1.0 + tau / 15.0).ln() * tau_norm.sqrt();
    let s10_raw = if z_anchor.abs() < 1.10 && m.diff_pct * m.vel_15s < 0.0 {
        // Zona elástica com desaceleração: gravidade parcial rumo ao strike + d_proj
        0.55 * d_proj - 0.25 * m.diff_pct * (1.0 - tau_norm) + 0.03 * news_macro_bias
    } else {
        // Zona de escape: inércia dominante a favor da barreira rompida
        1.15 * d_proj + 0.45 * m.diff_pct + 0.03 * news_macro_bias
    };
    let v10: i8 = if s10_raw >= 0.0 { 1 } else { -1 };
    let s10 = (s10_raw.abs() * 14.0).clamp(0.30, 1.0);
    let m10 = format!("Δ{:+.2}% Z{:+.1}σ", m.diff_pct, z_anchor);

    // ====================== SENSORES 11-25 (SUPERINTELIGÊNCIA EXPANDIDA 25 AGENTES) ======================
    let up_mid = 0.5 * (m.up_bid + m.up_ask);
    let p_pure = |spot: f64| norm_cdf((spot - strike) / sigma_rem);
    let h10 = hist_at(&mem.poly_hist, now_m.saturating_sub(10_000));
    let h5 = hist_at(&mem.poly_hist, now_m.saturating_sub(5_000));

    // --- AGENT 11 (S11): Polymarket Contract Multi-Horizon Velocity & Binomial Curvature ---
    let v5_mid = h5.map(|h| up_mid - h.1).unwrap_or(0.0);
    let (poly_mom, _mom_age_s) = h10.map(|h| (up_mid - h.1, now_m.saturating_sub(h.0) as f64 / 1000.0)).unwrap_or((0.0, 0.0));
    let a_mid = 2.0 * v5_mid - poly_mom;
    let v_star_11 = 0.60 * v5_mid + 0.25 * poly_mom + 0.15 * a_mid;
    let binom_sd = (up_mid * (1.0 - up_mid)).max(0.04).sqrt();
    let v_norm_11 = v_star_11 / binom_sd;
    let d_eq_11 = up_mid - 0.50;
    let psi_11 = if v5_mid.signum() == poly_mom.signum() && v5_mid.signum() == d_eq_11.signum() && d_eq_11.abs() <= 0.24 {
        1.30
    } else if v5_mid * poly_mom < 0.0 {
        0.40
    } else {
        1.00
    };
    let s11_score = psi_11 * v_norm_11 + 0.14 * d_eq_11;
    let v11: i8 = if s11_score >= 0.0 { 1 } else { -1 };
    let s11 = (s11_score.abs() * 5.0).clamp(0.28, 1.0);
    let m11 = format!("v5:{:+.1}¢ v10:{:+.1}¢", v5_mid * 100.0, poly_mom * 100.0);

    // --- AGENT 12 (S12): Cross-Market Spot-to-CLOB Co-Momentum & Taker-Confirmed Lead-Lag ---
    let dp_pure_10 = h10.filter(|h| h.2 > 0.0).map(|h| p_pure(m.current_price) - p_pure(h.2)).unwrap_or(p_fair_up - up_mid);
    let poly_lag = dp_pure_10 - poly_mom;
    let lag_ratio = if dp_pure_10.abs() > 1e-4 && dp_pure_10 * taker_imb > 0.0 {
        (1.0 - (poly_mom / dp_pure_10)).clamp(0.0, 1.0)
    } else {
        0.0
    };
    let confirmed_lag = dp_pure_10 * lag_ratio * (1.0 + taker_imb.abs());
    let co_momentum = if dp_pure_10 * poly_mom > 0.0 {
        dp_pure_10.signum() * (dp_pure_10.abs() * poly_mom.abs()).sqrt()
    } else {
        0.0
    };
    let s12_score = (0.55 * confirmed_lag + 0.45 * co_momentum + 0.15 * (p_fair_up - up_mid)).clamp(-1.0, 1.0);
    let v12: i8 = if s12_score >= 0.0 { 1 } else { -1 };
    let s12 = (s12_score.abs() * 10.0).clamp(0.28, 1.0);
    let m12 = format!("CoM:{:+.1}¢ Lag:{:+.1}¢", co_momentum * 100.0, confirmed_lag * 100.0);

    // --- AGENT 13 (S13): Symmetric 4-Level Complementary Microprice (Anti-Spoofing L0-L3 UP+DOWN) ---
    let mid_sym = 0.25 * (
        m.up_bids_top4[0].0 + m.up_asks_top4[0].0
        + (1.0 - m.down_asks_top4[0].0) + (1.0 - m.down_bids_top4[0].0)
    );
    let (mut wb_sum, mut wa_sum, mut pb_num, mut pa_num) = (1e-9_f64, 1e-9_f64, 0.0_f64, 0.0_f64);
    for i in 0..4 {
        let (ub_p, ub_q) = m.up_bids_top4[i];
        let (da_p, da_q) = (1.0 - m.down_asks_top4[i].0, m.down_asks_top4[i].1);
        let (ua_p, ua_q) = m.up_asks_top4[i];
        let (db_p, db_q) = (1.0 - m.down_bids_top4[i].0, m.down_bids_top4[i].1);
        let wb_u = ub_q * (-25.0 * (mid_sym - ub_p).abs()).exp();
        let wb_d = da_q * (-25.0 * (mid_sym - da_p).abs()).exp();
        let wa_u = ua_q * (-25.0 * (ua_p - mid_sym).abs()).exp();
        let wa_d = db_q * (-25.0 * (db_p - mid_sym).abs()).exp();
        pb_num += ub_p * wb_u + da_p * wb_d;
        wb_sum += wb_u + wb_d;
        pa_num += ua_p * wa_u + db_p * wa_d;
        wa_sum += wa_u + wa_d;
    }
    let sym_micro = ((pb_num / wb_sum) * wa_sum + (pa_num / wa_sum) * wb_sum) / (wb_sum + wa_sum);
    let spread = (m.up_ask - m.up_bid).max(0.01);
    let micro_dev = (sym_micro - mid_sym) / spread;
    let v13: i8 = if micro_dev >= 0.0 { 1 } else { -1 };
    let s13 = (micro_dev.abs() * 2.5).clamp(0.28, 1.0);
    let m13 = format!("μSym {:+.2} Spr {:.0}¢", micro_dev, spread * 100.0);

    // --- AGENT 14 (S14): Aggressive Liquidity Sweep vs Passive Cancel ---
    let (r_up, r_dn) = match h5 {
        Some(h) if h.3 > 1.0 && h.4 > 1.0 => (up_ask_depth / h.3, down_ask_depth / h.4),
        _ => (1.0, 1.0),
    };
    let ask_depl = 1.0 - r_up; // >0 means UP asks were consumed
    let bid_depl = 1.0 - r_dn; // >0 means DOWN asks (equivalent to UP bids) were consumed
    let net_depl = ask_depl - bid_depl;
    let sweep_confirmed = net_depl * v5_mid >= 0.0 && net_depl * m.vel_5s >= 0.0;
    let s14_val = if sweep_confirmed && net_depl.abs() >= 0.01 {
        (net_depl * (1.0 + v5_mid.abs() * 35.0)).tanh()
    } else {
        0.5 * (down_ask_depth - up_ask_depth) / (down_ask_depth + up_ask_depth + 1.0)
    };
    let v14: i8 = if s14_val >= 0.0 { 1 } else { -1 };
    let s14 = s14_val.abs().clamp(0.26, 1.0);
    let m14 = format!("Swp {:+.2} ({:.0}%/{:.0}%)", s14_val, r_up * 100.0, r_dn * 100.0);

    // --- AGENTS 15 & 16 (S15 / S16): Online Huber-RLS +15s / +30s with Strike Barrier Fusion ---
    let taker_imb_f = taker_imb.clamp(-1.0, 1.0);
    let x: [f64; N_FEAT] = [
        1.0,
        (z5 / 2.5).tanh() * 2.5,
        (z15 / 2.5).tanh() * 2.5,
        (z45 / 2.5).tanh() * 2.5,
        taker_imb_f,
        m.binance_obi_20.clamp(-1.0, 1.0),
        m.obi.clamp(-1.0, 1.0),
        cross_z_div.clamp(-3.5, 3.5),
        (v_star_11 * 12.0).clamp(-3.0, 3.0),
        (s12_score * 4.0).clamp(-3.0, 3.0),
    ];
    let ready15 = mem.rls15.updates >= MIN_CALIB;
    let ready30 = mem.rls30.updates >= MIN_CALIB;
    let prior = (0.20 * hawkes_01 + 0.16 * z15 + 0.20 * s02_val + 0.16 * l2_vac_nl + 0.18 * z_plus).clamp(-1.8, 1.8);
    let pred15_raw = if ready15 { 0.78 * mem.rls15.predict(&x) + 0.22 * prior } else { prior };
    let pred30_raw = if ready30 { 0.75 * mem.rls30.predict(&x) + 0.25 * (prior * 0.90) } else { prior * 0.88 };
    let w15_blend = 1.0 / (1.0 + (-(25.0 - tau) * 0.15).exp()) * 0.35 + 0.15;
    let pred15 = pred15_raw;
    let pred30 = 0.62 * (w15_blend * pred15_raw + (1.0 - w15_blend) * pred30_raw) + 0.38 * (z_dist * 0.45);
    let v15: i8 = if pred15 >= 0.0 { 1 } else { -1 };
    let v16: i8 = if pred30 >= 0.0 { 1 } else { -1 };
    let s15 = pred15.abs().clamp(0.30, 1.0);
    let s16 = pred30.abs().clamp(0.30, 1.0);
    let m15 = format!("ŷ15 {:+.2}σ Δ${:+.1}", pred15, pred15 * sig * 15f64.sqrt());
    let m16 = format!("ŷ30 {:+.2}σ Δ${:+.1}", pred30, pred30 * sig * 30f64.sqrt());

    // --- AGENT 17 (S17): Realized Vol Regime & Lo-MacKinlay Variance Ratio ---
    let regime = vol_regime(&mem.spot_samples, now_m).unwrap_or(1.0);
    let vr_60_15 = {
        let var15 = (m.vel_15s.abs() / 3.873).max(0.2).powi(2);
        let var45 = (m.vel_45s.abs() / 6.708).max(0.2).powi(2);
        (var45 / var15).clamp(0.3, 2.5)
    };
    let s17_score = if regime > 1.05 {
        if vr_60_15 >= 0.98 { 1.25 * z15 + 0.65 * z_dist } else { 0.40 * z15 + 0.80 * z45 }
    } else if regime < 0.95 && z_dist.abs() < 1.35 {
        0.85 * z_dist - 0.35 * z15.clamp(-2.0, 2.0)
    } else {
        z15 + 0.55 * z_dist
    };
    let v17: i8 = if s17_score >= 0.0 { 1 } else { -1 };
    let s17 = regime.clamp(0.30, 1.0);
    let m17 = format!("Reg x{:.2} VR:{:.2}", regime, vr_60_15);
    m.sigma_regime = regime;

    // --- AGENT 18 (S18): Ornstein-Uhlenbeck Exhaustion with Drift-Ratio & Kinetic Hook ---
    let mean60 = if !m.spot_history_60.is_empty() {
        m.spot_history_60.iter().sum::<f64>() / m.spot_history_60.len() as f64
    } else {
        m.current_price
    };
    let d_60 = if m.spot_history_60.len() >= 2 {
        (m.current_price - m.spot_history_60[0]).abs() / (sig * 7.746)
    } else {
        0.0
    };
    let z_ext = (m.current_price - mean60) / (sig * 20f64.sqrt());
    let (v18, s18, m18) = if z_ext >= 2.35 && d_60 <= 2.25 && m.vel_5s <= 0.15 * m.vel_15s {
        (-1i8, 0.78, format!("OU Topo +{:.1}σ ↩", z_ext))
    } else if z_ext <= -2.35 && d_60 <= 2.25 && m.vel_5s >= 0.15 * m.vel_15s {
        (1i8, 0.78, format!("OU Fundo {:.1}σ ↪", z_ext))
    } else if z_ext >= 0.0 {
        (1i8, (z_ext.abs() * 0.55).clamp(0.28, 0.95), format!("Tend {:+.1}σ ▲", z_ext))
    } else {
        (-1i8, (z_ext.abs() * 0.55).clamp(0.28, 0.95), format!("Tend {:+.1}σ ▼", z_ext))
    };

    // --- AGENT 19 (S19): Time-Decayed (8s vs 20s) & Magnitude-Weighted Tick Imbalance ---
    let (v19, s19, m19) = {
        let hist = &m.spot_history_60;
        let start = hist.len().saturating_sub(21);
        let slice = &hist[start..];
        let nw = slice.len().saturating_sub(1);
        let (mut ups, mut dns) = (0i32, 0i32);
        let (mut w_sign_num, mut w_sign_den) = (0.0_f64, 1e-9_f64);
        let (mut w_mag_num, mut w_mag_den) = (0.0_f64, 1e-9_f64);
        for (idx, w) in slice.windows(2).enumerate() {
            let dp = w[1] - w[0];
            let wt = if idx + 8 >= nw { 2.0 } else { 1.0 }; // 2.0x nos últimos 8s
            if dp > 0.0 {
                ups += 1;
                w_sign_num += wt;
                w_sign_den += wt;
                let mg = dp.abs().sqrt();
                w_mag_num += wt * mg;
                w_mag_den += wt * mg;
            } else if dp < 0.0 {
                dns += 1;
                w_sign_num -= wt;
                w_sign_den += wt;
                let mg = dp.abs().sqrt();
                w_mag_num -= wt * mg;
                w_mag_den += wt * mg;
            }
        }
        let i_sign = w_sign_num / w_sign_den;
        let i_mag = w_mag_num / w_mag_den;
        let s19_val = (0.65 * i_sign + 0.35 * i_mag).tanh();
        let dir19 = if s19_val >= 0.0 { 1i8 } else { -1i8 };
        (dir19, s19_val.abs().clamp(0.30, 1.0), format!("Ticks ▲{} ▼{}", ups, dns))
    };

    // --- AGENT 21 (S21): Kaufman Fractal Efficiency Ratio (15s/45s) & Hurst Regime ---
    let (v21, s21, m21) = {
        let hist = &m.spot_history_60;
        let n_h = hist.len();
        let calc_er = |w: usize| -> (f64, f64) {
            if n_h <= w {
                return (0.35, m_diff.signum());
            }
            let s = &hist[n_h - w - 1..];
            let net = s[w] - s[0];
            let path: f64 = s.windows(2).map(|p| (p[1] - p[0]).abs()).sum();
            (if path > 1e-6 { (net.abs() / path).clamp(0.0, 1.0) } else { 0.35 }, net.signum())
        };
        let ((er15, dir15), (er45, dir45)) = (calc_er(15), calc_er(45));
        let er = 0.60 * er15 + 0.40 * er45;
        let sig_21 = if er > 0.35 {
            z_dist + (er - 0.35) * (0.65 * dir15 + 0.35 * dir45) * 1.8
        } else {
            z_dist * (er / 0.35).powi(2) + 0.25 * z15
        };
        let d21 = if sig_21 >= 0.0 { 1i8 } else { -1i8 };
        (d21, er.clamp(0.30, 1.0), format!("ER:{:.2} H:{:.2}", er, 0.5 + 0.4 * (er - 0.35)))
    };

    // --- AGENT 22 (S22): Polymarket Binary Put-Call Parity Dislocation & MM Spread Skew ---
    let s22_val = {
        let s_up = (m.up_ask - m.up_bid).max(1e-4);
        let s_dn = (m.down_ask - m.down_bid).max(1e-4);
        let vig_ask = m.up_ask + m.down_ask - 1.0;
        let vig_bid = 1.0 - (m.up_bid + m.down_bid);
        let mid_disloc = 0.5 * (vig_ask - vig_bid) / (s_up + s_dn);
        let spread_skew = (s_dn - s_up) / (s_up + s_dn);
        let vig_decay = (1.0 - ((vig_ask + vig_bid - 0.06).max(0.0) * 10.0)).clamp(0.15, 1.0);
        let raw_skew = ((mid_disloc * 2.2 + spread_skew * 0.8) * vig_decay).tanh();
        if raw_skew.abs() >= 0.03 { raw_skew } else { p_fair_up - up_mid }
    };
    let v22: i8 = if s22_val >= 0.0 { 1 } else { -1 };
    let s22 = s22_val.abs().clamp(0.28, 1.0);
    let m22 = format!("Skew {:+.2} Vig {:.0}¢", s22_val, ((m.up_ask + m.down_ask - 1.0) * 100.0).abs());

    // --- AGENT 23 (S23): Brownian Bridge Non-Crossing Survival & Accelerated Binary Theta ---
    let s23_val = {
        let x0 = (m.current_price.max(1.0) / strike).ln();
        let v_log = (m.vel_15s / 15.0) / strike;
        let x_tau = x0 + v_log * tau.min(90.0);
        let var_t = ((sig / strike).powi(2) * tau).max(1e-12);
        let p_cross = if x0 * x_tau > 0.0 {
            (-2.0 * x0 * x_tau / var_t).exp().clamp(0.0, 1.0)
        } else {
            0.85
        };
        let p_pin = if x0 >= 0.0 { 1.0 - 0.5 * p_cross } else { 0.5 * p_cross };
        let theta_boost = if tau < 180.0 { 1.0 + (180.0 - tau) / 90.0 } else { 1.0 };
        ((p_pin - 0.50) * theta_boost).clamp(-1.0, 1.0)
    };
    let v23: i8 = if s23_val >= 0.0 { 1 } else { -1 };
    let s23 = s23_val.abs().clamp(0.30, 1.0);
    let m23 = format!("ΘPin {:+.2} ({}s)", s23_val, m.rem_sec);

    // --- AGENT 24 (S24): 4-Scale Phase Coherence Matrix (5s / 15s / 45s / 60s) ---
    let s24_val = {
        let vel_60s = if m.spot_history_60.len() >= 2 {
            m.spot_history_60[m.spot_history_60.len() - 1] - m.spot_history_60[0]
        } else {
            m.vel_45s
        };
        let v_scales = [m.vel_5s, m.vel_15s, m.vel_45s, vel_60s];
        let phase_sync: f64 = v_scales.iter().map(|x| x.signum()).sum::<f64>() * 0.25;
        let cascade_gain = if (m.vel_5s - m.vel_15s / 3.0) * phase_sync >= 0.0 { 1.25 } else { 0.80 };
        let grad_align = if phase_sync * m_diff.signum() > 0.0 { 1.20 } else { 0.85 };
        let res = (phase_sync * cascade_gain * grad_align).clamp(-1.0, 1.0);
        if res.abs() >= 0.05 { res } else { z_dist.clamp(-1.0, 1.0) }
    };
    let v24: i8 = if s24_val >= 0.0 { 1 } else { -1 };
    let s24 = s24_val.abs().clamp(0.30, 1.0);
    let m24 = format!("Sync4 {:+.0}%", s24_val * 100.0);

    // --- AGENT 08 (S08): Asymmetric Kelly-Sharpe Mispricing Edge (59.4% Top Alpha) ---
    let u_ask_c = m.up_ask.clamp(0.03, 0.97);
    let d_ask_c = m.down_ask.clamp(0.03, 0.97);
    let ev_up_08 = (p_fair_up - u_ask_c) / (u_ask_c * (1.0 - u_ask_c)).sqrt();
    let ev_dn_08 = (p_fair_down - d_ask_c) / (d_ask_c * (1.0 - d_ask_c)).sqrt();
    let others22 = [
        v1, v2, v3, v4, v5_vote, v6, v7, v9, v10, v11, v12,
        v13, v14, v15, v16, v17, v18, v19, v21, v22, v23, v24,
    ];
    let prelim_up = others22.iter().filter(|&&v| v > 0).count();
    let prelim_down = others22.iter().filter(|&&v| v < 0).count();
    let check_up = if (ev_up_08 - ev_dn_08).abs() >= 0.04 {
        ev_up_08 > ev_dn_08
    } else {
        prelim_up >= prelim_down
    };
    let side_ask = if check_up { m.up_ask } else { m.down_ask };
    let payoff_exp = if side_ask > 0.01 { (1.0 - side_ask) / side_ask } else { 0.0 };
    let (v8, s8, m8) = if (0.06..=0.86).contains(&side_ask) {
        let vote_dir = if check_up { 1i8 } else { -1i8 };
        (vote_dir, 0.94, format!("Ask ${:.2} (+{:.0}%)", side_ask, payoff_exp * 100.0))
    } else {
        let fallback_dir = if check_up { 1i8 } else { -1i8 };
        (fallback_dir, 0.30, format!("Cota ${:.2}", side_ask))
    };

    // --- AGENT 20 (S20): Dual-EMA Markov Consensus Hysteresis (Schmitt Trigger) ---
    let all23 = [
        v1, v2, v3, v4, v5_vote, v6, v7, v8, v9, v10, v11, v12,
        v13, v14, v15, v16, v17, v18, v19, v21, v22, v23, v24,
    ];
    let mut c_num = 0.0_f64;
    let mut c_den = 1e-9_f64;
    for (idx, &v) in all23.iter().enumerate() {
        let w = mem.bots[idx].weight;
        c_num += (v as f64) * w;
        c_den += w;
    }
    let c_t = (c_num / c_den).clamp(-1.0, 1.0);
    mem.ema_fast = 0.45 * c_t + 0.55 * mem.ema_fast;
    mem.ema_slow = 0.20 * c_t + 0.80 * mem.ema_slow;
    let prelim_dir: i8 = if mem.ema_fast >= 0.0 { 1 } else { -1 };
    if prelim_dir == mem.prev_dir || (mem.ema_fast * (mem.prev_dir as f64) > -0.08) {
        mem.streak = mem.streak.saturating_add(1);
    } else {
        mem.streak = 1;
        mem.prev_dir = prelim_dir;
    }
    let streak = mem.streak;
    m.streak = streak;
    let v20 = prelim_dir;
    let s20 = ((streak as f64) / 6.0).clamp(0.32, 1.0);
    let m20 = format!("EMA {:+.2} ({}t)", mem.ema_fast, streak);

    // --- AGENT 25 (S25): Meta-Oracle Bayesian EXP3 / Hedge Elite Aggregator ---
    let prelim24 = [
        v1, v2, v3, v4, v5_vote, v6, v7, v8, v9, v10, v11, v12,
        v13, v14, v15, v16, v17, v18, v19, v20, v21, v22, v23, v24,
    ];
    let (mut exp3_num, mut exp3_den, mut elite_cnt) = (0.0_f64, 0.0_f64, 0u32);
    for (idx, &dv) in prelim24.iter().enumerate() {
        let w = mem.bots[idx].weight;
        if w >= 1.25 {
            let ew = (w * 1.35).exp();
            exp3_num += (dv as f64) * ew;
            exp3_den += ew;
            elite_cnt += 1;
        }
    }
    let s25_score = if exp3_den > 0.0 {
        (exp3_num / exp3_den).clamp(-1.0, 1.0)
    } else {
        c_t
    };
    let v25: i8 = if s25_score >= 0.0 { 1 } else { -1 };
    let s25 = s25_score.abs().clamp(0.35, 1.0);
    let m25 = format!("EXP3 {:+.0}% ({}★)", s25_score * 100.0, elite_cnt.max(1));

    let votes_arr: [i8; 25] = [
        v1, v2, v3, v4, v5_vote, v6, v7, v8, v9, v10,
        v11, v12, v13, v14, v15, v16, v17, v18, v19, v20,
        v21, v22, v23, v24, v25,
    ];
    let strengths_arr: [f64; 25] = [
        s1, s2, s3, s4, s5, s6, s7, s8, s9, s10,
        s11, s12, s13, s14, s15, s16, s17, s18, s19, s20,
        s21, s22, s23, s24, s25,
    ];
    let metrics_arr: [String; 25] = [
        m1, m2, m3, m4, m5, m6, m7, m8, m9, m10,
        m11, m12, m13, m14, m15, m16, m17, m18, m19, m20,
        m21, m22, m23, m24, m25,
    ];
    let bot_meta: [(&'static str, &'static str); 25] = [
        ("S01", "IMPULSO HAWKES-KALMAN"),
        ("S02", "FLUXO TAKER VPIN"),
        ("S03", "PAREDES BALEIA CLOB"),
        ("S04", "COINTEGRAÇÃO ETH→BTC"),
        ("S05", "BARREIRA FOKKER-PLANCK"),
        ("S06", "CONVEXIDADE TAYLOR 3ª"),
        ("S07", "VÁCUO L2 NÃO-LINEAR"),
        ("S08", "BORDA KELLY-SHARPE"),
        ("S09", "BALEIA ANTI-SPOOF"),
        ("S10", "PONTE ÂNCORA & MACRO"),
        ("S11", "VELOCIDADE COTA BIN."),
        ("S12", "ARBITRAGEM SPOT→CLOB"),
        ("S13", "MICROPREÇO 4-NÍVEIS"),
        ("S14", "VARREDURA LIQUIDEZ"),
        ("S15", "ORÁCULO RLS +15s"),
        ("S16", "ORÁCULO RLS +30s"),
        ("S17", "RAZÃO VARIÂNCIA VR"),
        ("S18", "EXAUSTÃO ORNSTEIN-UHL"),
        ("S19", "TICKS PONDERADOS 8/20s"),
        ("S20", "HISTERESE MARKOV EMA"),
        ("S21", "EFICIÊNCIA FRACTAL ER"),
        ("S22", "SKEW & PARIDADE MM"),
        ("S23", "THETA & PONTE BROWN."),
        ("S24", "COERÊNCIA 4-ESCALAS"),
        ("S25", "META-ORÁCULO EXP3 ★"),
    ];

    for i in 0..25 {
        let bl = mem.bots[i];
        m.sensors[i] = SensorCell {
            id: bot_meta[i].0,
            name: bot_meta[i].1,
            vote: votes_arr[i],
            strength: strengths_arr[i],
            metric: metrics_arr[i].clone(),
            pnl_usd: bl.pnl_usd,
            wins: bl.wins,
            losses: bl.losses,
            weight: bl.weight,
        };
    }

    let mut up_c = 0u8;
    let mut down_c = 0u8;
    let mut neut_c = 0u8;
    let mut w_up = 0.0_f64;
    let mut w_down = 0.0_f64;
    let mut up_bots_pnl = 0.0_f64;
    let mut down_bots_pnl = 0.0_f64;
    let mut swarm_total_bot_pnl = 0.0_f64;
    let mut profitable_bots_count = 0u8;

    for s in &m.sensors {
        swarm_total_bot_pnl += s.pnl_usd;
        if s.pnl_usd > 0.001 || (s.wins > s.losses) {
            profitable_bots_count += 1;
        }
        if s.vote > 0 {
            up_c += 1;
            w_up += s.weight;
            up_bots_pnl += s.pnl_usd;
        } else if s.vote < 0 {
            down_c += 1;
            w_down += s.weight;
            down_bots_pnl += s.pnl_usd;
        } else {
            neut_c += 1;
        }
    }
    m.up_votes = up_c;
    m.down_votes = down_c;
    m.neutral_votes = neut_c;

    let w_active = (w_up + w_down).max(1e-6);
    let score_up = if w_up + w_down > 0.0 { 25.0 * (w_up / w_active) } else { up_c as f64 };
    let score_down = if w_up + w_down > 0.0 { 25.0 * (w_down / w_active) } else { down_c as f64 };

    let (dom_dir, aligned, weighted_score, opp_weighted_score, _aligned_bots_pnl, target_ask, target_bid, p_fair_side) = if score_up > score_down + 0.05 {
        ("UP", up_c, score_up, score_down, up_bots_pnl, m.up_ask, m.up_bid, p_fair_up)
    } else if score_down > score_up + 0.05 {
        ("DOWN", down_c, score_down, score_up, down_bots_pnl, m.down_ask, m.down_bid, p_fair_down)
    } else if p_fair_up >= 0.5 {
        ("UP", up_c, score_up, score_down, up_bots_pnl, m.up_ask, m.up_bid, p_fair_up)
    } else {
        ("DOWN", down_c, score_down, score_up, down_bots_pnl, m.down_ask, m.down_bid, p_fair_down)
    };
    let dom_sign: i8 = if dom_dir == "UP" { 1 } else { -1 };

    m.dominant_dir = dom_dir;
    m.aligned_count = aligned;
    m.weighted_score = weighted_score;
    m.target_ask = target_ask;
    m.latency_gap_cents = (p_fair_side - target_ask) * 100.0;

    // ====================== PREVISÃO +15s / +30s HÍBRIDA (RLS + TAYLOR + FOKKER-PLANCK + ENXAME 25) ======================
    let swarm_bias = ((score_up - score_down) / 25.0) * 0.45;
    let calc_fc = |pred_rls: f64, h: f64| -> (f64, f64, f64) {
        let h_eff = h.min(tau).max(1.0);
        let taylor_part = (v_eff_06 * h_eff + 0.5 * a_eff_06 * h_eff * h_eff).clamp(-sig * h_eff.sqrt() * 2.2, sig * h_eff.sqrt() * 2.2);
        let rls_part = (0.72 * pred_rls + 0.28 * swarm_bias) * sig * h_eff.sqrt();
        let dspot = 0.68 * rls_part + 0.32 * taylor_part;
        let band = sig * h_eff.sqrt();
        let p_up = norm_cdf((m_diff + dspot) / sigma_rem).clamp(0.01, 0.99);
        (dspot, band, p_up)
    };
    let (fc15_dspot, fc15_band, fc15_up) = calc_fc(pred15, 15.0);
    let (fc30_dspot, fc30_band, fc30_up) = calc_fc(pred30, 30.0);
    let side_val = |p_up: f64| if dom_sign > 0 { p_up } else { 1.0 - p_up };
    let fc15_gain_c = (side_val(fc15_up) - target_ask) * 100.0;
    let fc30_gain_c = (side_val(fc30_up) - target_ask) * 100.0;

    let s15d: i8 = if pred15 >= 0.0 { 1 } else { -1 };
    let s30d: i8 = if pred30 >= 0.0 { 1 } else { -1 };
    let (validity_label, validity_code) = if s15d == dom_sign && s30d == dom_sign {
        ("⚡ ENTER $+ CONSOLIDADO (SUSTENTA +15s / +30s)", "SUSTAIN")
    } else if s15d == dom_sign {
        ("⚡ ENTER $+ RÁPIDO (HORIZONTE +15s ALINHADO)", "SHORT")
    } else {
        ("⚠️ HORIZONTE +15/30s EM AJUSTE", "CALIB")
    };

    m.fc_ready = ready15 && ready30;
    m.calib_n = mem.rls15.updates.min(mem.rls30.updates);
    m.fc15_pred = pred15;
    m.fc30_pred = pred30;
    m.fc15_dspot = fc15_dspot;
    m.fc30_dspot = fc30_dspot;
    m.fc15_band = fc15_band;
    m.fc30_band = fc30_band;
    m.fc15_up = fc15_up;
    m.fc30_up = fc30_up;
    m.fc15_gain_c = fc15_gain_c;
    m.fc30_gain_c = fc30_gain_c;
    m.validity_label = validity_label;
    m.validity_code = validity_code;

    // Entry limit, Scalp TP (+% / +$), and Stop level (instantaneous candidate)
    let entry_lim = if target_ask > target_bid + 0.01 {
        (target_ask - 0.01).max(0.01)
    } else {
        target_ask.max(0.01)
    };
    let tp_step = if weighted_score >= 18.8 || aligned >= 19 {
        0.14
    } else if weighted_score >= 16.2 || aligned >= 16 {
        0.12
    } else {
        0.10
    };
    let scalp_tp = (entry_lim + tp_step).min(0.96);
    let stop_lvl = (entry_lim * 0.72).max(0.02);
    m.entry_lim = entry_lim;
    m.scalp_tp = scalp_tp;
    m.stop_lvl = stop_lvl;

    // Helper para cotação justa de marcação da cota travada
    let compute_locked_mark = |dir_s: i8| -> f64 {
        let (b, a) = if dir_s > 0 { (m.up_bid, m.up_ask) } else { (m.down_bid, m.down_ask) };
        if b > 0.01 && (a - b) <= 0.06 {
            0.5 * (b + a)
        } else if b > 0.01 {
            b
        } else {
            a
        }
    };

    // ====================== MÁQUINA DE ESTADOS: PREDICTOR GOD ENTER $+ (TRAVA CONSOLIDADA 25 AGENTES) ======================
    if mem.locked.active {
        let age_sec = now_m.saturating_sub(mem.locked.locked_at_ms) / 1000;
        let mark = compute_locked_mark(mem.locked.dir_sign);
        if mark > mem.locked.peak_mark {
            mem.locked.peak_mark = mark;
        }
        let live_pnl_usd = mem.locked.shares * (mark - mem.locked.entry_price);
        let live_pnl_pct = if mem.locked.entry_price > 0.01 {
            ((mark - mem.locked.entry_price) / mem.locked.entry_price) * 100.0
        } else {
            0.0
        };
        let peak_pnl_pct = if mem.locked.entry_price > 0.01 {
            ((mem.locked.peak_mark - mem.locked.entry_price) / mem.locked.entry_price) * 100.0
        } else {
            0.0
        };

        // Realiza lucro (%$) ao atingir o Alvo Scalp, ou via Trailing Profit Lock (bateu >= +14% e recuou 3.5%), ou reta final no lucro
        let trailing_tp = peak_pnl_pct >= 14.0 && live_pnl_pct >= 9.5 && (peak_pnl_pct - live_pnl_pct) >= 3.5;
        let tp_reached = mark >= mem.locked.tp_price || trailing_tp || (m.rem_sec <= 22 && live_pnl_pct >= 5.0);
        if tp_reached && mem.locked.tp_hit_at_ms == 0 {
            mem.locked.tp_hit_at_ms = now_m;
            let best_pnl_usd = mem.locked.shares * (mem.locked.peak_mark.max(mark) - mem.locked.entry_price);
            let realized = best_pnl_usd.max(live_pnl_usd).max(mem.locked.scalp_profit_usd * 0.75);
            mem.session_wins += 1;
            mem.session_pnl_usd += realized;
            for b_idx in 0..25 {
                let v = mem.locked.votes_at_lock[b_idx];
                if v != 0 {
                    let sign_match = if v == mem.locked.dir_sign { 1.0 } else { -1.0 };
                    let outcome = if sign_match > 0.0 { 0.22 } else { -0.12 };
                    mem.bots[b_idx].record_outcome(outcome);
                }
            }
        }

        if mem.locked.tp_hit_at_ms > 0 {
            if now_m.saturating_sub(mem.locked.tp_hit_at_ms) >= 10_000 {
                mem.locked.active = false;
                mem.calc_confirm_ticks = 0;
                mem.cooldown_until_ms = now_m + 1_500;
            }
        } else {
            let is_contrary = (dom_sign != mem.locked.dir_sign && weighted_score >= 18.5) || opp_weighted_score >= 18.5;
            if is_contrary {
                mem.locked.contrary_ticks += 1;
            } else {
                mem.locked.contrary_ticks = 0;
            }
            if mark <= mem.locked.stop_price {
                mem.locked.stop_ticks += 1;
            } else {
                mem.locked.stop_ticks = 0;
            }

            // Só invalida se realmente perder o Stop Fixo sustentadamente + reversão estrutural forte (nunca em ruído/pullback curto!)
            if age_sec >= 40 && mem.locked.stop_ticks >= 10 && mem.locked.contrary_ticks >= 8 {
                if live_pnl_usd >= 0.0 {
                    mem.session_wins += 1;
                } else {
                    mem.session_losses += 1;
                }
                mem.session_pnl_usd += live_pnl_usd;
                let move_sign = if live_pnl_usd >= 0.0 { 1.0 } else { -1.0 };
                for b_idx in 0..25 {
                    let v = mem.locked.votes_at_lock[b_idx];
                    if v != 0 {
                        let sign_match = if v == mem.locked.dir_sign { move_sign } else { -move_sign };
                        let outcome = if sign_match > 0.0 { 0.16 } else { -0.10 };
                        mem.bots[b_idx].record_outcome(outcome);
                    }
                }
                mem.locked.active = false;
                mem.calc_confirm_ticks = 0;
                mem.cooldown_until_ms = now_m + 2_000;
            }
        }
    }

    // Consolidação rápida (~1s) do Predictor GOD (25 Agentes) para travar a previsão "ENTER $+" com filtro Anti-Topo
    if !mem.locked.active {
        let target_spread = (target_ask - target_bid).max(0.0);
        let good_price = (0.08..=0.80).contains(&target_ask) && target_spread <= 0.09;
        let good_time = m.rem_sec >= 18;
        let impulse_ok = (dom_sign > 0 && (z5 >= -0.10 || pred15 >= 0.0 || p_fair_up >= target_ask))
            || (dom_sign < 0 && (z5 <= 0.10 || pred15 <= 0.0 || p_fair_down >= target_ask));
        let good_consensus = (weighted_score >= 14.2 && aligned >= 14 && impulse_ok)
            || (weighted_score >= 16.0 && aligned >= 15);
        let cooldown_ok = now_m >= mem.cooldown_until_ms;

        if good_price && good_time && good_consensus && cooldown_ok {
            if mem.calc_candidate_dir == dom_sign {
                mem.calc_confirm_ticks += 1;
            } else {
                mem.calc_candidate_dir = dom_sign;
                mem.calc_confirm_ticks = 1;
            }
        } else if !good_price || !good_time {
            mem.calc_confirm_ticks = 0;
        }

        if mem.calc_confirm_ticks >= 3 {
            let stake = if weighted_score >= 18.8 || aligned >= 19 {
                3.0
            } else if weighted_score >= 16.0 || aligned >= 16 {
                2.0
            } else {
                1.0
            };
            let sh = (stake / entry_lim).max(1.0);
            let sc_prof = sh * scalp_tp - stake;
            let sc_roi = (sc_prof / stake) * 100.0;
            let ex_prof = sh * 1.00 - stake;
            let ex_roi = (ex_prof / stake) * 100.0;
            mem.locked = LockedSignal {
                active: true,
                window_ts: m.window_ts,
                locked_at_ms: now_m,
                dir: dom_dir,
                dir_sign: dom_sign,
                entry_price: entry_lim,
                tp_price: scalp_tp,
                stop_price: stop_lvl,
                stake_usd: stake,
                shares: sh,
                scalp_profit_usd: sc_prof,
                scalp_roi_pct: sc_roi,
                expiry_profit_usd: ex_prof,
                expiry_roi_pct: ex_roi,
                locked_weighted_score: weighted_score,
                locked_aligned_bots: aligned,
                votes_at_lock: votes_arr,
                tp_hit_at_ms: 0,
                peak_mark: entry_lim,
                contrary_ticks: 0,
                stop_ticks: 0,
            };
            mem.calc_confirm_ticks = 0;
        }
    }

    // Popula FixedSignalView (`m.fixed`) com decisão consolidada "ENTER $+"
    if mem.locked.active {
        let age_sec = now_m.saturating_sub(mem.locked.locked_at_ms) / 1000;
        let mark = compute_locked_mark(mem.locked.dir_sign);
        let live_pnl_usd = mem.locked.shares * (mark - mem.locked.entry_price);
        let live_pnl_pct = if mem.locked.entry_price > 0.01 {
            ((mark - mem.locked.entry_price) / mem.locked.entry_price) * 100.0
        } else {
            0.0
        };

        let (phase_code, phase_title, phase_reason) = if mem.locked.tp_hit_at_ms > 0 {
            (
                "TAKE_PROFIT",
                "💰 PREDICTOR GOD — SACAR LUCRO AGORA $+! (ALVO ATINGIDO)",
                format!(
                    "Alvo Fixo (${:.2}) atingido! Lucro de +${:.2} ({:+.1}%). Realize o saque agora!",
                    mem.locked.tp_price,
                    live_pnl_usd.max(mem.locked.scalp_profit_usd),
                    live_pnl_pct.max(mem.locked.scalp_roi_pct)
                ),
            )
        } else if mem.locked.stop_ticks >= 2 {
            (
                "STOP_WARN",
                "⚠️ ATENÇÃO: TESTANDO STOP FIXO — MONITORANDO REVERSÃO",
                format!(
                    "Cota atual (${:.2}) próxima ao Stop Fixo (${:.2}). Validando força ({}/10).",
                    mark, mem.locked.stop_price, mem.locked.stop_ticks
                ),
            )
        } else if age_sec <= 45 {
            (
                "ENTER_NOW",
                "⚡ PREDICTOR GOD — ENTER $+ (PREVISÃO CONSOLIDADA TRAVADA)",
                format!(
                    "Previsão consolidada por {}/25 bots ({:.1}/25 pts EXP3)! Entre em {} @ ${:.2} visando Alvo ${:.2} (+${:.2}).",
                    mem.locked.locked_aligned_bots,
                    mem.locked.locked_weighted_score,
                    mem.locked.dir,
                    mem.locked.entry_price,
                    mem.locked.tp_price,
                    mem.locked.scalp_profit_usd
                ),
            )
        } else {
            (
                "HOLD",
                "🛡️ PREDICTOR GOD — MANTER POSIÇÃO $+ (EM OPERAÇÃO)",
                format!(
                    "Entrada ENTER $+ (${:.2}) ativa há {}s. Mantenha posição até o Alvo Fixo (${:.2}) ou expiração 5m.",
                    mem.locked.entry_price, age_sec, mem.locked.tp_price
                ),
            )
        };

        m.fixed = FixedSignalView {
            locked: true,
            dir: mem.locked.dir,
            entry_price: mem.locked.entry_price,
            tp_price: mem.locked.tp_price,
            stop_price: mem.locked.stop_price,
            stake_usd: mem.locked.stake_usd,
            shares: mem.locked.shares,
            scalp_profit_usd: mem.locked.scalp_profit_usd,
            scalp_roi_pct: mem.locked.scalp_roi_pct,
            expiry_profit_usd: mem.locked.expiry_profit_usd,
            expiry_roi_pct: mem.locked.expiry_roi_pct,
            locked_age_sec: age_sec,
            locked_weighted_score: mem.locked.locked_weighted_score,
            locked_aligned_bots: mem.locked.locked_aligned_bots,
            current_mark: mark,
            live_pnl_usd,
            live_pnl_pct,
            phase_code,
            phase_title,
            phase_reason,
            calc_progress_pct: 100.0,
            session_wins: mem.session_wins,
            session_losses: mem.session_losses,
            session_pnl_usd: mem.session_pnl_usd,
            swarm_total_bot_pnl,
            profitable_bots_count,
        };

        m.entry_lim = mem.locked.entry_price;
        m.scalp_tp = mem.locked.tp_price;
        m.stop_lvl = mem.locked.stop_price;
        m.recommended_stake_usd = mem.locked.stake_usd;
        m.anticipation_status = phase_title;
        m.anticipation_code = if phase_code == "STOP_WARN" { "LATE" } else { "GOLD" };
        m.oracle_command = if mem.locked.dir_sign > 0 {
            "⚡ ENTER $+ : COMPRAR UP ▲"
        } else {
            "⚡ ENTER $+ : COMPRAR DOWN ▼"
        };
        m.confidence_tier_label = if mem.locked.stake_usd >= 3.0 {
            "ENTER $+ TIER S → $3.00"
        } else if mem.locked.stake_usd >= 2.0 {
            "ENTER $+ TIER A → $2.00"
        } else {
            "ENTER $+ TIER B → $1.00"
        };
    } else {
        let prog = ((mem.calc_confirm_ticks as f64 / 3.0) * 100.0).clamp(15.0, 95.0);
        let reason = if target_ask > 0.88 {
            format!("Contrato finalizando (${:.2} > $0.88) — aguardando próxima oportunidade para travar ENTER $+.", target_ask)
        } else if target_ask < 0.06 {
            format!("Aguardando liquidez no livro (${:.2}) para travar ENTER $+.", target_ask)
        } else if m.rem_sec < 15 {
            format!("Faltam {}s para fechar a vela 5m — preparando ENTER $+ da próxima vela.", m.rem_sec)
        } else {
            format!(
                "Consolidando ENTER $+ ({:.1}/25 pts | confirmação {}/3) ➔ Direção dominante: {}",
                weighted_score, mem.calc_confirm_ticks, dom_dir
            )
        };

        m.fixed = FixedSignalView {
            locked: false,
            dir: dom_dir,
            entry_price: entry_lim,
            tp_price: scalp_tp,
            stop_price: stop_lvl,
            stake_usd: 1.0,
            shares: (1.0 / entry_lim).max(1.0),
            scalp_profit_usd: (1.0 / entry_lim).max(1.0) * scalp_tp - 1.0,
            scalp_roi_pct: (((1.0 / entry_lim).max(1.0) * scalp_tp - 1.0) / 1.0) * 100.0,
            expiry_profit_usd: (1.0 / entry_lim).max(1.0) - 1.0,
            expiry_roi_pct: (((1.0 / entry_lim).max(1.0) - 1.0) / 1.0) * 100.0,
            locked_age_sec: 0,
            locked_weighted_score: weighted_score,
            locked_aligned_bots: aligned,
            current_mark: target_ask,
            live_pnl_usd: 0.0,
            live_pnl_pct: 0.0,
            phase_code: "CALC",
            phase_title: "🔬 CONSOLIDANDO PREVISÃO ENTER $+ (~1s)...",
            phase_reason: reason,
            calc_progress_pct: prog,
            session_wins: mem.session_wins,
            session_losses: mem.session_losses,
            session_pnl_usd: mem.session_pnl_usd,
            swarm_total_bot_pnl,
            profitable_bots_count,
        };
        m.anticipation_status = "🔬 CONSOLIDANDO ENTER $+";
        m.anticipation_code = "WAIT";
        m.oracle_command = "CONSOLIDANDO ENTER $+";
        m.recommended_stake_usd = 1.0;
        m.confidence_tier_label = "CONSOLIDANDO ($1.00)";
    }

    // ÍNDICE SHARK [0..100]: concordância ponderada pelo lucro + barateza do livro + ganho projetado
    let cheapness_bonus = if (0.18..=0.68).contains(&target_ask) {
        ((0.72 - target_ask) * 35.0).clamp(0.0, 18.0)
    } else if target_ask > 0.82 {
        -35.0
    } else {
        0.0
    };
    let val_bonus = match validity_code {
        "SUSTAIN" => 10.0,
        "SHORT" => 2.0,
        "FADE" => -15.0,
        _ => 0.0,
    };
    m.intuition_index = (weighted_score * 3.2 + cheapness_bonus + fc30_gain_c.max(0.0) * 0.8 + val_bonus).clamp(3.0, 99.0);
    m.acc = mem.acc;

    // Registra a previsão deste tick para ser auditada contra o preço real em +15s e +30s
    if m.current_price > 0.0 {
        let do_bot_eval = now_m.saturating_sub(mem.last_bot_eval_ms) >= 4_000;
        if do_bot_eval {
            mem.last_bot_eval_ms = now_m;
        }
        mem.pending.push_back(PendingPred {
            ts_ms: now_m,
            window_ts: m.window_ts,
            spot: m.current_price,
            sigma: sig,
            up_mid,
            feats: x,
            votes: votes_arr,
            eval_bots: do_bot_eval,
            cons_dir: if aligned >= 14 { dom_sign } else { 0 },
            aligned,
            rls15_dir: if ready15 && pred15.abs() >= 0.10 { s15d } else { 0 },
            rls30_dir: if ready30 && pred30.abs() >= 0.10 { s30d } else { 0 },
            done15: false,
        });
        while mem.pending.len() > 150 {
            mem.pending.pop_front();
        }
        mem.poly_hist.push_back((now_m, up_mid, m.current_price, up_ask_depth, down_ask_depth));
        while let Some(f) = mem.poly_hist.front() {
            if now_m.saturating_sub(f.0) > 60_000 {
                mem.poly_hist.pop_front();
            } else {
                break;
            }
        }
        mem.spot_samples.push_back((now_m, m.current_price));
        while let Some(f) = mem.spot_samples.front() {
            if now_m.saturating_sub(f.0) > 150_000 {
                mem.spot_samples.pop_front();
            } else {
                break;
            }
        }
    }

    // Compute exact $1.00, $2.00, $3.00 profit matrix at m.entry_lim (uses fixed entry when locked!)
    let stakes = [1.0_f64, 2.0_f64, 3.0_f64];
    for (idx, &st) in stakes.iter().enumerate() {
        if m.entry_lim >= 0.02 && m.entry_lim < 0.99 {
            let sh = st / m.entry_lim;
            let scalp_pay = sh * m.scalp_tp;
            let scalp_prof = scalp_pay - st;
            let scalp_roi = (scalp_prof / st) * 100.0;
            let exp_pay = sh * 1.00;
            let exp_prof = exp_pay - st;
            let exp_roi = (exp_prof / st) * 100.0;
            m.stake_rows[idx] = StakeRow {
                stake_usd: st,
                shares: sh,
                scalp_profit_usd: scalp_prof,
                scalp_roi_pct: scalp_roi,
                expiry_profit_usd: exp_prof,
                expiry_roi_pct: exp_roi,
            };
        }
    }
}

fn classify_headline_sentiment(title: &str) -> (&'static str, f64) {
    let lower = title.to_lowercase();
    let bull_words = ["surge", "jump", "bull", "inflow", "record", "approve", "adoption", "rally", "buy", "breakout", "gain", "high", "soar", "accumulate", "etf"];
    let bear_words = ["crash", "drop", "hack", "ban", "lawsuit", "outflow", "sell", "bear", "dump", "slump", "fear", "liquidation", "plunge", "reject", "quit"];
    let mut score = 0.0_f64;
    for w in &bull_words {
        if lower.contains(w) {
            score += 0.45;
        }
    }
    for w in &bear_words {
        if lower.contains(w) {
            score -= 0.45;
        }
    }
    if score >= 0.3 {
        ("BULLISH", score.clamp(-1.0, 1.0))
    } else if score <= -0.3 {
        ("BEARISH", score.clamp(-1.0, 1.0))
    } else {
        ("NEUTRAL", 0.0)
    }
}

fn fetch_realtime_crypto_news() -> (Vec<NewsItem>, f64) {
    let mut items = Vec::new();
    let mut bias_sum = 0.0_f64;
    if let Some(xml) = curl_get("https://cointelegraph.com/rss", 2.8) {
        for block in xml.split("<item>").skip(1).take(7) {
            let title = if let Some(s) = block.find("<title>") {
                let rem = &block[s + 7..];
                if let Some(e) = rem.find("</title>") {
                    rem[..e]
                        .replace("<![CDATA[", "")
                        .replace("]]>", "")
                        .replace("&amp;", "&")
                        .replace("&#039;", "'")
                        .replace("&quot;", "\"")
                        .trim()
                        .to_string()
                } else {
                    continue;
                }
            } else {
                continue;
            };
            let pub_time = if let Some(s) = block.find("<pubDate>") {
                let rem = &block[s + 9..];
                if let Some(e) = rem.find("</pubDate>") {
                    let raw = rem[..e].trim();
                    if raw.len() >= 22 { raw[17..22].to_string() } else { raw.to_string() }
                } else {
                    "LIVE".to_string()
                }
            } else {
                "LIVE".to_string()
            };
            let (sent, sc) = classify_headline_sentiment(&title);
            bias_sum += sc;
            items.push(NewsItem {
                title,
                source: "CoinTelegraph".to_string(),
                pub_time,
                sentiment: sent,
                impact_score: sc,
            });
        }
    }
    let avg_bias = if !items.is_empty() { (bias_sum / (items.len() as f64)).clamp(-1.0, 1.0) } else { 0.0 };
    (items, avg_bias)
}

fn read_recent_journal() -> Vec<String> {
    let mut out = Vec::new();
    if let Ok(content) = fs::read_to_string("/home/god/.gemini/antigravity/scratch/polymarket_agent_bundle/trade_journal.csv") {
        let lines: Vec<&str> = content.lines().collect();
        let start = lines.len().saturating_sub(6);
        for l in &lines[start..] {
            out.push(l.to_string());
        }
    }
    out
}

fn append_operator_journal(action: &str, slug: &str, side: &str, price: f64, shares: f64, usd: f64, status: &str) {
    if let Ok(mut f) = OpenOptions::new()
        .create(true)
        .append(true)
        .open("/home/god/.gemini/antigravity/scratch/polymarket_agent_bundle/trade_journal.csv")
    {
        let _ = writeln!(f, "{},{},{},{},{:.4},{:.2},{:.4},{}", now_sec(), action, slug, side, price, shares, usd, status);
    }
}

fn spawn_telemetry_loop(state: Arc<Mutex<CockpitState>>) {
    thread::spawn(move || {
        let cfg = crypto_tx::load_polymarket_config();
        let mut last_wallet_refresh = Instant::now() - Duration::from_secs(60);
        let mut last_pos_refresh = Instant::now() - Duration::from_secs(60);
        let mut last_news_refresh = Instant::now() - Duration::from_secs(120);
        let mut cached_pusd = 0.0_f64;
        let mut cached_usdce = 0.0_f64;
        let mut cached_up_raw: u64 = 0;
        let mut cached_dn_raw: u64 = 0;
        let mut cached_api_pos: Vec<RawApiPos> = Vec::new();
        let mut fallback_up_entry = 0.45_f64;
        let mut fallback_dn_entry = 0.45_f64;
        let mut cached_news: Vec<NewsItem> = Vec::new();
        let mut cached_news_bias = 0.0_f64;
        let mut btc_token_cache: (u64, String, String) = (0, String::new(), String::new());
        let mut prev_whales: (f64, f64) = (0.0, 0.0);
        let mut mem = MarketMemory::new();
        let mut last_diff_usd = 0.0_f64;
        let default_book = DetailedBook {
            best_bid: 0.49,
            best_ask: 0.51,
            bid_depth_top5: 0.0,
            whale_usd_35to70: 0.0,
            top_wall_usd: 0.0,
            bids_top4: [(0.0, 0.0); 4],
            asks_top4: [(0.0, 0.0); 4],
        };
        let mut last_up_book = default_book.clone();
        let mut last_dn_book = default_book.clone();

        loop {
            let loop_start = Instant::now();
            let ts = now_sec();
            let window_ts = (ts / 300) * 300;
            let rem_sec = (window_ts + 300) as i64 - (ts as i64);
            let window_start_ms = window_ts * 1000;

            // Check if user requested manual unlock / recalculation or position refresh
            let mut want_pos_refresh = last_pos_refresh.elapsed() >= Duration::from_millis(1400);
            if let Ok(mut st) = state.lock() {
                if st.force_recalc {
                    st.force_recalc = false;
                    mem.locked.active = false;
                    mem.calc_confirm_ticks = 0;
                    mem.cooldown_until_ms = 0;
                    st.last_action_log = "🔄 Destravado manualmente — 25 Bots Superinteligência iniciando novo cálculo de Entrada Fixa...".to_string();
                }
                if st.force_pos_refresh {
                    st.force_pos_refresh = false;
                    want_pos_refresh = true;
                }
            }

            if last_wallet_refresh.elapsed() >= Duration::from_secs(5) || want_pos_refresh {
                if last_wallet_refresh.elapsed() >= Duration::from_secs(5) {
                    if let Some(p) = query_erc20_balance(PUSD_TOKEN, &cfg.proxy_wallet) {
                        cached_pusd = p;
                    }
                    if let Some(u) = query_erc20_balance(USDCE_TOKEN, &cfg.proxy_wallet) {
                        cached_usdce = u;
                    }
                    last_wallet_refresh = Instant::now();
                }
            }

            if last_news_refresh.elapsed() >= Duration::from_secs(45) {
                let (n_items, n_bias) = fetch_realtime_crypto_news();
                if !n_items.is_empty() {
                    cached_news = n_items;
                    cached_news_bias = n_bias;
                }
                last_news_refresh = Instant::now();
            }

            let btc_slug = format!("btc-updown-5m-{}", window_ts);
            if btc_token_cache.0 != window_ts || btc_token_cache.1.is_empty() {
                if let Some((up_t, down_t)) = fetch_market_tokens_by_slug(&btc_slug) {
                    if btc_token_cache.0 != window_ts {
                        last_up_book = default_book.clone();
                        last_dn_book = default_book.clone();
                        cached_up_raw = 0;
                        cached_dn_raw = 0;
                        want_pos_refresh = true;
                    }
                    btc_token_cache = (window_ts, up_t, down_t);
                }
            }

            let up_tok = btc_token_cache.1.clone();
            let down_tok = btc_token_cache.2.clone();
            let fallback_up = last_up_book.clone();
            let fallback_dn = last_dn_book.clone();
            let wallet_ref = cfg.proxy_wallet.as_str();

            let (
                (cur, strike, sigma_1s, v5, v15, v45, taker_ratio, vpin_10s, absorption, hist_60),
                (bin_obi_20, bin_vai, _),
                b_up,
                b_down,
                eth_mic,
                pos_snap,
            ) = thread::scope(|inner| {
                let h_mic = inner.spawn(|| {
                    fetch_binance_microstructure("BTCUSDT", window_start_ms).unwrap_or((
                        0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.5, 0.0, 1.0, Vec::new(),
                    ))
                });
                let h_dep = inner.spawn(|| fetch_binance_depth_20("BTCUSDT"));
                let h_up = inner.spawn(|| parse_book_detailed(&up_tok).unwrap_or(fallback_up));
                let h_dn = inner.spawn(|| parse_book_detailed(&down_tok).unwrap_or(fallback_dn));
                let h_eth = inner.spawn(|| fetch_binance_microstructure("ETHUSDT", window_start_ms));
                let h_pos = inner.spawn(|| {
                    if want_pos_refresh {
                        let pair = query_erc1155_balances_pair(wallet_ref, &up_tok, &down_tok);
                        let api = fetch_data_api_open_positions(wallet_ref);
                        Some((pair.0, pair.1, api))
                    } else {
                        None
                    }
                });
                (
                    h_mic.join().unwrap(),
                    h_dep.join().unwrap(),
                    h_up.join().unwrap(),
                    h_dn.join().unwrap(),
                    h_eth.join().unwrap(),
                    h_pos.join().unwrap(),
                )
            });
            if let Some((ur, dr, ap)) = pos_snap {
                if cached_up_raw < 50_000 && ur >= 50_000 {
                    fallback_up_entry = b_up.best_ask.clamp(0.02, 0.98);
                }
                if cached_dn_raw < 50_000 && dr >= 50_000 {
                    fallback_dn_entry = b_down.best_ask.clamp(0.02, 0.98);
                }
                cached_up_raw = ur;
                cached_dn_raw = dr;
                cached_api_pos = ap;
                last_pos_refresh = Instant::now();
            }
            if b_up.best_ask > 0.01 && (b_up.best_bid != 0.49 || b_up.best_ask != 0.51 || b_up.bid_depth_top5 > 0.0) {
                last_up_book = b_up.clone();
            }
            if b_down.best_ask > 0.01 && (b_down.best_bid != 0.49 || b_down.best_ask != 0.51 || b_down.bid_depth_top5 > 0.0) {
                last_dn_book = b_down.clone();
            }

            let cross_z_div = if let Some((_, _, eth_sig, eth_v5, eth_v15, _, _, _, _, _)) = eth_mic {
                let z_btc = (0.65 * (v5 / (sigma_1s * 2.236).max(1e-6))) + (0.35 * (v15 / (sigma_1s * 3.873).max(1e-6)));
                let z_eth = (0.65 * (eth_v5 / (eth_sig * 2.236).max(1e-6))) + (0.35 * (eth_v15 / (eth_sig * 3.873).max(1e-6)));
                let joint_impulse = 0.60 * z_eth + 0.40 * z_btc;
                let lead_lag_catchup = (z_eth - 0.85 * z_btc).clamp(-3.0, 3.0);
                let coint_gate = if z_eth * z_btc >= -0.25 { 1.0 } else { 0.35 };
                (0.65 * joint_impulse + 0.35 * lead_lag_catchup) * coint_gate
            } else {
                0.0
            };

            let diff_usd = cur - strike;
            let diff_pct = if strike > 0.0 { (diff_usd / strike) * 100.0 } else { 0.0 };
            let tot_depth = b_up.bid_depth_top5 + b_down.bid_depth_top5;
            let obi = if tot_depth > 0.0 {
                (b_up.bid_depth_top5 - b_down.bid_depth_top5) / tot_depth
            } else {
                0.0
            };

            let mut m = MarketTelemetry {
                key: "BTC".to_string(),
                label: "BITCOIN 5M (UP OR DOWN)".to_string(),
                slug: btc_slug,
                window_ts,
                rem_sec,
                price_to_beat: strike,
                current_price: cur,
                diff_usd,
                diff_pct,
                sigma_1s,
                vel_5s: v5,
                vel_15s: v15,
                vel_45s: v45,
                taker_buy_ratio: taker_ratio,
                binance_obi_20: bin_obi_20,
                binance_vai: bin_vai,
                up_token: up_tok,
                down_token: down_tok,
                up_bid: b_up.best_bid,
                up_ask: b_up.best_ask,
                down_bid: b_down.best_bid,
                down_ask: b_down.best_ask,
                obi,
                whale_up_usd: b_up.whale_usd_35to70,
                whale_down_usd: b_down.whale_usd_35to70,
                delta_whale_usd: 0.0,
                top_wall_up_usd: b_up.top_wall_usd,
                top_wall_down_usd: b_down.top_wall_usd,
                up_bids_top4: b_up.bids_top4,
                up_asks_top4: b_up.asks_top4,
                down_bids_top4: b_down.bids_top4,
                down_asks_top4: b_down.asks_top4,
                spot_history_60: hist_60,
                ..Default::default()
            };

            let now_m = now_ms();
            if mem.window_ts != window_ts {
                // Se havia uma Entrada Fixa travada na vela que acabou de fechar, liquida o resultado real da vela!
                if mem.locked.active && mem.locked.tp_hit_at_ms == 0 && mem.window_ts > 0 {
                    let candle_won = (mem.locked.dir_sign > 0 && last_diff_usd >= 0.0)
                        || (mem.locked.dir_sign < 0 && last_diff_usd < 0.0);
                    if candle_won {
                        mem.session_wins += 1;
                        mem.session_pnl_usd += mem.locked.expiry_profit_usd;
                    } else {
                        mem.session_losses += 1;
                        mem.session_pnl_usd -= mem.locked.stake_usd;
                    }
                    let outcome_sign = if candle_won { 1.0 } else { -1.0 };
                    for b_idx in 0..25 {
                        let v = mem.locked.votes_at_lock[b_idx];
                        if v != 0 {
                            let sign_match = if v == mem.locked.dir_sign { outcome_sign } else { -outcome_sign };
                            mem.bots[b_idx].record_outcome(sign_match * 0.22);
                        }
                    }
                }
                mem.locked.active = false;
                mem.calc_confirm_ticks = 0;
                mem.poly_hist.clear();
                mem.window_ts = window_ts;
            }
            last_diff_usd = diff_usd;

            let up_mid_now = 0.5 * (m.up_bid + m.up_ask);
            mature_predictions(&mut mem, now_m, m.current_price, up_mid_now, &m.key);
            evaluate_20_agent_swarm(
                &mut m,
                prev_whales.0,
                prev_whales.1,
                vpin_10s,
                absorption,
                cross_z_div,
                true,
                cached_news_bias,
                &mut mem,
                now_m,
            );
            prev_whales = (m.whale_up_usd, m.whale_down_usd);

            // Constrói a lista de Opções Abertas & Valor das Shares em Tempo Real (350ms)
            let mut live_open_positions: Vec<OpenPositionView> = Vec::new();
            if cached_up_raw >= 50_000 && !m.up_token.is_empty() {
                let sh = (cached_up_raw as f64) / 1_000_000.0;
                let api_match = cached_api_pos.iter().find(|p| p.token_id == m.up_token);
                let avg_entry = api_match
                    .map(|p| p.avg_price)
                    .unwrap_or_else(|| if m.fixed.locked && m.fixed.dir == "UP" { m.fixed.entry_price } else { fallback_up_entry });
                let cost_usd = api_match.map(|p| p.cost_usd).unwrap_or(sh * avg_entry);
                let l_bid = m.up_bid.max(0.0);
                let v_bid = compute_vwap_bid(l_bid, &m.up_bids_top4, sh);
                let cash_usd = sh * v_bid;
                let prof_usd = cash_usd - cost_usd;
                let prof_pct = if cost_usd > 0.001 { (prof_usd / cost_usd) * 100.0 } else { 0.0 };
                live_open_positions.push(OpenPositionView {
                    token_id: m.up_token.clone(),
                    slug: m.slug.clone(),
                    side: "UP".to_string(),
                    shares: sh,
                    raw_shares: cached_up_raw,
                    avg_entry,
                    cost_usd,
                    live_bid: l_bid,
                    vwap_bid: v_bid,
                    cashout_usd: cash_usd,
                    profit_usd: prof_usd,
                    profit_pct: prof_pct,
                    expiry_win_usd: sh * 1.00,
                    is_current_candle: true,
                });
            }
            if cached_dn_raw >= 50_000 && !m.down_token.is_empty() {
                let sh = (cached_dn_raw as f64) / 1_000_000.0;
                let api_match = cached_api_pos.iter().find(|p| p.token_id == m.down_token);
                let avg_entry = api_match
                    .map(|p| p.avg_price)
                    .unwrap_or_else(|| if m.fixed.locked && m.fixed.dir == "DOWN" { m.fixed.entry_price } else { fallback_dn_entry });
                let cost_usd = api_match.map(|p| p.cost_usd).unwrap_or(sh * avg_entry);
                let l_bid = m.down_bid.max(0.0);
                let v_bid = compute_vwap_bid(l_bid, &m.down_bids_top4, sh);
                let cash_usd = sh * v_bid;
                let prof_usd = cash_usd - cost_usd;
                let prof_pct = if cost_usd > 0.001 { (prof_usd / cost_usd) * 100.0 } else { 0.0 };
                live_open_positions.push(OpenPositionView {
                    token_id: m.down_token.clone(),
                    slug: m.slug.clone(),
                    side: "DOWN".to_string(),
                    shares: sh,
                    raw_shares: cached_dn_raw,
                    avg_entry,
                    cost_usd,
                    live_bid: l_bid,
                    vwap_bid: v_bid,
                    cashout_usd: cash_usd,
                    profit_usd: prof_usd,
                    profit_pct: prof_pct,
                    expiry_win_usd: sh * 1.00,
                    is_current_candle: true,
                });
            }
            // Inclui eventuais outras opções abertas indexadas na Data API v2 que ainda tenham valor de mercado (> 0.2¢)
            for ap in &cached_api_pos {
                if ap.token_id != m.up_token && ap.token_id != m.down_token && ap.cur_price >= 0.002 && ap.size >= 0.05 {
                    let sh = ap.size;
                    let raw_sh = (sh * 1_000_000.0).floor() as u64;
                    let cash_usd = sh * ap.cur_price;
                    let prof_usd = cash_usd - ap.cost_usd;
                    let prof_pct = if ap.cost_usd > 0.001 { (prof_usd / ap.cost_usd) * 100.0 } else { 0.0 };
                    live_open_positions.push(OpenPositionView {
                        token_id: ap.token_id.clone(),
                        slug: ap.slug.clone(),
                        side: ap.side.clone(),
                        shares: sh,
                        raw_shares: raw_sh,
                        avg_entry: ap.avg_price,
                        cost_usd: ap.cost_usd,
                        live_bid: ap.cur_price,
                        vwap_bid: ap.cur_price,
                        cashout_usd: cash_usd,
                        profit_usd: prof_usd,
                        profit_pct: prof_pct,
                        expiry_win_usd: sh * 1.00,
                        is_current_candle: false,
                    });
                }
            }

            let journal = read_recent_journal();
            let elapsed_ms = loop_start.elapsed().as_millis() as u64;

            if let Ok(mut st) = state.lock() {
                st.updated_ms = now_ms();
                st.proxy_wallet = cfg.proxy_wallet.clone();
                st.pusd_balance = cached_pusd;
                st.usdce_balance = cached_usdce;
                st.proxy_latency_ms = elapsed_ms;
                st.markets = vec![m];
                st.open_positions = live_open_positions;
                st.news = cached_news.clone();
                st.news_macro_bias = cached_news_bias;
                st.recent_journal_rows = journal;
            }

            thread::sleep(Duration::from_millis(350));
        }
    });
}

fn format_book_levels_json(levels: &[(f64, f64); 4]) -> String {
    let mut s = String::from("[");
    for (i, &(p, sz)) in levels.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!("[{:.3},{:.1}]", p, sz));
    }
    s.push(']');
    s
}

fn format_f64_vec_json(v: &[f64]) -> String {
    let mut s = String::from("[");
    for (i, &val) in v.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!("{:.2}", val));
    }
    s.push(']');
    s
}

fn escape_json(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', " ").replace('\r', "")
}

fn build_state_json(st: &CockpitState) -> String {
    let mut m_json = String::from("[");
    for (i, m) in st.markets.iter().enumerate() {
        if i > 0 {
            m_json.push(',');
        }
        let mut sens_json = String::from("[");
        for (si, sc) in m.sensors.iter().enumerate() {
            if si > 0 {
                sens_json.push(',');
            }
            sens_json.push_str(&format!(
                "{{\"id\":\"{}\",\"name\":\"{}\",\"vote\":{},\"strength\":{:.2},\"metric\":\"{}\",\"pnl\":{:.2},\"wins\":{},\"losses\":{},\"weight\":{:.2}}}",
                sc.id,
                sc.name,
                sc.vote,
                sc.strength,
                escape_json(&sc.metric),
                sc.pnl_usd,
                sc.wins,
                sc.losses,
                sc.weight
            ));
        }
        sens_json.push(']');

        let mut stakes_json = String::from("[");
        for (ri, r) in m.stake_rows.iter().enumerate() {
            if ri > 0 {
                stakes_json.push(',');
            }
            stakes_json.push_str(&format!(
                "{{\"stake\":{:.2},\"shares\":{:.2},\"scalp_profit\":{:.2},\"scalp_roi\":{:.1},\"exp_profit\":{:.2},\"exp_roi\":{:.1}}}",
                r.stake_usd, r.shares, r.scalp_profit_usd, r.scalp_roi_pct, r.expiry_profit_usd, r.expiry_roi_pct
            ));
        }
        stakes_json.push(']');

        let fx = &m.fixed;
        let fixed_json = format!(
            "{{\"locked\":{},\"dir\":\"{}\",\"entry_price\":{:.3},\"tp_price\":{:.3},\"stop_price\":{:.3},\"stake_usd\":{:.2},\"shares\":{:.2},\"scalp_profit_usd\":{:.2},\"scalp_roi_pct\":{:.1},\"expiry_profit_usd\":{:.2},\"expiry_roi_pct\":{:.1},\"locked_age_sec\":{},\"locked_weighted_score\":{:.1},\"locked_aligned_bots\":{},\"current_mark\":{:.3},\"live_pnl_usd\":{:.2},\"live_pnl_pct\":{:.1},\"phase_code\":\"{}\",\"phase_title\":\"{}\",\"phase_reason\":\"{}\",\"calc_progress_pct\":{:.0},\"session_wins\":{},\"session_losses\":{},\"session_pnl_usd\":{:.2},\"swarm_total_bot_pnl\":{:.2},\"profitable_bots_count\":{}}}",
            fx.locked,
            fx.dir,
            fx.entry_price,
            fx.tp_price,
            fx.stop_price,
            fx.stake_usd,
            fx.shares,
            fx.scalp_profit_usd,
            fx.scalp_roi_pct,
            fx.expiry_profit_usd,
            fx.expiry_roi_pct,
            fx.locked_age_sec,
            fx.locked_weighted_score,
            fx.locked_aligned_bots,
            fx.current_mark,
            fx.live_pnl_usd,
            fx.live_pnl_pct,
            fx.phase_code,
            escape_json(fx.phase_title),
            escape_json(&fx.phase_reason),
            fx.calc_progress_pct,
            fx.session_wins,
            fx.session_losses,
            fx.session_pnl_usd,
            fx.swarm_total_bot_pnl,
            fx.profitable_bots_count
        );

        let acc_json = m.acc.iter().map(|a| format!("[{},{}]", a.hits, a.n)).collect::<Vec<_>>().join(",");
        let fc_json = format!(
            "\"fc_ready\":{},\"calib_n\":{},\"fc15_pred\":{:.3},\"fc30_pred\":{:.3},\"fc15_dspot\":{:.2},\"fc30_dspot\":{:.2},\"fc15_band\":{:.2},\"fc30_band\":{:.2},\"fc15_up\":{:.4},\"fc30_up\":{:.4},\"fc15_gain_c\":{:.1},\"fc30_gain_c\":{:.1},\"validity_label\":\"{}\",\"validity_code\":\"{}\",\"streak\":{},\"sigma_regime\":{:.3},\"acc\":[{}]",
            m.fc_ready, m.calib_n, m.fc15_pred, m.fc30_pred, m.fc15_dspot, m.fc30_dspot, m.fc15_band, m.fc30_band,
            m.fc15_up, m.fc30_up, m.fc15_gain_c, m.fc30_gain_c, escape_json(m.validity_label), m.validity_code,
            m.streak, m.sigma_regime, acc_json
        );

        m_json.push_str(&format!(
            "{{\"key\":\"{}\",\"label\":\"{}\",\"slug\":\"{}\",\"rem_sec\":{},\"price_to_beat\":{:.2},\"current_price\":{:.2},\"diff_usd\":{:.2},\"diff_pct\":{:.3},\"sigma_1s\":{:.4},\"vel_5s\":{:.2},\"vel_15s\":{:.2},\"vel_45s\":{:.2},\"taker_buy_ratio\":{:.3},\"binance_obi_20\":{:.3},\"binance_vai\":{:.3},\"up_token\":\"{}\",\"down_token\":\"{}\",\"up_bid\":{:.3},\"up_ask\":{:.3},\"down_bid\":{:.3},\"down_ask\":{:.3},\"obi\":{:.3},\"whale_up_usd\":{:.1},\"whale_down_usd\":{:.1},\"delta_whale_usd\":{:.1},\"top_wall_up_usd\":{:.1},\"top_wall_down_usd\":{:.1},\"up_bids\":{},\"up_asks\":{},\"down_bids\":{},\"down_asks\":{},\"history_60\":{},\"sensors\":{},\"up_votes\":{},\"down_votes\":{},\"neutral_votes\":{},\"dominant_dir\":\"{}\",\"aligned_count\":{},\"weighted_score\":{:.1},\"fixed\":{},\"intuition_index\":{:.1},\"anticipation_status\":\"{}\",\"anticipation_code\":\"{}\",\"oracle_command\":\"{}\",\"recommended_stake_usd\":{:.2},\"confidence_tier_label\":\"{}\",\"target_ask\":{:.3},\"entry_lim\":{:.3},\"scalp_tp\":{:.3},\"stop_lvl\":{:.3},\"latency_gap_cents\":{:.1},\"stake_rows\":{},{}}}",
            escape_json(&m.key),
            escape_json(&m.label),
            escape_json(&m.slug),
            m.rem_sec,
            m.price_to_beat,
            m.current_price,
            m.diff_usd,
            m.diff_pct,
            m.sigma_1s,
            m.vel_5s,
            m.vel_15s,
            m.vel_45s,
            m.taker_buy_ratio,
            m.binance_obi_20,
            m.binance_vai,
            escape_json(&m.up_token),
            escape_json(&m.down_token),
            m.up_bid,
            m.up_ask,
            m.down_bid,
            m.down_ask,
            m.obi,
            m.whale_up_usd,
            m.whale_down_usd,
            m.delta_whale_usd,
            m.top_wall_up_usd,
            m.top_wall_down_usd,
            format_book_levels_json(&m.up_bids_top4),
            format_book_levels_json(&m.up_asks_top4),
            format_book_levels_json(&m.down_bids_top4),
            format_book_levels_json(&m.down_asks_top4),
            format_f64_vec_json(&m.spot_history_60),
            sens_json,
            m.up_votes,
            m.down_votes,
            m.neutral_votes,
            m.dominant_dir,
            m.aligned_count,
            m.weighted_score,
            fixed_json,
            m.intuition_index,
            escape_json(m.anticipation_status),
            m.anticipation_code,
            escape_json(m.oracle_command),
            m.recommended_stake_usd,
            escape_json(m.confidence_tier_label),
            m.target_ask,
            m.entry_lim,
            m.scalp_tp,
            m.stop_lvl,
            m.latency_gap_cents,
            stakes_json,
            fc_json
        ));
    }
    m_json.push(']');

    let mut pos_json = String::from("[");
    for (i, p) in st.open_positions.iter().enumerate() {
        if i > 0 {
            pos_json.push(',');
        }
        pos_json.push_str(&format!(
            "{{\"token_id\":\"{}\",\"slug\":\"{}\",\"side\":\"{}\",\"shares\":{:.2},\"raw_shares\":{},\"avg_entry\":{:.3},\"cost_usd\":{:.2},\"live_bid\":{:.3},\"vwap_bid\":{:.3},\"cashout_usd\":{:.2},\"profit_usd\":{:.2},\"profit_pct\":{:.1},\"expiry_win_usd\":{:.2},\"is_current_candle\":{}}}",
            escape_json(&p.token_id),
            escape_json(&p.slug),
            escape_json(&p.side),
            p.shares,
            p.raw_shares,
            p.avg_entry,
            p.cost_usd,
            p.live_bid,
            p.vwap_bid,
            p.cashout_usd,
            p.profit_usd,
            p.profit_pct,
            p.expiry_win_usd,
            p.is_current_candle
        ));
    }
    pos_json.push(']');

    let mut n_json = String::from("[");
    for (i, n) in st.news.iter().enumerate() {
        if i > 0 {
            n_json.push(',');
        }
        n_json.push_str(&format!(
            "{{\"title\":\"{}\",\"source\":\"{}\",\"pub_time\":\"{}\",\"sentiment\":\"{}\",\"impact\":{:.2}}}",
            escape_json(&n.title),
            escape_json(&n.source),
            escape_json(&n.pub_time),
            n.sentiment,
            n.impact_score
        ));
    }
    n_json.push(']');

    let mut j_json = String::from("[");
    for (i, r) in st.recent_journal_rows.iter().enumerate() {
        if i > 0 {
            j_json.push(',');
        }
        j_json.push_str(&format!("\"{}\"", escape_json(r)));
    }
    j_json.push(']');

    format!(
        "{{\"updated_ms\":{},\"proxy_wallet\":\"{}\",\"pusd_balance\":{:.6},\"usdce_balance\":{:.6},\"active_proxy_tag\":\"{}\",\"proxy_latency_ms\":{},\"news_macro_bias\":{:.3},\"last_action_log\":\"{}\",\"markets\":{},\"open_positions\":{},\"news\":{},\"journal\":{}}}",
        st.updated_ms,
        escape_json(&st.proxy_wallet),
        st.pusd_balance,
        st.usdce_balance,
        escape_json(&st.active_proxy_tag),
        st.proxy_latency_ms,
        st.news_macro_bias,
        escape_json(&st.last_action_log),
        m_json,
        pos_json,
        n_json,
        j_json
    )
}

const EXCHANGE_V2: &str = "0xE111180000d2663C0091e4f400237545B87B996B";

fn sha256_block(h: &mut [u32; 8], block: &[u8; 64]) {
    const K: [u32; 64] = [
        0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
        0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
        0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
        0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
        0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
        0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
        0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
    ];
    let mut w = [0u32; 64];
    for i in 0..16 {
        w[i] = u32::from_be_bytes([block[i * 4], block[i * 4 + 1], block[i * 4 + 2], block[i * 4 + 3]]);
    }
    for i in 16..64 {
        let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
        let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
        w[i] = w[i - 16].wrapping_add(s0).wrapping_add(w[i - 7]).wrapping_add(s1);
    }
    let (mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh) = (h[0], h[1], h[2], h[3], h[4], h[5], h[6], h[7]);
    for i in 0..64 {
        let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
        let ch = (e & f) ^ ((!e) & g);
        let temp1 = hh.wrapping_add(s1).wrapping_add(ch).wrapping_add(K[i]).wrapping_add(w[i]);
        let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
        let maj = (a & b) ^ (a & c) ^ (b & c);
        let temp2 = s0.wrapping_add(maj);
        hh = g; g = f; f = e; e = d.wrapping_add(temp1);
        d = c; c = b; b = a; a = temp1.wrapping_add(temp2);
    }
    h[0] = h[0].wrapping_add(a); h[1] = h[1].wrapping_add(b);
    h[2] = h[2].wrapping_add(c); h[3] = h[3].wrapping_add(d);
    h[4] = h[4].wrapping_add(e); h[5] = h[5].wrapping_add(f);
    h[6] = h[6].wrapping_add(g); h[7] = h[7].wrapping_add(hh);
}

fn sha256(data: &[u8]) -> [u8; 32] {
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
        0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
    ];
    let bit_len = (data.len() as u64) * 8;
    let mut buf = data.to_vec();
    buf.push(0x80);
    while (buf.len() + 8) % 64 != 0 {
        buf.push(0);
    }
    buf.extend_from_slice(&bit_len.to_be_bytes());
    for chunk in buf.chunks_exact(64) {
        let mut block = [0u8; 64];
        block.copy_from_slice(chunk);
        sha256_block(&mut h, &block);
    }
    let mut out = [0u8; 32];
    for i in 0..8 {
        out[i * 4..(i + 1) * 4].copy_from_slice(&h[i].to_be_bytes());
    }
    out
}

fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; 32] {
    let mut k = [0u8; 64];
    if key.len() > 64 {
        let kh = sha256(key);
        k[..32].copy_from_slice(&kh);
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let mut ipad = [0x36u8; 64];
    let mut opad = [0x5cu8; 64];
    for i in 0..64 {
        ipad[i] ^= k[i];
        opad[i] ^= k[i];
    }
    let mut inner = ipad.to_vec();
    inner.extend_from_slice(msg);
    let inner_hash = sha256(&inner);
    let mut outer = opad.to_vec();
    outer.extend_from_slice(&inner_hash);
    sha256(&outer)
}

fn base64_decode(s: &str) -> Vec<u8> {
    let tbl: Vec<u8> = (0..256)
        .map(|i| match i as u8 {
            b'A'..=b'Z' => i as u8 - b'A',
            b'a'..=b'z' => i as u8 - b'a' + 26,
            b'0'..=b'9' => i as u8 - b'0' + 52,
            b'+' | b'-' => 62,
            b'/' | b'_' => 63,
            _ => 255,
        })
        .collect();
    let clean: Vec<u8> = s.bytes().filter(|&b| b != b'=' && b != b'\n' && b != b'\r').collect();
    let mut out = Vec::new();
    for chunk in clean.chunks(4) {
        let b0 = tbl[chunk[0] as usize];
        let b1 = if chunk.len() > 1 { tbl[chunk[1] as usize] } else { 0 };
        let b2 = if chunk.len() > 2 { tbl[chunk[2] as usize] } else { 0 };
        let b3 = if chunk.len() > 3 { tbl[chunk[3] as usize] } else { 0 };
        out.push((b0 << 2) | (b1 >> 4));
        if chunk.len() > 2 {
            out.push(((b1 & 0xf) << 4) | (b2 >> 2));
        }
        if chunk.len() > 3 {
            out.push(((b2 & 0x3) << 6) | b3);
        }
    }
    out
}

fn base64_encode_urlsafe(data: &[u8]) -> String {
    const B64: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::new();
    for chunk in data.chunks(3) {
        let b0 = chunk[0];
        let b1 = if chunk.len() > 1 { chunk[1] } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] } else { 0 };
        out.push(B64[(b0 >> 2) as usize] as char);
        out.push(B64[(((b0 & 3) << 4) | (b1 >> 4)) as usize] as char);
        if chunk.len() > 1 {
            out.push(B64[(((b1 & 0xf) << 2) | (b2 >> 6)) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(B64[(b2 & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

fn compute_l2_sig(secret: &str, ts: &str, method: &str, path: &str, body: &str) -> String {
    let msg = format!("{}{}{}{}", ts, method, path, body);
    let key_bytes = base64_decode(secret);
    let hmac = hmac_sha256(&key_bytes, msg.as_bytes());
    base64_encode_urlsafe(&hmac)
}

fn u256_from_dec_str(s: &str) -> [u8; 32] {
    let mut bytes = [0u8; 32];
    for c in s.bytes() {
        if c.is_ascii_digit() {
            let digit = (c - b'0') as u32;
            let mut carry = digit;
            for i in (0..32).rev() {
                let val = (bytes[i] as u32) * 10 + carry;
                bytes[i] = (val & 0xff) as u8;
                carry = val >> 8;
            }
        }
    }
    bytes
}

fn u256_from_u64(v: u64) -> [u8; 32] {
    let mut out = [0u8; 32];
    out[24..32].copy_from_slice(&v.to_be_bytes());
    out
}

fn address_word(addr: &[u8; 20]) -> [u8; 32] {
    let mut out = [0u8; 32];
    out[12..32].copy_from_slice(addr);
    out
}

fn post_signed_clob_order(
    cfg: &crypto_tx::PolymarketConfig,
    pk2: crypto_tx::U256,
    pk2_addr: [u8; 20],
    maker_addr: [u8; 20],
    token_id: &str,
    side_str: &str,
    side_u8: u64,
    maker_amt: u64,
    taker_amt: u64,
    order_type: &str,
) -> (bool, String) {
    let now_m = now_ms();
    let salt: u64 = (now_m ^ 0x59c3e1a7 ^ maker_amt) & 0x7fff_ffff_ffff;
    let exch_addr = crypto_tx::parse_addr20(EXCHANGE_V2).unwrap();

    let dom_type_hash = crypto_tx::keccak256(b"EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)");
    let name_hash = crypto_tx::keccak256(b"Polymarket CTF Exchange");
    let ver_hash = crypto_tx::keccak256(b"2");
    let mut dom_buf = Vec::with_capacity(160);
    dom_buf.extend_from_slice(&dom_type_hash);
    dom_buf.extend_from_slice(&name_hash);
    dom_buf.extend_from_slice(&ver_hash);
    dom_buf.extend_from_slice(&u256_from_u64(137));
    dom_buf.extend_from_slice(&address_word(&exch_addr));
    let domain_separator = crypto_tx::keccak256(&dom_buf);

    let order_type_hash = crypto_tx::keccak256(
        b"Order(uint256 salt,address maker,address signer,uint256 tokenId,uint256 makerAmount,uint256 takerAmount,uint8 side,uint8 signatureType,uint256 timestamp,bytes32 metadata,bytes32 builder)",
    );
    let mut struct_buf = Vec::with_capacity(384);
    struct_buf.extend_from_slice(&order_type_hash);
    struct_buf.extend_from_slice(&u256_from_u64(salt));
    struct_buf.extend_from_slice(&address_word(&maker_addr));
    struct_buf.extend_from_slice(&address_word(&pk2_addr));
    struct_buf.extend_from_slice(&u256_from_dec_str(token_id));
    struct_buf.extend_from_slice(&u256_from_u64(maker_amt));
    struct_buf.extend_from_slice(&u256_from_u64(taker_amt));
    struct_buf.extend_from_slice(&u256_from_u64(side_u8));
    struct_buf.extend_from_slice(&u256_from_u64(1)); // signatureType = 1 (POLY_PROXY)
    struct_buf.extend_from_slice(&u256_from_u64(now_m));
    struct_buf.extend_from_slice(&[0u8; 32]);
    struct_buf.extend_from_slice(&[0u8; 32]);
    let struct_hash = crypto_tx::keccak256(&struct_buf);

    let mut digest_input = Vec::with_capacity(66);
    digest_input.push(0x19);
    digest_input.push(0x01);
    digest_input.extend_from_slice(&domain_separator);
    digest_input.extend_from_slice(&struct_hash);
    let digest = crypto_tx::keccak256(&digest_input);

    let sig = match crypto_tx::sign_hash_deterministic(pk2, &digest) {
        Some(s) => s,
        None => return (false, "Falha ao assinar EIP-712".to_string()),
    };
    let mut sig_bytes = [0u8; 65];
    sig_bytes[0..32].copy_from_slice(&sig.r.to_be_bytes());
    sig_bytes[32..64].copy_from_slice(&sig.s.to_be_bytes());
    sig_bytes[64] = 27 + sig.y_parity;
    let sig_hex = crypto_tx::bytes_to_hex(&sig_bytes);

    let zero_b32 = "0x0000000000000000000000000000000000000000000000000000000000000000";
    let payload = format!(
        r#"{{"order":{{"salt":{},"maker":"{}","signer":"{}","tokenId":"{}","makerAmount":"{}","takerAmount":"{}","side":"{}","expiration":"0","signatureType":1,"timestamp":"{}","metadata":"{}","builder":"{}","signature":"{}"}},"owner":"{}","orderType":"{}","deferExec":false}}"#,
        salt, cfg.proxy_wallet, cfg.magic_signer, token_id, maker_amt, taker_amt, side_str, now_m, zero_b32, zero_b32, sig_hex, cfg.api_key, order_type
    );

    let ts_str = now_sec().to_string();
    let l2_sig = compute_l2_sig(&cfg.api_secret, &ts_str, "POST", "/order", &payload);

    let mut last_resp = String::new();
    for proxy_url in &cfg.proxies {
        if let Ok(out) = Command::new("curl")
            .args([
                "-s",
                "-m",
                "4",
                "--proxy",
                proxy_url,
                "-X",
                "POST",
                "https://clob.polymarket.com/order",
                "-H",
                "Content-Type: application/json",
                "-H",
                &format!("POLY_ADDRESS: {}", cfg.magic_signer),
                "-H",
                &format!("POLY_SIGNATURE: {}", l2_sig),
                "-H",
                &format!("POLY_TIMESTAMP: {}", ts_str),
                "-H",
                &format!("POLY_API_KEY: {}", cfg.api_key),
                "-H",
                &format!("POLY_PASSPHRASE: {}", cfg.api_passphrase),
                "--data",
                &payload,
            ])
            .output()
        {
            let body = String::from_utf8_lossy(&out.stdout).to_string();
            if (body.contains("\"success\":true") || body.contains("\"orderID\"")) && !body.contains("\"error\"") {
                return (true, body);
            } else if !body.is_empty() {
                last_resp = body;
            }
        }
    }
    (false, last_resp)
}

/// Executa compra estilo Front-End da Polymarket: aceita $1.00, $2.00, $3.00+ comprando cotas quebradinhas (ex: 2.33 cotas) via FAK/FOK/GTC!
fn execute_order_via_proxy(market_key: &str, side_label: &str, price: f64, usd_amount: f64, st_arc: &Arc<Mutex<CockpitState>>) -> String {
    let (slug, token_id, live_ask) = {
        let st = st_arc.lock().unwrap();
        let m = st.markets.iter().find(|x| x.key == market_key).or_else(|| st.markets.first());
        match m {
            Some(mkt) => {
                let (tok, ask) = if side_label == "UP" {
                    (mkt.up_token.clone(), mkt.up_ask)
                } else {
                    (mkt.down_token.clone(), mkt.down_ask)
                };
                (mkt.slug.clone(), tok, ask)
            }
            None => return "{\"ok\":false,\"msg\":\"Mercado não encontrado\"}".to_string(),
        }
    };

    if token_id.is_empty() {
        return "{\"ok\":false,\"msg\":\"Token ID indisponível\"}".to_string();
    }

    let vault = match crypto_tx::load_vault_signers() {
        Ok(v) => v,
        Err(e) => return format!("{{\"ok\":false,\"msg\":\"Erro cofre: {}\"}}", escape_json(&e)),
    };
    let (pk2, pk2_addr) = match vault.pk2 {
        Some(p) => p,
        None => return "{\"ok\":false,\"msg\":\"PK2 Magic Signer ausente\"}".to_string(),
    };
    let cfg = crypto_tx::load_polymarket_config();
    let maker_addr = match crypto_tx::parse_addr20(&cfg.proxy_wallet) {
        Some(a) => a,
        None => return "{\"ok\":false,\"msg\":\"PROXY_WALLET inválida\"}".to_string(),
    };

    let target_usd = usd_amount.max(1.00);
    let usd_cents = (target_usd * 100.0).round() as u64;
    let req_cents = (price * 100.0).round().clamp(1.0, 99.0) as u64;
    let ask_cents = (live_ask * 100.0).round().clamp(1.0, 99.0) as u64;

    // Calcula cotas quebradinhas (centi-shares) garantindo maker_amt >= $1.00 (1_000_000) igual ao Front-End da Polymarket
    let calc_buy_amounts = |px_c: u64| -> (u64, u64, f64, f64) {
        let centi_sh = ((usd_cents * 100 + px_c - 1) / px_c).max(100);
        let taker_a = centi_sh * 10_000;          // cotas quebradinhas (2 casas decimais)
        let maker_a = centi_sh * px_c * 100;       // valor em USDC (>= $1.00 garantido)
        let sh_f = (centi_sh as f64) / 100.0;
        let px_f = (px_c as f64) / 100.0;
        (maker_a, taker_a, sh_f, px_f)
    };

    // Se a compra de $1/$2 gera menos de 5 cotas, a Polymarket exige ordem Marketable (FAK/FOK) no Ask atual
    let prelim_centi_sh = (usd_cents * 100 + req_cents - 1) / req_cents;
    let eff_cents = if prelim_centi_sh < 500 && req_cents < ask_cents {
        ask_cents
    } else {
        req_cents
    };

    let (maker_amt, taker_amt, shares, clean_price) = calc_buy_amounts(eff_cents);
    let tifs: [&str; 3] = if prelim_centi_sh < 500 || eff_cents >= ask_cents {
        ["FAK", "FOK", "GTC"]
    } else {
        ["GTC", "FAK", "FOK"]
    };

    let mut success = false;
    let mut last_resp = String::new();
    let mut used_px = clean_price;
    let mut used_sh = shares;

    for &tif in &tifs {
        let (ok, resp) = post_signed_clob_order(&cfg, pk2, pk2_addr, maker_addr, &token_id, "BUY", 0, maker_amt, taker_amt, tif);
        if ok {
            success = true;
            last_resp = resp;
            break;
        }
        last_resp = resp;
    }

    // Fallback instantâneo: se tentou no Ask exato e o livro subiu 1¢ no milissegundo do clique, tenta 1¢ acima com FAK
    if !success && eff_cents < 98 {
        let bump_c = (ask_cents + 1).min(98);
        let (m2, t2, sh2, px2) = calc_buy_amounts(bump_c);
        let (ok2, r2) = post_signed_clob_order(&cfg, pk2, pk2_addr, maker_addr, &token_id, "BUY", 0, m2, t2, "FAK");
        if ok2 {
            success = true;
            last_resp = r2;
            used_px = px2;
            used_sh = sh2;
        }
    }

    let spent_usd = used_sh * used_px;
    let status_lbl = if success { "LIVE_POSTED_VIA_PROXY" } else { "REJECTED_OR_GEOBLOCKED" };
    append_operator_journal("HUMAN_ORCA_ENTER", &slug, side_label, used_px, used_sh, spent_usd, status_lbl);

    let msg = format!(
        "⚡ COMPRA {} ${:.2} -> {:.2} cotas quebradinhas @ ${:.2} (Ganho Expiração: ${:.2}) -> {}",
        side_label,
        spent_usd,
        used_sh,
        used_px,
        used_sh,
        if success { "EXECUTADA COM SUCESSO!" } else { &last_resp }
    );
    if let Ok(mut st) = st_arc.lock() {
        st.last_action_log = msg.clone();
        st.force_pos_refresh = true;
    }
    format!("{{\"ok\":{},\"msg\":\"{}\"}}", success, escape_json(&msg))
}

/// Executa CASH OUT (Ordem SELL) imediato aceitando cotas quebradinhas (ex: 2.33 cotas de $1 ou 32.50 cotas) via FAK/FOK/GTC!
fn execute_cashout_via_proxy(
    req_token: &str,
    side_label: &str,
    pct: u32,
    mode: &str,
    custom_price: f64,
    st_arc: &Arc<Mutex<CockpitState>>,
) -> String {
    let (slug, token_id, live_bid, tp_price, cached_raw) = {
        let st = st_arc.lock().unwrap();
        let m = st.markets.first();
        let pos = st
            .open_positions
            .iter()
            .find(|p| (!req_token.is_empty() && p.token_id == req_token) || p.side == side_label);
        match (m, pos) {
            (_, Some(p)) => {
                let tp = m.map(|mk| mk.scalp_tp).unwrap_or(0.80);
                (p.slug.clone(), p.token_id.clone(), p.live_bid, tp, p.raw_shares)
            }
            (Some(mk), None) => {
                let tok = if !req_token.is_empty() {
                    req_token.to_string()
                } else if side_label == "UP" {
                    mk.up_token.clone()
                } else {
                    mk.down_token.clone()
                };
                let bid = if side_label == "UP" { mk.up_bid } else { mk.down_bid };
                (mk.slug.clone(), tok, bid, mk.scalp_tp, 0u64)
            }
            (None, None) => return "{\"ok\":false,\"msg\":\"Nenhuma opção aberta encontrada\"}".to_string(),
        }
    };

    if token_id.is_empty() {
        return "{\"ok\":false,\"msg\":\"Token ID indisponível para Cash Out\"}".to_string();
    }

    let vault = match crypto_tx::load_vault_signers() {
        Ok(v) => v,
        Err(e) => return format!("{{\"ok\":false,\"msg\":\"Erro cofre: {}\"}}", escape_json(&e)),
    };
    let (pk2, pk2_addr) = match vault.pk2 {
        Some(p) => p,
        None => return "{\"ok\":false,\"msg\":\"PK2 Magic Signer ausente\"}".to_string(),
    };
    let cfg = crypto_tx::load_polymarket_config();
    let maker_addr = match crypto_tx::parse_addr20(&cfg.proxy_wallet) {
        Some(a) => a,
        None => return "{\"ok\":false,\"msg\":\"PROXY_WALLET inválida\"}".to_string(),
    };

    // Lê o saldo exato on-chain ERC-1155 na Polygon antes de assinar (suporta qualquer fração quebradinha >= 0.05 share)
    let onchain_raw = query_erc1155_balance_single(&cfg.proxy_wallet, &token_id).unwrap_or(cached_raw);
    let raw_balance = if onchain_raw >= 50_000 { onchain_raw } else { cached_raw };
    if raw_balance < 50_000 {
        let msg = format!("⚠️ Saldo de shares {} na carteira é 0 (já sacado ou liquidado).", side_label);
        if let Ok(mut st) = st_arc.lock() {
            st.last_action_log = msg.clone();
            st.force_pos_refresh = true;
        }
        return format!("{{\"ok\":false,\"msg\":\"{}\"}}", escape_json(&msg));
    }

    let sell_raw = if pct == 50 { (raw_balance / 2).max(50_000) } else { raw_balance };
    let target_price = if mode == "TP" {
        if custom_price > 0.02 { custom_price } else { tp_price }
    } else if custom_price > 0.01 {
        custom_price
    } else {
        // Cash Out Imediato: envia até 2 centavos abaixo do Best Bid com FAK para casar 100% na hora com melhoria de preço!
        (live_bid - 0.02).max(0.01)
    };
    let price_cents = (target_price * 100.0).round().clamp(1.0, 99.0) as u64;
    let clean_price = (price_cents as f64) / 100.0;

    let tifs: [&str; 3] = if mode == "TP" {
        ["GTC", "FAK", "FOK"]
    } else {
        ["FAK", "FOK", "GTC"]
    };

    let try_sell = |raw_amt: u64| -> (bool, String, f64) {
        let centi_sh = (raw_amt / 10_000).max(5);
        let maker_c = centi_sh * 10_000;
        let taker_c = centi_sh * price_cents * 100;
        let mut last_err = String::new();
        for &tif in &tifs {
            let (ok1, r1) = post_signed_clob_order(&cfg, pk2, pk2_addr, maker_addr, &token_id, "SELL", 1, maker_c, taker_c, tif);
            if ok1 {
                return (true, r1, (centi_sh as f64) / 100.0);
            }
            last_err = r1;
        }
        if raw_amt >= 1_000_000 {
            let int_sh = raw_amt / 1_000_000;
            let maker_i = int_sh * 1_000_000;
            let taker_i = int_sh * price_cents * 10_000;
            for &tif in &tifs {
                let (ok2, r2) = post_signed_clob_order(&cfg, pk2, pk2_addr, maker_addr, &token_id, "SELL", 1, maker_i, taker_i, tif);
                if ok2 {
                    return (true, r2, int_sh as f64);
                }
                last_err = r2;
            }
        }
        (false, last_err, (centi_sh as f64) / 100.0)
    };

    let (mut success, mut last_resp, mut sold_sh) = try_sell(sell_raw);

    // Se havia ordem Limit aberta travando as shares e o usuário clicou SACAR AGORA, cancela tudo e vende imediatamente!
    if !success && mode != "TP" && (last_resp.contains("balance") || last_resp.contains("allowance")) {
        let _ = cancel_all_via_proxy(st_arc);
        thread::sleep(Duration::from_millis(180));
        let res2 = try_sell(sell_raw);
        success = res2.0;
        last_resp = res2.1;
        sold_sh = res2.2;
    }

    let est_usd = sold_sh * (if mode == "TP" { clean_price } else { live_bid.max(clean_price) });
    let status_lbl = if success {
        if mode == "TP" { "LIMIT_TP_POSTED" } else { "CASHOUT_EXECUTED" }
    } else {
        "CASHOUT_REJECTED"
    };
    append_operator_journal("ORCA_CASHOUT", &slug, side_label, clean_price, sold_sh, est_usd, status_lbl);

    let msg = format!(
        "💰 SAQUE (CASH OUT {}%) {} — {:.2} cotas quebradinhas @ ${:.2} ➔ Recebido ~${:.2} -> {}",
        pct,
        side_label,
        sold_sh,
        if mode == "TP" { clean_price } else { live_bid.max(clean_price) },
        est_usd,
        if success {
            if mode == "TP" { "VENDA PENDURADA NO ALVO COM SUCESSO!" } else { "SAQUE EXECUTADO NA HORA COM SUCESSO!" }
        } else {
            &last_resp
        }
    );
    if let Ok(mut st) = st_arc.lock() {
        st.last_action_log = msg.clone();
        st.force_pos_refresh = true;
    }
    format!("{{\"ok\":{},\"msg\":\"{}\"}}", success, escape_json(&msg))
}

fn cancel_all_via_proxy(st_arc: &Arc<Mutex<CockpitState>>) -> String {
    let cfg = crypto_tx::load_polymarket_config();
    let ts_str = now_sec().to_string();
    let l2_sig = compute_l2_sig(&cfg.api_secret, &ts_str, "DELETE", "/cancel-all", "");
    let mut resp = String::new();
    for proxy_url in &cfg.proxies {
        if let Ok(out) = Command::new("curl")
            .args([
                "-s",
                "-m",
                "4",
                "--proxy",
                proxy_url,
                "-X",
                "DELETE",
                "https://clob.polymarket.com/cancel-all",
                "-H",
                &format!("POLY_ADDRESS: {}", cfg.magic_signer),
                "-H",
                &format!("POLY_SIGNATURE: {}", l2_sig),
                "-H",
                &format!("POLY_TIMESTAMP: {}", ts_str),
                "-H",
                &format!("POLY_API_KEY: {}", cfg.api_key),
                "-H",
                &format!("POLY_PASSPHRASE: {}", cfg.api_passphrase),
            ])
            .output()
        {
            let b = String::from_utf8_lossy(&out.stdout).to_string();
            if !b.is_empty() {
                resp = b;
                break;
            }
        }
    }
    let msg = format!("CANCEL-ALL EXECUTADO VIA PROXY: {}", resp);
    if let Ok(mut st) = st_arc.lock() {
        st.last_action_log = msg.clone();
    }
    format!("{{\"ok\":true,\"msg\":\"{}\"}}", escape_json(&msg))
}

const INDEX_HTML: &str = include_str!("cockpit_ui.html");

const PROXY_PAC_JS: &str = r#"function FindProxyForURL(url, host) {
  host = host.toLowerCase();
  if (
    host === "polymarket.com" ||
    host === "www.polymarket.com" ||
    host === "clob.polymarket.com"
  ) {
    return "PROXY 127.0.0.1:8899; DIRECT";
  }
  return "DIRECT";
}
"#;

fn base64_encode_std(data: &[u8]) -> String {
    const B64: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in data.chunks(3) {
        let b0 = chunk[0];
        let b1 = if chunk.len() > 1 { chunk[1] } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] } else { 0 };
        out.push(B64[(b0 >> 2) as usize] as char);
        out.push(B64[(((b0 & 3) << 4) | (b1 >> 4)) as usize] as char);
        if chunk.len() > 1 {
            out.push(B64[(((b1 & 0xf) << 2) | (b2 >> 6)) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(B64[(b2 & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

fn parse_proxy_endpoint(raw: &str) -> Option<(String, String)> {
    let clean = raw
        .trim()
        .trim_start_matches("http://")
        .trim_start_matches("https://");
    if clean.is_empty() {
        return None;
    }
    if let Some((user_pass, host_port)) = clean.rsplit_once('@') {
        let b64 = base64_encode_std(user_pass.as_bytes());
        Some((host_port.to_string(), b64))
    } else {
        Some((clean.to_string(), String::new()))
    }
}

fn connect_upstream_proxy_fresh(endpoints: &[(String, String)]) -> Option<(TcpStream, String)> {
    for (host_port, auth_b64) in endpoints {
        if let Ok(mut addrs) = host_port.to_socket_addrs() {
            if let Some(addr) = addrs.next() {
                if let Ok(s) = TcpStream::connect_timeout(&addr, Duration::from_millis(2400)) {
                    let _ = s.set_nodelay(true);
                    return Some((s, auth_b64.clone()));
                }
            }
        }
    }
    None
}

type WarmQueue = Mutex<VecDeque<(TcpStream, String, Instant)>>;

fn pop_or_connect_upstream(pool: &WarmQueue, endpoints: &[(String, String)]) -> Option<(TcpStream, String)> {
    if let Ok(mut q) = pool.lock() {
        while let Some((s, auth, created)) = q.pop_front() {
            if created.elapsed() < Duration::from_secs(10) {
                return Some((s, auth));
            }
        }
    }
    connect_upstream_proxy_fresh(endpoints)
}

fn extract_tls_sni(buf: &[u8]) -> Option<String> {
    if buf.len() < 43 || buf[0] != 0x16 || buf[5] != 0x01 {
        return None;
    }
    let mut pos = 43usize;
    if pos >= buf.len() {
        return None;
    }
    let sess_len = buf[pos] as usize;
    pos += 1 + sess_len;
    if pos + 2 > buf.len() {
        return None;
    }
    let cipher_len = u16::from_be_bytes([buf[pos], buf[pos + 1]]) as usize;
    pos += 2 + cipher_len;
    if pos + 1 > buf.len() {
        return None;
    }
    let comp_len = buf[pos] as usize;
    pos += 1 + comp_len;
    if pos + 2 > buf.len() {
        return None;
    }
    let ext_total_len = u16::from_be_bytes([buf[pos], buf[pos + 1]]) as usize;
    pos += 2;
    let end = (pos + ext_total_len).min(buf.len());
    while pos + 4 <= end {
        let ext_type = u16::from_be_bytes([buf[pos], buf[pos + 1]]);
        let ext_len = u16::from_be_bytes([buf[pos + 2], buf[pos + 3]]) as usize;
        pos += 4;
        if pos + ext_len > end {
            break;
        }
        if ext_type == 0x0000 && ext_len >= 5 {
            let name_type = buf[pos + 2];
            let name_len = u16::from_be_bytes([buf[pos + 3], buf[pos + 4]]) as usize;
            if name_type == 0 && 5 + name_len <= ext_len {
                if let Ok(s) = std::str::from_utf8(&buf[pos + 5..pos + 5 + name_len]) {
                    return Some(s.to_string());
                }
            }
        }
        pos += ext_len;
    }
    None
}

fn handle_proxy_tunnel_client(
    mut client: TcpStream,
    endpoints: Arc<Vec<(String, String)>>,
    pool: Arc<WarmQueue>,
) {
    let _ = client.set_nodelay(true);
    let _ = client.set_read_timeout(Some(Duration::from_secs(15)));
    let mut buf = [0u8; 8192];
    let mut total = match client.read(&mut buf) {
        Ok(n) if n > 0 => n,
        _ => return,
    };

    if buf[0] == 0x16 {
        // MODO 1: Túnel TLS Transparente via Kernel (iptables OUTPUT -> 127.0.0.1:8899)
        if total >= 5 {
            let rec_len = u16::from_be_bytes([buf[3], buf[4]]) as usize;
            let needed = (5 + rec_len).min(buf.len());
            while total < needed {
                match client.read(&mut buf[total..needed]) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => total += n,
                }
            }
        }
        let sni = extract_tls_sni(&buf[..total]).unwrap_or_else(|| "polymarket.com".to_string());
        let (mut upstream, auth_b64) = match pop_or_connect_upstream(&pool, &endpoints) {
            Some(p) => p,
            None => return,
        };
        let _ = upstream.set_read_timeout(Some(Duration::from_secs(8)));
        let connect_req = if auth_b64.is_empty() {
            format!(
                "CONNECT {}:443 HTTP/1.1\r\nHost: {}:443\r\nProxy-Connection: keep-alive\r\n\r\n",
                sni, sni
            )
        } else {
            format!(
                "CONNECT {}:443 HTTP/1.1\r\nHost: {}:443\r\nProxy-Authorization: Basic {}\r\nProxy-Connection: keep-alive\r\n\r\n",
                sni, sni, auth_b64
            )
        };
        if upstream.write_all(connect_req.as_bytes()).is_err() {
            return;
        }
        let mut resp_buf = [0u8; 2048];
        let mut r_tot = 0usize;
        let hdr_end = loop {
            match upstream.read(&mut resp_buf[r_tot..]) {
                Ok(0) | Err(_) => return,
                Ok(n) => {
                    r_tot += n;
                    if let Some(pos) = resp_buf[..r_tot].windows(4).position(|w| w == b"\r\n\r\n") {
                        break pos + 4;
                    }
                    if r_tot >= resp_buf.len() {
                        return;
                    }
                }
            }
        };
        let status_line = String::from_utf8_lossy(&resp_buf[..hdr_end]);
        if !status_line.contains(" 200 ") {
            return;
        }
        let _ = upstream.set_read_timeout(None);
        if upstream.write_all(&buf[..total]).is_err() {
            return;
        }
        if r_tot > hdr_end {
            let _ = client.write_all(&resp_buf[hdr_end..r_tot]);
        }

        let _ = client.set_read_timeout(None);
        let mut client_reader = match client.try_clone() {
            Ok(c) => c,
            Err(_) => return,
        };
        let mut upstream_writer = match upstream.try_clone() {
            Ok(u) => u,
            Err(_) => return,
        };
        let c2u = thread::spawn(move || {
            let _ = io::copy(&mut client_reader, &mut upstream_writer);
            let _ = upstream_writer.shutdown(std::net::Shutdown::Write);
        });
        let _ = io::copy(&mut upstream, &mut client);
        let _ = client.shutdown(std::net::Shutdown::Write);
        let _ = c2u.join();
        return;
    }

    // MODO 2: Proxy HTTP / CONNECT explícito
    let header_end = loop {
        if let Some(pos) = buf[..total].windows(4).position(|w| w == b"\r\n\r\n") {
            break pos + 4;
        }
        if total >= buf.len() {
            return;
        }
        match client.read(&mut buf[total..]) {
            Ok(0) | Err(_) => return,
            Ok(n) => total += n,
        }
    };

    let (mut upstream, auth_b64) = match pop_or_connect_upstream(&pool, &endpoints) {
        Some(p) => p,
        None => return,
    };

    let header_str = String::from_utf8_lossy(&buf[..header_end]);
    let mut out_req = Vec::with_capacity(total + 160);
    if let Some(first_crlf) = header_str.find("\r\n") {
        out_req.extend_from_slice(&buf[..first_crlf + 2]);
        if !auth_b64.is_empty() && !header_str.to_ascii_lowercase().contains("proxy-authorization:") {
            let auth_hdr = format!(
                "Proxy-Authorization: Basic {}\r\nProxy-Connection: keep-alive\r\n",
                auth_b64
            );
            out_req.extend_from_slice(auth_hdr.as_bytes());
        }
        out_req.extend_from_slice(&buf[first_crlf + 2..total]);
    } else {
        out_req.extend_from_slice(&buf[..total]);
    }

    if upstream.write_all(&out_req).is_err() {
        return;
    }

    let _ = client.set_read_timeout(None);
    let mut client_reader = match client.try_clone() {
        Ok(c) => c,
        Err(_) => return,
    };
    let mut upstream_writer = match upstream.try_clone() {
        Ok(u) => u,
        Err(_) => return,
    };

    let c2u = thread::spawn(move || {
        let _ = io::copy(&mut client_reader, &mut upstream_writer);
        let _ = upstream_writer.shutdown(std::net::Shutdown::Write);
    });

    let _ = io::copy(&mut upstream, &mut client);
    let _ = client.shutdown(std::net::Shutdown::Write);
    let _ = c2u.join();
}

fn spawn_local_polymarket_proxy_tunnel(state: Arc<Mutex<CockpitState>>) {
    thread::spawn(move || {
        let cfg = crypto_tx::load_polymarket_config();
        let mut parsed: Vec<(String, String)> = cfg
            .proxies
            .iter()
            .filter_map(|p| parse_proxy_endpoint(p))
            .collect();
        if parsed.is_empty() {
            parsed.push(("127.0.0.1:8080".to_string(), String::new()));
        }
        let primary_host = parsed[0].0.clone();
        if let Ok(mut st) = state.lock() {
            st.active_proxy_tag = format!("PT/IBÉRIA DIRETO ({})", primary_host);
        }
        let endpoints = Arc::new(parsed);
        let pool: Arc<WarmQueue> = Arc::new(Mutex::new(VecDeque::new()));

        // Thread que mantém pool de conexões TCP pré-aquecidas para a rota Ibérica (zero delay de handshake)
        {
            let ep_warm = Arc::clone(&endpoints);
            let pool_warm = Arc::clone(&pool);
            thread::spawn(move || loop {
                let cur_len = if let Ok(mut q) = pool_warm.lock() {
                    q.retain(|(_, _, t)| t.elapsed() < Duration::from_secs(10));
                    q.len()
                } else {
                    0
                };
                if cur_len < 6 {
                    if let Some((s, auth)) = connect_upstream_proxy_fresh(&ep_warm) {
                        if let Ok(mut q) = pool_warm.lock() {
                            q.push_back((s, auth, Instant::now()));
                        }
                    }
                    thread::sleep(Duration::from_millis(80));
                } else {
                    thread::sleep(Duration::from_millis(400));
                }
            });
        }

        if let Ok(listener) = TcpListener::bind("127.0.0.1:8899") {
            println!(" ⚡ Túnel Transparente + Proxy Polymarket ativo em 127.0.0.1:8899 (Rota: {})", primary_host);
            for stream in listener.incoming().flatten() {
                let ep = Arc::clone(&endpoints);
                let pl = Arc::clone(&pool);
                thread::spawn(move || handle_proxy_tunnel_client(stream, ep, pl));
            }
        }
    });
}

fn open_polymarket_browser(state: &Arc<Mutex<CockpitState>>) -> String {
    let slug = {
        let st = state.lock().unwrap();
        st.markets
            .first()
            .map(|m| m.slug.clone())
            .unwrap_or_else(|| "btc-updown-5m".to_string())
    };
    let target_url = format!("https://polymarket.com/event/{}", slug);
    let launched = Command::new("xdg-open")
        .arg(&target_url)
        .spawn()
        .is_ok();
    let msg = format!(
        "🌐 Abrindo {} no seu navegador (Rota Direta Transparente ativa)!",
        target_url
    );
    if let Ok(mut st) = state.lock() {
        st.last_action_log = msg.clone();
    }
    format!(
        "{{\"ok\":{},\"url\":\"{}\",\"msg\":\"{}\"}}",
        launched,
        escape_json(&target_url),
        escape_json(&msg)
    )
}

fn parse_query_param(query: &str, key: &str) -> Option<String> {
    for kv in query.split('&') {
        let mut parts = kv.splitn(2, '=');
        let k = parts.next()?;
        let v = parts.next().unwrap_or("");
        if k == key {
            return Some(v.to_string());
        }
    }
    None
}

fn handle_client(mut stream: TcpStream, state: Arc<Mutex<CockpitState>>) {
    let mut buf = [0u8; 4096];
    let n = match stream.read(&mut buf) {
        Ok(n) if n > 0 => n,
        _ => return,
    };
    let req = String::from_utf8_lossy(&buf[..n]);
    let first_line = req.lines().next().unwrap_or("");
    let parts: Vec<&str> = first_line.split_whitespace().collect();
    if parts.len() < 2 {
        return;
    }
    let method = parts[0];
    let full_path = parts[1];

    if method == "GET" && full_path.starts_with("/api/state") {
        let json = {
            let st = state.lock().unwrap();
            build_state_json(&st)
        };
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json; charset=utf-8\r\nAccess-Control-Allow-Origin: *\r\nCache-Control: no-cache\r\nContent-Length: {}\r\n\r\n{}",
            json.len(),
            json
        );
        let _ = stream.write_all(resp.as_bytes());
    } else if method == "GET" && full_path.starts_with("/proxy.pac") {
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/x-ns-proxy-autoconfig; charset=utf-8\r\nCache-Control: no-cache\r\nContent-Length: {}\r\n\r\n{}",
            PROXY_PAC_JS.len(),
            PROXY_PAC_JS
        );
        let _ = stream.write_all(resp.as_bytes());
    } else if method == "POST" && full_path.starts_with("/api/open_browser") {
        let res_json = open_polymarket_browser(&state);
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json; charset=utf-8\r\nContent-Length: {}\r\n\r\n{}",
            res_json.len(),
            res_json
        );
        let _ = stream.write_all(resp.as_bytes());
    } else if method == "POST" && full_path.starts_with("/api/order") {
        let q = full_path.split('?').nth(1).unwrap_or("");
        let mkt = parse_query_param(q, "market").unwrap_or_else(|| "BTC".to_string());
        let side = parse_query_param(q, "side").unwrap_or_else(|| "UP".to_string());
        let price = parse_query_param(q, "price").and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.45);
        let usd = parse_query_param(q, "usd").and_then(|s| s.parse::<f64>().ok()).unwrap_or(1.00);
        let res_json = execute_order_via_proxy(&mkt, &side, price, usd, &state);
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json; charset=utf-8\r\nContent-Length: {}\r\n\r\n{}",
            res_json.len(),
            res_json
        );
        let _ = stream.write_all(resp.as_bytes());
    } else if method == "POST" && full_path.starts_with("/api/cashout") {
        let q = full_path.split('?').nth(1).unwrap_or("");
        let tok = parse_query_param(q, "token_id").unwrap_or_default();
        let side = parse_query_param(q, "side").unwrap_or_else(|| "UP".to_string());
        let pct = parse_query_param(q, "pct").and_then(|s| s.parse::<u32>().ok()).unwrap_or(100);
        let mode = parse_query_param(q, "mode").unwrap_or_else(|| "NOW".to_string());
        let price = parse_query_param(q, "price").and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
        let res_json = execute_cashout_via_proxy(&tok, &side, pct, &mode, price, &state);
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json; charset=utf-8\r\nContent-Length: {}\r\n\r\n{}",
            res_json.len(),
            res_json
        );
        let _ = stream.write_all(resp.as_bytes());
    } else if method == "POST" && full_path.starts_with("/api/refresh_pos") {
        if let Ok(mut st) = state.lock() {
            st.force_pos_refresh = true;
            st.last_action_log = "🔄 Atualizando saldo de Shares (Polygon ERC-1155 + Data API v2) em tempo real...".to_string();
        }
        let res_json = "{\"ok\":true,\"msg\":\"Atualizando posições abertas...\"}";
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json; charset=utf-8\r\nContent-Length: {}\r\n\r\n{}",
            res_json.len(),
            res_json
        );
        let _ = stream.write_all(resp.as_bytes());
    } else if method == "POST" && full_path.starts_with("/api/recalc") {
        if let Ok(mut st) = state.lock() {
            st.force_recalc = true;
            st.force_pos_refresh = true;
            st.last_action_log = "🔄 Solicitação recebida: Destravando entrada e recalculando com os 25 Bots...".to_string();
        }
        let res_json = "{\"ok\":true,\"msg\":\"Recalculando nova Entrada Fixa com os 25 Bots...\"}";
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json; charset=utf-8\r\nContent-Length: {}\r\n\r\n{}",
            res_json.len(),
            res_json
        );
        let _ = stream.write_all(resp.as_bytes());
    } else if method == "POST" && full_path.starts_with("/api/cancel_all") {
        let res_json = cancel_all_via_proxy(&state);
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json; charset=utf-8\r\nContent-Length: {}\r\n\r\n{}",
            res_json.len(),
            res_json
        );
        let _ = stream.write_all(resp.as_bytes());
    } else {
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nCache-Control: no-cache\r\nContent-Length: {}\r\n\r\n{}",
            INDEX_HTML.len(),
            INDEX_HTML
        );
        let _ = stream.write_all(resp.as_bytes());
    }
}

fn main() {
    let port = std::env::args().nth(1).unwrap_or_else(|| "8787".to_string());
    let bind_addr = format!("127.0.0.1:{}", port);
    let state = Arc::new(Mutex::new(CockpitState::default()));
    spawn_local_polymarket_proxy_tunnel(Arc::clone(&state));
    spawn_telemetry_loop(Arc::clone(&state));

    let listener = TcpListener::bind(&bind_addr).expect("Falha ao abrir porta do Cockpit");
    println!("==========================================================================");
    println!(" 🦈 ORCA SHARK 20 — FOCO EXCLUSIVO BITCOIN 5M (ENTRADAS FIXAS & BOTS POR LUCRO)");
    println!(" 🌐 URL Local: http://{}", bind_addr);
    println!(" 🇵🇹 Proxy Split-Tunnel Polymarket: 127.0.0.1:8899 | PAC: http://{}/proxy.pac", bind_addr);
    println!("==========================================================================");

    for stream in listener.incoming().flatten() {
        let st = Arc::clone(&state);
        thread::spawn(move || handle_client(stream, st));
    }
}
