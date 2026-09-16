//! Notifications: PushPlus, SMTP, Telegram and Webhook channels, the login
//! audit notice, and the node up/down watcher.
//!
//! Every channel has its own switch under the master one, and each is invoked
//! only when its minimum configuration is present. Everything is opt-in: with no
//! channel configured, `send` is a no-op and the watcher records state
//! transitions without saying anything. A channel that fails logs the failure
//! and never takes the hub down -- and one failing channel never suppresses
//! another: a notification is worth a warn, not a crash.

use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Duration;

use chrono::Utc;
use tracing::warn;

use crate::App;
use crate::Shared;

/// The PushPlus send endpoint. Free tier, one POST per message.
const PUSHPLUS_URL: &str = "https://www.pushplus.plus/send";
/// The Bot API, with the token spliced into the path.
const TELEGRAM_API: &str = "https://api.telegram.org";
/// How much of a failing response a warning quotes: enough for the reason
/// (Telegram's `description`, a webhook's error text) without an error page.
const HEAD: usize = 300;
/// How long a node may go unreported before it counts as offline. One shared
/// heartbeat for every protocol: WebSocket sessions, whose teardown the watcher
/// would otherwise catch instantly, and HTTP-POST-only komari agents, which have
/// no session at all.
const ONLINE_WINDOW: i64 = 120;
/// How often the watcher re-evaluates every node's online state.
const WATCH_TICK: Duration = Duration::from_secs(30);

/// Sends `content` through every configured channel. No configuration means
/// no-op; a failure on one channel does not stop the others.
pub async fn send(app: &App, title: &str, content: &str) {
    if app.db.get("notify_enabled").as_deref() != Some("on") {
        return;
    }
    // The master switch is on: every channel whose own switch is on and whose
    // minimum config is present is invoked -- one failing channel never
    // suppresses another.
    let mut attempted = false;
    let mut delivered = false;
    if app.db.get("pushplus_enabled").as_deref() == Some("on")
        && app.db.get("pushplus_token").is_some_and(|t| !t.is_empty())
    {
        attempted = true;
        if pushplus(app, title, content).await {
            delivered = true;
        }
    }
    if app.db.get("smtp_enabled").as_deref() == Some("on")
        && app.db.get("smtp_host").is_some_and(|h| !h.is_empty())
        && app.db.get("smtp_from").is_some_and(|f| !f.is_empty())
        && app.db.get("smtp_to").is_some_and(|t| !t.is_empty())
    {
        attempted = true;
        match smtp(app, title, content).await {
            Ok(()) => delivered = true,
            Err(e) => warn!("smtp notification failed: {e:#}"),
        }
    }
    if app.db.get("telegram_enabled").as_deref() == Some("on")
        && app.db.get("telegram_token").is_some_and(|t| !t.is_empty())
        && app.db.get("telegram_chat").is_some_and(|c| !c.is_empty())
    {
        attempted = true;
        match telegram(app, title, content).await {
            Ok(()) => delivered = true,
            Err(e) => warn!("telegram notification failed: {e:#}"),
        }
    }
    if app.db.get("webhook_enabled").as_deref() == Some("on")
        && app.db.get("webhook_url").is_some_and(|u| !u.is_empty())
    {
        attempted = true;
        match webhook(app, title, content).await {
            Ok(()) => delivered = true,
            Err(e) => warn!("webhook notification failed: {e:#}"),
        }
    }
    if !attempted {
        warn!("notification '{title}' skipped: no enabled channel is configured");
    } else if !delivered {
        warn!("notification '{title}' had no working channel");
    }
}

/// One PushPlus POST. Returns whether the service accepted the request.
async fn pushplus(app: &App, title: &str, content: &str) -> bool {
    let token = app.db.get("pushplus_token").unwrap_or_default();
    match app
        .http
        .post(PUSHPLUS_URL)
        .json(&serde_json::json!({
            "token": token,
            "title": title,
            "content": content,
            "template": "html",
        }))
        .send()
        .await
    {
        Ok(resp) => {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            if status.is_success() {
                let code = serde_json::from_str::<serde_json::Value>(&body)
                    .ok()
                    .and_then(|v| v.get("code").and_then(|c| c.as_i64()).map(|c| c.to_string()))
                    .unwrap_or_else(|| "?".into());
                // code 200 only means "received"; anything else is a config or
                // quota error worth surfacing.
                if code == "200" {
                    true
                } else {
                    warn!("pushplus refused the notification: code {code} {body}");
                    false
                }
            } else {
                warn!("pushplus returned HTTP {status}: {}", body.chars().take(300).collect::<String>());
                false
            }
        }
        Err(e) => {
            warn!("pushplus request failed: {e:#}");
            false
        }
    }
}

/// One SMTP send through lettre. The transport is rebuilt per message: a
/// notification is rare, and a pooled connection would hold the socket open for
/// the life of the process for a message every few days.
async fn smtp(app: &App, title: &str, content: &str) -> anyhow::Result<()> {
    use lettre::message::Mailbox;
    use lettre::transport::smtp::authentication::Credentials;
    use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};

    let host = app.db.get("smtp_host").unwrap_or_default();
    let port: u16 = app
        .db
        .get("smtp_port")
        .and_then(|p| p.parse().ok())
        .unwrap_or(if app.db.get("smtp_security").as_deref() == Some("tls") { 465 } else { 587 });
    let from: Mailbox = app.db.get("smtp_from").unwrap_or_default().parse()?;
    let to: Mailbox = app.db.get("smtp_to").unwrap_or_default().parse()?;
    let security = app.db.get("smtp_security").unwrap_or_default();

    let mut transport = match security.as_str() {
        "tls" => AsyncSmtpTransport::<Tokio1Executor>::relay(&host)?.port(port),
        "none" => AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(&host).port(port),
        _ => AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&host)?.port(port),
    };
    if let (Some(user), Some(pass)) = (app.db.get("smtp_username"), app.db.get("smtp_password")) {
        if !user.is_empty() {
            transport = transport.credentials(Credentials::new(user, pass));
        }
    }
    let transport = transport.build();

    let email = Message::builder()
        .from(from)
        .to(to)
        .subject(title)
        .header(lettre::message::header::ContentType::TEXT_HTML)
        .body(content.to_owned())?;
    transport.send(email).await?;
    Ok(())
}

/// The client Telegram and webhook alerts are sent with, kept apart from
/// `App::http`: that one follows redirects, and reqwest resends a POST answered
/// with 301 or 302 as a GET without its body, which the far end typically
/// accepts -- the panel would report success while nothing arrives. A redirect
/// to another host also withholds only `Authorization` and cookies, so a
/// credential header such as `X-Gotify-Key` would follow it. Built on first use,
/// so a hub with neither channel never allocates it.
fn alert_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("http client")
    })
}

/// One Telegram send through the Bot API. The message is plain text and no parse
/// mode is set: under one, a node name that happens to contain markup makes
/// Telegram reject the message outright.
async fn telegram(app: &App, title: &str, content: &str) -> anyhow::Result<()> {
    let token = app.db.get("telegram_token").unwrap_or_default();
    let chat = app.db.get("telegram_chat").unwrap_or_default();
    let text = plain(&format!("{title}\n{content}"));
    // The URL carries the bot token, and this error reaches the journal.
    let response = alert_client()
        .post(format!("{TELEGRAM_API}/bot{token}/sendMessage"))
        .json(&serde_json::json!({ "chat_id": chat, "text": text }))
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("{:#}", anyhow::Error::from(e.without_url())))?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    // The Bot API answers 200 with `ok: false` for a request it declined, so the
    // status alone would report a delivery that never happened.
    let accepted = status.is_success()
        && serde_json::from_str::<serde_json::Value>(&body)
            .ok()
            .and_then(|v| v.get("ok").and_then(|ok| ok.as_bool()))
            .unwrap_or(true);
    if !accepted {
        warn!("telegram returned HTTP {status}: {}", head(&body));
        anyhow::bail!("telegram refused the notification");
    }
    Ok(())
}

/// One webhook POST: a JSON object carrying the message and the time, plus
/// whatever headers the operator configured. Neither the URL nor a header value
/// reaches an error message -- both are commonly the credential itself.
async fn webhook(app: &App, title: &str, content: &str) -> anyhow::Result<()> {
    let url = app.db.get("webhook_url").unwrap_or_default();
    let configured = app.db.get("webhook_headers").unwrap_or_default();
    // `insert` into one map rather than `.header()` and `extend`: both append, so
    // a configured Content-Type would be sent alongside the default instead of in
    // its place.
    let mut headers = reqwest::header::HeaderMap::new();
    headers
        .insert(reqwest::header::CONTENT_TYPE, reqwest::header::HeaderValue::from_static("application/json"));
    for (name, value) in parse_headers(&configured)? {
        headers.insert(name, value);
    }
    let body = serde_json::json!({
        "title": title,
        "content": content,
        "time": Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
    });
    let response = alert_client()
        .post(&url)
        .headers(headers)
        .json(&body)
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("{:#}", anyhow::Error::from(e.without_url())))?;
    let status = response.status();
    if status.is_redirection() {
        anyhow::bail!("HTTP {status}: the URL redirects; enter the address it redirects to");
    }
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("HTTP {status}: {}", head(&body));
    }
    Ok(())
}

/// `Name: value`, one per line. A configured value is never echoed back: it is
/// typically the credential.
fn parse_headers(
    text: &str,
) -> anyhow::Result<Vec<(reqwest::header::HeaderName, reqwest::header::HeaderValue)>> {
    use reqwest::header::{HeaderName, HeaderValue};
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| {
            let (name, value) = line
                .split_once(':')
                .ok_or_else(|| anyhow::anyhow!("every webhook header line must read Name: value"))?;
            let name = HeaderName::try_from(name.trim())
                .map_err(|_| anyhow::anyhow!("invalid header name {:?}", name.trim()))?;
            let value = HeaderValue::try_from(value.trim())
                .map_err(|_| anyhow::anyhow!("invalid value for header {name}"))?;
            Ok((name, value))
        })
        .collect()
}

/// Why a channel-specific setting cannot be stored, or `None` when it can. The
/// switches and the shared keys are validated in `api::setting_error`.
pub fn setting_error(key: &str, value: &str) -> Option<String> {
    let only = |s: &str, extra: &[u8]| {
        !s.is_empty() && s.bytes().all(|b| b.is_ascii_alphanumeric() || extra.contains(&b))
    };
    let digits = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    match key {
        // Empty clears a channel's field. A secret is only ever cleared this way
        // deliberately: the save path skips an empty one before it is stored.
        "telegram_token" | "telegram_chat" | "webhook_url" | "webhook_headers" if value.is_empty() => None,
        // Interpolated into the request path, so only the shape BotFather issues
        // is accepted.
        "telegram_token" => {
            if value.split_once(':').is_some_and(|(id, secret)| digits(id) && only(secret, b"_-")) {
                None
            } else {
                Some("Telegram Bot Token 形如 123456:ABC-DEF".into())
            }
        }
        "telegram_chat" => {
            let valid = match value.strip_prefix('@') {
                Some(name) => only(name, b"_"),
                None => digits(value.strip_prefix('-').unwrap_or(value)),
            };
            if valid {
                None
            } else {
                Some("Telegram 会话需为数字 id 或 @用户名".into())
            }
        }
        // Plain http is accepted: a relay on the hub's own host or network is a
        // common target, and only an admin can set this.
        "webhook_url" => {
            if reqwest::Url::parse(value).is_ok_and(|u| matches!(u.scheme(), "http" | "https")) {
                None
            } else {
                Some("Webhook 地址需以 http:// 或 https:// 开头".into())
            }
        }
        "webhook_headers" => parse_headers(value).err().map(|e| e.to_string()),
        _ => None,
    }
}

/// The first `HEAD` characters of a response body.
fn head(body: &str) -> String {
    body.chars().take(HEAD).collect()
}

/// The message as plain text: the panel builds its notifications with `<b>` and
/// `<br/>`, which Telegram would show verbatim without a parse mode.
fn plain(content: &str) -> String {
    content.replace("<br/>", "\n").replace("<br>", "\n").replace("<b>", "").replace("</b>", "")
}

/// Human-readable device from a User-Agent: OS and browser, nothing more. A
/// missing or unparsable header becomes "未知设备" rather than the raw string,
/// which could carry junk.
pub fn device_from_user_agent(ua: Option<&str>) -> String {
    let ua = ua.unwrap_or_default();
    let os = if ua.contains("iPhone") || ua.contains("iPad") || ua.contains("iPod") {
        "iOS"
    } else if ua.contains("Android") {
        "Android"
    } else if ua.contains("Windows") {
        "Windows"
    } else if ua.contains("Mac OS X") || ua.contains("Macintosh") {
        "macOS"
    } else if ua.contains("Linux") {
        "Linux"
    } else {
        "未知系统"
    };
    let browser = if ua.contains("Edg/") {
        "Edge"
    } else if ua.contains("Chrome/") {
        "Chrome"
    } else if ua.contains("Firefox/") {
        "Firefox"
    } else if ua.contains("Safari/") {
        "Safari"
    } else {
        "未知浏览器"
    };
    format!("{os} · {browser}")
}

/// Sends the login audit notice: time, address, device and method. Fired after
/// a successful sign-in; the record itself is written unconditionally to the
/// login_log table by the caller.
pub async fn login_notice(app: &App, method: &str, username: &str, ip: &str, device: &str) {
    if app.db.get("notify_login").as_deref() != Some("on") {
        return;
    }
    let when = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let method = match method {
        "password" => "应急密码",
        "account" => "账号密码",
        "github" => "GitHub 单点登录",
        other => other,
    };
    let title = "Monitor 后台登录提醒";
    let content =
        format!("<b>{when}</b><br/>登录方式：{method}<br/>账号：{username}<br/>IP：{ip}<br/>设备：{device}");
    send(app, title, &content).await;
}

/// Watches every node for online/offline transitions and notifies on each
/// change. The first sight of a node only records its state: a hub restart
/// would otherwise report every connected node as freshly "up".
pub fn spawn_node_watcher(app: Shared) {
    tokio::spawn(async move {
        let mut last: HashMap<i64, bool> = HashMap::new();
        let mut ticker = tokio::time::interval(WATCH_TICK);
        ticker.tick().await; // First tick completes immediately; skip it.
        loop {
            ticker.tick().await;
            let Ok(nodes) = app.db.nodes() else { continue };
            for node in nodes {
                let online = is_online(&app, &node);
                match last.get(&node.id) {
                    Some(previous) if *previous != online => {
                        let when = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();
                        if online {
                            let title = "节点上线";
                            let content = format!("<b>{when}</b><br/>节点「{}」已上线", node.name);
                            send(&app, title, &content).await;
                        } else {
                            let title = "节点掉线";
                            let content = format!("<b>{when}</b><br/>节点「{}」已掉线", node.name);
                            send(&app, title, &content).await;
                        }
                    }
                    // First observation: remember it, say nothing.
                    _ => {}
                }
                last.insert(node.id, online);
            }
        }
    });
}

/// A node counts as online until it has gone unreported for `ONLINE_WINDOW`.
/// Every report path — native WebSocket, komari WebSocket, komari HTTP POST —
/// stamps `node.last_seen`, so the window is the one heartbeat every protocol
/// shares: a blip that reconnects within the window never counts as a
/// disconnect, and a node that simply stops reporting is declared down once the
/// window lapses.
fn is_online(_app: &App, node: &crate::db::Node) -> bool {
    node.last_seen > 0 && Utc::now().timestamp() - node.last_seen <= ONLINE_WINDOW
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_user_agent_becomes_an_os_and_browser() {
        assert_eq!(device_from_user_agent(Some("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36 Edg/120.0")), "Windows · Edge");
        assert_eq!(device_from_user_agent(Some("Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 Version/17.0 Mobile/15E148 Safari/604.1")), "iOS · Safari");
        assert_eq!(
            device_from_user_agent(Some(
                "Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36 Chrome/125.0 Mobile Safari/537.36"
            )),
            "Android · Chrome"
        );
        assert_eq!(
            device_from_user_agent(Some(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Gecko/20100101 Firefox/128.0"
            )),
            "macOS · Firefox"
        );
        assert_eq!(device_from_user_agent(Some("curl/8.0")), "未知系统 · 未知浏览器");
        assert_eq!(device_from_user_agent(None), "未知系统 · 未知浏览器");
    }
}
