//! Komari-agent compatibility: the wire format of a komari-agent, converted
//! into this hub's own model. Everything komari-shaped lives here -- the
//! methods it speaks, the token it presents, and the two converters that turn
//! its reports into the fields this hub stores. The native handler never reads
//! this module and the komari handler never reads the native structures, so the
//! two agents stay fully isolated and this module can be removed wholesale if
//! komari support is ever retired.

use serde_json::{json, Value};

/// Methods a komari-agent sends. Only the first two are acted on; the rest are
/// logged and ignored, exactly as the plan intends (ping / exec results are a
/// later iteration).
pub const METHOD_REPORT: &str = "agent.report";
pub const METHOD_BASIC_INFO: &str = "agent.basicInfo";
pub const METHOD_PING_RESULT: &str = "agent.pingResult";
pub const METHOD_TASK_RESULT: &str = "agent.taskResult";
pub const METHOD_EVENT: &str = "agent.event";

/// The boot identity every komari report carries. A komari report has no boot
/// id of its own (the server assigns a UUID on its side), and this hub's
/// traffic accumulator needs one stable baseline per node; a fixed value means
/// "komari agent" -- a node switching between the native agent and a komari
/// agent re-baselines its traffic once at the switch, then accumulates exactly
/// like a native agent.
pub const KOMARI_BOOT_ID: &str = "komari";

/// A komari credential. Komari itself generates 22 characters from
/// `[0-9A-Za-z]`; the panel accepts a slightly wider range so an operator can
/// carry over an older or hand-made token, but still refuses anything that
/// could not plausibly come from a komari hub.
pub fn valid_komari_token(token: &str) -> bool {
    (8..=128).contains(&token.len()) && token.bytes().all(|b| b.is_ascii_alphanumeric())
}

/// A fresh komari token in komari's own charset and length, so an operator can
/// move a token between hubs without tripping a format check.
pub fn generate_komari_token() -> String {
    use rand::Rng;
    const CHARSET: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    let mut rng = rand::rng();
    (0..22).map(|_| CHARSET[rng.random_range(0..CHARSET.len())] as char).collect()
}

/// Reads a value at a dotted path, so the converters below stay free of nested
/// `get().get()` chains.
fn pick<'a>(root: &'a Value, path: &[&str]) -> Option<&'a Value> {
    let mut cur = root;
    for key in path {
        cur = cur.get(*key)?;
    }
    Some(cur)
}

/// A non-negative finite number. Anything else -- a string, a negative value,
/// NaN -- is "no reading", which the native pipeline already treats as absent
/// rather than as zero, so a malformed komari field cannot become a live frame
/// showing a negative load.
fn num(v: &Value) -> Option<f64> {
    v.as_f64().filter(|n| n.is_finite() && *n >= 0.0)
}

fn int(v: &Value) -> Option<i64> {
    v.as_i64().filter(|n| *n >= 0)
}

fn insert_num(m: &mut serde_json::Map<String, Value>, key: &str, v: Option<f64>) {
    if let Some(v) = v {
        m.insert(key.into(), json!(v));
    }
}

fn insert_int(m: &mut serde_json::Map<String, Value>, key: &str, v: Option<i64>) {
    if let Some(v) = v {
        m.insert(key.into(), json!(v));
    }
}

/// Converts a komari `agent.report` payload (the object under `params.report`)
/// into this hub's internal metrics object. Field names and units follow the
/// komari protocol: capacities in bytes, rates in bytes per second, uptime in
/// seconds, load as three floats. `totalUp` / `totalDown` become the cumulative
/// counters this hub's traffic accumulator feeds on.
pub fn convert_komari_report(report: &Value) -> Value {
    let f = |path: &[&str]| pick(report, path).and_then(num);
    let n = |path: &[&str]| pick(report, path).and_then(int);

    let mut m = serde_json::Map::new();
    insert_num(&mut m, "cpu", f(&["cpu", "usage"]));
    if let (Some(l1), Some(l5), Some(l15)) =
        (f(&["load", "load1"]), f(&["load", "load5"]), f(&["load", "load15"]))
    {
        m.insert("load".into(), json!([l1, l5, l15]));
    }
    // Capacities. The UI reads these from the live report while connected, so a
    // machine that grows a disk mid-run shows it without waiting for a
    // reconnect.
    insert_int(&mut m, "mem_total", n(&["ram", "total"]));
    insert_int(&mut m, "mem_used", n(&["ram", "used"]));
    insert_int(&mut m, "swap_total", n(&["swap", "total"]));
    insert_int(&mut m, "swap_used", n(&["swap", "used"]));
    insert_int(&mut m, "disk_total", n(&["disk", "total"]));
    insert_int(&mut m, "disk_used", n(&["disk", "used"]));
    // Instant rates, in bytes per second.
    insert_int(&mut m, "net_rx", n(&["network", "up"]));
    insert_int(&mut m, "net_tx", n(&["network", "down"]));
    // Cumulative counters, in bytes. The accumulator derives rates and totals
    // from these.
    insert_int(&mut m, "net_rx_total", n(&["network", "totalUp"]));
    insert_int(&mut m, "net_tx_total", n(&["network", "totalDown"]));
    insert_int(&mut m, "tcp", n(&["connections", "tcp"]));
    insert_int(&mut m, "udp", n(&["connections", "udp"]));
    insert_int(&mut m, "procs", n(&["process"]));
    insert_int(&mut m, "uptime", n(&["uptime"]));
    m.insert("boot_id".into(), json!(KOMARI_BOOT_ID));
    Value::Object(m)
}

/// Converts the object under `params.info` of a komari `agent.basicInfo` into
/// the fields this hub stores on the `node` row. Extra komari fields (gpu_name,
/// cpu_physical_cores, ...) are simply not picked up; missing fields become
/// empty strings / zeros, which the native `save_facts` already tolerates.
pub fn convert_komari_basic_info(info: &Value) -> Value {
    // The same sanitisation `save_facts` applies to a native hello: values come
    // from an unvouched machine, control characters break the panel's rows, and
    // the length must be bounded.
    let s = |k: &str| {
        info.get(k)
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .chars()
            .filter(|c| !c.is_control())
            .take(128)
            .collect::<String>()
    };
    let n = |k: &str| info.get(k).and_then(|v| v.as_i64()).unwrap_or(0);
    json!({
        "os": s("os"),
        "kernel": s("kernel_version"),
        "arch": s("arch"),
        "virt": s("virtualization"),
        "cpu_name": s("cpu_name"),
        "cpu_cores": n("cpu_cores"),
        "mem_total": n("mem_total"),
        "swap_total": n("swap_total"),
        "disk_total": n("disk_total"),
        "agent_version": s("version"),
        "ipv4": s("ipv4"),
        "ipv6": s("ipv6"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report() -> Value {
        serde_json::from_str(
            r#"{
              "cpu": {"usage": 12.5},
              "ram": {"total": 1024, "used": 512},
              "swap": {"total": 2048, "used": 128},
              "load": {"load1": 0.1, "load5": 0.2, "load15": 0.3},
              "disk": {"total": 10000, "used": 2000},
              "network": {"up": 100, "down": 200, "totalUp": 1000000, "totalDown": 2000000},
              "connections": {"tcp": 12, "udp": 1},
              "uptime": 10000,
              "process": 10,
              "message": "ok"
            }"#,
        )
        .unwrap()
    }

    #[test]
    fn a_komari_report_becomes_the_native_metrics_shape() {
        let m = convert_komari_report(&report());
        assert_eq!(m["cpu"], 12.5);
        assert_eq!(m["load"], json!([0.1, 0.2, 0.3]));
        assert_eq!(m["mem_total"], 1024);
        assert_eq!(m["mem_used"], 512);
        assert_eq!(m["swap_total"], 2048);
        assert_eq!(m["swap_used"], 128);
        assert_eq!(m["disk_total"], 10000);
        assert_eq!(m["disk_used"], 2000);
        // Rates and cumulative counters map onto the accumulator's inputs.
        assert_eq!(m["net_rx"], 100);
        assert_eq!(m["net_tx"], 200);
        assert_eq!(m["net_rx_total"], 1_000_000);
        assert_eq!(m["net_tx_total"], 2_000_000);
        assert_eq!(m["tcp"], 12);
        assert_eq!(m["udp"], 1);
        assert_eq!(m["procs"], 10);
        assert_eq!(m["uptime"], 10000);
        assert_eq!(m["boot_id"], KOMARI_BOOT_ID);
    }

    #[test]
    fn malformed_komari_fields_vanish_instead_of_becoming_readings() {
        let mut bad = report();
        bad["cpu"] = json!({"usage": -1.0});
        bad["ram"] = json!({"total": "big", "used": 512});
        bad["network"] = json!({"up": 1, "down": 1, "totalUp": -5, "totalDown": "nope"});
        let m = convert_komari_report(&bad);
        assert!(m.get("cpu").is_none(), "a negative reading is not a reading");
        assert!(m.get("mem_total").is_none(), "a string capacity is not a reading");
        assert!(m.get("net_rx_total").is_none(), "a negative counter must not become a baseline");
        assert!(m.get("net_tx_total").is_none());
        // Fields that survived stay.
        assert_eq!(m["mem_used"], 512);
        assert_eq!(m["net_rx"], 1);
    }

    #[test]
    fn a_partial_report_keeps_what_it_has() {
        let mut partial = report();
        partial["connections"] = json!({"tcp": 5});
        let m = convert_komari_report(&partial);
        assert_eq!(m["tcp"], 5);
        assert!(m.get("udp").is_none());
    }

    #[test]
    fn basic_info_maps_komari_names_onto_native_columns() {
        let info: Value = serde_json::from_str(
            r#"{
              "arch": "amd64", "cpu_cores": 12, "cpu_physical_cores": 6,
              "cpu_name": "AMD Ryzen 9", "os": "Debian 12", "kernel_version": "6.1.0",
              "mem_total": 137438953472, "swap_total": 51539607552, "disk_total": 1099511627776,
              "ipv4": "1.2.3.4", "ipv6": "::1", "gpu_name": "", "virtualization": "kvm",
              "version": "0.0.1-rust"
            }"#,
        )
        .unwrap();
        let f = convert_komari_basic_info(&info);
        assert_eq!(f["arch"], "amd64");
        assert_eq!(f["os"], "Debian 12");
        assert_eq!(f["kernel"], "6.1.0");
        assert_eq!(f["virt"], "kvm");
        assert_eq!(f["cpu_name"], "AMD Ryzen 9");
        assert_eq!(f["cpu_cores"], 12);
        assert_eq!(f["mem_total"], 137438953472i64);
        assert_eq!(f["disk_total"], 1099511627776i64);
        assert_eq!(f["agent_version"], "0.0.1-rust");
        assert_eq!(f["ipv4"], "1.2.3.4");
        assert_eq!(f["ipv6"], "::1");
    }

    #[test]
    fn a_missing_basic_info_field_reads_as_empty() {
        let f = convert_komari_basic_info(&json!({"arch": "x86_64"}));
        assert_eq!(f["arch"], "x86_64");
        assert_eq!(f["os"], "");
        assert_eq!(f["cpu_cores"], 0);
    }

    #[test]
    fn generated_tokens_match_komari_style_and_validation() {
        for _ in 0..50 {
            let t = generate_komari_token();
            assert_eq!(t.len(), 22);
            assert!(valid_komari_token(&t), "{t}");
            assert!(t.bytes().all(|b| b.is_ascii_alphanumeric()), "{t}");
        }
        assert!(valid_komari_token("vomtLDXyggveYfjFxdoo7Z"));
        assert!(!valid_komari_token("short"));
        assert!(!valid_komari_token("has spaces"));
        assert!(!valid_komari_token("has_underscore"));
        assert!(!valid_komari_token(&"x".repeat(200)));
    }
}
