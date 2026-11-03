//! `queue host-runner`: runs the queue's host jobs on this machine, one at a
//! time. It claims the oldest pending host job, runs the template's command
//! in the template's working directory at background priority (`taskpolicy
//! -b` on macOS, `nice` elsewhere), sends its output back as it comes, stops
//! it when the queue asks (cancel or deadline), and reports its exit.

use std::process::Stdio;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::AsyncReadExt as _;
use tokio::process::Command;

/// How often to ask for work when there is none.
const IDLE: Duration = Duration::from_secs(15);

pub async fn run(base: &str, name: &str) -> ! {
    let base = base.trim_end_matches('/').to_owned();
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("an HTTP client");
    eprintln!("queue host-runner {name}: asking {base} for host jobs");
    loop {
        match claim(&http, &base, name).await {
            Ok(Some(work)) => run_one(&http, &base, &work).await,
            Ok(None) => tokio::time::sleep(IDLE).await,
            Err(e) => {
                eprintln!("queue host-runner: {e}");
                tokio::time::sleep(IDLE).await;
            }
        }
    }
}

async fn claim(http: &reqwest::Client, base: &str, name: &str) -> Result<Option<Value>, String> {
    let r = http
        .post(format!("{base}/claim"))
        .json(&json!({ "runner": name }))
        .send()
        .await
        .map_err(|e| e.without_url().to_string())?;
    if r.status().as_u16() == 204 {
        return Ok(None);
    }
    if !r.status().is_success() {
        return Err(format!("claim: {}", r.status()));
    }
    r.json().await.map(Some).map_err(|e| e.to_string())
}

/// `~` and `${VAR}` from this machine's environment.
fn expand(s: &str) -> String {
    let home = std::env::var("HOME").unwrap_or_default();
    let s = if s == "~" || s.starts_with("~/") {
        format!("{home}{}", &s[1..])
    } else {
        s.to_owned()
    };
    let re = regex::Regex::new(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}").expect("a valid pattern");
    re.replace_all(&s, |c: &regex::Captures| {
        std::env::var(&c[1]).unwrap_or_default()
    })
    .into_owned()
}

async fn run_one(http: &reqwest::Client, base: &str, work: &Value) {
    let job = &work["job"];
    let id = job["id"].as_str().unwrap_or_default().to_owned();
    let argv: Vec<String> = job["command"]
        .as_array()
        .into_iter()
        .flatten()
        .chain(job["args"].as_array().into_iter().flatten())
        // As given: parameter values are never expanded (only the template's
        // working directory and environment values are).
        .filter_map(|v| v.as_str().map(str::to_owned))
        .collect();
    eprintln!("queue host-runner: {id}: {}", argv.join(" "));
    let (prog, prefix): (&str, Vec<&str>) = if cfg!(target_os = "macos") {
        ("taskpolicy", vec!["-b"])
    } else {
        ("nice", vec!["-n", "19"])
    };
    let mut cmd = Command::new(prog);
    cmd.args(prefix)
        .args(&argv)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .stdin(Stdio::null());
    // Its own process group, so a stop reaches everything it started.
    cmd.process_group(0);
    if let Some(dir) = work["workingDir"].as_str() {
        cmd.current_dir(expand(dir));
    }
    for e in work["env"].as_array().into_iter().flatten() {
        if let (Some(k), Some(v)) = (e["name"].as_str(), e["value"].as_str()) {
            cmd.env(k, expand(v));
        }
    }
    let send_log = |text: Vec<u8>| {
        let url = format!("{base}/jobs/{id}/log");
        let http = http.clone();
        async move {
            let _ = http
                .post(url)
                .header("content-type", "application/octet-stream")
                .body(text)
                .send()
                .await;
        }
    };
    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            send_log(
                format!("queue host-runner: cannot start {}: {e}\n", argv.join(" ")).into_bytes(),
            )
            .await;
            finish(http, base, &id, 127).await;
            return;
        }
    };
    let pgid = child.id();
    let (mut out, mut err) = (
        child.stdout.take().expect("piped"),
        child.stderr.take().expect("piped"),
    );
    #[expect(
        clippy::disallowed_methods,
        reason = "known debt (CONTRIBUTING, Shared state): a job's output chunks; bound when touched"
    )]
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
    let tx2 = tx.clone();
    tokio::spawn(async move {
        let mut buf = vec![0; 8192];
        while let Ok(n) = out.read(&mut buf).await {
            if n == 0 || tx.send(buf[..n].to_vec()).is_err() {
                break;
            }
        }
    });
    tokio::spawn(async move {
        let mut buf = vec![0; 8192];
        while let Ok(n) = err.read(&mut buf).await {
            if n == 0 || tx2.send(buf[..n].to_vec()).is_err() {
                break;
            }
        }
    });
    let mut flush = tokio::time::interval(Duration::from_secs(2));
    let mut check = tokio::time::interval(Duration::from_secs(5));
    let mut pending = Vec::new();
    let mut stopping = false;
    let code = loop {
        tokio::select! {
            status = child.wait() => {
                while let Ok(chunk) = rx.try_recv() { pending.extend(chunk); }
                break status.ok().and_then(|s| s.code()).map_or(-1, i64::from);
            }
            Some(chunk) = rx.recv() => pending.extend(chunk),
            _ = flush.tick() => {
                if !pending.is_empty() { send_log(std::mem::take(&mut pending)).await; }
            }
            _ = check.tick() => {
                // Asking about the job also tells the queue this runner is alive.
                let asked = http.get(format!("{base}/jobs/{id}")).send().await;
                let cancel = match asked {
                    Ok(r) => r.json::<Value>().await.ok().is_some_and(|v| v["cancelRequested"].as_bool() == Some(true)),
                    Err(_) => false,
                };
                if cancel && !stopping {
                    stopping = true;
                    if let Some(pg) = pgid {
                        let _ = std::process::Command::new("kill").args(["-TERM", &format!("-{pg}")]).status();
                        let pg = pg.to_string();
                        tokio::spawn(async move {
                            tokio::time::sleep(Duration::from_secs(10)).await;
                            let _ = std::process::Command::new("kill").args(["-KILL", &format!("-{pg}")]).status();
                        });
                    }
                }
            }
        }
    };
    // Let the readers drain what the process left behind.
    tokio::time::sleep(Duration::from_millis(200)).await;
    while let Ok(chunk) = rx.try_recv() {
        pending.extend(chunk);
    }
    if !pending.is_empty() {
        send_log(pending).await;
    }
    finish(http, base, &id, code).await;
}

async fn finish(http: &reqwest::Client, base: &str, id: &str, code: i64) {
    for _ in 0..10 {
        let r = http
            .post(format!("{base}/jobs/{id}/finish"))
            .json(&json!({ "exitCode": code }))
            .send()
            .await;
        if r.is_ok_and(|r| r.status().is_success() || r.status().as_u16() == 409) {
            return;
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
    eprintln!("queue host-runner: {id}: couldn't report its exit ({code})");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn home_and_variables_expand() {
        let home = std::env::var("HOME").unwrap();
        assert_eq!(expand("~/repos/x"), format!("{home}/repos/x"));
        assert_eq!(expand("${HOME}/y"), format!("{home}/y"));
        assert_eq!(expand("plain"), "plain");
    }
}
